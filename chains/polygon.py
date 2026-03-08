"""
Polygon PoS whale transaction monitor using Alchemy alchemy_getAssetTransfers.

Replaces the deprecated Polygonscan V1 API with Alchemy's Transfers API
(120 CU per call, polled every 60 seconds).  Scans for large ERC-20 token
transfers across all monitored Polygon tokens.

CU budget: ~7,200 CU/hour.
"""

import time
import logging

from config.settings import (
    polygon_last_processed_block,
    GLOBAL_USD_THRESHOLD,
    shutdown_flag,
)
from data.tokens import POLYGON_TOKENS_TO_MONITOR, TOKEN_PRICES
from utils.base_helpers import safe_print, log_error
from utils.alchemy_rpc import fetch_asset_transfers, get_alchemy_rpc, _rpc_call

logger = logging.getLogger(__name__)

POLL_INTERVAL = 60  # Restored — concurrency semaphore in alchemy_rpc.py handles CU budget

_last_block_hex: str = "latest"


def _get_polygon_block_number() -> int | None:
    """Get the latest Polygon block number via Alchemy (10 CU)."""
    rpc_url = get_alchemy_rpc('polygon')
    if not rpc_url:
        return None
    result = _rpc_call(rpc_url, 'eth_blockNumber', [], cu_cost=10)
    if result:
        try:
            return int(result, 16)
        except (ValueError, TypeError):
            pass
    return None


def print_new_polygon_transfers():
    """
    Continuously polls Alchemy alchemy_getAssetTransfers for large Polygon
    ERC-20 transfers.  Runs in its own thread.
    """
    safe_print("✅ Polygon Alchemy monitor started (60s interval)")

    contract_addresses = [
        info["contract"] for info in POLYGON_TOKENS_TO_MONITOR.values()
    ]
    contract_map = {
        info["contract"].lower(): (symbol, info["decimals"])
        for symbol, info in POLYGON_TOKENS_TO_MONITOR.items()
    }

    current_block = _get_polygon_block_number()
    if current_block:
        safe_print(f"   Polygon tip: block {current_block}")
        last_block = current_block
    else:
        safe_print("⚠️  Polygon: could not fetch initial block, will retry")
        last_block = None

    while not shutdown_flag.is_set():
        try:
            tip = _get_polygon_block_number()
            if tip is None:
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            if last_block is None:
                last_block = tip
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            if tip <= last_block:
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            from_hex = hex(last_block + 1)
            to_hex = hex(tip)

            transfers = fetch_asset_transfers(
                blockchain='polygon',
                from_block=from_hex,
                to_block=to_hex,
                contract_addresses=contract_addresses,
                category=["erc20"],
            )

            last_block = tip

            if not transfers:
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            for tx in transfers:
                try:
                    raw_contract = (tx.get("rawContract") or {})
                    contract_addr = raw_contract.get("address", "").lower()
                    symbol_info = contract_map.get(contract_addr)
                    if not symbol_info:
                        continue

                    symbol, decimals = symbol_info
                    price = TOKEN_PRICES.get(symbol, 0)
                    if price == 0:
                        continue

                    value = tx.get("value")
                    if value is None:
                        raw_hex = raw_contract.get("value", "0x0")
                        try:
                            raw_int = int(raw_hex, 16)
                            value = raw_int / (10 ** decimals)
                        except (ValueError, TypeError):
                            continue

                    usd_value = float(value) * price
                    if usd_value < GLOBAL_USD_THRESHOLD:
                        continue

                    from_addr = tx.get("from", "")
                    to_addr = tx.get("to", "")
                    tx_hash = tx.get("hash", "")
                    block_num = tx.get("blockNum", "")

                    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
                    safe_print(
                        f"\n[POLYGON - {symbol} | ${usd_value:,.0f} USD] "
                        f"Block {block_num}"
                    )
                    safe_print(f"  Time : {current_time}")
                    safe_print(f"  TX   : {tx_hash[:24]}...")
                    safe_print(f"  From : {from_addr}")
                    safe_print(f"  To   : {to_addr}")
                    safe_print(f"  Amount: {float(value):,.4f} {symbol}")

                    from utils.dedup import handle_event
                    event = {
                        "blockchain": "polygon",
                        "tx_hash": tx_hash,
                        "from": from_addr,
                        "to": to_addr,
                        "symbol": symbol,
                        "amount": float(value),
                        "usd_value": usd_value,
                        "timestamp": time.time(),
                        "source": "polygon_alchemy",
                    }
                    handle_event(event)

                    from config.settings import polygon_buy_counts, polygon_sell_counts

                except Exception as e:
                    logger.warning(f"Polygon transfer processing error: {e}")
                    continue

        except Exception as e:
            logger.warning(f"Polygon poll error: {e}")

        shutdown_flag.wait(timeout=POLL_INTERVAL)


def test_polygonscan_connection():
    """Quick check that the Alchemy Polygon RPC is reachable."""
    block = _get_polygon_block_number()
    if block:
        safe_print(f"✅ Polygon Alchemy connection OK (block {block})")
        return True
    safe_print("⚠️  Polygon Alchemy connection failed")
    return False

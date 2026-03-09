"""
Polygon PoS whale transaction monitor.

Primary: Alchemy alchemy_getAssetTransfers (single call covers all tokens).
Fallback: PolygonScan tokentx per contract (if Alchemy fails).

Polls every 60 seconds.
"""

import time
import logging
import requests

from config.api_keys import POLYGONSCAN_API_KEY
from config.settings import (
    polygon_last_processed_block,
    GLOBAL_USD_THRESHOLD,
    shutdown_flag,
)
from data.tokens import POLYGON_TOKENS_TO_MONITOR, TOKEN_PRICES
from utils.base_helpers import safe_print, log_error

logger = logging.getLogger(__name__)

POLL_INTERVAL = 60
POLYGONSCAN_API_BASE = "https://api.polygonscan.com/api"


def _alchemy_get_block_number() -> int | None:
    """Get latest Polygon block number via Alchemy."""
    try:
        from utils.alchemy_rpc import get_alchemy_rpc, _rpc_call
        rpc_url = get_alchemy_rpc('polygon')
        if not rpc_url:
            return None
        result = _rpc_call(rpc_url, 'eth_blockNumber', [], cu_cost=10)
        if result:
            return int(result, 16)
    except Exception as e:
        logger.warning(f"Alchemy Polygon block number error: {e}")
    return None


def _polygonscan_get_block_number() -> int | None:
    """Get latest Polygon block number via PolygonScan (fallback)."""
    try:
        resp = requests.get(POLYGONSCAN_API_BASE, params={
            "module": "proxy",
            "action": "eth_blockNumber",
            "apikey": POLYGONSCAN_API_KEY,
        }, timeout=10)
        data = resp.json()
        result = data.get("result")
        if result:
            return int(result, 16)
    except Exception as e:
        logger.warning(f"PolygonScan block number error: {e}")
    return None


def _get_polygon_block_number() -> int | None:
    """Get block number — try Alchemy first, PolygonScan fallback."""
    block = _alchemy_get_block_number()
    if block:
        return block
    return _polygonscan_get_block_number()


def _alchemy_fetch_transfers(from_hex: str, to_hex: str, contract_addresses: list) -> list | None:
    """Fetch ERC-20 transfers via Alchemy alchemy_getAssetTransfers (single call for all tokens)."""
    try:
        from utils.alchemy_rpc import fetch_asset_transfers
        transfers = fetch_asset_transfers(
            blockchain='polygon',
            from_block=from_hex,
            to_block=to_hex,
            contract_addresses=contract_addresses,
            category=["erc20"],
        )
        return transfers
    except Exception as e:
        logger.warning(f"Alchemy Polygon transfers error: {e}")
        return None


def _polygonscan_fetch_transfers(contract_address: str, start_block: int, end_block: int) -> list:
    """Fetch ERC-20 token transfers from PolygonScan (per-token fallback)."""
    try:
        resp = requests.get(POLYGONSCAN_API_BASE, params={
            "module": "account",
            "action": "tokentx",
            "contractaddress": contract_address,
            "startblock": start_block,
            "endblock": end_block,
            "page": 1,
            "offset": 100,
            "sort": "desc",
            "apikey": POLYGONSCAN_API_KEY,
        }, timeout=15)
        data = resp.json()
        if data.get("status") == "1" and data.get("result"):
            return data["result"]
    except Exception as e:
        logger.warning(f"PolygonScan token transfer error: {e}")
    return []


def _process_alchemy_transfer(tx: dict, contract_map: dict) -> dict | None:
    """Process an Alchemy alchemy_getAssetTransfers result into an event."""
    raw_contract = (tx.get("rawContract") or {})
    contract_addr = raw_contract.get("address", "").lower()
    symbol_info = contract_map.get(contract_addr)
    if not symbol_info:
        return None

    symbol, decimals = symbol_info
    price = TOKEN_PRICES.get(symbol, 0)
    if price == 0:
        return None

    value = tx.get("value")
    if value is None:
        raw_hex = raw_contract.get("value", "0x0")
        try:
            raw_int = int(raw_hex, 16)
            value = raw_int / (10 ** decimals)
        except (ValueError, TypeError):
            return None

    usd_value = float(value) * price
    if usd_value < GLOBAL_USD_THRESHOLD:
        return None

    return {
        "blockchain": "polygon",
        "tx_hash": tx.get("hash", ""),
        "from": tx.get("from", ""),
        "to": tx.get("to", ""),
        "symbol": symbol,
        "amount": float(value),
        "usd_value": usd_value,
        "timestamp": time.time(),
        "source": "polygon_alchemy",
        "block_num": tx.get("blockNum", ""),
    }


def _process_polygonscan_transfer(tx: dict, symbol: str, decimals: int, price: float) -> dict | None:
    """Process a PolygonScan tokentx result into an event."""
    try:
        raw_value = int(tx.get("value", "0"))
        token_amount = raw_value / (10 ** decimals)
        usd_value = token_amount * price

        if usd_value < GLOBAL_USD_THRESHOLD:
            return None

        return {
            "blockchain": "polygon",
            "tx_hash": tx.get("hash", ""),
            "from": tx.get("from", ""),
            "to": tx.get("to", ""),
            "symbol": symbol,
            "amount": token_amount,
            "usd_value": usd_value,
            "timestamp": int(tx.get("timeStamp", time.time())),
            "source": "polygonscan",
            "block_num": tx.get("blockNumber", ""),
        }
    except (ValueError, TypeError):
        return None


def _classify_and_store(event: dict):
    """Classify a Polygon event and route through the dedup/storage pipeline."""
    from utils.dedup import handle_event

    # Classify via WhaleIntelligenceEngine
    try:
        from utils.classification_final import process_and_enrich_transaction
        enriched = process_and_enrich_transaction(event)
        if enriched and isinstance(enriched, dict):
            classification = enriched.get('classification', 'TRANSFER').upper()
        else:
            classification = 'TRANSFER'
    except Exception:
        classification = 'TRANSFER'

    event['classification'] = classification
    handle_event(event)

    from config.settings import polygon_buy_counts, polygon_sell_counts
    if classification in ('BUY', 'MODERATE_BUY', 'BUY_MODERATE'):
        polygon_buy_counts[event['symbol']] += 1
    elif classification in ('SELL', 'MODERATE_SELL', 'SELL_MODERATE'):
        polygon_sell_counts[event['symbol']] += 1

    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    safe_print(
        f"\n[POLYGON - {event['symbol']} | ${event['usd_value']:,.0f} USD] "
        f"Block {event.get('block_num', '?')}"
    )
    safe_print(f"  Time : {current_time}")
    safe_print(f"  TX   : {event['tx_hash'][:24]}...")
    safe_print(f"  From : {event['from']}")
    safe_print(f"  To   : {event['to']}")
    safe_print(f"  Amount: {event['amount']:,.4f} {event['symbol']}")
    safe_print(f"  Classification: {classification}")


def print_new_polygon_transfers():
    """
    Continuously polls for large Polygon ERC-20 transfers.
    Primary: Alchemy (single call). Fallback: PolygonScan (per-token).
    """
    safe_print("✅ Polygon monitor started (60s interval)")

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

    seen_hashes = set()

    while not shutdown_flag.is_set():
        try:
            tip = _get_polygon_block_number()
            if tip is None:
                safe_print("⚠️  Polygon: block fetch failed, retrying...")
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
            blocks_scanned = tip - last_block

            # Try Alchemy first (single call for all tokens)
            transfers = _alchemy_fetch_transfers(from_hex, to_hex, contract_addresses)

            if transfers is not None:
                # Process Alchemy results
                processed = 0
                for tx in transfers:
                    event = _process_alchemy_transfer(tx, contract_map)
                    if event and event['tx_hash'] not in seen_hashes:
                        seen_hashes.add(event['tx_hash'])
                        _classify_and_store(event)
                        processed += 1
                if processed > 0:
                    safe_print(f"  Polygon: {processed} whale tx in {blocks_scanned} blocks (Alchemy)")
            else:
                # Fallback: PolygonScan per-token
                safe_print(f"  Polygon: Alchemy unavailable, using PolygonScan fallback")
                processed = 0
                for symbol, info in POLYGON_TOKENS_TO_MONITOR.items():
                    price = TOKEN_PRICES.get(symbol, 0)
                    if price == 0:
                        continue
                    raw_transfers = _polygonscan_fetch_transfers(
                        info["contract"], last_block + 1, tip
                    )
                    time.sleep(0.25)  # Rate limit: 5 calls/sec free tier

                    for tx in raw_transfers:
                        tx_hash = tx.get("hash", "")
                        if tx_hash in seen_hashes:
                            continue
                        event = _process_polygonscan_transfer(tx, symbol, info["decimals"], price)
                        if event:
                            seen_hashes.add(tx_hash)
                            _classify_and_store(event)
                            processed += 1

                if processed > 0:
                    safe_print(f"  Polygon: {processed} whale tx in {blocks_scanned} blocks (PolygonScan)")

            last_block = tip

            # Trim seen_hashes to prevent unbounded growth
            if len(seen_hashes) > 10000:
                seen_hashes.clear()

        except Exception as e:
            logger.warning(f"Polygon poll error: {e}")

        shutdown_flag.wait(timeout=POLL_INTERVAL)


def test_polygonscan_connection():
    """Quick check that Polygon APIs are reachable."""
    block = _get_polygon_block_number()
    if block:
        safe_print(f"✅ Polygon connection OK (block {block})")
        return True
    safe_print("⚠️  Polygon connection failed")
    return False

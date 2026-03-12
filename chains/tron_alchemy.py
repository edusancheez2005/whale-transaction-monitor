"""
Tron whale transaction monitor using Alchemy Tron HTTP API.

Polls /wallet/getnowblock every 30 seconds.  When new blocks appear,
fetches transaction info for each block and scans for large native TRX
transfers above the USD threshold.

Stablecoin transfers (USDT/USDC) are excluded — they are high-volume noise
that floods the database with meaningless transfers.

CU budget: ~1,600 CU/hour (reduced from ~3,200 by skipping stablecoin parsing).
"""

import time
import logging
from typing import Optional, List

from config.settings import shutdown_flag, GLOBAL_USD_THRESHOLD
from data.tokens import TOKEN_PRICES
from utils.base_helpers import safe_print
from utils.alchemy_rpc import fetch_tron_now_block, fetch_tron_block_txinfo

logger = logging.getLogger(__name__)

POLL_INTERVAL = 30  # Reduced from 15s to save Alchemy CU budget

# Well-known Tron TRC-20 contracts
TRON_USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
TRON_USDC_CONTRACT = "TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8"

# keccak256("Transfer(address,address,uint256)") prefix
TRANSFER_TOPIC = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

_last_seen_block: Optional[int] = None


def _trx_price() -> float:
    return TOKEN_PRICES.get("TRX", 0.12)


def _tron_address_from_hex(hex_addr: str) -> str:
    """Convert a 0x-prefixed or raw 20-byte hex address to Tron base58 format."""
    try:
        import base58
        hex_addr = hex_addr.replace("0x", "").lstrip("0").zfill(40)
        raw = bytes.fromhex("41" + hex_addr)
        import hashlib
        d1 = hashlib.sha256(raw).digest()
        d2 = hashlib.sha256(d1).digest()
        return base58.b58encode(raw + d2[:4]).decode()
    except Exception:
        return hex_addr


def _process_block_txinfo(block_num: int, tx_infos: List[dict]) -> int:
    """Scan transaction info list for large transfers.  Returns qualifying tx count."""
    usdt_price = TOKEN_PRICES.get("USDT", 1.0)
    trx_price = _trx_price()
    found = 0

    for info in tx_infos:
        tx_id = info.get("id", "")
        fee = info.get("fee", 0)

        # --- TRC-20 Transfer events in logs ---
        receipt = info.get("receipt", {})
        logs = info.get("log", [])
        for log_entry in logs:
            topics = log_entry.get("topics", [])
            if not topics or topics[0] != TRANSFER_TOPIC:
                continue
            if len(topics) < 3:
                continue

            contract_addr = log_entry.get("address", "")
            data_hex = log_entry.get("data", "")

            symbol = "TRC20"
            decimals = 18
            if contract_addr.lower() == TRON_USDT_CONTRACT.lower() or "a614f803b6fd780986a42c78ec9c7f77e6ded13c" in contract_addr.lower():
                # Skip USDT — high-volume noise that floods the database
                continue
            elif "cead2b" in contract_addr.lower():
                # Skip USDC — same reason as USDT
                continue

            try:
                raw_amount = int(data_hex, 16) if data_hex else 0
                token_amount = raw_amount / (10 ** decimals)
                price = TOKEN_PRICES.get(symbol, usdt_price if symbol in ("USDT", "USDC") else 0)
                usd_value = token_amount * price
            except (ValueError, OverflowError):
                continue

            if usd_value < GLOBAL_USD_THRESHOLD:
                continue

            from_hex = topics[1][-40:] if len(topics) > 1 else ""
            to_hex = topics[2][-40:] if len(topics) > 2 else ""
            from_addr = _tron_address_from_hex(from_hex)
            to_addr = _tron_address_from_hex(to_hex)

            found += 1
            current_time = time.strftime('%Y-%m-%d %H:%M:%S')
            safe_print(
                f"\n[TRON - {symbol} | {token_amount:,.2f} {symbol} | ${usd_value:,.0f} USD] "
                f"Block {block_num}"
            )
            safe_print(f"  Time : {current_time}")
            safe_print(f"  TX   : {tx_id[:24]}...")
            safe_print(f"  From : {from_addr}")
            safe_print(f"  To   : {to_addr}")

            try:
                from utils.dedup import handle_event
                event = {
                    "blockchain": "tron",
                    "tx_hash": tx_id,
                    "from": from_addr,
                    "to": to_addr,
                    "amount": token_amount,
                    "symbol": symbol,
                    "usd_value": usd_value,
                    "timestamp": info.get("blockTimeStamp", time.time() * 1000) / 1000,
                    "source": "tron_alchemy",
                }
                handle_event(event)
            except Exception as e:
                logger.warning(f"Tron dedup/event error: {e}")

        # --- Native TRX transfers (from contract_result / internal_transactions) ---
        internal_txs = info.get("internal_transactions", [])
        for itx in internal_txs:
            call_value = itx.get("callValueInfo", [])
            for cv in call_value:
                trx_sun = cv.get("callValue", 0)
                if trx_sun <= 0:
                    continue
                trx_amount = trx_sun / 1_000_000
                usd_value = trx_amount * trx_price
                if usd_value < GLOBAL_USD_THRESHOLD:
                    continue

                from_addr = _tron_address_from_hex(itx.get("caller_address", ""))
                to_addr = _tron_address_from_hex(itx.get("transferTo_address", ""))

                found += 1
                current_time = time.strftime('%Y-%m-%d %H:%M:%S')
                safe_print(
                    f"\n[TRON - TRX | {trx_amount:,.2f} TRX | ${usd_value:,.0f} USD] "
                    f"Block {block_num}"
                )
                safe_print(f"  Time : {current_time}")
                safe_print(f"  TX   : {tx_id[:24]}...")
                safe_print(f"  From : {from_addr}")
                safe_print(f"  To   : {to_addr}")

    return found


def poll_tron_blocks():
    """Main loop — called as a thread target from enhanced_monitor.py."""
    global _last_seen_block

    safe_print("✅ Tron Alchemy monitor started (polling every 30s)")

    now_block = fetch_tron_now_block()
    if now_block and "block_header" in now_block:
        raw_header = now_block["block_header"].get("raw_data", {})
        _last_seen_block = raw_header.get("number")
        safe_print(f"   Tron tip: block {_last_seen_block}")
    else:
        safe_print("⚠️  Tron: could not fetch initial block")

    while not shutdown_flag.is_set():
        try:
            now_block = fetch_tron_now_block()
            if not now_block or "block_header" not in now_block:
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            raw_header = now_block["block_header"].get("raw_data", {})
            current_block = raw_header.get("number")

            if current_block is None:
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            if _last_seen_block is None:
                _last_seen_block = current_block
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            if current_block <= _last_seen_block:
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            # Process new blocks (batch of ~5 at 3s block time with 15s poll)
            for blk in range(_last_seen_block + 1, current_block + 1):
                if shutdown_flag.is_set():
                    break
                tx_infos = fetch_tron_block_txinfo(blk)
                if tx_infos is None:
                    continue
                if isinstance(tx_infos, list) and len(tx_infos) > 0:
                    found = _process_block_txinfo(blk, tx_infos)
                    if found:
                        safe_print(f"  Tron block {blk}: {found} whale transfer(s)")

            _last_seen_block = current_block

        except Exception as e:
            logger.warning(f"Tron poll error: {e}")

        shutdown_flag.wait(timeout=POLL_INTERVAL)

"""
Polygon PoS whale transaction monitor using PolygonScan API.

Uses PolygonScan token transfer endpoint (same pattern as Etherscan) to
discover large ERC-20 transfers.  No Alchemy dependency — works with just
a free PolygonScan API key.

Polls every 60 seconds.
"""

import time
import logging
import random
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

# Track last seen block per token
_last_blocks = {}


def _polygonscan_get_block_number() -> int | None:
    """Get latest Polygon block number via PolygonScan."""
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


def _fetch_token_transfers(contract_address: str, start_block: int, end_block: int) -> list:
    """Fetch ERC-20 token transfers from PolygonScan."""
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


def print_new_polygon_transfers():
    """
    Continuously polls PolygonScan for large Polygon ERC-20 transfers.
    Runs in its own thread.
    """
    safe_print("✅ Polygon monitor started (60s interval, PolygonScan API)")

    contract_map = {
        info["contract"].lower(): (symbol, info["decimals"])
        for symbol, info in POLYGON_TOKENS_TO_MONITOR.items()
    }

    current_block = _polygonscan_get_block_number()
    if current_block:
        safe_print(f"   Polygon tip: block {current_block}")
        last_block = current_block
    else:
        safe_print("⚠️  Polygon: could not fetch initial block, will retry")
        last_block = None

    seen_hashes = set()

    while not shutdown_flag.is_set():
        try:
            tip = _polygonscan_get_block_number()
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

            # Query each monitored token
            for symbol, info in POLYGON_TOKENS_TO_MONITOR.items():
                contract = info["contract"]
                decimals = info["decimals"]
                price = TOKEN_PRICES.get(symbol, 0)
                if price == 0:
                    continue

                transfers = _fetch_token_transfers(contract, last_block + 1, tip)
                time.sleep(0.25)  # Rate limit: 5 calls/sec on free tier

                for tx in transfers:
                    tx_hash = tx.get("hash", "")
                    if tx_hash in seen_hashes:
                        continue

                    try:
                        raw_value = int(tx.get("value", "0"))
                        token_amount = raw_value / (10 ** decimals)
                        usd_value = token_amount * price

                        if usd_value < GLOBAL_USD_THRESHOLD:
                            continue

                        seen_hashes.add(tx_hash)
                        if len(seen_hashes) > 5000:
                            # Trim oldest entries
                            seen_hashes.clear()

                        from_addr = tx.get("from", "")
                        to_addr = tx.get("to", "")
                        block_num = tx.get("blockNumber", "")

                        current_time = time.strftime('%Y-%m-%d %H:%M:%S')
                        safe_print(
                            f"\n[POLYGON - {symbol} | ${usd_value:,.0f} USD] "
                            f"Block {block_num}"
                        )
                        safe_print(f"  Time : {current_time}")
                        safe_print(f"  TX   : {tx_hash[:24]}...")
                        safe_print(f"  From : {from_addr}")
                        safe_print(f"  To   : {to_addr}")
                        safe_print(f"  Amount: {token_amount:,.4f} {symbol}")

                        from utils.dedup import handle_event
                        event = {
                            "blockchain": "polygon",
                            "tx_hash": tx_hash,
                            "from": from_addr,
                            "to": to_addr,
                            "symbol": symbol,
                            "amount": token_amount,
                            "usd_value": usd_value,
                            "timestamp": int(tx.get("timeStamp", time.time())),
                            "source": "polygonscan",
                        }

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
                            polygon_buy_counts[symbol] += 1
                        elif classification in ('SELL', 'MODERATE_SELL', 'SELL_MODERATE'):
                            polygon_sell_counts[symbol] += 1

                    except Exception as e:
                        logger.warning(f"Polygon transfer processing error: {e}")
                        continue

            last_block = tip

        except Exception as e:
            logger.warning(f"Polygon poll error: {e}")

        shutdown_flag.wait(timeout=POLL_INTERVAL)


def test_polygonscan_connection():
    """Quick check that PolygonScan API is reachable."""
    block = _polygonscan_get_block_number()
    if block:
        safe_print(f"✅ PolygonScan connection OK (block {block})")
        return True
    safe_print("⚠️  PolygonScan connection failed")
    return False

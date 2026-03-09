"""
Bitcoin whale transaction monitor using Alchemy Bitcoin RPC.

Polls for new blocks every 30 seconds.  When a new block arrives the full
block (verbosity=2) is fetched and every transaction output is checked
against the USD threshold.  Qualifying transfers are printed and routed
through the classification / dedup pipeline.

CU budget: ~700 CU/hour (extremely efficient — Bitcoin produces ~6 blocks/hr).
"""

import time
import logging
from typing import Optional

from config.settings import shutdown_flag, GLOBAL_USD_THRESHOLD
from data.tokens import TOKEN_PRICES
from utils.base_helpers import safe_print
from utils.alchemy_rpc import (
    fetch_bitcoin_blockcount,
    fetch_bitcoin_blockhash,
    fetch_bitcoin_block,
)

logger = logging.getLogger(__name__)

_last_seen_height: Optional[int] = None

POLL_INTERVAL = 30  # seconds between blockcount checks


def _btc_price() -> float:
    return TOKEN_PRICES.get("WBTC", TOKEN_PRICES.get("BTC", 65_000))


def _process_block(block: dict) -> int:
    """Parse a decoded Bitcoin block for large-value outputs.  Returns count of qualifying txs."""
    btc_usd = _btc_price()
    threshold_btc = GLOBAL_USD_THRESHOLD / btc_usd
    block_height = block.get("height", "?")
    txs = block.get("tx", [])
    found = 0

    for tx in txs:
        tx_hash = tx.get("txid", "")
        vout = tx.get("vout", [])

        for out in vout:
            value_btc = out.get("value", 0)
            if value_btc < threshold_btc:
                continue

            usd_value = value_btc * btc_usd
            spk = out.get("scriptPubKey", {})
            to_addr = ""
            if spk.get("address"):
                to_addr = spk["address"]
            elif spk.get("addresses"):
                to_addr = spk["addresses"][0]

            vin = tx.get("vin", [])
            from_addr = ""
            if vin and vin[0].get("prevout", {}).get("scriptPubKey", {}).get("address"):
                from_addr = vin[0]["prevout"]["scriptPubKey"]["address"]

            found += 1
            current_time = time.strftime('%Y-%m-%d %H:%M:%S')
            safe_print(
                f"\n[BITCOIN | {value_btc:,.4f} BTC | ${usd_value:,.0f} USD] "
                f"Block {block_height}"
            )
            safe_print(f"  Time : {current_time}")
            safe_print(f"  TX   : {tx_hash[:24]}...")
            safe_print(f"  From : {from_addr or 'coinbase'}")
            safe_print(f"  To   : {to_addr}")

            try:
                from utils.dedup import handle_event
                event = {
                    "blockchain": "bitcoin",
                    "tx_hash": tx_hash,
                    "from": from_addr or "coinbase",
                    "to": to_addr,
                    "amount": value_btc,
                    "symbol": "BTC",
                    "usd_value": usd_value,
                    "timestamp": block.get("time", time.time()),
                    "source": "bitcoin_alchemy",
                }

                # Classify via WhaleIntelligenceEngine (CEX/DEX detection)
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

                # Update buy/sell counters
                from config.settings import bitcoin_buy_counts, bitcoin_sell_counts
                if classification in ('BUY', 'MODERATE_BUY', 'BUY_MODERATE'):
                    bitcoin_buy_counts['BTC'] += 1
                elif classification in ('SELL', 'MODERATE_SELL', 'SELL_MODERATE'):
                    bitcoin_sell_counts['BTC'] += 1

                handle_event(event)
            except Exception as e:
                logger.warning(f"Bitcoin dedup/event error: {e}")

    return found


def poll_bitcoin_blocks():
    """Main loop — called as a thread target from enhanced_monitor.py."""
    global _last_seen_height

    safe_print("✅ Bitcoin Alchemy monitor started (polling every 30s)")

    height = fetch_bitcoin_blockcount()
    if height is not None:
        _last_seen_height = height
        safe_print(f"   Bitcoin tip: block {height}")
    else:
        safe_print("⚠️  Bitcoin: could not fetch initial block height")

    while not shutdown_flag.is_set():
        try:
            current_height = fetch_bitcoin_blockcount()
            if current_height is None:
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            if _last_seen_height is None:
                _last_seen_height = current_height
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            if current_height <= _last_seen_height:
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            for h in range(_last_seen_height + 1, current_height + 1):
                if shutdown_flag.is_set():
                    break
                blockhash = fetch_bitcoin_blockhash(h)
                if not blockhash:
                    continue
                block = fetch_bitcoin_block(blockhash, verbosity=2)
                if not block:
                    continue
                found = _process_block(block)
                if found:
                    safe_print(f"  Bitcoin block {h}: {found} whale transaction(s)")

            _last_seen_height = current_height

        except Exception as e:
            logger.warning(f"Bitcoin poll error: {e}")

        shutdown_flag.wait(timeout=POLL_INTERVAL)

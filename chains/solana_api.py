"""
Solana API poller using Solscan Pro API v2 for token transfers, with
Helius RPC fallback for transaction details.

Polls recent large token transfers for the top Solana tokens every 60 seconds.
Uses Solscan Pro API to get actual token transfers (getSignaturesForAddress
on mint addresses does NOT return transfers — only mint/burn ops).
"""

import time
import logging
import requests

from config.api_keys import HELIUS_API_KEY, SOLSCAN_API_KEY
from config.settings import (
    solana_last_processed_signature,
    print_lock,
    GLOBAL_USD_THRESHOLD
)
from data.tokens import SOL_TOKENS_TO_MONITOR, TOKEN_PRICES
from utils.base_helpers import safe_print
from utils.alchemy_rpc import fetch_solana_signatures, fetch_solana_transaction, get_alchemy_rpc

logger = logging.getLogger(__name__)

try:
    from config.api_keys import SOLANA_PRIMARY_RPC
    HELIUS_RPC_URL = SOLANA_PRIMARY_RPC
except ImportError:
    HELIUS_RPC_URL = f"https://rpc.helius.xyz/?api-key={HELIUS_API_KEY}"

SOLSCAN_API_BASE = "https://pro-api.solscan.io/v2.0"

# Top tokens by volume/relevance
TOP_SOLANA_TOKENS = [
    "SOL", "JTO", "WIF", "PYTH", "RENDER",
]

# Track last seen tx hash per token to avoid reprocessing
_last_seen_tx = {}

parsed_cache = {}
PARSED_CACHE_MAX = 500

def _active_tokens():
    """Return the subset of SOL_TOKENS_TO_MONITOR limited to TOP_SOLANA_TOKENS."""
    return {k: v for k, v in SOL_TOKENS_TO_MONITOR.items() if k in TOP_SOLANA_TOKENS}


def _solscan_get(endpoint: str, params: dict = None) -> list:
    """Fetch from Solscan Pro API v2 with auth header."""
    headers = {"token": SOLSCAN_API_KEY} if SOLSCAN_API_KEY else {}
    try:
        resp = requests.get(
            f"{SOLSCAN_API_BASE}{endpoint}",
            params=params or {},
            headers=headers,
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("success") and "data" in data:
                return data["data"]
            # Some endpoints return list directly
            if isinstance(data, list):
                return data
        else:
            logger.warning(f"Solscan API {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        logger.warning(f"Solscan API error ({endpoint}): {e}")
    return []


def _helius_token_transfers(mint_addr: str, limit: int = 20) -> list:
    """Fallback: use Helius Enhanced Transactions API to get recent transfers."""
    if not HELIUS_API_KEY:
        return []
    try:
        url = f"https://api.helius.xyz/v0/token-events?api-key={HELIUS_API_KEY}"
        params = {
            "mint": mint_addr,
            "type": "TRANSFER",
            "limit": limit,
        }
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code == 200:
            return resp.json() if isinstance(resp.json(), list) else []
    except Exception as e:
        logger.warning(f"Helius token events error: {e}")
    return []


def initialize_baseline():
    """Initialize last-seen transaction hashes for each token to skip historical data."""
    tokens = _active_tokens()
    for symbol, info in tokens.items():
        mint_addr = info["mint"]
        transfers = _solscan_get("/token/transfer", {
            "token": mint_addr,
            "page_size": 1,
            "sort_by": "block_time",
            "sort_order": "desc",
        })
        if transfers and isinstance(transfers, list) and len(transfers) > 0:
            first = transfers[0]
            tx_hash = first.get("trans_id") or first.get("tx_hash") or first.get("signature", "")
            _last_seen_tx[symbol] = tx_hash
            safe_print(f"  Solana baseline {symbol}: {tx_hash[:16]}...")
        else:
            _last_seen_tx[symbol] = None
        time.sleep(0.5)


def fetch_solana_token_transfers():
    """
    Poll new SPL token transfer events using Solscan Pro API.
    Returns a list of normalized transfer dicts.
    """
    results = []

    for symbol, info in _active_tokens().items():
        mint_addr = info["mint"]
        decimals = info["decimals"]

        # Fetch recent transfers from Solscan
        transfers = _solscan_get("/token/transfer", {
            "token": mint_addr,
            "page_size": 20,
            "sort_by": "block_time",
            "sort_order": "desc",
        })

        if not transfers:
            # Fallback to Helius token events
            helius_events = _helius_token_transfers(mint_addr, limit=20)
            for evt in helius_events:
                tx_hash = evt.get("signature", "")
                if _last_seen_tx.get(symbol) and tx_hash == _last_seen_tx[symbol]:
                    break
                token_transfers = evt.get("tokenTransfers", [])
                for tt in token_transfers:
                    if tt.get("mint") == mint_addr:
                        results.append({
                            "blockchain": "solana",
                            "from": tt.get("fromUserAccount", ""),
                            "to": tt.get("toUserAccount", ""),
                            "symbol": symbol,
                            "amount": str(tt.get("tokenAmount", 0)),
                            "tx_hash": tx_hash,
                            "timestamp": evt.get("timestamp"),
                            "decimals": decimals,
                        })
                if helius_events:
                    _last_seen_tx[symbol] = helius_events[0].get("signature", "")
            time.sleep(0.3)
            continue

        new_transfers = []
        last_seen = _last_seen_tx.get(symbol)
        for t in transfers:
            tx_hash = t.get("trans_id") or t.get("tx_hash") or t.get("signature", "")
            if last_seen and tx_hash == last_seen:
                break
            new_transfers.append(t)

        if new_transfers:
            # Update baseline to newest
            first = new_transfers[0]
            _last_seen_tx[symbol] = first.get("trans_id") or first.get("tx_hash") or first.get("signature", "")

        for t in reversed(new_transfers):  # Process oldest first
            tx_hash = t.get("trans_id") or t.get("tx_hash") or t.get("signature", "")
            from_addr = t.get("from_address") or t.get("source", "")
            to_addr = t.get("to_address") or t.get("destination", "")
            raw_amount = t.get("amount") or t.get("token_amount") or "0"
            block_time = t.get("block_time") or t.get("time")

            results.append({
                "blockchain": "solana",
                "from": from_addr,
                "to": to_addr,
                "symbol": symbol,
                "amount": str(raw_amount),
                "tx_hash": tx_hash,
                "timestamp": int(block_time) if block_time else None,
                "decimals": decimals,
            })

        time.sleep(0.3)

    return results

def print_new_solana_transfers():
    """
    Continuously polls and prints new Solana token transfers for monitored tokens.
    Runs in its own thread with a polling interval.
    """
    from config.settings import shutdown_flag as _shutdown_flag

    safe_print("✅ Solana API polling thread started (60s interval, top 5 tokens via Solscan)")

    if not _last_seen_tx:
        safe_print("   Initializing Solana token baselines...")
        initialize_baseline()

    poll_interval = 60
    backoff_multiplier = 1

    while not _shutdown_flag.is_set():
        try:
            transfers = fetch_solana_token_transfers()
            backoff_multiplier = 1  # reset on success

            for event in transfers:
                try:
                    symbol = event["symbol"]
                    decimals = event["decimals"]
                    raw_amount = event["amount"]

                    # Handle both raw integer and already-decimal amounts
                    try:
                        amount_val = float(raw_amount)
                    except (ValueError, TypeError):
                        amount_val = 0

                    # If amount looks like raw (very large), apply decimals
                    if amount_val > 10 ** decimals:
                        token_amount = amount_val / (10 ** decimals)
                    else:
                        token_amount = amount_val

                    price = TOKEN_PRICES.get(symbol, 0)
                    estimated_usd = token_amount * price

                    if estimated_usd < GLOBAL_USD_THRESHOLD:
                        continue

                    from_addr = event["from"]
                    to_addr = event["to"]
                    tx_hash = event["tx_hash"]

                    from utils.classification_final import process_and_enrich_transaction

                    enriched_transaction = process_and_enrich_transaction(event)

                    # Always route through dedup so transactions show on dashboard
                    from utils.dedup import handle_event
                    if enriched_transaction:
                        event.update({
                            'classification': enriched_transaction.get('classification', 'TRANSFER').upper(),
                            'confidence': enriched_transaction.get('confidence_score', 0),
                            'whale_signals': enriched_transaction.get('whale_signals', []),
                            'whale_score': enriched_transaction.get('whale_score', 0),
                            'is_whale_transaction': enriched_transaction.get('is_whale_transaction', False),
                            'usd_value': estimated_usd,
                            'source': 'solana_api'
                        })
                    else:
                        # Enrichment failed — still show the transaction
                        event.update({
                            'classification': 'TRANSFER',
                            'usd_value': estimated_usd,
                            'source': 'solana_api'
                        })

                    handle_event(event)

                    classification = event.get('classification', 'TRANSFER').upper()

                    from config.settings import solana_api_buy_counts, solana_api_sell_counts
                    if classification in ('BUY', 'MODERATE_BUY', 'BUY_MODERATE'):
                        solana_api_buy_counts[symbol] += 1
                    elif classification in ('SELL', 'MODERATE_SELL', 'SELL_MODERATE'):
                        solana_api_sell_counts[symbol] += 1

                    whale_indicator = " 🐋" if (enriched_transaction or {}).get('is_whale_transaction') else ""
                    safe_print(f"\n[SOLANA - {symbol} | ${estimated_usd:,.2f} USD] Tx {tx_hash}{whale_indicator}")
                    safe_print(f"  From: {from_addr}")
                    safe_print(f"  To:   {to_addr}")
                    safe_print(f"  Amount: {token_amount:,.6f} {symbol} (~${estimated_usd:,.2f} USD)")
                    safe_print(f"  Classification: {classification}")

                    if enriched_transaction and enriched_transaction.get('whale_classification'):
                        safe_print(f"  Whale Analysis: {enriched_transaction['whale_classification']}")

                        # Persist enriched transaction to Supabase
                        try:
                            from utils.supabase_writer import store_transaction
                            classification_data = {
                                'classification': classification.upper(),
                                'confidence': enriched_transaction.get('confidence_score', 0),
                                'whale_score': enriched_transaction.get('whale_score', 0),
                                'reasoning': enriched_transaction.get('whale_classification', ''),
                            }
                            store_transaction(event, classification_data)
                        except Exception:
                            pass

                except Exception as e:
                    safe_print(f"Error processing Solana transfer: {str(e)}")
                    continue

        except Exception as e:
            error_str = str(e)
            if "429" in error_str or "rate" in error_str.lower():
                backoff_multiplier = min(backoff_multiplier * 2, 8)
                safe_print(f"[Solana] Rate limited, backing off {poll_interval * backoff_multiplier}s")
            else:
                safe_print(f"Solana API polling error: {e}")

        _shutdown_flag.wait(timeout=poll_interval * backoff_multiplier)

def test_helius_connection():
    """Test Helius API connection"""
    try:
        safe_print("Testing Helius API connection...")
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getHealth"
        }
        r = requests.post(HELIUS_RPC_URL, json=payload, timeout=20)
        data = r.json()
        if r.status_code == 200 and not data.get("error"):
            safe_print("✅ Helius API connection successful")
            return True
        else:
            safe_print(f"❌ Helius API error: {data.get('error', 'Unknown error')}")
            return False
    except Exception as e:
        safe_print(f"❌ Error connecting to Helius: {e}")
        return False

"""
Solana API poller using Alchemy as primary RPC with shared rate limiter.

Polls getSignaturesForAddress for the top 10 Solana tokens every 60 seconds,
then fetches full transaction data for qualifying transfers.  All calls are
routed through the shared AlchemyRateLimiter to stay within the CU budget.
"""

import time
import logging
import requests

from config.api_keys import HELIUS_API_KEY
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

# Top tokens by volume/relevance — limited to 5 to stay within Alchemy CU budget
TOP_SOLANA_TOKENS = [
    "SOL", "JTO", "WIF", "PYTH", "RENDER",
]

parsed_cache = {}
PARSED_CACHE_MAX = 500

def _active_tokens():
    """Return the subset of SOL_TOKENS_TO_MONITOR limited to TOP_SOLANA_TOKENS."""
    return {k: v for k, v in SOL_TOKENS_TO_MONITOR.items() if k in TOP_SOLANA_TOKENS}


def initialize_baseline():
    """Initialize last processed signatures for tokens to skip historical transfers.
    Retries failed tokens up to 3 times with increasing delay."""
    tokens = _active_tokens()
    pending = list(tokens.keys())
    max_attempts = 3

    for attempt in range(1, max_attempts + 1):
        still_pending = []
        for mint in pending:
            info = tokens[mint]
            mint_addr = info["mint"]
            try:
                sigs = fetch_solana_signatures(mint_addr, limit=1)
                if isinstance(sigs, list) and len(sigs) > 0 and isinstance(sigs[0], dict):
                    solana_last_processed_signature[mint] = sigs[0].get("signature")
                    safe_print(f"Initialized baseline for {mint} with signature: {sigs[0].get('signature', '')[:16]}...")
                elif sigs is None:
                    still_pending.append(mint)
                else:
                    solana_last_processed_signature[mint] = None
            except Exception as e:
                safe_print(f"[Solana] Warning: Could not initialize {mint} (attempt {attempt}): {e}")
                still_pending.append(mint)
            time.sleep(1.0)

        pending = still_pending
        if not pending:
            break
        if attempt < max_attempts:
            wait = 5 * attempt
            safe_print(f"[Solana] Retrying {len(pending)} failed tokens in {wait}s ({', '.join(pending)})...")
            time.sleep(wait)

    for mint in pending:
        safe_print(f"[Solana] {mint} baseline init failed after {max_attempts} attempts — will poll from latest")
        solana_last_processed_signature[mint] = None

def fetch_solana_token_transfers():
    """
    Poll new SPL token transfer events for the top Solana tokens via the
    shared Alchemy rate limiter.  Returns a list of normalized transfer dicts.
    """
    results = []

    for mint, info in _active_tokens().items():
        mint_addr = info["mint"]
        symbol = mint
        decimals = info["decimals"]

        last_sig = solana_last_processed_signature.get(mint)
        new_signatures = []

        sig_infos = fetch_solana_signatures(mint_addr, limit=20)
        if not isinstance(sig_infos, list) or not sig_infos:
            continue

        for entry in sig_infos:
            if not isinstance(entry, dict):
                continue
            sig = entry.get("signature")
            if not sig:
                continue
            if last_sig and sig == last_sig:
                break
            new_signatures.append(sig)

        new_signatures.reverse()

        if new_signatures:
            solana_last_processed_signature[mint] = new_signatures[-1]
        elif last_sig is None and sig_infos:
            first = sig_infos[0] if isinstance(sig_infos[0], dict) else {}
            solana_last_processed_signature[mint] = first.get("signature")
            continue
        else:
            continue

        for sig in new_signatures:
            if sig in parsed_cache:
                tx_data = parsed_cache[sig]
            else:
                tx_data = fetch_solana_transaction(sig)
                if not tx_data:
                    continue
                parsed_cache[sig] = tx_data
                if len(parsed_cache) > PARSED_CACHE_MAX:
                    oldest_keys = list(parsed_cache.keys())[:len(parsed_cache) - PARSED_CACHE_MAX]
                    for k in oldest_keys:
                        del parsed_cache[k]

            block_time = tx_data.get("blockTime")
            if block_time is None:
                for entry in sig_infos:
                    if entry.get("signature") == sig:
                        block_time = entry.get("blockTime")
                        break

            transfers_extracted = []
            try:
                transaction = tx_data.get("transaction", {})
                message = transaction.get("message", {})
                instructs = message.get("instructions", [])
                meta = tx_data.get("meta", {})
                inner_instructions = meta.get("innerInstructions", [])

                def extract_transfers_from_instructions(instructions):
                    for ix in instructions:
                        program_id = ix.get("programId")
                        if program_id and program_id.endswith("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"):
                            parsed = ix.get("parsed")
                            if parsed and parsed.get("type") == "transfer":
                                info_data = parsed.get("info", {})
                                if info_data.get("mint") == mint_addr:
                                    transfers_extracted.append({
                                        "source": info_data.get("source"),
                                        "destination": info_data.get("destination"),
                                        "amount": str(info_data.get("amount"))
                                    })

                extract_transfers_from_instructions(instructs)
                for inner in inner_instructions:
                    if inner.get("instructions"):
                        extract_transfers_from_instructions(inner["instructions"])
            except Exception as e:
                safe_print(f"[Solana] Error parsing transaction {sig}: {e}")
                transfers_extracted = []

            for t in transfers_extracted:
                event = {
                    "blockchain": "solana",
                    "from": t.get("source"),
                    "to": t.get("destination"),
                    "symbol": symbol,
                    "amount": t.get("amount", "0"),
                    "tx_hash": sig,
                    "timestamp": int(block_time) if block_time else None,
                    "decimals": decimals,
                    "raw_tx": tx_data,
                }
                results.append(event)

        time.sleep(0.3)

    return results

def print_new_solana_transfers():
    """
    Continuously polls and prints new Solana token transfers for monitored tokens.
    Runs in its own thread with a polling interval.
    """
    from config.settings import shutdown_flag as _shutdown_flag

    safe_print("✅ Solana API polling thread started (60s interval, top 5 tokens)")

    if not any(solana_last_processed_signature.values()):
        safe_print("🔍 Initializing Solana token baselines...")
        initialize_baseline()

    poll_interval = 60  # Restored to 60s — concurrency semaphore in alchemy_rpc.py handles CU budget
    backoff_multiplier = 1

    while not _shutdown_flag.is_set():
        try:
            transfers = fetch_solana_token_transfers()
            backoff_multiplier = 1  # reset on success

            for event in transfers:
                try:
                    symbol = event["symbol"]
                    raw_amount = int(event["amount"])
                    decimals = event["decimals"]
                    token_amount = raw_amount / (10 ** decimals)
                    price = TOKEN_PRICES.get(symbol, 0)
                    estimated_usd = token_amount * price

                    if estimated_usd < GLOBAL_USD_THRESHOLD:
                        continue

                    from_addr = event["from"]
                    to_addr = event["to"]
                    tx_hash = event["tx_hash"]

                    from utils.classification_final import process_and_enrich_transaction

                    enriched_transaction = process_and_enrich_transaction(event)

                    if enriched_transaction:
                        event.update({
                            'classification': enriched_transaction.get('classification', 'UNKNOWN'),
                            'confidence': enriched_transaction.get('confidence_score', 0),
                            'whale_signals': enriched_transaction.get('whale_signals', []),
                            'whale_score': enriched_transaction.get('whale_score', 0),
                            'is_whale_transaction': enriched_transaction.get('is_whale_transaction', False),
                            'usd_value': estimated_usd,
                            'source': 'solana_api'
                        })

                        from utils.dedup import handle_event
                        handle_event(event)

                        classification = enriched_transaction.get('classification', 'UNKNOWN').lower()

                        from config.settings import solana_api_buy_counts, solana_api_sell_counts
                        if classification == "buy":
                            solana_api_buy_counts[symbol] += 1
                        elif classification == "sell":
                            solana_api_sell_counts[symbol] += 1

                        whale_indicator = " 🐋" if enriched_transaction.get('is_whale_transaction') else ""
                        safe_print(f"\n[SOLANA - {symbol} | ${estimated_usd:,.2f} USD] Tx {tx_hash}{whale_indicator}")
                        safe_print(f"  From: {from_addr}")
                        safe_print(f"  To:   {to_addr}")
                        safe_print(f"  Amount: {token_amount:,.6f} {symbol} (~${estimated_usd:,.2f} USD)")
                        safe_print(f"  Classification: {classification.upper()} (confidence: {enriched_transaction.get('confidence_score', 0):.2f})")

                        if enriched_transaction.get('whale_classification'):
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
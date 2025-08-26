import time
import requests

from config.api_keys import HELIUS_API_KEY
from config.settings import (
    solana_last_processed_signature,
    print_lock,
    GLOBAL_USD_THRESHOLD
)
from data.tokens import SOL_TOKENS_TO_MONITOR, TOKEN_PRICES
from utils.base_helpers import safe_print

# Base URL for Helius RPC (free tier)
HELIUS_RPC_URL = f"https://rpc.helius.xyz/?api-key={HELIUS_API_KEY}"

# Cache for parsed transactions to avoid duplicate processing
parsed_cache = {}

def initialize_baseline():
    """Initialize last processed signatures for tokens to skip historical transfers."""
    for mint, info in SOL_TOKENS_TO_MONITOR.items():
        mint_addr = info["mint"]
        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSignaturesForAddress",
                "params": [
                    mint_addr,
                    {"limit": 1}  # get only the most recent signature
                ]
            }
            resp = requests.post(HELIUS_RPC_URL, json=payload, timeout=10)
            data = resp.json()
            if resp.status_code == 200 and data.get("result"):
                latest_sig_info = data["result"][0]
                # Save the latest signature as baseline (skip historical transactions including this one)
                solana_last_processed_signature[mint] = latest_sig_info.get("signature")
                safe_print(f"Initialized baseline for {mint} with signature: {latest_sig_info.get('signature')[:16]}...")
            else:
                solana_last_processed_signature[mint] = None  # no transactions exist for this token yet
        except Exception as e:
            safe_print(f"[Solana] Warning: Could not initialize token {mint}: {e}")
            solana_last_processed_signature[mint] = None

def fetch_solana_token_transfers():
    """
    Poll new SPL token transfer events for each mint in SOL_TOKENS_TO_MONITOR using Helius API.
    Returns a list of normalized transfer event dicts.
    """
    results = []
    
    for mint, info in SOL_TOKENS_TO_MONITOR.items():
        mint_addr = info["mint"]
        symbol = mint
        decimals = info["decimals"]

        # Get recent signatures for the mint address, potentially using pagination
        last_sig = solana_last_processed_signature.get(mint)
        new_signatures = []  # will collect signatures newer than last processed
        
        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSignaturesForAddress",
                "params": [
                    mint_addr,
                    {"limit": 100}  # fetch up to 100 recent signatures in one batch
                ]
            }
            resp = requests.post(HELIUS_RPC_URL, json=payload, timeout=10)
        except requests.RequestException as e:
            safe_print(f"[Solana] API request error for mint {mint}: {e}")
            continue

        if resp.status_code != 200:
            safe_print(f"[Solana] Unexpected status code {resp.status_code} for mint {mint}")
            continue

        data = resp.json()
        if "error" in data:
            safe_print(f"[Solana] RPC error for mint {mint}: {data['error']}")
            continue

        sig_infos = data.get("result", [])
        if not sig_infos:
            continue

        # Filter out signatures that we have already processed.
        # The result is sorted newest-first. We stop when we encounter the last processed signature.
        for info in sig_infos:
            sig = info.get("signature")
            if not sig:
                continue
            if last_sig and sig == last_sig:
                # We've reached the last seen signature, stop here.
                break
            new_signatures.append(sig)

        # At this point, new_signatures contains all signatures newer than last_sig (in descending order).
        # We will process them from oldest to newest for chronological order.
        new_signatures.reverse()

        # Update last processed signature to the most recent one (last in chronological order) for this mint.
        if new_signatures:
            solana_last_processed_signature[mint] = new_signatures[-1]
        else:
            # If we found nothing new (e.g., last_sig was the latest), keep it unchanged.
            continue

        # Fetch and parse each new transaction
        for sig in new_signatures:
            if sig in parsed_cache:
                # Avoid duplicate API calls: use cached parsed transaction if available
                tx_data = parsed_cache[sig]
            else:
                # Call getTransaction for this signature with "jsonParsed" encoding for easier parsing
                try:
                    tx_payload = {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "getTransaction",
                        "params": [
                            sig,
                            {"encoding": "jsonParsed", "commitment": "confirmed"}
                        ]
                    }
                    tx_resp = requests.post(HELIUS_RPC_URL, json=tx_payload, timeout=10)
                except requests.RequestException as e:
                    safe_print(f"[Solana] Error fetching transaction {sig}: {e}")
                    continue
                if tx_resp.status_code != 200:
                    safe_print(f"[Solana] Unexpected status code {tx_resp.status_code} for transaction {sig}")
                    continue
                tx_data = tx_resp.json().get("result")
                if not tx_data:
                    # If transaction not found or error, skip
                    continue
                # Cache the parsed transaction data
                parsed_cache[sig] = tx_data

            # Extract timestamp
            block_time = tx_data.get("blockTime")
            if block_time is None:
                # Check the signature info list for this signature's timestamp
                for info in sig_infos:
                    if info.get("signature") == sig:
                        block_time = info.get("blockTime")
                        break

            # Parse token transfer events from the transaction.
            transfers_extracted = []
            try:
                # Navigate the parsed transaction JSON to find token transfer instructions
                transaction = tx_data.get("transaction", {})
                message = transaction.get("message", {})
                instructs = message.get("instructions", [])
                # Also consider inner instructions
                meta = tx_data.get("meta", {})
                inner_instructions = meta.get("innerInstructions", [])

                # Helper to process a list of instructions for token transfers
                def extract_transfers_from_instructions(instructions):
                    for ix in instructions:
                        program_id = ix.get("programId")
                        # Check if instruction is from the SPL Token program
                        if program_id and program_id.endswith("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"):
                            # It's a token program instruction; check if it's a Transfer
                            parsed = ix.get("parsed")
                            if parsed and parsed.get("type") == "transfer":
                                info_data = parsed.get("info", {})
                                # Each transfer instruction info typically has 'mint', 'source', 'destination', 'amount'
                                if info_data.get("mint") == mint_addr:
                                    transfers_extracted.append({
                                        "source": info_data.get("source"),
                                        "destination": info_data.get("destination"),
                                        "amount": str(info_data.get("amount"))
                                    })
                
                # Extract from top-level instructions
                extract_transfers_from_instructions(instructs)
                # Extract from any inner instructions
                for inner in inner_instructions:
                    if inner.get("instructions"):
                        extract_transfers_from_instructions(inner["instructions"])
            except Exception as e:
                safe_print(f"[Solana] Error parsing transaction {sig}: {e}")
                transfers_extracted = []

            # Create an event entry for each transfer involving our monitored mint
            for t in transfers_extracted:
                source_account = t.get("source")
                dest_account = t.get("destination")
                from_addr = source_account
                to_addr = dest_account

                event = {
                    "blockchain": "solana",
                    "from": from_addr,
                    "to": to_addr,
                    "symbol": symbol,
                    "amount": t.get("amount", "0"),  # raw amount as string
                    "tx_hash": sig,
                    "timestamp": int(block_time) if block_time else None,
                    "decimals": decimals,
                    "raw_tx": tx_data  # raw transaction data for reference
                }
                results.append(event)

            # Throttle between fetching each transaction to respect any rate limits
            time.sleep(0.1)

        # Throttle between tokens (to avoid rapid bursts if many tokens)
        time.sleep(0.2)

    return results

def print_new_solana_transfers():
    """
    Polls and prints new Solana token transfers for monitored tokens.
    This function will be updated in Phase 2 to call the universal processor.
    """
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    safe_print(f"\n[{current_time}] 🔍 Checking Solana token transfers...")

    # Initialize baseline if not done yet
    if not any(solana_last_processed_signature.values()):
        initialize_baseline()

    transfers = fetch_solana_token_transfers()
    
    for event in transfers:
        try:
            symbol = event["symbol"]
            raw_amount = int(event["amount"])
            decimals = event["decimals"]
            token_amount = raw_amount / (10 ** decimals)
            price = TOKEN_PRICES.get(symbol, 0)
            estimated_usd = token_amount * price
            
            if estimated_usd >= GLOBAL_USD_THRESHOLD:
                from_addr = event["from"]
                to_addr = event["to"]
                tx_hash = event["tx_hash"]
                
                # Process through the universal processor
                from utils.classification_final import process_and_enrich_transaction
                
                enriched_transaction = process_and_enrich_transaction(event)
                
                if enriched_transaction:
                    # Add enriched data back to the event for main monitor display
                    event.update({
                        'classification': enriched_transaction.get('classification', 'UNKNOWN'),
                        'confidence': enriched_transaction.get('confidence_score', 0),
                        'whale_signals': enriched_transaction.get('whale_signals', []),
                        'whale_score': enriched_transaction.get('whale_score', 0),
                        'is_whale_transaction': enriched_transaction.get('is_whale_transaction', False),
                        'usd_value': estimated_usd,
                        'source': 'solana_api'
                    })
                    
                    # Add to main monitoring system
                    from utils.dedup import handle_event
                    handle_event(event)
                    
                    classification = enriched_transaction.get('classification', 'UNKNOWN').lower()
                    
                    # Update counters
                    from config.settings import solana_api_buy_counts, solana_api_sell_counts
                    if classification == "buy":
                        solana_api_buy_counts[symbol] += 1
                    elif classification == "sell":
                        solana_api_sell_counts[symbol] += 1
                    
                    # Print with enhanced information
                    whale_indicator = " 🐋" if enriched_transaction.get('is_whale_transaction') else ""
                    safe_print(f"\n[SOLANA - {symbol} | ${estimated_usd:,.2f} USD] Tx {tx_hash}{whale_indicator}")
                    safe_print(f"  From: {from_addr}")
                    safe_print(f"  To:   {to_addr}")
                    safe_print(f"  Amount: {token_amount:,.6f} {symbol} (~${estimated_usd:,.2f} USD)")
                    safe_print(f"  Classification: {classification.upper()} (confidence: {enriched_transaction.get('confidence_score', 0):.2f})")
                    
                    if enriched_transaction.get('whale_classification'):
                        safe_print(f"  Whale Analysis: {enriched_transaction['whale_classification']}")
                    
                    # TODO: Store enriched_transaction in Supabase here

        except Exception as e:
            safe_print(f"Error processing Solana transfer: {str(e)}")
            continue

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
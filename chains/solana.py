import json
import time
import threading
import traceback
import websocket
from typing import Dict, Optional
from config.api_keys import HELIUS_API_KEY
from config.settings import (
    GLOBAL_USD_THRESHOLD,
    solana_previous_balances,
    solana_buy_counts,
    solana_sell_counts,
    shutdown_flag,
    print_lock
)
from data.tokens import SOL_TOKENS_TO_MONITOR, TOKEN_PRICES
from utils.classification_final import enhanced_solana_classification
from utils.base_helpers import safe_print, log_error
from utils.summary import record_transfer
from utils.summary import has_been_classified, mark_as_classified
from utils.dedup import deduplicator, get_dedup_stats, deduped_transactions, handle_event




total_transfers_fetched = 0
filtered_by_threshold = 0

# In solana.py - update the on_solana_message function

# In solana.py - Update the on_solana_message function

def on_solana_message(ws, message):
    try:
        global total_transfers_fetched
        total_transfers_fetched += 1
        
        data = json.loads(message)
        if "params" not in data:
            return

        result = data["params"].get("result", {})
        if "value" not in result:
            return

        value = result["value"]
        if "account" not in value or "data" not in value["account"]:
            return

        parsed_data = value["account"]["data"].get("parsed", {})
        if parsed_data.get("type") != "account":
            return

        info = parsed_data.get("info", {})
        mint = info.get("mint")
        current_amount = info.get("tokenAmount", {}).get("uiAmount", 0)
        owner = info.get("owner")
        tx_hash = data["params"].get("result", {}).get("signature", "")

        # Skip transactions without a hash or with empty addresses
        if not tx_hash or not owner or not mint:
            return
            
        # Get previous state
        prev_owner = None
        prev_amount = 0
        
        if mint in solana_previous_balances:
            prev_owner = solana_previous_balances[mint].get("owner")
            
        if owner in solana_previous_balances:
            prev_amount = solana_previous_balances.get(owner, {}).get(mint, 0)
            
        amount_change = current_amount - prev_amount
        
        # Skip negligible changes (can be noise)
        if abs(amount_change) < 0.0001:
            return

        # Check monitored tokens
        for symbol, token_info in SOL_TOKENS_TO_MONITOR.items():
            if token_info["mint"] == mint:
                price = TOKEN_PRICES.get(symbol, 0)
                usd_value = abs(amount_change) * price
                min_threshold = token_info.get("min_threshold", GLOBAL_USD_THRESHOLD)

                # Skip low-value transactions
                if usd_value < min_threshold:
                    continue

                # Create standardized event with unique transaction identifier
                # Use a combination of tx_hash, owner, and amount for better uniqueness
                unique_id = f"{tx_hash}_{owner}_{amount_change:.6f}"
                
                event = {
                    "blockchain": "solana",
                    "tx_hash": unique_id,  # Use enhanced ID to avoid duplicates
                    "original_hash": tx_hash,  # Keep original hash for reference
                    "from": prev_owner or "unknown",
                    "to": owner,
                    "amount": abs(amount_change),
                    "symbol": symbol,
                    "usd_value": usd_value,
                    "timestamp": time.time(),
                    "source": "solana"
                }

                # Check if it's a duplicate before processing
                if not handle_event(event):
                    continue

                # Only proceed with classification and counting if it's not a duplicate
                classification, confidence = enhanced_solana_classification(
                    owner=owner,
                    prev_owner=prev_owner,
                    amount_change=amount_change,
                    tx_hash=tx_hash,
                    token=symbol,
                    source="solana"
                )

                # Add classification to the event
                event["classification"] = classification
                
                # Only count transactions with sufficient confidence
                if confidence >= 2:  # Increased confidence threshold
                    if classification == "buy":
                        solana_buy_counts[symbol] += 1
                    elif classification == "sell":
                        solana_sell_counts[symbol] += 1

                    # Print transaction details
                    current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    safe_print(f"\n[{symbol} | ${usd_value:,.2f} USD] Solana {classification.upper()}")
                    safe_print(f"  Time: {current_time}")
                    safe_print(f"  TX Hash: {tx_hash[:16]}...")
                    safe_print(f"  Amount: {abs(amount_change):,.2f} {symbol}")
                    safe_print(f"  Classification: {classification} (confidence: {confidence})")

                # Update balance tracking
                if owner not in solana_previous_balances:
                    solana_previous_balances[owner] = {}
                solana_previous_balances[owner][mint] = current_amount

    except Exception as e:
        error_msg = f"Error processing Solana transfer: {str(e)}"
        safe_print(error_msg)
        log_error(error_msg)
        traceback.print_exc()


def connect_solana_websocket(retry_count=0, max_retries=5):
    ws_url = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    
    def on_open(ws):
        print("Solana monitoring started...")
        subscribe_msg = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "programSubscribe",
            "params": [
                "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                {
                    "encoding": "jsonParsed",
                    "commitment": "confirmed"
                }
            ]
        }
        ws.send(json.dumps(subscribe_msg))

    def on_error(ws, error):
        error_msg = f"Solana connection error: {error}"
        print(error_msg)
        log_error(error_msg)

    def on_close(ws, close_status_code, close_msg):
        if not shutdown_flag.is_set():  # Only retry if we're not shutting down
            nonlocal retry_count
            retry_count += 1
            if retry_count <= max_retries:
                wait_time = min(30, 2 ** retry_count)
                safe_print(f"Solana connection closed. Reconnecting... ({retry_count}/{max_retries})")
                time.sleep(wait_time)
                connect_solana_websocket(retry_count, max_retries)

    ws_app = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_solana_message,
        on_error=on_error,
        on_close=on_close
    )
    
    ws_thread = threading.Thread(target=ws_app.run_forever, kwargs={"ping_interval": 60})
    ws_thread.daemon = True
    ws_thread.start()
    
    return ws_thread


def start_solana_thread():
    solana_thread = threading.Thread(target=connect_solana_websocket, daemon=True)
    solana_thread.start()
    return solana_thread
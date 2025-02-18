import json
import time
import threading
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
from utils.classification import enhanced_solana_classification
from utils.base_helpers import safe_print
from utils.summary import record_transfer

def on_solana_message(ws, message):
    try:
        data = json.loads(message)
        if "params" in data:
            result = data["params"].get("result", {})
            if "value" in result:
                value = result["value"]
                if "account" in value and "data" in value["account"]:
                    parsed_data = value["account"]["data"].get("parsed", {})
                    if parsed_data.get("type") == "account":
                        info = parsed_data.get("info", {})
                        mint = info.get("mint")
                        current_amount = info.get("tokenAmount", {}).get("uiAmount", 0)
                        owner = info.get("owner")
                        
                        # Get previous owner and amount
                        prev_owner = None
                        if mint in solana_previous_balances:
                            prev_owner = solana_previous_balances[mint].get("owner")
                        prev_amount = solana_previous_balances.get(owner, {}).get(mint, 0)
                        amount_change = current_amount - prev_amount
                        
                        # Check if this mint is one we're monitoring
                        for symbol, token_info in SOL_TOKENS_TO_MONITOR.items():
                            if token_info["mint"] == mint:
                                price = TOKEN_PRICES.get(symbol, 0)
                                usd_value = abs(amount_change) * price
                                min_threshold = token_info["min_threshold"]
                                
                                if usd_value >= min_threshold:
                                    # Get classification and confidence score
                                    classification, confidence = enhanced_solana_classification(
                                        owner=owner,
                                        prev_owner=prev_owner,
                                        amount_change=amount_change
                                    )
                                    
                                    # Only process if we have high confidence
                                    if confidence >= 2:
                                        # Update counters based on classification
                                        if classification == "buy":
                                            solana_buy_counts[symbol] += 1
                                        elif classification == "sell":
                                            solana_sell_counts[symbol] += 1
                                        
                                        record_transfer("SOL", abs(amount_change), owner, prev_owner or "unknown")
                                            
                                        # Print transaction details
                                        current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                                        print(f"\n[{symbol} | ${usd_value:,.2f} USD] Solana {classification.upper()}")
                                        print(f"  Time: {current_time}")
                                        print(f"  Amount: {abs(amount_change):,.2f} {symbol}")
                                        print(f"  Owner: {owner}")
                                        print(f"  Classification: {classification} (confidence: {confidence})")
                                
                                # Update balance tracking
                                if owner not in solana_previous_balances:
                                    solana_previous_balances[owner] = {}
                                solana_previous_balances[owner][mint] = current_amount
                                
    except Exception as e:
        if "KeyError" not in str(e):
            print(f"Error processing Solana transfer: {str(e)}")


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
        print(f"Solana connection error: {error}")

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

    ws_app = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_solana_message,
        on_error=on_error,
        on_close=on_close
    )
    ws_app.run_forever(ping_interval=60)  # Add ping_interval
    
    try:
        ws_app.run_forever()
    except Exception as e:
        print(f"Solana websocket error: {str(e)}")
        if retry_count < max_retries:
            time.sleep(5)
            connect_solana_websocket(retry_count, max_retries)

def start_solana_thread():
    solana_thread = threading.Thread(target=connect_solana_websocket, daemon=True)
    solana_thread.start()
    return solana_thread
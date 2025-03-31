import json
import time
import threading
import websocket
import traceback
from typing import Dict, Optional
from config.api_keys import WHALE_ALERT_API_KEY, WHALE_WS_URL
from config.settings import (
    GLOBAL_USD_THRESHOLD,
    whale_buy_counts,
    whale_sell_counts,
    whale_trending_counts,
    shutdown_flag,
    print_lock
)
from data.tokens import STABLE_COINS
from utils.classification import transaction_classifier
from utils.base_helpers import safe_print
from config.settings import etherscan_buy_counts, etherscan_sell_counts
from data.tokens import TOKEN_PRICES
from utils.summary import has_been_classified, mark_as_classified, record_transfer
from utils.dedup import get_dedup_stats, deduped_transactions, handle_event


total_transfers_fetched = 0
filtered_by_threshold = 0
stablecoin_skip_count = 0

# ----------------------
# WHALE ALERT WEBSOCKET FUNCTIONS
# ----------------------

# In whale_alert.py

def on_whale_message(ws, message):
    try:
        data = json.loads(message)
        if data.get("type") != "alert":
            return

        amounts = data.get("amounts", [])
        blockchain = data.get("blockchain", "unknown").lower()
        tx_type = data.get("transaction_type", "unknown")
        tx_from = data.get("from", "unknown")
        tx_to = data.get("to", "unknown")
        tx_hash = data.get("transaction", {}).get("hash", "")
        
        if not amounts:
            return

        valid_transfers = []
        total_usd_value = 0

        for amt in amounts:
            symbol = amt.get("symbol", "").upper()
            amount = amt.get("amount", 0)
            usd_value = amt.get("value_usd", 0)

            if symbol.lower() in STABLE_COINS:
                continue

            try:
                amount = float(amount)
                usd_value = float(usd_value)
                
                if amount > 0 and usd_value >= GLOBAL_USD_THRESHOLD:
                    # Create standardized event for each token transfer
                    event = {
                        "blockchain": blockchain,
                        "tx_hash": tx_hash,
                        "from": tx_from,
                        "to": tx_to,
                        "symbol": symbol,
                        "amount": amount,
                        "usd_value": usd_value,
                        "timestamp": data.get("timestamp", time.time()),
                        "source": "whale_alert"
                    }

                    # Check for duplicates
                    if handle_event(event):
                        valid_transfers.append({
                            "symbol": symbol,
                            "amount": amount,
                            "usd_value": usd_value
                        })
                        total_usd_value += usd_value

            except (ValueError, TypeError) as e:
                print(f"Error converting values: {e}")
                continue

        if not valid_transfers:
            return

        # Get classification for the overall transaction
        classification, confidence = transaction_classifier(
            tx_from=tx_from, 
            tx_to=tx_to,
            tx_hash=tx_hash,
            source="whale_alert"
        )

        # Normalize probable classifications (e.g. "probable_buy" becomes "buy")
        if classification.startswith("probable_"):
            classification = classification.split("_")[-1]

        # Add classification to the event
        for i in range(len(valid_transfers)):
            # Add classification to each event
            valid_transfers[i]["classification"] = classification

        if classification in ("buy", "sell", "transfer"):
            for transfer in valid_transfers:
                symbol = transfer["symbol"]
                whale_trending_counts[symbol] += 1
                
                if classification == "buy":
                    whale_buy_counts[symbol] += 1
                elif classification == "sell":
                    whale_sell_counts[symbol] += 1

            # Print alert with valid transfers only
            ts = data.get("timestamp", 0)
            human_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))
            
            print("\n" + "="*50)
            print("🐋 WHALE ALERT DETECTED:")
            print("="*50)
            print(f"Time: {human_time}")
            print(f"Blockchain: {blockchain}")
            print(f"Transaction Type: {tx_type}")
            print(f"TX Hash: {tx_hash[:24]}...")
            print(f"From: {tx_from}")
            print(f"To:   {tx_to}")
            print(f"Classification: {classification.upper()} (confidence: {confidence})")
            
            print("\nAmounts Transferred:")
            for transfer in valid_transfers:
                print(f"  • {transfer['symbol']}: {transfer['amount']:,.2f} (~${transfer['usd_value']:,.2f} USD)")
            
            print(f"\nTotal USD Value: ${total_usd_value:,.2f}")
            print("="*50 + "\n")

    except Exception as e:
        print(f"Error processing Whale Alert message: {e}")

def on_whale_error(ws, error):
    print(f"\n[Whale Alert WS Error] {error}")
    if "429" in str(error):
        print("Rate limit encountered – pausing 120 seconds before reconnect.")
        time.sleep(120)  # Longer pause for rate limits

def on_whale_close(ws, close_status_code, close_msg):
    print(f"Whale Alert WS closed (code: {close_status_code}). Message: {close_msg}")
    wait_time = 30 if close_status_code else 120  # Increased wait times
    print(f"Reconnecting in {wait_time} seconds...")
    time.sleep(wait_time)
    connect_whale_websocket()

def on_whale_open(ws):
    print("Whale Alert WS connection established.")
    subscription_request = {
        "type": "subscribe_alerts",
        "min_value_usd": 100000,  # Updated threshold to $2M
        "tx_types": ["transfer", "mint", "burn"],
        "blockchain": [
            "ethereum",
            "bitcoin",
            "solana",
            "ripple",
            "polygon",
            "tron",
            "algorand",
            "bitcoin cash",
            "dogecoin",
            "litecoin"
        ]
    }
    ws.send(json.dumps(subscription_request))
    print("Whale Alert subscription request sent with configuration:")
    print(json.dumps(subscription_request, indent=2))

def connect_whale_websocket():
    ws_app = websocket.WebSocketApp(
        WHALE_WS_URL,
        on_open=on_whale_open,
        on_message=on_whale_message,
        on_error=on_whale_error,
        on_close=on_whale_close
    )
    ws_app.run_forever(ping_interval=60)  # Add ping_interval

def start_whale_thread():
    """
    Start the Whale Alert monitoring thread with proper error handling
    """
    try:
        # Check if API key is valid
        if not WHALE_ALERT_API_KEY or len(WHALE_ALERT_API_KEY) < 10:
            safe_print("⚠️ Invalid Whale Alert API key. Whale monitoring disabled.")
            return None
            
        # Start the websocket connection
        thread = connect_whale_websocket()
        if thread:
            thread.name = "WhaleAlert"  # Name the thread for monitoring
            return thread
        else:
            safe_print("⚠️ Failed to start Whale Alert monitoring.")
            return None
    except Exception as e:
        safe_print(f"Error starting Whale Alert thread: {e}")
        traceback.print_exc()
        return None
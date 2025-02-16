import json
import time
import threading
import websocket
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
from utils.summary import record_transfer

# ----------------------
# WHALE ALERT WEBSOCKET FUNCTIONS
# ----------------------
def on_whale_message(ws, message):
    try:
        print("\n🔍 WHALE ALERT DEBUG 🔍")
        print("Raw message received:", message[:500], "...")
        
        data = json.loads(message)
        print(f"Message type: {data.get('type')}")
        
        if data.get("type") != "alert":
            print("⏭️  Skipping - not an alert message")
            return
            
        # Extract and debug transaction details
        amounts = data.get("amounts", [])
        blockchain = data.get("blockchain", "unknown")
        tx_type = data.get("transaction_type", "unknown")
        tx_from = data.get("from", "unknown")
        tx_to = data.get("to", "unknown")
        
        print(f"\n📊 Transaction Overview:")
        print(f"Blockchain: {blockchain}")
        print(f"Transaction Type: {tx_type}")
        print(f"From: {tx_from}")
        print(f"To: {tx_to}")
        
        if not amounts:
            print("⚠️  No amounts present in alert")
            return
            
        # Initialize collection variables
        coins_in_alert = set()
        total_usd_value = 0
        valid_transfers = []
        
        print("\n💰 Raw Amount Data:")
        for amt in amounts:
            symbol = amt.get("symbol", "").lower()
            amount = amt.get("amount", 0)
            usd_value = amt.get("value_usd", 0)
            print(f"  • {symbol.upper()}: Amount={amount}, USD=${usd_value}")
            
            if symbol in STABLE_COINS:
                print(f"    ⏭️  Skipping {symbol.upper()} - is stablecoin")
                continue
                
            try:
                amount = float(amount)
                usd_value = float(usd_value)
                if amount > 0 and usd_value > 0:
                    print(f"    ✅ Valid transfer: {amount:,.2f} {symbol.upper()} (${usd_value:,.2f})")
                    coins_in_alert.add(symbol)
                    total_usd_value += usd_value
                    valid_transfers.append({
                        "symbol": symbol.upper(),
                        "amount": amount,
                        "usd_value": usd_value
                    })
                    record_transfer(symbol, amount, tx_from, tx_to)
                else:
                    print(f"    ❌ Invalid amounts: amount={amount}, usd={usd_value}")
            except (ValueError, TypeError) as e:
                print(f"    ❌ Error converting values: {e}")
                continue
        
        # Only proceed if we have valid transfers
        if not valid_transfers:
            print("No valid transfers found after filtering")
            return
            
        if total_usd_value == 0:
            print("Skipping - total USD value is 0")
            return
            
        classification, confidence = transaction_classifier(tx_from, tx_to)
        if confidence >= 2:
            # Update counters based on classification
            if classification == "buy":
                etherscan_buy_counts[symbol] += 1
            elif classification == "sell":
                etherscan_sell_counts[symbol] += 1
        else:
            classification = "transfer"
        
        print(f"\nProcessing whale alert:")
        print(f"From: {tx_from}")
        print(f"To: {tx_to}")
        print(f"Classification: {classification}")
        
        if classification in ("buy", "sell"):
            # Update counters
            for coin in coins_in_alert:
                whale_trending_counts[coin] += 1
                if classification == "buy":
                    whale_buy_counts[coin] += 1
                else:
                    whale_sell_counts[coin] += 1

            # Print alert with valid transfers only
            ts = data.get("timestamp", 0)
            human_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))
            print("\n" + "="*50)
            print("🐋 WHALE ALERT DETECTED:")
            print("="*50)
            print(f"Time: {human_time}")
            print(f"Blockchain: {blockchain}")
            print(f"Transaction Type: {tx_type}")
            print(f"From: {tx_from}")
            print(f"To:   {tx_to}")
            print(f"Classification: {classification.upper()}")
            
            print("\nAmounts Transferred:")
            for transfer in valid_transfers:
                print(f"  • {transfer['symbol']}: {transfer['amount']:,.2f} (~${transfer['usd_value']:,.2f} USD)")
            
            print(f"\nTotal USD Value: ${total_usd_value:,.2f}")
            if data.get("transaction", {}).get("hash"):
                print(f"Tx Hash: {data['transaction']['hash']}")
            print("="*50 + "\n")
            
    except Exception as e:
        print(f"Error processing Whale Alert message: {e}")
        if isinstance(message, str):
            print(f"Raw message that caused error: {message[:200]}...")

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
    whale_thread = threading.Thread(target=connect_whale_websocket, daemon=True)
    whale_thread.start()
    return whale_thread
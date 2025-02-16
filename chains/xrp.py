import json
import time
import threading
import websocket
from typing import Dict, Optional
from config.settings import (
    GLOBAL_USD_THRESHOLD,
    xrp_buy_counts,
    xrp_sell_counts,
    xrp_payment_count,
    xrp_total_amount,
    shutdown_flag,
    print_lock
)
from data.addresses import xrp_exchange_addresses
from utils.classification import classify_xrp_transaction
from utils.base_helpers import safe_print
from data.tokens import TOKEN_PRICES
from utils.summary import record_transfer



def on_xrp_message(ws, message):
    """Process incoming XRP websocket messages with enhanced classification"""
    global xrp_payment_count, xrp_total_amount, xrp_buy_counts, xrp_sell_counts
    try:
        data = json.loads(message)
        txn = data.get("transaction")
        if txn and txn.get("TransactionType") == "Payment":
            amount = txn.get("Amount")
            if isinstance(amount, str):
                try:
                    amount_xrp = float(amount) / 10_000_000
                except Exception:
                    amount_xrp = 0
            else:
                amount_xrp = 0
                
            # Only process significant transactions
            if amount_xrp * TOKEN_PRICES.get("XRP", 0.5) >= GLOBAL_USD_THRESHOLD:
                classification, processed_amount = classify_xrp_transaction(txn)
                
                # Update counters
                xrp_payment_count += 1
                xrp_total_amount += processed_amount
                
                if classification == "buy":
                    xrp_buy_counts += 1
                elif classification == "sell":
                    xrp_sell_counts += 1
                
                record_transfer("XRP", amount_xrp, txn.get("Account", ""), txn.get("Destination", ""))

                
                # Print transaction details
                current_time = time.strftime('%Y-%m-%d %H:%M:%S')
                print(f"\n[XRP | {processed_amount:,.2f} XRP] {classification.upper()}")
                print(f"Time: {current_time}")
                print(f"From: {txn.get('Account', '')}")
                print(f"To: {txn.get('Destination', '')}")
                print(f"Classification: {classification}")
                if "DestinationTag" in txn:
                    print(f"Destination Tag: {txn['DestinationTag']}")
                
    except Exception as e:
        print("Error processing XRP message:", e)

def on_xrp_open(ws):
    """Handle XRP websocket connection opening"""
    print("XRP WS connection established.")
    subscribe_msg = json.dumps({
        "command": "subscribe",
        "streams": ["transactions"]
    })
    ws.send(subscribe_msg)
    print("Subscribed to XRP transactions.")

def on_xrp_error(ws, error):
    """Handle XRP websocket errors"""
    print(f"[XRP WS Error] {error}")

def on_xrp_close(ws, close_status_code, close_msg):
    """Handle XRP websocket connection closing"""
    print(f"XRP WS closed (code: {close_status_code}). Message: {close_msg}")
    time.sleep(10)
    connect_xrp_websocket()

def connect_xrp_websocket():
    """Create and configure XRP websocket connection"""
    ws_app = websocket.WebSocketApp(
        "wss://s1.ripple.com/",
        on_open=on_xrp_open,
        on_message=on_xrp_message,
        on_error=on_xrp_error,
        on_close=on_xrp_close
    )
    ws_app.run_forever(ping_interval=60)

def start_xrp_thread():
    """Start XRP monitoring in a separate thread"""
    xrp_thread = threading.Thread(target=connect_xrp_websocket, daemon=True)
    xrp_thread.start()
    return xrp_thread


def analyze_address_pattern(addr):
    """
    Analyze XRP address patterns to identify likely exchange addresses
    """
    # Exchange addresses often have specific patterns
    if not addr.startswith('r'):
        return "unknown"
        
    # Count uppercase letters (exchanges often use more uppercase)
    uppercase_ratio = sum(1 for c in addr[1:] if c.isupper()) / len(addr[1:])
    
    # Check address length (exchange addresses often have specific lengths)
    addr_len = len(addr)
    
    if addr_len >= 25 and addr_len <= 35:
        if uppercase_ratio > 0.4:
            return "likely_exchange"
    
    # Look for patterns common in user addresses
    if addr_len > 30 and uppercase_ratio < 0.3:
        return "likely_user"
        
    return "unknown"
    
    return "transfer"  # Changed from "unknown" to "transfer" for clearer meaning

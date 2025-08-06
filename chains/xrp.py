import json
import time
import threading
import websocket
import traceback
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
from utils.classification_final import classify_xrp_transaction
from utils.base_helpers import safe_print
from data.tokens import TOKEN_PRICES
from utils.summary import has_been_classified, mark_as_classified, record_transfer
from utils.dedup import get_dedup_stats, deduped_transactions, handle_event


total_transfers_fetched = 0
filtered_by_threshold = 0
connection_attempts = 0

def on_xrp_message(ws, message):
    """Process incoming XRP websocket messages with enhanced classification and deduplication"""
    global xrp_payment_count, xrp_total_amount, xrp_buy_counts, xrp_sell_counts
    global total_transfers_fetched, filtered_by_threshold
    
    try:
        data = json.loads(message)
        txn = data.get("transaction")
        if txn and txn.get("TransactionType") == "Payment":
            total_transfers_fetched += 1
            
            # Get transaction hash for deduplication
            tx_hash = txn.get("hash", "")
            
            # Skip already classified transactions
            if has_been_classified("XRP", tx_hash):
                return
                
            amount = txn.get("Amount")
            if isinstance(amount, str):
                try:
                    amount_xrp = float(amount) / 10_000_000
                except Exception:
                    amount_xrp = 0
            else:
                amount_xrp = 0
                
            # Only process significant transactions
            xrp_price = TOKEN_PRICES.get("XRP", 0.5)
            usd_value = amount_xrp * xrp_price
            if usd_value < GLOBAL_USD_THRESHOLD:
                filtered_by_threshold += 1
                return
                
            classification, processed_amount = classify_xrp_transaction(txn)
            
            # Skip already classified transactions
            if classification == "already_classified":
                return
                
            # Update counters
            xrp_payment_count += 1
            xrp_total_amount += float(processed_amount)
            
            # Create event for deduplication 
            event = {
                "blockchain": "xrp",
                "tx_hash": tx_hash,
                "classification": classification,
                "from": txn.get("Account", ""),
                "to": txn.get("Destination", ""),
                "amount": amount_xrp,
                "usd_value": usd_value,
                "symbol": "XRP"
            }
            if handle_event(event):
                record_transfer("XRP", amount_xrp, txn.get("Account", ""),
              txn.get("Destination", ""), tx_hash)
            
            if classification == "buy":
                xrp_buy_counts += 1
            elif classification == "sell":
                xrp_sell_counts += 1
                     
            # Print transaction details
            current_time = time.strftime('%Y-%m-%d %H:%M:%S')
            safe_print(f"\n[XRP | {processed_amount:,.2f} XRP | ${usd_value:,.2f} USD] {classification.upper()}")
            safe_print(f"Time: {current_time}")
            safe_print(f"TX Hash: {tx_hash[:16]}...")
            safe_print(f"From: {txn.get('Account', '')}")
            safe_print(f"To: {txn.get('Destination', '')}")
            safe_print(f"Classification: {classification}")
            if "DestinationTag" in txn:
                safe_print(f"Destination Tag: {txn['DestinationTag']}")
            
    except Exception as e:
        safe_print(f"Error processing XRP message: {e}")
        traceback.print_exc()


def on_xrp_open(ws):
    """Handle XRP websocket connection opening"""
    global connection_attempts
    connection_attempts = 0
    safe_print("XRP WS connection established.")
    try:
        subscribe_msg = json.dumps({
            "id": "xrp_monitor",
            "command": "subscribe",
            "streams": ["transactions"]
        })
        ws.send(subscribe_msg)
        safe_print("Subscribed to XRP transactions.")
    except Exception as e:
        safe_print(f"Error subscribing to XRP transactions: {e}")
        traceback.print_exc()

def on_xrp_error(ws, error):
    """Handle XRP websocket errors"""
    safe_print(f"[XRP WS Error] {error}")
    # Don't close the connection on most errors, let the websocket library handle reconnection

def on_xrp_close(ws, close_status_code, close_msg):
    """Handle XRP websocket connection closing"""
    global connection_attempts
    connection_attempts += 1
    
    safe_print(f"XRP WS closed (code: {close_status_code}). Message: {close_msg}")
    
    # Implement exponential backoff
    wait_time = min(120, 5 * (2 ** min(connection_attempts, 5)))
    safe_print(f"Reconnecting in {wait_time} seconds... (attempt {connection_attempts})")
    
    if not shutdown_flag.is_set():
        time.sleep(wait_time)
        try:
            connect_xrp_websocket()
        except Exception as e:
            safe_print(f"Error reconnecting to XRP: {e}")
            traceback.print_exc()

def connect_xrp_websocket():
    """Create and configure XRP websocket connection"""
    try:
        # Try multiple XRP websocket servers if needed
        xrp_servers = [
            "wss://s1.ripple.com/",
            "wss://s2.ripple.com/",
            "wss://xrplcluster.com/"
        ]
        
        server = xrp_servers[0]  # Start with the first server
        
        websocket.enableTrace(False)  # Set to True for debugging
        ws_app = websocket.WebSocketApp(
            server,
            on_open=on_xrp_open,
            on_message=on_xrp_message,
            on_error=on_xrp_error,
            on_close=on_xrp_close
        )
        
        # Run the websocket connection in its own thread
        wst = threading.Thread(
            target=lambda: ws_app.run_forever(
                ping_interval=30,
                ping_timeout=10,
                ping_payload="ping",
                sslopt={"cert_reqs": 0}  # Disable certificate verification if needed
            ),
            daemon=True
        )
        wst.start()
        return wst
    except Exception as e:
        safe_print(f"Failed to connect to XRP websocket: {e}")
        traceback.print_exc()
        return None

def start_xrp_thread():
    """Start XRP monitoring in a separate thread"""
    try:
        thread = connect_xrp_websocket()
        if thread:
            thread.name = "XRP"  # Name the thread for monitoring
            return thread
        else:
            safe_print("⚠️ Failed to start XRP monitoring.")
            return None
    except Exception as e:
        safe_print(f"Error starting XRP thread: {e}")
        traceback.print_exc()
        return None


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
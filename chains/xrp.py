import json
import time
import threading
import websocket
import traceback
from typing import Dict, Optional
import config.settings as _settings
from config.settings import (
    GLOBAL_USD_THRESHOLD,
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

            # Skip already classified transactions
            if has_been_classified("XRP", tx_hash):
                return

            # Update counters (use mutable containers for cross-module access)
            _settings.xrp_payment_count[0] += 1
            _settings.xrp_total_amount[0] += amount_xrp

            from_addr = txn.get("Account", "")
            to_addr = txn.get("Destination", "")

            # Classify using known XRP exchange addresses
            from_is_exchange = from_addr in xrp_exchange_addresses
            to_is_exchange = to_addr in xrp_exchange_addresses

            if from_is_exchange and not to_is_exchange:
                # Withdrawal from exchange → BUY (someone bought and is withdrawing)
                classification = "BUY"
            elif to_is_exchange and not from_is_exchange:
                # Deposit to exchange → SELL (someone is depositing to sell)
                classification = "SELL"
            elif from_is_exchange and to_is_exchange:
                # Exchange to exchange → TRANSFER
                classification = "TRANSFER"
            else:
                # Wallet to wallet — check DestinationTag (exchange deposits use tags)
                if "DestinationTag" in txn:
                    classification = "SELL"  # DestinationTag usually means exchange deposit
                else:
                    classification = "TRANSFER"

            mark_as_classified("XRP", tx_hash)

            # Create event for deduplication
            event = {
                "blockchain": "xrp",
                "tx_hash": tx_hash,
                "from": from_addr,
                "to": to_addr,
                "amount": amount_xrp,
                "usd_value": usd_value,
                "symbol": "XRP",
                "classification": classification,
            }

            if handle_event(event):
                record_transfer("XRP", amount_xrp, txn.get("Account", ""),
                    txn.get("Destination", ""), tx_hash)

            # Update buy/sell counters (dict-style, same as all other chains)
            if classification in ("BUY", "MODERATE_BUY", "VERIFIED_SWAP_BUY"):
                _settings.xrp_buy_counts['XRP'] += 1
            elif classification in ("SELL", "MODERATE_SELL", "VERIFIED_SWAP_SELL"):
                _settings.xrp_sell_counts['XRP'] += 1

            # Print transaction details
            current_time = time.strftime('%Y-%m-%d %H:%M:%S')
            safe_print(f"\n[XRP | {amount_xrp:,.2f} XRP | ${usd_value:,.2f} USD] {classification.upper()}")
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
    safe_print("✅ XRP WebSocket connected – subscribing to transactions...")
    try:
        subscribe_msg = json.dumps({
            "id": "xrp_monitor",
            "command": "subscribe",
            "streams": ["transactions"]
        })
        ws.send(subscribe_msg)
        safe_print("   Subscribed to XRP transaction stream.")
    except Exception as e:
        safe_print(f"Error subscribing to XRP transactions: {e}")
        traceback.print_exc()

def on_xrp_error(ws, error):
    """Handle XRP websocket errors"""
    safe_print(f"[XRP WS Error] {error}")

def on_xrp_close(ws, close_status_code, close_msg):
    """Handle XRP websocket connection closing with exponential backoff"""
    global connection_attempts
    connection_attempts += 1

    safe_print(f"XRP WS closed (code: {close_status_code}).")

    if shutdown_flag.is_set():
        return

    max_attempts = 20
    if connection_attempts > max_attempts:
        safe_print(f"XRP WS: max reconnect attempts ({max_attempts}) reached. Giving up.")
        return

    wait_time = min(120, 5 * (2 ** min(connection_attempts, 5)))
    safe_print(f"XRP WS reconnecting in {wait_time}s (attempt {connection_attempts})...")
    time.sleep(wait_time)
    try:
        connect_xrp_websocket()
    except Exception as e:
        safe_print(f"Error reconnecting to XRP: {e}")
        traceback.print_exc()

def connect_xrp_websocket():
    """Create and configure XRP websocket connection"""
    try:
        xrp_servers = [
            "wss://s1.ripple.com/",
            "wss://s2.ripple.com/",
            "wss://xrplcluster.com/"
        ]

        server = xrp_servers[0]

        websocket.enableTrace(False)
        ws_app = websocket.WebSocketApp(
            server,
            on_open=on_xrp_open,
            on_message=on_xrp_message,
            on_error=on_xrp_error,
            on_close=on_xrp_close
        )

        wst = threading.Thread(
            target=lambda: ws_app.run_forever(
                ping_interval=30,
                ping_timeout=10,
                ping_payload="ping",
                sslopt={"cert_reqs": 0}
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
            thread.name = "XRP"
            return thread
        else:
            safe_print("⚠️ Failed to start XRP monitoring.")
            return None
    except Exception as e:
        safe_print(f"Error starting XRP thread: {e}")
        traceback.print_exc()
        return None

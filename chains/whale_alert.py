import json
import logging
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
from utils.classification_final import transaction_classifier
from utils.base_helpers import safe_print
from data.tokens import TOKEN_PRICES
from utils.summary import has_been_classified, mark_as_classified, record_transfer
from utils.dedup import get_dedup_stats, deduped_transactions, handle_event


total_transfers_fetched = 0
filtered_by_threshold = 0
stablecoin_skip_count = 0


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

        # --- CLASSIFY FIRST, before handle_event ---
        # Build a transaction object for the classification engine
        primary_symbol = ""
        primary_amount = 0
        total_usd_value = 0

        # Pre-scan amounts to get primary token and total value
        non_stable_amounts = []
        for amt in amounts:
            symbol = amt.get("symbol", "").upper()
            amount = float(amt.get("amount", 0))
            usd_value = float(amt.get("value_usd", 0))
            if symbol.lower() in STABLE_COINS:
                continue
            if amount > 0 and usd_value >= GLOBAL_USD_THRESHOLD:
                non_stable_amounts.append({
                    "symbol": symbol,
                    "amount": amount,
                    "usd_value": usd_value,
                })
                total_usd_value += usd_value
                if not primary_symbol:
                    primary_symbol = symbol
                    primary_amount = amount

        if not non_stable_amounts:
            return

        # Run classification BEFORE storing events
        classification = "transfer"
        confidence = 0.0
        whale_score = 0
        reasoning = "Basic whale alert classification"

        try:
            from utils.classification_final import WhaleIntelligenceEngine
            whale_engine = WhaleIntelligenceEngine()
            enhanced_tx = {
                'hash': tx_hash,
                'from_address': tx_from,
                'to_address': tx_to,
                'blockchain': blockchain,
                'value_usd': total_usd_value,
                'symbol': primary_symbol,
                'amount': primary_amount,
                'timestamp': data.get("timestamp", time.time())
            }
            analysis_result = whale_engine.analyze_transaction_comprehensive(enhanced_tx)

            if hasattr(analysis_result, '__dict__'):
                classification = getattr(analysis_result, 'classification', 'TRANSFER')
                if hasattr(classification, 'value'):
                    classification = classification.value
                confidence = getattr(analysis_result, 'confidence', 0.0)
                whale_score = getattr(analysis_result, 'whale_score', 0)
                reasoning = getattr(analysis_result, 'reasoning', 'Enhanced whale alert classification')
            else:
                classification = analysis_result.get('classification', 'TRANSFER')
                confidence = analysis_result.get('confidence', 0.0)
                whale_score = analysis_result.get('whale_score', 0)
                reasoning = analysis_result.get('master_classifier_reasoning', 'Basic whale alert classification')

            if classification in ['BUY', 'SELL', 'TRANSFER']:
                classification = classification.lower()

        except Exception as e:
            print(f"Enhanced classification failed, using basic: {e}")
            classification, confidence = transaction_classifier(
                tx_from=tx_from,
                tx_to=tx_to,
                tx_hash=tx_hash,
                source="whale_alert"
            )
            whale_score = 0
            reasoning = "Basic whale alert classification"

        # Normalize probable classifications
        if classification.startswith("probable_"):
            classification = classification.split("_")[-1]

        # --- NOW store events with classification included ---
        valid_transfers = []
        for amt_info in non_stable_amounts:
            symbol = amt_info["symbol"]
            amount = amt_info["amount"]
            usd_value = amt_info["usd_value"]

            event = {
                "blockchain": blockchain,
                "tx_hash": tx_hash,
                "from": tx_from,
                "to": tx_to,
                "symbol": symbol,
                "amount": amount,
                "usd_value": usd_value,
                "timestamp": data.get("timestamp", time.time()),
                "source": "whale_alert",
                "classification": classification.upper(),  # Include classification!
            }

            if handle_event(event):
                valid_transfers.append({
                    "symbol": symbol,
                    "amount": amount,
                    "usd_value": usd_value,
                    "classification": classification,
                })

        if not valid_transfers:
            return

        # Update buy/sell/trending counters
        if classification in ("buy", "sell", "transfer"):
            for transfer in valid_transfers:
                symbol = transfer["symbol"]
                whale_trending_counts[symbol] += 1

                if classification == "buy":
                    whale_buy_counts[symbol] += 1
                elif classification == "sell":
                    whale_sell_counts[symbol] += 1

            # Print alert
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

            # Persist to Supabase with classification data
            try:
                from utils.supabase_writer import store_transaction
                for transfer in valid_transfers:
                    wa_event = {
                        "blockchain": blockchain,
                        "tx_hash": tx_hash,
                        "from": tx_from,
                        "to": tx_to,
                        "symbol": transfer["symbol"],
                        "usd_value": transfer["usd_value"],
                        "timestamp": data.get("timestamp", time.time()),
                        "source": "whale_alert"
                    }
                    classification_data = {
                        'classification': classification.upper(),
                        'confidence': confidence,
                        'whale_score': whale_score,
                        'reasoning': reasoning,
                    }
                    store_transaction(wa_event, classification_data)
            except Exception as e:
                print(f"  Supabase write error: {e}")

    except Exception as e:
        print(f"Error processing Whale Alert message: {e}")

def on_whale_error(ws, error):
    error_str = str(error)
    if "401" in error_str or "Unauthorized" in error_str:
        safe_print("⚠️  Whale Alert API key expired or invalid. Renew at https://whale-alert.io/")
        safe_print("   Whale Alert monitoring paused until key is renewed.")
    elif "429" in error_str:
        safe_print("Whale Alert rate limit – pausing 120 seconds before reconnect.")
        time.sleep(120)
    else:
        safe_print(f"[Whale Alert WS Error] {type(error).__name__}: {error_str[:200]}")

def on_whale_close(ws, close_status_code, close_msg):
    close_msg_str = str(close_msg) if close_msg else ""
    if close_status_code == 401 or "401" in close_msg_str or "Unauthorized" in close_msg_str:
        safe_print("⚠️  Whale Alert: 401 Unauthorized – API key expired.")
        safe_print("   → Renew your key at https://whale-alert.io/")
        safe_print("   → Whale Alert monitoring disabled until key is renewed.")
        return
    safe_print(f"Whale Alert WS closed (code: {close_status_code}).")
    wait_time = 30 if close_status_code else 120
    safe_print(f"Reconnecting in {wait_time} seconds...")
    if not shutdown_flag.is_set():
        time.sleep(wait_time)
        connect_whale_websocket()

def on_whale_open(ws):
    print("Whale Alert WS connection established.")
    subscription_request = {
        "type": "subscribe_alerts",
        "min_value_usd": 25000,
        "tx_types": ["transfer", "mint", "burn"],
        "blockchain": [
            "ethereum",
            "bitcoin",
            "solana",
            "ripple",
            "polygon",
            "algorand",
            "bitcoin cash",
            "dogecoin",
            "litecoin"
        ]
    }
    ws.send(json.dumps(subscription_request))
    print("🚀 Whale Alert subscription active")
    print(f"   → Min Value: $25K | Chains: {len(subscription_request['blockchain'])}")

def connect_whale_websocket():
    logging.getLogger("websocket").setLevel(logging.WARNING)
    ws_app = websocket.WebSocketApp(
        WHALE_WS_URL,
        on_open=on_whale_open,
        on_message=on_whale_message,
        on_error=on_whale_error,
        on_close=on_whale_close
    )
    wst = threading.Thread(
        target=lambda: ws_app.run_forever(ping_interval=60),
        daemon=True
    )
    wst.start()
    return wst

def start_whale_thread():
    """Start the Whale Alert monitoring thread with proper error handling."""
    try:
        if not WHALE_ALERT_API_KEY or len(WHALE_ALERT_API_KEY) < 10:
            safe_print("⚠️  Whale Alert API key missing or invalid. Monitoring disabled.")
            safe_print("   → Set your key in config/api_keys.py or renew at https://whale-alert.io/")
            return None

        thread = connect_whale_websocket()
        if thread:
            thread.name = "WhaleAlert"
            return thread
        else:
            safe_print("⚠️ Failed to start Whale Alert monitoring.")
            return None
    except Exception as e:
        safe_print(f"Error starting Whale Alert thread: {e}")
        traceback.print_exc()
        return None

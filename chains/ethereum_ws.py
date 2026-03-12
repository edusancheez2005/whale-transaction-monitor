"""
Ethereum real-time WebSocket monitor using Alchemy eth_subscribe.

Subscribes to ERC-20 Transfer event logs for all monitored tokens.
Receives every Transfer event in real-time - no polling gaps.
"""

import json
import time
import threading
import traceback
import websocket
import logging

from config.api_keys import ALCHEMY_ETHEREUM_WS, ALCHEMY_API_KEY
from config.settings import shutdown_flag
from data.tokens import TOP_100_ERC20_TOKENS, TOKEN_PRICES
from utils.base_helpers import safe_print, log_error
from utils.dedup import handle_event

logger = logging.getLogger(__name__)

# ERC-20 Transfer(address,address,uint256) topic
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# Threshold
ETH_USD_THRESHOLD = 20_000

# Tokens to exclude from tracking
_EXCLUDED_SYMBOLS = {'OXT'}

# Build lookup tables at module level
_addr_to_meta = {}
_all_contracts = []
for _t in TOP_100_ERC20_TOKENS:
    if _t['symbol'] in _EXCLUDED_SYMBOLS:
        continue
    _addr = _t['address'].lower()
    _addr_to_meta[_addr] = {
        'symbol': _t['symbol'],
        'decimals': _t.get('decimals', 18),
    }
    _all_contracts.append(_t['address'])

# Counters
_eth_ws_received = 0
_eth_ws_stored = 0


def _on_eth_ws_message(ws, message):
    """Handle incoming ERC-20 Transfer log from Alchemy WebSocket."""
    global _eth_ws_received, _eth_ws_stored
    try:
        data = json.loads(message)

        # Subscription confirmation
        if 'result' in data and 'params' not in data:
            safe_print(f"  Ethereum WS: subscription confirmed (id={data.get('result', '?')[:16]}...)")
            return

        # Actual log event
        params = data.get('params')
        if not params:
            return

        log = params.get('result')
        if not log:
            return

        _eth_ws_received += 1

        # Parse the Transfer event
        contract_addr = log.get('address', '').lower()
        topics = log.get('topics', [])
        log_data = log.get('data', '0x0')
        tx_hash = log.get('transactionHash', '')
        block_hex = log.get('blockNumber', '0x0')

        # Must be a Transfer event with from and to
        if len(topics) < 3 or topics[0].lower() != TRANSFER_TOPIC:
            return

        # Look up token
        meta = _addr_to_meta.get(contract_addr)
        if not meta:
            return

        symbol = meta['symbol']
        decimals = meta['decimals']
        price = TOKEN_PRICES.get(symbol, 0)
        if price == 0:
            return

        # Parse from/to from topics (remove 0x padding to get 20-byte address)
        from_addr = '0x' + topics[1][-40:]
        to_addr = '0x' + topics[2][-40:]

        # Parse amount from data (first 32 bytes / 64 hex chars only)
        try:
            # Standard ERC-20 Transfer data is exactly 32 bytes (uint256 amount)
            # Some contracts append extra data — only take the first 64 hex chars
            clean_data = log_data[2:] if log_data.startswith('0x') else log_data
            amount_hex = clean_data[:64]  # First 32 bytes only
            raw_amount = int(amount_hex, 16) if amount_hex else 0
        except (ValueError, TypeError):
            return
        token_amount = raw_amount / (10 ** decimals)
        usd_value = token_amount * price

        # Sanity check: reject impossibly large values (> $10B for a single transfer)
        if usd_value > 10_000_000_000:
            return

        # Apply threshold
        if usd_value < ETH_USD_THRESHOLD:
            return

        # Build event
        event = {
            'blockchain': 'ethereum',
            'tx_hash': tx_hash,
            'from': from_addr,
            'to': to_addr,
            'symbol': symbol,
            'amount': token_amount,
            'estimated_usd': usd_value,
            'usd_value': usd_value,
            'timestamp': int(time.time()),
            'source': 'ethereum_ws',
            'block_number': int(block_hex, 16) if block_hex.startswith('0x') else 0,
            'log_index': int(log.get('logIndex', '0x0'), 16) if log.get('logIndex', '').startswith('0x') else 0,
        }

        # Classify using known addresses
        try:
            from utils.classification_final import process_and_enrich_transaction
            enriched = process_and_enrich_transaction(event)
            if enriched and isinstance(enriched, dict):
                event['classification'] = enriched.get('classification', 'TRANSFER').upper()
            else:
                event['classification'] = 'TRANSFER'
        except Exception:
            event['classification'] = 'TRANSFER'

        # Route through dedup -> Supabase
        if handle_event(event):
            _eth_ws_stored += 1

    except Exception as e:
        if not shutdown_flag.is_set():
            logger.warning(f"Ethereum WS message error: {e}")


def _on_eth_ws_open(ws):
    """Subscribe to ERC-20 Transfer logs for all monitored tokens."""
    safe_print("Ethereum WebSocket connected - subscribing to ERC-20 transfers...")

    # Alchemy supports up to ~10K addresses in a single eth_subscribe logs filter.
    # We have ~100 tokens, well within limits.
    subscribe_msg = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_subscribe",
        "params": [
            "logs",
            {
                "address": _all_contracts,
                "topics": [TRANSFER_TOPIC]
            }
        ]
    }
    ws.send(json.dumps(subscribe_msg))
    safe_print(f"  Subscribed to Transfer events for {len(_all_contracts)} ERC-20 tokens")


def _on_eth_ws_error(ws, error):
    error_str = str(error)[:200]
    if "429" in error_str:
        safe_print(f"Ethereum WS rate limited, will reconnect...")
    else:
        safe_print(f"Ethereum WS error: {error_str}")
    log_error(f"Ethereum WS: {error_str}")


def _on_eth_ws_close(ws, close_status_code, close_msg):
    if shutdown_flag.is_set():
        return
    wait = min(30, 5)
    safe_print(f"Ethereum WS closed (code: {close_status_code}). Reconnecting in {wait}s...")
    time.sleep(wait)
    connect_ethereum_websocket()


def connect_ethereum_websocket(retry_count=0, max_retries=10):
    """Connect to Alchemy Ethereum WebSocket for real-time ERC-20 Transfer events."""
    ws_url = ALCHEMY_ETHEREUM_WS
    if not ws_url:
        safe_print("Ethereum WS: ALCHEMY_ETHEREUM_WS not configured")
        return None

    ws_app = websocket.WebSocketApp(
        ws_url,
        on_open=_on_eth_ws_open,
        on_message=_on_eth_ws_message,
        on_error=_on_eth_ws_error,
        on_close=_on_eth_ws_close,
    )

    ws_thread = threading.Thread(
        target=ws_app.run_forever,
        kwargs={"ping_interval": 30, "ping_timeout": 10},
        daemon=True,
    )
    ws_thread.start()
    return ws_thread


def start_ethereum_ws_thread():
    """Start the Ethereum WebSocket monitor thread."""
    try:
        thread = connect_ethereum_websocket()
        if thread:
            thread.name = "Ethereum-WS"
            return thread
        else:
            safe_print("Failed to start Ethereum WebSocket monitor")
            return None
    except Exception as e:
        safe_print(f"Error starting Ethereum WS: {e}")
        traceback.print_exc()
        return None


def get_eth_ws_stats():
    return {'received': _eth_ws_received, 'stored': _eth_ws_stored}

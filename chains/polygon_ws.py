"""
Polygon real-time WebSocket monitor using Alchemy eth_subscribe.

Subscribes to ERC-20 Transfer event logs for all monitored Polygon tokens.
Receives every Transfer event in real-time on Polygon PoS.
Native MATIC transfers still use HTTPS polling (eth_subscribe logs doesn't cover them).
"""

import json
import time
import threading
import traceback
import websocket
import logging

from config.api_keys import ALCHEMY_POLYGON_WS, ALCHEMY_API_KEY
from config.settings import shutdown_flag
from data.tokens import POLYGON_TOKENS_TO_MONITOR, TOKEN_PRICES
from utils.base_helpers import safe_print, log_error
from utils.dedup import handle_event

logger = logging.getLogger(__name__)

# ERC-20 Transfer topic
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# Threshold
POLYGON_USD_THRESHOLD = 5_000

# Polygon-specific DEX/CEX addresses for classification
POLYGON_CEX_ADDRESSES = {
    '0x28c6c06298d514db089934071355e5743bf21d60',  # Binance
    '0x21a31ee1afc51d94c2efccaa2092ad1028285549',  # Binance 2
    '0x1ab4973a48dc892cd9971ece8e01dcc7688f8f23',  # Binance 3
    '0xe7804c37c13166ff0b37f5ae0bb07a3aebb6e245',  # Binance 48
    '0xf977814e90da44bfa03b6295a0616a897441acec',  # Binance 8
    '0x72a53cdbbcc1b9efa39c834a540550e23463aacb',  # Crypto.com
    '0x6cc5f688a315f3dc28a7781717a9a798a59fda7b',  # OKX
    '0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43',  # Coinbase
    '0x71660c4005ba85c37ccec55d0c4493e66fe775d3',  # Coinbase 2
    '0x2910543af39aba0cd09dbb2d50200b3e800a63d2',  # Kraken
    '0xf89d7b9c864f589bbf53a82105107622b35eaa40',  # Bybit
}
POLYGON_DEX_ADDRESSES = {
    '0xa5e0829caced82f9edc736e8167366c1e5104d41',  # QuickSwap
    '0xe592427a0aece92de3edee1f18e0157c05861564',  # Uniswap V3
    '0x1b02da8cb0d097eb8d57a175b88c7d8b47997506',  # SushiSwap
    '0xdef1c0ded9bec7f1a1670819833240f027b25eff',  # 0x Exchange
    '0x1111111254fb6c44bac0bed2854e76f90643097d',  # 1inch V4
    '0x1111111254eeb25477b68fb85ed929f73a960582',  # 1inch V5
    '0xba12222222228d8ba445958a75a0704d566bf2c8',  # Balancer V2
    '0xf5b509bb0909a69b1c207e495f687a596c168e12',  # QuickSwap V3
    '0x6131b5fae19ea4f9d964eac0408e4408b66337b5',  # Kyber
    '0xdef171fe48cf0115b1d80b88dc8eab59176fee57',  # Paraswap
}

# Polygon DeFi protocols — deposits = BUY signal, withdrawals = SELL signal
POLYGON_DEFI_ADDRESSES = {
    '0x794a61358d6845594f94dc1db02a252b5b4814ad',  # Aave V3 Pool
    '0x8dff5e27ea6b7ac08ebfdf9eb090f32ee9a30fcf',  # Aave V2 Pool
    '0x1a13f4ca1d028320a707d99520abfefca3998b7f',  # Aave V2 amUSDC
    '0xa0c68c638235ee32657e8f720a23cec1bfc77c77',  # Polygon PoS Bridge
    '0x40ec5b33f54e0e8a33a975908c5ba1c14e5bbbdf',  # Polygon Bridge (Ethereum side root)
    '0x22f9dcf4647084d6c31b2765f6910cd85c178c18',  # Stargate Router
}

# Build lookup tables
_addr_to_meta = {}
_all_contracts = []
for _sym, _info in POLYGON_TOKENS_TO_MONITOR.items():
    _addr = _info['contract'].lower()
    _addr_to_meta[_addr] = {
        'symbol': _sym,
        'decimals': _info.get('decimals', 18),
    }
    _all_contracts.append(_info['contract'])

# Counters
_poly_ws_received = 0
_poly_ws_stored = 0


def _classify_polygon(from_addr, to_addr):
    """Classify a Polygon transfer using CEX/DEX/DeFi address databases."""
    fa = from_addr.lower()
    ta = to_addr.lower()
    
    from_is_cex = fa in POLYGON_CEX_ADDRESSES
    to_is_cex = ta in POLYGON_CEX_ADDRESSES
    from_is_dex = fa in POLYGON_DEX_ADDRESSES
    to_is_dex = ta in POLYGON_DEX_ADDRESSES
    from_is_defi = fa in POLYGON_DEFI_ADDRESSES
    to_is_defi = ta in POLYGON_DEFI_ADDRESSES
    
    # CEX flow (strongest signal)
    if from_is_cex and not to_is_cex:
        return 'BUY'    # Withdrawal from exchange = someone bought and is withdrawing
    if to_is_cex and not from_is_cex:
        return 'SELL'   # Deposit to exchange = someone is depositing to sell
    
    # DEX interaction (swap)
    if from_is_dex and not to_is_dex:
        return 'BUY'    # Receiving from DEX = bought via swap
    if to_is_dex and not from_is_dex:
        return 'SELL'   # Sending to DEX = selling via swap
    
    # DeFi protocol interaction
    if to_is_defi and not from_is_defi:
        return 'BUY'    # Depositing into Aave/bridge = investment/accumulation
    if from_is_defi and not to_is_defi:
        return 'SELL'   # Withdrawing from Aave/bridge = taking profit
    
    # Bridge detection by address pattern (contracts that interact with L1)
    # Large stablecoin transfers to unknown contracts are often bridge deposits
    
    return 'TRANSFER'


def _on_poly_ws_message(ws, message):
    """Handle incoming ERC-20 Transfer log from Polygon WebSocket."""
    global _poly_ws_received, _poly_ws_stored
    try:
        data = json.loads(message)

        if 'result' in data and 'params' not in data:
            safe_print(f"  Polygon WS: subscription confirmed")
            return

        params = data.get('params')
        if not params:
            return

        log = params.get('result')
        if not log:
            return

        _poly_ws_received += 1

        contract_addr = log.get('address', '').lower()
        topics = log.get('topics', [])
        log_data = log.get('data', '0x0')
        tx_hash = log.get('transactionHash', '')
        block_hex = log.get('blockNumber', '0x0')

        if len(topics) < 3 or topics[0].lower() != TRANSFER_TOPIC:
            return

        meta = _addr_to_meta.get(contract_addr)
        if not meta:
            return

        symbol = meta['symbol']
        decimals = meta['decimals']
        price = TOKEN_PRICES.get(symbol, 0)
        if price == 0:
            return

        from_addr = '0x' + topics[1][-40:]
        to_addr = '0x' + topics[2][-40:]

        try:
            clean_data = log_data[2:] if log_data.startswith('0x') else log_data
            amount_hex = clean_data[:64]  # First 32 bytes only
            raw_amount = int(amount_hex, 16) if amount_hex else 0
        except (ValueError, TypeError):
            return
        token_amount = raw_amount / (10 ** decimals)
        usd_value = token_amount * price

        if usd_value > 10_000_000_000:  # Sanity: reject > $10B
            return

        if usd_value < POLYGON_USD_THRESHOLD:
            return

        classification = _classify_polygon(from_addr, to_addr)

        event = {
            'blockchain': 'polygon',
            'tx_hash': tx_hash,
            'from': from_addr,
            'to': to_addr,
            'symbol': symbol,
            'amount': token_amount,
            'usd_value': usd_value,
            'classification': classification,
            'timestamp': int(time.time()),
            'source': 'polygon_ws',
            'block_num': int(block_hex, 16) if block_hex.startswith('0x') else 0,
            'log_index': int(log.get('logIndex', '0x0'), 16) if log.get('logIndex', '').startswith('0x') else 0,
        }

        if handle_event(event):
            _poly_ws_stored += 1
            safe_print(f"  [POLYGON WS - {symbol} | ${usd_value:,.0f}] {classification}")

    except Exception as e:
        if not shutdown_flag.is_set():
            logger.warning(f"Polygon WS message error: {e}")


def _on_poly_ws_open(ws):
    safe_print("Polygon WebSocket connected - subscribing to ERC-20 transfers...")

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
    safe_print(f"  Subscribed to Transfer events for {len(_all_contracts)} Polygon tokens")


def _on_poly_ws_error(ws, error):
    safe_print(f"Polygon WS error: {str(error)[:200]}")
    log_error(f"Polygon WS: {str(error)[:200]}")


def _on_poly_ws_close(ws, close_status_code, close_msg):
    if shutdown_flag.is_set():
        return
    safe_print(f"Polygon WS closed (code: {close_status_code}). Reconnecting in 5s...")
    time.sleep(5)
    connect_polygon_websocket()


def connect_polygon_websocket(retry_count=0, max_retries=10):
    """Connect to Alchemy Polygon WebSocket."""
    ws_url = ALCHEMY_POLYGON_WS
    if not ws_url:
        safe_print("Polygon WS: ALCHEMY_POLYGON_WS not configured")
        return None

    ws_app = websocket.WebSocketApp(
        ws_url,
        on_open=_on_poly_ws_open,
        on_message=_on_poly_ws_message,
        on_error=_on_poly_ws_error,
        on_close=_on_poly_ws_close,
    )

    ws_thread = threading.Thread(
        target=ws_app.run_forever,
        kwargs={"ping_interval": 30, "ping_timeout": 10},
        daemon=True,
    )
    ws_thread.start()
    return ws_thread


def start_polygon_ws_thread():
    """Start the Polygon WebSocket monitor thread."""
    try:
        thread = connect_polygon_websocket()
        if thread:
            thread.name = "Polygon-WS"
            return thread
        else:
            safe_print("Failed to start Polygon WebSocket monitor")
            return None
    except Exception as e:
        safe_print(f"Error starting Polygon WS: {e}")
        traceback.print_exc()
        return None


def get_poly_ws_stats():
    return {'received': _poly_ws_received, 'stored': _poly_ws_stored}

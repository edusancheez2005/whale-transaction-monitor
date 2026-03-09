# In app.py

from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO
import time
import threading
import json

# Import your existing monitoring code and data structures
from chains.ethereum import print_new_erc20_transfers
from chains.whale_alert import start_whale_thread
from chains.xrp import start_xrp_thread
from chains.solana import start_solana_thread
from chains.polygon import print_new_polygon_transfers
from chains.bitcoin_alchemy import poll_bitcoin_blocks
from chains.solana_api import print_new_solana_transfers
from models.classes import initialize_prices
from utils.dedup import get_stats as get_dedup_stats, deduplicator, deduped_transactions
from config.settings import (
    GLOBAL_USD_THRESHOLD,
    etherscan_buy_counts,
    etherscan_sell_counts,
    whale_buy_counts,
    whale_sell_counts,
    solana_buy_counts,
    solana_sell_counts,
    xrp_buy_counts,
    xrp_sell_counts,
    polygon_buy_counts,
    polygon_sell_counts,
    bitcoin_buy_counts,
    bitcoin_sell_counts,
    solana_api_buy_counts,
    solana_api_sell_counts,
)

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="gevent")

# Home route - render the main template
@app.route('/')
def index():
    return render_template('index.html')

# API route to get transaction data
@app.route('/api/transactions')
def get_transactions():
    # Get query parameters for filtering
    min_value = request.args.get('min_value', type=float, default=GLOBAL_USD_THRESHOLD)
    blockchain = request.args.get('blockchain', default=None)
    symbol = request.args.get('symbol', default=None)
    tx_type = request.args.get('type', default=None)
    limit = request.args.get('limit', type=int, default=50)
    
    # Get transactions from your deduplicator
    transactions = list(deduped_transactions.values())
    
    # Filter transactions
    filtered_txs = []
    for tx in transactions:
        # Apply filters
        tx_usd_value = tx.get("usd_value", 0) or tx.get("estimated_usd", 0)
        
        if tx_usd_value < min_value:
            continue
            
        if blockchain and tx.get("blockchain", "").lower() != blockchain.lower():
            continue
            
        if symbol and tx.get("symbol", "").upper() != symbol.upper():
            continue
            
        if tx_type and tx.get("classification", "").lower() != tx_type.lower():
            continue
            
        # Add to filtered list
        filtered_txs.append(tx)
        
        # Respect the limit
        if len(filtered_txs) >= limit:
            break
    
    # Sort by timestamp (newest first)
    filtered_txs.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
    
    return jsonify(filtered_txs)

# API route to get statistics
@app.route('/api/stats')
def get_stats():
    # Collect token statistics from your global counters
    token_stats = {}
    
    # Process Ethereum transactions
    for symbol, count in etherscan_buy_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['buys'] += count
        
    for symbol, count in etherscan_sell_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['sells'] += count
    
    # Process Whale Alert transactions
    for symbol, count in whale_buy_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['buys'] += count
        
    for symbol, count in whale_sell_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['sells'] += count
    
    # Process Solana transactions
    for symbol, count in solana_buy_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['buys'] += count
        
    for symbol, count in solana_sell_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['sells'] += count
    
    # Process XRP transactions
    if 'XRP' not in token_stats:
        token_stats['XRP'] = {'buys': 0, 'sells': 0}
    token_stats['XRP']['buys'] += xrp_buy_counts
    token_stats['XRP']['sells'] += xrp_sell_counts

    # Process Polygon transactions
    for symbol, count in polygon_buy_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['buys'] += count
    for symbol, count in polygon_sell_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['sells'] += count

    # Process Bitcoin transactions
    for symbol, count in bitcoin_buy_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['buys'] += count
    for symbol, count in bitcoin_sell_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['sells'] += count


    # Process Solana API transactions
    for symbol, count in solana_api_buy_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['buys'] += count
    for symbol, count in solana_api_sell_counts.items():
        if symbol not in token_stats:
            token_stats[symbol] = {'buys': 0, 'sells': 0}
        token_stats[symbol]['sells'] += count

    # Calculate additional statistics
    stats_list = []
    for symbol, stats in token_stats.items():
        total = stats['buys'] + stats['sells']
        if total > 0:
            buy_percentage = (stats['buys'] / total) * 100
        else:
            buy_percentage = 0
            
        stats_list.append({
            'symbol': symbol,
            'buys': stats['buys'],
            'sells': stats['sells'],
            'total': total,
            'buy_percentage': round(buy_percentage, 1),
            'trend': 'bullish' if buy_percentage > 60 else 'bearish' if buy_percentage < 40 else 'neutral'
        })
    
    # Sort by total volume
    stats_list.sort(key=lambda x: x['total'], reverse=True)
    
    # Get deduplication stats
    dedup_stats = get_dedup_stats()
    
    return jsonify({
        'tokens': stats_list,
        'deduplication': {
            'total_transactions': dedup_stats.get('total_received', 0),
            'unique_transactions': dedup_stats.get('total_transactions', 0),
            'duplicates_caught': dedup_stats.get('duplicates_caught', 0),
            'dedup_ratio': dedup_stats.get('dedup_ratio', 0)
        },
        'monitoring': {
            'active_threads': [t.name for t in threading.enumerate() if t.daemon],
            'min_transaction_value': GLOBAL_USD_THRESHOLD
        }
    })

def push_new_transaction(event):
    """Push a new transaction to all connected clients via SocketIO"""
    socketio.emit('new_transaction', event)

def start_monitors():
    """Start all transaction monitoring threads"""
    # Initialize token prices
    initialize_prices()

    # Register real-time push callback
    deduplicator.on_new_transaction = push_new_transaction

    threads = []

    # Start Ethereum monitoring (Etherscan discovery + Alchemy receipts)
    eth_thread = threading.Thread(target=print_new_erc20_transfers, daemon=True, name="Ethereum")
    eth_thread.start()
    threads.append(eth_thread)

    # Start Whale Alert monitoring
    whale_thread = start_whale_thread()
    if whale_thread:
        threads.append(whale_thread)

    # Start XRP monitoring
    xrp_thread = start_xrp_thread()
    if xrp_thread:
        threads.append(xrp_thread)

    # Start Solana WebSocket monitoring (Helius)
    solana_thread = start_solana_thread()
    if solana_thread:
        threads.append(solana_thread)

    # Start Solana API polling (Alchemy primary RPC)
    solana_api_thread = threading.Thread(target=print_new_solana_transfers, daemon=True, name="Solana-API")
    solana_api_thread.start()
    threads.append(solana_api_thread)

    # Start Polygon monitoring (PolygonScan API)
    polygon_thread = threading.Thread(target=print_new_polygon_transfers, daemon=True, name="Polygon")
    polygon_thread.start()
    threads.append(polygon_thread)

    # Start Bitcoin monitoring (mempool.space block polling)
    btc_thread = threading.Thread(target=poll_bitcoin_blocks, daemon=True, name="Bitcoin")
    btc_thread.start()
    threads.append(btc_thread)


    return threads

if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 8080))
    print(f"Starting Flask server on http://0.0.0.0:{port}")

    # Start monitoring threads
    monitor_threads = start_monitors()
    print(f"Started {len([t for t in monitor_threads if t and t.is_alive()])} monitoring threads")

    # Start the Flask app with SocketIO
    socketio.run(app, debug=False, host='0.0.0.0', port=port)
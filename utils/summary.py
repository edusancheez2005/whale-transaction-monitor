# utils/summary.py
import time
import requests
from datetime import datetime
from collections import defaultdict
from typing import List, Dict
from config.api_keys import NEWS_API_KEY
from config.settings import (
    etherscan_buy_counts, etherscan_sell_counts,
    whale_buy_counts, whale_sell_counts, whale_trending_counts,
    solana_buy_counts, solana_sell_counts,
    xrp_buy_counts, xrp_sell_counts, xrp_payment_count, xrp_total_amount
)
from chains.dune import get_transfer_volumes
from models.classes import BitQueryAPI, DuneAnalytics, DefiLlamaData
from config.settings import DUNE_QUERIES
# To:
from utils.base_helpers import safe_print
from utils.dedup import (
    get_dedup_stats, 
    get_transactions, 
    handle_event, 
    deduplicator  # Import the deduplicator instance instead
)

# Then anywhere you were using deduped_transactions, use:
deduped_transactions = deduplicator.transactions

classified_transaction_ids = defaultdict(set)
processed_transactions_by_source = defaultdict(set)
recorded_transactions = defaultdict(set)
transfer_volumes = defaultdict(float)

def has_been_classified(token: str, tx_id: str) -> bool:
    """Check if a transaction has already been classified"""
    if not token or not tx_id:
        return False
    return tx_id in classified_transaction_ids[token]

def mark_as_classified(token: str, tx_id: str, classification: str, source: str = None) -> None:
    """Mark a transaction as classified with optional source tracking"""
    if not token or not tx_id:
        return
    classified_transaction_ids[token].add(tx_id)
    if source:
        processed_transactions_by_source[source].add(tx_id)
# Step 1: Edit the summary.py file
class TransferTracker:
    def __init__(self):
        self.volume = defaultdict(float)
        self.count = defaultdict(int)
        self.addresses = defaultdict(set)
        self.transaction_hashes = defaultdict(set)
        self.recent_transactions = defaultdict(dict)  # For time-based deduplication
        self.duplicates_prevented = defaultdict(int)  # Track prevented duplicates

    def add_transfer(self, token: str, amount: float, from_addr: str, to_addr: str, tx_hash: str = None, source: str = None):
        """Record a transfer with improved deduplication"""
        if not token:
            return False
            
        current_time = time.time()
        
        # Create compound key without time
        amount_key = f"{amount:.4f}"  # Reduce precision to help match similar amounts
        key = f"{token}_{from_addr}_{to_addr}_{amount_key}"
        reverse_key = f"{token}_{to_addr}_{from_addr}_{amount_key}"
        
        # If we've seen this exact transaction recently, skip it
        for existing_key in [key, reverse_key]:
            if existing_key in self.recent_transactions:
                prev_tx = self.recent_transactions[existing_key]
                time_diff = current_time - prev_tx['time']
                
                # Use different windows based on token type
                window = 30 if token in ['SOL', 'BTC', 'ETH'] else 10
                if time_diff <= window:
                    self.duplicates_prevented[token] += 1
                    return False
        
        # Store with both keys to catch transfers in both directions
        self.recent_transactions[key] = {
            'amount': amount,
            'time': current_time,
            'from': from_addr,
            'to': to_addr
        }
        
        # Record transaction hash if available
        if tx_hash:
            self.transaction_hashes[token].add(tx_hash)
        
        # Update metrics
        self.volume[token] += amount
        self.count[token] += 1
        self.addresses[token].add(from_addr)
        self.addresses[token].add(to_addr)
        
        return True

    def _cleanup_recent_transactions(self, token):
        """Remove old transactions from tracking"""
        current_time = time.time()
        cleanup_threshold = 30  # Keep last 30 seconds of history
        
        if token in self.recent_transactions:
            keys_to_remove = []
            for key, data in self.recent_transactions[token].items():
                if current_time - data['time'] > cleanup_threshold:
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                del self.recent_transactions[token][key]

# Global transfer tracker
transfer_tracker = TransferTracker()


def print_final_aggregated_summary():
    """Print final analysis with improved transaction tracking"""
    # Get deduplication stats
    dedup_stats = get_dedup_stats()
    
    # Process all unique transactions (make a safe copy)
    import copy
    
    try:
        # Create a safe shallow copy of the keys
        tx_keys = list(deduplicator.transactions.keys())
        
        # Aggregate by token
        token_stats = defaultdict(lambda: {
            'buys': 0,
            'sells': 0,
            'transfers': 0,
            'total': 0,
            'volume_usd': 0.0,
            'unique_addresses': set(),
            'hourly_data': defaultdict(lambda: {'buys': 0, 'sells': 0, 'transfers': 0})
        })

        # Process each transaction with enhanced tracking
        for tx_key in tx_keys:
            if tx_key in deduplicator.transactions:  # Check if key still exists
                tx = deduplicator.transactions[tx_key]
                
                symbol = tx.get('symbol', '')
                if not symbol:
                    continue
                    
                classification = tx.get('classification', '').lower()
                timestamp = tx.get('timestamp', 0)
                hour = time.strftime('%H', time.localtime(timestamp)) if timestamp else "00"
                
                # Update classification counting
                if classification == 'buy' or classification.startswith('probable_buy'):
                    token_stats[symbol]['buys'] += 1
                    token_stats[symbol]['total'] += 1
                    token_stats[symbol]['hourly_data'][hour]['buys'] += 1
                elif classification == 'sell' or classification.startswith('probable_sell'):
                    token_stats[symbol]['sells'] += 1
                    token_stats[symbol]['total'] += 1
                    token_stats[symbol]['hourly_data'][hour]['sells'] += 1
                else:
                    token_stats[symbol]['transfers'] += 1
                    token_stats[symbol]['total'] += 1
                    token_stats[symbol]['hourly_data'][hour]['transfers'] += 1

                # Add volume if available
                if 'usd_value' in tx:
                    volume = float(tx.get('usd_value', 0))
                    token_stats[symbol]['volume_usd'] += volume
                elif 'estimated_usd' in tx:
                    volume = float(tx.get('estimated_usd', 0))
                    token_stats[symbol]['volume_usd'] += volume
                
                # Track addresses
                if 'from' in tx:
                    token_stats[symbol]['unique_addresses'].add(tx.get('from', ''))
                if 'to' in tx:
                    token_stats[symbol]['unique_addresses'].add(tx.get('to', ''))

        # Print the enhanced report
        print("\n" + "="*100)
        print(" "*40 + "ENHANCED ANALYSIS REPORT")
        print("="*100)
        print(f"\nAnalysis Period: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Transaction Statistics
        print("\n1. TRANSACTION STATISTICS")
        print("-"*100)
        print(f"{'COIN':<10} {'BUYS':>8} {'SELLS':>8} {'TRANSFERS':>8} {'TOTAL':>8} {'BUY %':>7} {'SELL %':>7} {'TREND':>6}")
        print("-"*100)
        
        # Sort by total volume
        sorted_tokens = sorted(
            token_stats.items(),
            key=lambda x: x[1]['total'],
            reverse=True
        )

        for symbol, stats in sorted_tokens:
            total = stats['total']
            if total < 10:  # Skip tokens with very few transactions
                continue
                
            buys = stats['buys']
            sells = stats['sells']
            transfers = stats['transfers']
            buy_pct = (buys / total * 100) if total else 0
            sell_pct = (sells / total * 100) if total else 0
            
            # Determine trend (with more balanced criteria)
            if buys == 0 and sells == 0:
                trend = "→"
            elif buy_pct > sell_pct + 10:  # Higher threshold for trend
                trend = "↑"
            elif sell_pct > buy_pct + 10:
                trend = "↓"
            else:
                trend = "→"
                
            print(f"{symbol:<10} {buys:>8,d} {sells:>8,d} {transfers:>8,d} {total:>8,d} "
                  f"{buy_pct:>6.1f}% {sell_pct:>6.1f}% {trend:>6}")

        # Market Momentum Analysis
        print("\n2. MARKET MOMENTUM ANALYSIS")
        print("-"*100)
        
        # First identify tokens with sufficient volume
        high_volume_tokens = [symbol for symbol, stats in token_stats.items() 
                          if stats['total'] > 100]  # Minimum threshold
                          
        # Sort by buy/sell ratio for high volume tokens
        momentum_tokens = []
        for symbol in high_volume_tokens:
            stats = token_stats[symbol]
            buys = stats['buys']
            sells = stats['sells']
            total = buys + sells
            
            if total == 0:
                continue
                
            buy_ratio = buys / total if total else 0
            momentum_tokens.append((symbol, buy_ratio, total))
        
        # Print top bullish and bearish tokens
        momentum_tokens.sort(key=lambda x: x[1], reverse=True)  # Sort by buy ratio
        
        print("TOP BULLISH TOKENS:")
        for symbol, buy_ratio, total in momentum_tokens[:3]:
            buy_pct = buy_ratio * 100
            sell_pct = 100 - buy_pct
            print(f"  • {symbol:<8} Buy: {buy_pct:>6.1f}%  Sell: {sell_pct:>6.1f}%  "
                  f"(Volume: {total:,} transactions)")
        
        print("\nTOP BEARISH TOKENS:")
        for symbol, buy_ratio, total in momentum_tokens[-3:]:
            buy_pct = buy_ratio * 100
            sell_pct = 100 - buy_pct
            print(f"  • {symbol:<8} Buy: {buy_pct:>6.1f}%  Sell: {sell_pct:>6.1f}%  "
                  f"(Volume: {total:,} transactions)")

        # Hourly analysis for top token
        if sorted_tokens:
            top_token, top_stats = sorted_tokens[0]
            hourly = top_stats['hourly_data']
            
            if hourly:
                print(f"\n3. HOURLY ANALYSIS FOR {top_token}")
                print("-"*100)
                print(f"{'HOUR':<6} {'BUYS':>8} {'SELLS':>8} {'RATIO':>8}")
                print("-"*100)
                
                for hour in sorted(hourly.keys()):
                    h_buys = hourly[hour]['buys']
                    h_sells = hourly[hour]['sells']
                    h_ratio = (h_buys / max(1, h_buys + h_sells)) * 100
                    
                    print(f"{hour:0>2}:00  {h_buys:>8,d} {h_sells:>8,d} {h_ratio:>7.1f}%")

        # Deduplication Statistics
        print("\n4. DEDUPLICATION EFFECTIVENESS")
        print("-"*100)
        print(f"Total Transactions Processed: {dedup_stats['total_received']:,}")
        print(f"Unique Transactions: {dedup_stats['total_transactions']:,}")
        print(f"Duplicates Prevented: {dedup_stats['duplicates_caught']:,}")
        print(f"Overall Deduplication Rate: {dedup_stats['dedup_ratio']:.1f}%")
        
        print("\nBy Blockchain:")
        for chain, stats in dedup_stats['by_chain'].items():
            if stats['total'] > 0:
                dedup_rate = (stats['duplicates'] / stats['total']) * 100
                print(f"  • {chain:<10}: {stats['duplicates']:,} duplicates of {stats['total']:,} "
                      f"({dedup_rate:.1f}% dedup rate)")

        # Token Volume Analysis
        print("\n5. TOKEN VOLUME ANALYSIS")
        print("-"*100)
        print(f"{'TOKEN':<10} {'VOLUME (USD)':>15} {'UNIQUE TXS':>12} {'ADDRESSES':>12}")
        print("-"*100)
        
        for symbol, stats in sorted(token_stats.items(), key=lambda x: x[1]['volume_usd'], reverse=True):
            if stats['total'] > 0:
                print(f"{symbol:<10} ${stats['volume_usd']:>14,.2f} {stats['total']:>12,d} "
                      f"{len(stats['unique_addresses']):>12,d}")
            
        print("\n6. LATEST CRYPTO NEWS")
        print("-"*100)
        
        # Get news for top tokens by volume
        top_tokens = [symbol for symbol, _ in sorted(token_stats.items(), 
                                               key=lambda x: x[1]['volume_usd'], 
                                               reverse=True)[:3]]
        for symbol in top_tokens:
            print(f"\n📰 Latest news for {symbol}:")
            news_items = get_news_for_token(symbol)
            for item in news_items:
                print(f"  • {item}")
        
        print("\nAnalysis complete.")
        print("="*100)
    except Exception as e:
        print(f"Error in summary: {e}")
        import traceback
        traceback.print_exc()

def record_transfer(token: str, amount: float, from_addr: str, to_addr: str, tx_hash: str = None) -> bool:
    """Record a transfer with basic deduplication"""
    if not token:
        return False
        
    if tx_hash and tx_hash in recorded_transactions[token]:
        return False
        
    if tx_hash:
        recorded_transactions[token].add(tx_hash)
    transfer_volumes[token] += amount
    
    return True

def print_deduplication_stats():
    """Print statistics about deduplication effectiveness"""
    # Create summary of classified transactions
    classified_counts = {}
    for token, tx_ids in classified_transaction_ids.items():
        classified_counts[token] = len(tx_ids)
    
    print("\n6. DEDUPLICATION EFFECTIVENESS")
    print("-"*100)
    print(f"{'TOKEN':<10} {'UNIQUE TXS':>12} {'BUYS+SELLS':>12} {'TRANSFERS':>12} {'DEDUP RATIO':>12}")
    print("-"*100)
    
    for token in sorted(set(list(classified_counts.keys()) + list(transfer_tracker.count.keys()))):
        unique_txs = classified_counts.get(token, 0)
        transfers = transfer_tracker.count.get(token, 0)
        
        # Calculate buys+sells
        buys = 0
        sells = 0
        if token == "XRP":
            buys = xrp_buy_counts
            sells = xrp_sell_counts
        else:
            buys = etherscan_buy_counts.get(token, 0) + whale_buy_counts.get(token, 0) + solana_buy_counts.get(token, 0)
            sells = etherscan_sell_counts.get(token, 0) + whale_sell_counts.get(token, 0) + solana_sell_counts.get(token, 0)
        
        buys_sells = buys + sells
        dedup_ratio = (transfers / max(1, buys_sells)) * 100 if buys_sells > 0 else 100
        
        print(f"{token:<10} {unique_txs:>12,d} {buys_sells:>12,d} {transfers:>12,d} {dedup_ratio:>11.1f}%")

def print_final_xrp_summary():
    """Updated XRP summary function"""
    total = xrp_buy_counts + xrp_sell_counts
    if total > 0:
        buy_percentage = (xrp_buy_counts / total) * 100
        trend = "More Buys" if buy_percentage > 55 else "More Sells" if buy_percentage < 45 else "Neutral"
        
        print("\nXRP:")
        print(f"  • Buy Transactions: {xrp_buy_counts}")
        print(f"  • Sell Transactions: {xrp_sell_counts}")
        print(f"  • Total Transactions: {xrp_payment_count}")
        print(f"  • Buy Percentage: {buy_percentage:.2f}%")
        print(f"  • Trend: {trend}")
        print(f"  • Total Amount: {xrp_total_amount:,.2f} XRP")

def get_news_for_token(token_symbol):
    """Get news for a specific token with improved error handling"""
    try:
        # Map of tokens to their common names for better news search
        crypto_terms = {
            "WETH": "Ethereum OR Wrapped Ethereum",
            "LINK": "Chainlink",
            "UNI": "Uniswap",
            "AAVE": "Aave protocol",
            "COMP": "Compound finance",
            "SNX": "Synthetix",
            "MKR": "MakerDAO",
            "YFI": "Yearn Finance",
            "SUSHI": "SushiSwap",
            "CRV": "Curve finance",
            "BAL": "Balancer",
            "BNT": "Bancor",
            "REN": "Ren Protocol",
            "OMG": "OMG Network",
            "ZRX": "0x Protocol",
            "BAT": "Basic Attention Token",
            "GRT": "The Graph Protocol",
            "LRC": "Loopring",
            "1INCH": "1inch exchange",
            "MATIC": "Polygon OR MATIC",
            "SOL": "Solana",
            "XRP": "Ripple OR XRP",
            "BONK": "Bonk token OR Bonk Solana",
            "RAY": "Raydium",
            "ATLAS": "Star Atlas"
        }

        search_term = crypto_terms.get(token_symbol.upper(), token_symbol)
        query = f'"{search_term}" AND (crypto OR cryptocurrency OR blockchain)'
        
        url = "https://newsapi.org/v2/everything"
        params = {
            "q": query,
            "sortBy": "publishedAt",
            "pageSize": 3,
            "language": "en",
            "apiKey": NEWS_API_KEY
        }
        
        safe_print(f"\nFetching news for {token_symbol}...")
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()  # Will raise an exception for error status codes
        
        data = response.json()
        if data.get("status") != "ok":
            return [f"API Error: {data.get('message', 'Unknown error')}"]
            
        articles = data.get("articles", [])
        if not articles:
            return [f"No recent news found for {token_symbol}"]
            
        formatted_news = []
        for article in articles:
            try:
                pub_date = datetime.strptime(
                    article["publishedAt"],
                    "%Y-%m-%dT%H:%M:%SZ"
                ).strftime("%Y-%m-%d")
                
                formatted_news.append(
                    f"[{pub_date}] {article['title']} - {article['source']['name']}"
                )
            except Exception as e:
                safe_print(f"Error formatting article: {str(e)}")
                continue
                
        return formatted_news
    except requests.exceptions.RequestException as e:
        return [f"Network error fetching news: {str(e)}"]
    except Exception as e:
        return [f"Error fetching news: {str(e)}"]




def print_filtering_statistics():
    """Print statistics about transaction filtering"""
    # Get stats from various modules
    try:
        from chains.whale_alert import stablecoin_skip_count
    except ImportError:
        stablecoin_skip_count = 0
        
    total_tx_fetched = 0
    total_tx_filtered = 0
    
    # Look through global variables to find counters
    import sys
    for module_name, module in sys.modules.items():
        if hasattr(module, 'total_transfers_fetched'):
            total_tx_fetched += getattr(module, 'total_transfers_fetched', 0)
        if hasattr(module, 'filtered_by_threshold'):
            total_tx_filtered += getattr(module, 'filtered_by_threshold', 0)
    
    print("\n5. TRANSACTION FILTERING STATISTICS")
    print("-"*100)
    print(f"Total transactions fetched: {total_tx_fetched:,}")
    print(f"Filtered by threshold: {total_tx_filtered:,}")
    print(f"Stablecoin transactions filtered: {stablecoin_skip_count:,}")
    
    # Count transactions that actually made it to statistics
    total_counted_tx = 0
    for coin_data in list(etherscan_buy_counts.items()) + list(etherscan_sell_counts.items()) + \
                     list(solana_buy_counts.items()) + list(solana_sell_counts.items()) + \
                     list(whale_buy_counts.items()) + list(whale_sell_counts.items()):
        total_counted_tx += coin_data[1]
        
    print(f"Transactions included in statistics: {total_counted_tx:,}")
    print(f"Percentage of fetched transactions included: {(total_counted_tx/max(1, total_tx_fetched))*100:.2f}%")
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

class TransferTracker:
    def __init__(self):
        self.volume = defaultdict(float)
        self.count = defaultdict(int)
        self.addresses = defaultdict(set)

    def add_transfer(self, token: str, amount: float, from_addr: str, to_addr: str):
        self.volume[token] += amount
        self.count[token] += 1
        self.addresses[token].add(from_addr)
        self.addresses[token].add(to_addr)

# Global transfer tracker
transfer_tracker = TransferTracker()


def print_final_aggregated_summary():
    """Print final analysis with improved transfer tracking"""
    # Aggregate transaction counts
    aggregated_buy = defaultdict(int)
    aggregated_sell = defaultdict(int)
    
    # Combine counts from all sources
    for source_counts in [etherscan_buy_counts, whale_buy_counts, solana_buy_counts]:
        for coin, count in source_counts.items():
            aggregated_buy[coin] += count
            
    for source_counts in [etherscan_sell_counts, whale_sell_counts, solana_sell_counts]:
        for coin, count in source_counts.items():
            aggregated_sell[coin] += count

    # Calculate statistics
    stats = {}
    for coin in set(list(aggregated_buy.keys()) + list(aggregated_sell.keys())):
        buys = aggregated_buy[coin]
        sells = aggregated_sell[coin]
        total = buys + sells
        if total > 0:
            buy_pct = (buys / total * 100) if total else 0
            sell_pct = (sells / total * 100) if total else 0
            trend = "↑" if buy_pct > 55 else ("↓" if buy_pct < 45 else "→")
            
            stats[coin] = {
                'buys': buys,
                'sells': sells,
                'total': total,
                'buy_pct': buy_pct,
                'sell_pct': sell_pct,
                'trend': trend
            }

    # Print Analysis
    print("\n" + "="*100)
    print(" "*40 + "FINAL ANALYSIS REPORT")
    print("="*100)
    print(f"\nAnalysis Period: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Transaction Statistics
    print("\n1. TRANSACTION STATISTICS")
    print("-"*100)
    print(f"{'COIN':<10} {'BUYS':>8} {'SELLS':>8} {'TOTAL':>8} {'BUY %':>8} {'SELL %':>8} {'TREND':>6}")
    print("-"*100)
    
    sorted_stats = sorted(stats.items(), key=lambda x: x[1]['total'], reverse=True)
    for coin, data in sorted_stats:
        print(f"{coin:<10} {data['buys']:>8,d} {data['sells']:>8,d} {data['total']:>8,d} "
              f"{data['buy_pct']:>7.1f}% {data['sell_pct']:>7.1f}% {data['trend']:>6}")

    # Market Momentum Analysis
    print("\n2. MARKET MOMENTUM ANALYSIS")
    print("-"*100)
    top_buy_momentum = sorted(stats.items(), key=lambda x: x[1]['buy_pct'], reverse=True)
    for coin, data in top_buy_momentum[:5]:
        print(f"  • {coin:<8} Buy: {data['buy_pct']:>6.1f}%  Sell: {data['sell_pct']:>6.1f}%  "
              f"(Volume: {data['total']} transactions)")

    # High Activity Analysis
    print("\n3. HIGH ACTIVITY ANALYSIS")
    print("-"*100)
    for coin, data in sorted_stats[:5]:
        print(f"  • {coin}: {data['total']:,} transactions")

    # Transfer Volume Analysis
    print("\n4. TRANSFER VOLUME ANALYSIS")
    print("-"*100)
    print(f"{'TOKEN':<10} {'VOLUME':>15} {'TRANSFERS':>12} {'ADDRESSES':>12}")
    print("-"*100)
    
    for token in transfer_tracker.volume:
        volume = transfer_tracker.volume[token]
        count = transfer_tracker.count[token]
        unique_addrs = len(transfer_tracker.addresses[token])
        print(f"{token:<10} {volume:>15,.2f} {count:>12,d} {unique_addrs:>12,d}")

    # XRP specific data
    if xrp_payment_count > 0:
        print("\n5. XRP ACTIVITY")
        print("-"*100)
        print(f"Total Transactions: {xrp_payment_count:,}")
        print(f"Total Volume: {xrp_total_amount:,.2f} XRP")

    print("\nAnalysis complete.")
    print("="*100)

def record_transfer(token: str, amount: float, from_addr: str, to_addr: str):
    """Record a transfer in the global tracker"""
    transfer_tracker.add_transfer(token, amount, from_addr, to_addr)


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
    crypto_terms = {
        "WETH": "Wrapped Ethereum",
        "LINK": "Chainlink",
        "UNI": "Uniswap UNI",
        "AAVE": "Aave",
        "COMP": "Compound",
        "SNX": "Synthetix",
        "MKR": "Maker",
        "YFI": "Yearn Finance",
        "SUSHI": "SushiSwap",
        "CRV": "Curve DAO",
        "BAL": "Balancer",
        "BNT": "Bancor",
        "REN": "Ren",
        "OMG": "OMG Network",
        "ZRX": "0x",
        "BAT": "Basic Attention Token",
        "GRT": "The Graph",
        "LRC": "Loopring",
        "1INCH": "1inch",
        "MATIC": "MATIC",
        "SOL": "Solana",
        "ONYX": "Onyx",
        "SRM": "Serum",
        "RAY": "Raydium"
    }

    specific_term = crypto_terms.get(token_symbol.upper(), token_symbol)
    query = f'"{specific_term}" AND (cryptocurrency OR crypto OR blockchain)'
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": query,
        "sortBy": "publishedAt",
        "pageSize": 3,
        "language": "en",
        "domains": "cointelegraph.com,coindesk.com,decrypt.co,theblock.co",
        "apiKey": NEWS_API_KEY
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        if data.get("status") == "ok":
            articles = data.get("articles", [])
            if not articles:
                return ["No recent crypto-specific news found."]
            formatted_news = []
            for article in articles:
                pub_date = datetime.strptime(article["publishedAt"], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d")
                formatted_news.append(f"[{pub_date}] {article['title']} - {article['source']['name']}")
            return formatted_news
        else:
            return [f"Error: {data.get('message', 'Unknown error')}"]
    except Exception as e:
        return [f"Error fetching news: {e}"]



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
                print(f"Error formatting article: {str(e)}")
                continue
                
        return formatted_news
    except requests.exceptions.RequestException as e:
        return [f"Network error fetching news: {str(e)}"]
    except Exception as e:
        return [f"Error fetching news: {str(e)}"]



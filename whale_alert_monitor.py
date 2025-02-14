#!/usr/bin/env python3
"""
Combined Multi-Chain Coin Monitor with Aggregated Analysis

This script:
 • Opens a Whale Alert websocket (using your API key) to receive live “whale” transactions.
 • Polls the Etherscan API for ERC‑20 token transfers (for a selected list of tokens on Ethereum).
 • Polls a Solscan API endpoint for token transfers on Solana (for selected tokens such as SOL, ONYX, SRM, RAY, etc.).
 • Opens an XRP Ledger websocket to subscribe to XRP Payment transactions.
 • Classifies Ethereum (ERC‑20) & Whale Alert transactions as “buy” or “sell” using simple heuristics.
 • Maintains counters for ERC‑20, Whale Alert, Solana (transfers) and XRP Payment transactions.
 • At termination (via Ctrl+C), displays a final aggregated summary of all coin activity (top 20 by number of transactions)
   and fetches crypto news headlines for those coins.
 
Adjust the API keys, thresholds, endpoints, and token lists as needed.
"""

import datetime
import json
import os
import requests
import time
import threading
from collections import defaultdict
import websocket  # pip install websocket-client
from typing import Dict, Optional
import time
from datetime import datetime, timedelta
import signal
import sys
import threading

class CoinGeckoAPI:
    def __init__(self):
        self.base_url = "https://api.coingecko.com/api/v3"
        self.api_key = "CG-KQUtZkRECi63h68Sv3YrEbsS"  # Add this line
        self.price_cache = {}
        self.cache_duration = 300  # 5 minutes
        self.last_request_time = 0
        self.min_request_interval = 1.5  # Rate limit protection (seconds)

    def _rate_limit(self):
        """Ensure we don't exceed rate limits"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last_request)
        self.last_request_time = time.time()

    def get_price(self, coin_id: str) -> Optional[float]:
        """Get current price for a coin"""
        # Check cache first
        if coin_id in self.price_cache:
            cache_time, price = self.price_cache[coin_id]
            if datetime.now() - cache_time < timedelta(seconds=self.cache_duration):
                return price

        self._rate_limit()
        try:
            # Add the API key to the request headers
            headers = {
                "x-cg-api-key": self.api_key
            }
            response = requests.get(
                f"{self.base_url}/simple/price",
                params={
                    "ids": coin_id,
                    "vs_currencies": "usd",
                    "include_24hr_change": "true"
                },
                headers=headers  # Add this line
            )
            if response.status_code == 200:
                data = response.json()
                if coin_id in data and "usd" in data[coin_id]:
                    price = data[coin_id]["usd"]
                    self.price_cache[coin_id] = (datetime.now(), price)
                    return price
            return None
        except Exception as e:
            print(f"Error fetching price for {coin_id}: {e}")
            return None

    # Do the same for other methods...
    def get_trending_coins(self) -> list:
        """Get trending coins in the last 24 hours"""
        self._rate_limit()
        try:
            headers = {
                "x-cg-api-key": self.api_key
            }
            response = requests.get(
                f"{self.base_url}/search/trending",
                headers=headers
            )
            if response.status_code == 200:
                return response.json()["coins"]
            return []
        except Exception as e:
            print(f"Error fetching trending coins: {e}")
            return []

    def get_coin_info(self, coin_id: str) -> Dict:
        """Get detailed information about a coin"""
        self._rate_limit()
        try:
            headers = {
                "x-cg-api-key": self.api_key
            }
            response = requests.get(
                f"{self.base_url}/coins/{coin_id}",
                params={
                    "localization": "false",
                    "tickers": "false",
                    "community_data": "true",
                    "developer_data": "false"
                },
                headers=headers
            )
            if response.status_code == 200:
                return response.json()
            return {}
        except Exception as e:
            print(f"Error fetching coin info for {coin_id}: {e}")
            return {}

def initialize_prices():
    """Initialize token prices from CoinGecko"""
    global coingecko_api
    updated_prices = {}
    
    # Mapping of your token symbols to CoinGecko IDs
    token_map = {
        "WETH": "weth",
        "LINK": "chainlink",
        "UNI": "uniswap",
        "AAVE": "aave",
        "COMP": "compound-governance-token",
        "SNX": "havven",
        "MKR": "maker",
        "YFI": "yearn-finance",
        "SUSHI": "sushi",
        "CRV": "curve-dao-token",
        "BAL": "balancer",
        "BNT": "bancor",
        "REN": "republic-protocol",
        "OMG": "omisego",
        "ZRX": "0x",
        "BAT": "basic-attention-token",
        "GRT": "the-graph",
        "LRC": "loopring",
        "1INCH": "1inch",
        "MATIC": "matic-network",
        "SOL": "solana",
        "BONK": "bonk",
        "RAY": "raydium",
        "SAMO": "samoyedcoin",
        "DUST": "dust-protocol",
        "ORCA": "orca",
        "MSOL": "msol",
        "SRM": "serum",
        "MNGO": "mango-markets",
        "ATLAS": "star-atlas"
    }
    
    print("Initializing token prices from CoinGecko...")
    for symbol, coin_id in token_map.items():
        try:
            price = coingecko_api.get_price(coin_id)
            if price:
                updated_prices[symbol] = price
                print(f"Retrieved {symbol} price: ${price}")
            else:
                # Fallback to existing price if available
                if symbol in TOKEN_PRICES:
                    updated_prices[symbol] = TOKEN_PRICES[symbol]
                    print(f"Using fallback price for {symbol}: ${TOKEN_PRICES[symbol]}")
        except Exception as e:
            print(f"Error getting price for {symbol}: {e}")
            if symbol in TOKEN_PRICES:
                updated_prices[symbol] = TOKEN_PRICES[symbol]
    
    return updated_prices

# Update prices periodically
def update_token_prices():
    global TOKEN_PRICES
    while not shutdown_flag.is_set():  # Add this condition
        TOKEN_PRICES = initialize_prices()
        print("\nToken prices updated from CoinGecko")
        time.sleep(300)

# =======================
# GLOBAL API KEYS & URLs
# =======================
WHALE_ALERT_API_KEY = "q3bE41zFxjtAVHPgMUSmHMhFrWQ8LSdK"
WHALE_WS_URL = f"wss://leviathan.whale-alert.io/ws?api_key={WHALE_ALERT_API_KEY}"
ETHERSCAN_API_KEY = "QY23IJ4D4EJTGFQNSNJHAD4G1IUEQYUJTN"
NEWS_API_KEY = "b7c1fdbffb8842f18a495bf8d32df7cf"
coingecko_api = CoinGeckoAPI()
SOLANA_RPC_URL = "https://api.mainnet-beta.solana.com"
SOLANA_COMMITMENT = "confirmed"
HELIUS_API_KEY = "10558bd5-fc3e-4ebd-bea9-ce70f2b5f26b"  # Replace with your actual key
HELIUS_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
solana_previous_balances = {}
solana_buy_counts = defaultdict(int)
solana_sell_counts = defaultdict(int)
print_lock = threading.Lock()
shutdown_flag = threading.Event()
active_threads = []

# =======================
# GLOBAL VARIABLES & COUNTERS
# =======================

# --- ERC‑20 tokens to monitor on Ethereum ---
TOKENS_TO_MONITOR = {
    "WETH": {"contract": "0xC02aaa39b223FE8D0A0e5C4F27ead9083C756Cc2", "decimals": 18, "min_threshold": 50_000},
    "LINK": {"contract": "0x514910771af9ca656af840dff83e8264ecf986ca", "decimals": 18, "min_threshold": 50_000},
    "UNI":  {"contract": "0x1F9840a85d5aF5bf1D1762F925BDADdC4201F984", "decimals": 18, "min_threshold": 50_000},
    "AAVE": {"contract": "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9", "decimals": 18, "min_threshold": 50_000},
    "COMP": {"contract": "0xc00e94Cb662C3520282E6f5717214004A7f26888", "decimals": 18, "min_threshold": 50_000},
    "SNX":  {"contract": "0xC011a73ee8576Fb46F5E1c5751cA3B9Fe0Af2a6F", "decimals": 18, "min_threshold": 50_000},
    "MKR":  {"contract": "0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2", "decimals": 18, "min_threshold": 50_000},
    "YFI":  {"contract": "0x0bc529c00C6401aEF6D220BE8C6Ea1667F6Ad93e", "decimals": 18, "min_threshold": 50_000},
    "SUSHI": {"contract": "0x6B3595068778DD592e39A122f4f5a5cF09C90fE2", "decimals": 18, "min_threshold": 50_000},
    "CRV":  {"contract": "0xD533a949740bb3306d119CC777fa900bA034cd52", "decimals": 18, "min_threshold": 50_000},
    "BAL":  {"contract": "0xba100000625a3754423978a60c9317c58a424e3D", "decimals": 18, "min_threshold": 50_000},
    "BNT":  {"contract": "0x1F573D6Fb3F13d689FF844B4cE37794d79A7FF1C", "decimals": 18, "min_threshold": 50_000},
    "REN":  {"contract": "0x408e41876cCCDC0F92210600ef50372656052a38", "decimals": 18, "min_threshold": 50_000},
    "OMG":  {"contract": "0xd26114cd6EE289AccF82350c8d8487fedB8A0C07", "decimals": 18, "min_threshold": 50_000},
    "ZRX":  {"contract": "0xE41d2489571d322189246DaFA5ebDe1F4699F498", "decimals": 18, "min_threshold": 50_000},
    "BAT":  {"contract": "0x0D8775F648430679A709E98d2b0Cb6250d2887EF", "decimals": 18, "min_threshold": 50_000},
    "GRT":  {"contract": "0xC944E90C64B2c07662A292be6244BDf05Cda44a7", "decimals": 18, "min_threshold": 50_000},
    "LRC":  {"contract": "0xBBbbCA6A901c926F240b89EacB641d8Aec7AEafD", "decimals": 18, "min_threshold": 50_000},
    "1INCH": {"contract": "0x111111111117dC0aa78b770fA6A738034120C302", "decimals": 18, "min_threshold": 50_000},
    "MATIC": {"contract": "0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0", "decimals": 18, "min_threshold": 50_000}
}

# --- TOKEN PRICES (USD) for ERC‑20 tokens ---
TOKEN_PRICES = {
    "WETH": 1600,
    "LINK": 7,
    "UNI": 5,
    "AAVE": 75,
    "COMP": 60,
    "SNX": 2.5,
    "MKR": 1500,
    "YFI": 25000,
    "SUSHI": 2.5,
    "CRV": 2.5,
    "BAL": 10,
    "BNT": 2,
    "REN": 0.3,
    "OMG": 1.5,
    "ZRX": 0.4,
    "BAT": 0.6,
    "GRT": 0.5,
    "LRC": 0.3,
    "1INCH": 2.5,
    "MATIC": 1
}

SOL_TOKENS_TO_MONITOR = {
    "SOL": {"mint": "So11111111111111111111111111111111111111112", "decimals": 9, "min_threshold": 15_000_000},
    "BONK": {"mint": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263", "decimals": 5, "min_threshold": 500},
    "RAY": {"mint": "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R", "decimals": 6, "min_threshold": 1000},
    "SAMO": {"mint": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU", "decimals": 9, "min_threshold": 500},
    "DUST": {"mint": "DUSTawucrTsGU8hcqRdHDCbuYhCPADMLM2VcCb8VnFnQ", "decimals": 9, "min_threshold": 200},
    "ORCA": {"mint": "orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE", "decimals": 6, "min_threshold": 1000},
    "MSOL": {"mint": "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So", "decimals": 9, "min_threshold": 1000},
    "SRM": {"mint": "SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt", "decimals": 6, "min_threshold": 500},
    "MNGO": {"mint": "MangoCzJ36AjZyKwVj3VnYU4GTonjfVEnJmvvWaxLac", "decimals": 6, "min_threshold": 300},
    "ATLAS": {"mint": "ATLASXmbPQxBUYbxPsV97usA3fPQYEqzQBUHgiFCUsXx", "decimals": 8, "min_threshold": 200}
}

# Update TOKEN_PRICES with current approximate values
TOKEN_PRICES.update({
    "SOL": 150,
    "BONK": 0.00001,
    "RAY": 0.35,
    "SAMO": 0.015,
    "DUST": 0.5,
    "ORCA": 0.45,
    "MSOL": 155,  # Slightly higher than SOL due to staking
    "SRM": 0.1,
    "MNGO": 0.02,
    "ATLAS": 0.01
})

# XRP is tracked via its websocket

GLOBAL_USD_THRESHOLD = 100_000

# ----------------------
# GLOBAL COUNTERS
# ----------------------
etherscan_buy_counts = defaultdict(int)
etherscan_sell_counts = defaultdict(int)
whale_buy_counts = defaultdict(int)
whale_sell_counts = defaultdict(int)
whale_trending_counts = defaultdict(int)
solana_transfer_counts = defaultdict(int)
xrp_payment_count = 0
xrp_total_amount = 0.0
last_processed_block = {symbol: 0 for symbol in TOKENS_TO_MONITOR}

# ----------------------
# Known exchange addresses (for Ethereum  and solanaclassification)
# ----------------------
known_exchange_addresses = {
    "0x28c6c06298d514db089934071355e5743bf21d60": "binance",
    "0xdfd5293d8e347dfe59e90efd55b2956a1343963d": "binance",
    "0x5a52e96bacdabb82fd05763e25335261b270efcb": "binance",
    "0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be": "binance",
    "0x71660c4005ba85c37ccec55d0c4493e66fe775d3": "coinbase",
    "0x503828976d22510aad0201ac7ec88293211d23da": "coinbase",
    "0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740": "coinbase",
    "0x2faf487a4414fe77e2327f0bf4ae2a264a776ad2": "ftx",
    "0xc098b2a3aa256d2140208c3de6543aaef5cd3a94": "ftx",
    "0x2910543af39aba0cd09dbb2d50200b3e800a63d2": "kraken",
    "0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13": "kraken",
    "0xa83b11093c858c86321fbc4c20fe82cdbd58e09e": "kraken",
    "0x2b5634c42055806a59e9107ed44d43c426e58258": "kucoin",
    "0x689c56aef474df92d44a1b70850f808488f9769c": "kucoin",
    "0x0d0707963952f2fba59dd06f2b425ace40b492fe": "huobi/gate.io",
    "0x1062a747393198f70f71ec65a582423dba7e5ab3": "huobi/gate.io",
    "0x6cc5f688a315f3dc28a7781717a9a798a59fda7b": "okx",
    "0x236f9f97e0e62388479bf9e5ba4889e46b0273c3": "okx",
    "0x7a250d5630b4cf539739df2c5dacb4c659f2488d": "uniswap"
}

solana_exchange_addresses = {
    "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1": "binance",
    "FdAXT4XPsswRhaJveaB45Lz9CFbKHGpyY3rSyRFRGArj": "bybit",
    "3Z4oLTsytjW5K2sgEYiDEbykerbYm6SnAJQm5kMQoZxd": "okx",
    "HSsAV4suBdFgaybUwzWDK1rR14FNQ73BcF3kAM7rRkbF": "kraken",
    "HU23r7UoZbqTUuh3vA7emAGztFtqwTeVips789vqxxBw": "kucoin",
    "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1": "Binance",
    "FdAXT4XPsswRhaJveaB45Lz9CFbKHGpyY3rSyRFRGArj": "Bybit",
    "3Z4oLTsytjW5K2sgEYiDEbykerbYm6SnAJQm5kMQoZxd": "OKX",
    "HSsAV4suBdFgaybUwzWDK1rR14FNQ73BcF3kAM7rRkbF": "Kraken",
    "HU23r7UoZbqTUuh3vA7emAGztFtqwTeVips789vqxxBw": "KuCoin",
    "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM": "Crypto.com",
    "73tF8uN3BwVzUzwETv59CvCF4oqzNtkUxKdJuFLHqmD9": "Gate.io",
    "AFrks6SxLK3FNKpKPdpx5DsFYhQZk8VKnz9BcVQxhYaY": "Huobi"
}

# ----------------------
# Stablecoin symbols to ignore (for Whale Alert)
# ----------------------
STABLE_COINS = {"usdt", "usdc", "dai", "tusd", "busd"}

# =======================
# NEWS API FUNCTIONS
# =======================
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
                pub_date = datetime.datetime.strptime(article["publishedAt"], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d")
                formatted_news.append(f"[{pub_date}] {article['title']} - {article['source']['name']}")
            return formatted_news
        else:
            return [f"Error: {data.get('message', 'Unknown error')}"]
    except Exception as e:
        return [f"Error fetching news: {e}"]

def get_news_for_tokens(token_list):
    print("\n=== NEWS HEADLINES FOR TOP TOKENS ===")
    for token in token_list:
        headlines = get_news_for_token(token)
        print(f"\n{token}:")
        for idx, headline in enumerate(headlines, start=1):
            print(f"  {idx}. {headline}")
    print("======================================\n")

# =======================
# HELPER FUNCTIONS
# =======================

def test_etherscan_connection():
    """Test Etherscan API connection"""
    url = "https://api.etherscan.io/api"
    params = {
        "module": "stats",
        "action": "ethsupply",
        "apikey": ETHERSCAN_API_KEY
    }
    try:
        safe_print("Testing Etherscan API connection...")
        r = requests.get(url, params=params, timeout=20)
        data = r.json()
        if data.get("status") == "1":
            safe_print("✅ Etherscan API connection successful")
            return True
        else:
            safe_print(f"❌ Etherscan API error: {data.get('message', 'No message')}")
            return False
    except Exception as e:
        safe_print(f"❌ Error connecting to Etherscan: {e}")
        return False
    
def signal_handler(sig, frame):
    """Handle keyboard interrupt"""
    print("\nReceived shutdown signal...")
    shutdown_flag.set()  # Set the shutdown flag
    
    # Give websockets time to close
    time.sleep(1)
    
    # Force close all threads
    for thread in threading.enumerate():
        if thread != threading.current_thread():
            try:
                thread._stop()
            except:
                pass
    
    # Print summary before exit
    try:
        os.system('cls' if os.name == 'nt' else 'clear')
        print_final_aggregated_summary()
        print_final_xrp_summary()
        print("Shutdown complete.")
    except:
        pass
    
    # Force exit
    os._exit(0)

# Register the signal handler
signal.signal(signal.SIGINT, signal_handler)

def safe_print(*args, **kwargs):
    """Thread-safe print function"""
    with print_lock:
        print(*args, **kwargs)

def clean_shutdown():
    try:
        # Stop all websocket connections
        for thread in threading.enumerate():
            if thread != threading.current_thread():
                if hasattr(thread, '_stop'):
                    thread._stop()

        # Force clear the screen
        os.system('cls' if os.name == 'nt' else 'clear')
        
        # Give time for screen to clear
        time.sleep(1)
        
        # Now print the summary data
        print("\n" + "=" * 80)
        print(" " * 30 + "FINAL ANALYSIS REPORT")
        print("=" * 80)
        
        # Combine and sort all transaction data
        aggregated_buy = defaultdict(int)
        aggregated_sell = defaultdict(int)
        
        for coin, count in etherscan_buy_counts.items():
            if count > 0:  # Only include non-zero counts
                aggregated_buy[coin] += count
        for coin, count in etherscan_sell_counts.items():
            if count > 0:
                aggregated_sell[coin] += count
        for coin, count in whale_buy_counts.items():
            if count > 0:
                aggregated_buy[coin] += count
        for coin, count in whale_sell_counts.items():
            if count > 0:
                aggregated_sell[coin] += count
        for coin, count in solana_buy_counts.items():
            if count > 0:
                aggregated_buy[coin] += count
        for coin, count in solana_sell_counts.items():
            if count > 0:
                aggregated_sell[coin] += count

        # Calculate summaries
        summaries = []
        for coin in set(list(aggregated_buy.keys()) + list(aggregated_sell.keys())):
            buys = aggregated_buy[coin]
            sells = aggregated_sell[coin]
            total = buys + sells
            if total > 0:  # Only include coins with activity
                buy_pct = (buys / total * 100) if total else 0
                summaries.append({
                    'coin': coin,
                    'buys': buys,
                    'sells': sells,
                    'total': total,
                    'buy_pct': buy_pct,
                    'trend': "↑" if buy_pct > 55 else "↓" if buy_pct < 45 else "→"
                })

        # Sort by total volume
        summaries.sort(key=lambda x: x['total'], reverse=True)

        # Print formatted table
        print("\n{:<8} {:>10} {:>10} {:>10} {:>10} {:>8}".format(
            "COIN", "BUYS", "SELLS", "TOTAL", "BUY %", "TREND"))
        print("-" * 60)

        for summary in summaries:
            print("{:<8} {:>10,d} {:>10,d} {:>10,d} {:>9.1f}% {:>8}".format(
                summary['coin'],
                summary['buys'],
                summary['sells'],
                summary['total'],
                summary['buy_pct'],
                summary['trend']
            ))

        if xrp_payment_count > 0:
            print("\nXRP Activity:")
            print(f"Transactions: {xrp_payment_count:,}")
            print(f"Total Volume: {xrp_total_amount:,.2f} XRP")

        print("\nSession ended:", time.strftime('%Y-%m-%d %H:%M:%S'))
        print("=" * 80)
        
        # Force exit
        os._exit(0)
        
    except Exception as e:
        print(f"Error during shutdown: {e}")
        os._exit(1)

def compute_buy_percentage(buys, sells):
    total = buys + sells
    return buys / total if total else 0

def classify_whale_transaction(from_addr, to_addr):
    from_lower = from_addr.lower() if from_addr else ""
    to_lower = to_addr.lower() if to_addr else ""
    from_is_exchange = any(exch in from_lower for exch in {"binance", "coinbase", "kraken", "huobi", "okex", "bitfinex", "bittrex", "poloniex", "uniswap"})
    to_is_exchange = any(exch in to_lower for exch in {"binance", "coinbase", "kraken", "huobi", "okex", "bitfinex", "bittrex", "poloniex", "uniswap"})
    if from_is_exchange and not to_is_exchange:
        return "buy"
    elif to_is_exchange and not from_is_exchange:
        return "sell"
    else:
        return "unknown"

def classify_buy_sell(from_addr, to_addr):
    from_lower = from_addr.lower()
    to_lower = to_addr.lower()
    if from_lower in known_exchange_addresses and to_lower not in known_exchange_addresses:
        return "buy"
    elif to_lower in known_exchange_addresses and from_lower not in known_exchange_addresses:
        return "sell"
    if "exchange" in from_lower:
        return "sell"
    elif "exchange" in to_lower:
        return "buy"
    return "unknown"

def fetch_erc20_transfers(contract_address, sort="desc"):
    url = "https://api.etherscan.io/api"
    params = {
        "module": "account",
        "action": "tokentx",
        "contractaddress": contract_address,
        "startblock": 0,
        "endblock": 99999999,
        "sort": sort,
        "apikey": ETHERSCAN_API_KEY
    }
    try:
        safe_print(f"\n📡 Fetching ERC-20 transfers for contract: {contract_address}")
        safe_print(f"Full URL: {url}?{'&'.join([f'{k}={v}' for k,v in params.items()])}")
        
        r = requests.get(url, params=params, timeout=20)
        data = r.json()
        
        if data.get("status") == "1":
            transfers = data.get("result", [])
            safe_print(f"✅ Found {len(transfers)} transfers")
            # Print a sample transaction
            if transfers:
                sample = transfers[0]
                safe_print(f"Sample transfer value: {sample.get('value', 'N/A')}")
                safe_print(f"Sample transfer block: {sample.get('blockNumber', 'N/A')}")
        else:
            msg = data.get("message", "No message")
            safe_print(f"❌ Etherscan API error: {msg}")
            # Print the full response for debugging
            safe_print(f"Full response: {data}")
        return data.get("result", [])
    except Exception as e:
        safe_print(f"❌ Error fetching transfers: {str(e)}")
        return []

# ----------------------
# ERC‑20 POLLING FUNCTIONS (ETHERSCAN)
# ----------------------
# Modify the print_new_erc20_transfers function
def print_new_erc20_transfers():
    global last_processed_block
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    safe_print(f"\n[{current_time}] 🔍 Checking ERC-20 transfers...")
    
    for symbol, info in TOKENS_TO_MONITOR.items():
        contract = info["contract"]
        decimals = info["decimals"]
        price = TOKEN_PRICES.get(symbol, 0)
        
        if price == 0:
            safe_print(f"Skipping {symbol} - no price data")
            continue
            
        transfers = fetch_erc20_transfers(contract, sort="desc")
        if not transfers:
            continue
            
        new_transfers = []
        for tx in transfers:
            block_num = int(tx["blockNumber"])
            if block_num <= last_processed_block.get(symbol, 0):
                break
            new_transfers.append(tx)
            
        if new_transfers:
            highest_block = max(int(t["blockNumber"]) for t in new_transfers)
            last_processed_block[symbol] = max(last_processed_block.get(symbol, 0), highest_block)
            
        for tx in reversed(new_transfers):
            raw_value = int(tx["value"])
            token_amount = raw_value / (10 ** decimals)
            estimated_usd = token_amount * price
            
            if estimated_usd >= GLOBAL_USD_THRESHOLD:
                tx_from = tx["from"]
                tx_to = tx["to"]
                classification = classify_buy_sell(tx_from, tx_to)
                
                # Update counters based on classification
                if classification == "buy":
                    etherscan_buy_counts[symbol] += 1
                elif classification == "sell":
                    etherscan_sell_counts[symbol] += 1
                    
                timestamp = int(tx["timeStamp"])
                human_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
                
                safe_print(f"\n[{symbol} | ${estimated_usd:,.2f} USD] Block {tx['blockNumber']} | Tx {tx['hash']}")
                safe_print(f"  Time: {human_time}")
                safe_print(f"  From: {tx_from}")
                safe_print(f"  To:   {tx_to}")
                safe_print(f"  Amount: {token_amount:,.2f} {symbol} (~${estimated_usd:,.2f} USD)")
                safe_print(f"  Classification: {classification}")
# SOLANA POLLING FUNCTIONS
# ----------------------

def classify_solana_transaction(current_owner, prev_owner=None, amount_change=0):
    """
    Classify Solana transaction as buy/sell based on ownership and amount changes
    """
    # First check if owners are known exchanges
    if current_owner in solana_exchange_addresses:
        if amount_change > 0:  # Exchange received tokens
            return "sell"
        return "buy"  # Exchange sent tokens
        
    # If previous owner was an exchange
    if prev_owner in solana_exchange_addresses:
        if amount_change > 0:  # New owner received tokens from exchange
            return "buy"
        return "sell"  # Tokens went back to exchange
    
    # Check for common exchange patterns in addresses
    current_owner_lower = current_owner.lower() if current_owner else ""
    exchanges = {"binance", "ftx", "okx", "bybit", "kraken", "kucoin", "exchange", "huobi", "gate"}
    
    if any(ex in current_owner_lower for ex in exchanges):
        return "sell" if amount_change > 0 else "buy"
        
    # If we have a significant amount change but no clear exchange involvement
    if abs(amount_change) > 0:
        return "buy" if amount_change > 0 else "sell"
        
    return "transfer"  # Default when we can't determine

def on_solana_message(ws, message):
    try:
        data = json.loads(message)
        if "params" in data:
            result = data["params"].get("result", {})
            if "value" in result:
                value = result["value"]
                if "account" in value and "data" in value["account"]:
                    parsed_data = value["account"]["data"].get("parsed", {})
                    if parsed_data.get("type") == "account":
                        info = parsed_data.get("info", {})
                        mint = info.get("mint")
                        current_amount = info.get("tokenAmount", {}).get("uiAmount", 0)
                        owner = info.get("owner")
                        
                        # Get previous amount if available
                        prev_amount = solana_previous_balances.get(owner, {}).get(mint, 0)
                        amount_change = current_amount - prev_amount
                        
                        # Check if this mint is one we're monitoring
                        for symbol, token_info in SOL_TOKENS_TO_MONITOR.items():
                            if token_info["mint"] == mint:
                                price = TOKEN_PRICES.get(symbol, 0)
                                usd_value = abs(amount_change) * price
                                min_threshold = token_info["min_threshold"]
                                
                                if usd_value >= min_threshold:
                                    classification = classify_solana_transaction(
                                        owner, 
                                        prev_owner=None,  # TODO: Track previous owners
                                        amount_change=amount_change
                                    )
                                    
                                    if classification != "transfer":
                                        if classification == "buy":
                                            solana_buy_counts[symbol] += 1
                                        elif classification == "sell":
                                            solana_sell_counts[symbol] += 1

                                            
                                        current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                                        print(f"\n[{symbol} | ${usd_value:,.2f} USD] Solana {classification.upper()}")
                                        print(f"  Time: {current_time}")
                                        print(f"  Amount: {abs(amount_change):,.2f} {symbol}")
                                        print(f"  Owner: {owner}")
                                        print(f"  Classification: {classification}")
                                
                                # Update balance tracking
                                if owner not in solana_previous_balances:
                                    solana_previous_balances[owner] = {}
                                solana_previous_balances[owner][mint] = current_amount
                                
    except Exception as e:
        if "KeyError" not in str(e):
            print(f"Error processing Solana transfer: {str(e)}")

def connect_solana_websocket(retry_count=0, max_retries=5):
    ws_url = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    
    def on_open(ws):
        print("Solana monitoring started...")
        subscribe_msg = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "programSubscribe",
            "params": [
                "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                {
                    "encoding": "jsonParsed",
                    "commitment": "confirmed"
                }
            ]
        }
        ws.send(json.dumps(subscribe_msg))

    def on_error(ws, error):
        print(f"Solana connection error: {error}")

    def on_close(ws, close_status_code, close_msg):
        nonlocal retry_count
        retry_count += 1
        
        if retry_count <= max_retries:
            wait_time = min(30, 2 ** retry_count)
            print(f"Solana connection closed. Reconnecting... ({retry_count}/{max_retries})")
            time.sleep(wait_time)
            connect_solana_websocket(retry_count, max_retries)
        else:
            print("Max Solana reconnection attempts reached.")

    ws_app = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_solana_message,
        on_error=on_error,
        on_close=on_close
    )
    ws_app.run_forever(ping_interval=60)  # Add ping_interval
    
    try:
        ws_app.run_forever()
    except Exception as e:
        print(f"Solana websocket error: {str(e)}")
        if retry_count < max_retries:
            time.sleep(5)
            connect_solana_websocket(retry_count, max_retries)

def start_solana_thread():
    solana_thread = threading.Thread(target=connect_solana_websocket, daemon=True)
    solana_thread.start()
    return solana_thread


# ----------------------
# XRP LEDGER WEBSOCKET FUNCTIONS
# ----------------------
def on_xrp_message(ws, message):
    global xrp_payment_count, xrp_total_amount
    try:
        data = json.loads(message)
        txn = data.get("transaction")
        if txn and txn.get("TransactionType") == "Payment":
            xrp_payment_count += 1
            amount = txn.get("Amount")
            if isinstance(amount, str):
                try:
                    amount_xrp = float(amount) / 1_000_000
                except Exception:
                    amount_xrp = 0
            else:
                amount_xrp = 0
            xrp_total_amount += amount_xrp
            # Removed all print statements here
    except Exception as e:
        print("Error processing XRP message:", e)

def on_xrp_error(ws, error):
    print(f"[XRP WS Error] {error}")

def on_xrp_close(ws, close_status_code, close_msg):
    print(f"XRP WS closed (code: {close_status_code}). Message: {close_msg}")
    time.sleep(10)
    connect_xrp_websocket()

def on_xrp_open(ws):
    print("XRP WS connection established.")
    subscribe_msg = json.dumps({
        "command": "subscribe",
        "streams": ["transactions"]
    })
    ws.send(subscribe_msg)
    print("Subscribed to XRP transactions.")

def connect_xrp_websocket():
    ws_app = websocket.WebSocketApp(
        "wss://s1.ripple.com/",
        on_open=on_xrp_open,
        on_message=on_xrp_message,
        on_error=on_xrp_error,
        on_close=on_xrp_close
    )
    ws_app.run_forever(ping_interval=60)  # Add ping_interval

def start_xrp_thread():
    xrp_thread = threading.Thread(target=connect_xrp_websocket, daemon=True)
    xrp_thread.start()
    return xrp_thread

# ----------------------
# WHALE ALERT WEBSOCKET FUNCTIONS
# ----------------------
def on_whale_message(ws, message):
    try:
        data = json.loads(message)
        if data.get("type") != "alert":
            return
        amounts = data.get("amounts", [])
        coins_in_alert = set()
        for amt in amounts:
            symbol = amt.get("symbol", "")
            if not symbol or symbol.lower() in STABLE_COINS:
                continue
            coins_in_alert.add(symbol.lower())
        tx_from = data.get("from", "unknown")
        tx_to = data.get("to", "unknown")
        classification = classify_whale_transaction(tx_from, tx_to)
        if classification in ("buy", "sell"):
            for coin in coins_in_alert:
                whale_trending_counts[coin] += 1
                if classification == "buy":
                    whale_buy_counts[coin] += 1
                else:
                    whale_sell_counts[coin] += 1

            ts = data.get("timestamp", 0)
            human_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))
            print("\n" + "="*50)
            print("🐋 WHALE ALERT DETECTED:")
            print("="*50)
            print(f"Time: {human_time}")
            print(f"Blockchain: {data.get('blockchain', 'N/A')}")
            print(f"Transaction Type: {data.get('transaction_type', 'N/A')}")
            print(f"From: {tx_from}")
            print(f"To:   {tx_to}")
            print(f"Classification: {classification.upper()}")
            total_usd_value = 0
            print("\nAmounts Transferred:")
            for amt in amounts:
                sym = amt.get("symbol", "N/A")
                if sym.lower() in STABLE_COINS:
                    continue
                val = amt.get("amount", 0)
                usd = amt.get("value_usd", 0)
                total_usd_value += usd
                print(f"  • {sym}: {val:,.2f} (~${usd:,.2f} USD)")
            print(f"\nTotal USD Value: ${total_usd_value:,.2f}")
            if data.get("transaction", {}).get("hash"):
                print(f"Tx Hash: {data['transaction']['hash']}")
            print("="*50 + "\n")
    except Exception as e:
        print("Error processing Whale Alert message:", e)

def on_whale_error(ws, error):
    print(f"\n[Whale Alert WS Error] {error}")
    if "429" in str(error):
        print("Rate limit encountered – pausing 60 seconds before reconnect.")
        time.sleep(60)

def on_whale_close(ws, close_status_code, close_msg):
    print(f"Whale Alert WS closed (code: {close_status_code}). Message: {close_msg}")
    wait_time = 10 if close_status_code else 60
    print(f"Reconnecting in {wait_time} seconds...")
    time.sleep(wait_time)
    connect_whale_websocket()

def on_whale_open(ws):
    print("Whale Alert WS connection established.")
    subscription_request = {
        "type": "subscribe_alerts",
        "min_value_usd": 100000,
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
    print("Whale Alert subscription request sent:")
    print(json.dumps(subscription_request, indent=4))

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

# ----------------------
# FINAL AGGREGATED SUMMARY FUNCTION
# ----------------------
def print_final_aggregated_summary():
    aggregated_buy = defaultdict(int)
    aggregated_sell = defaultdict(int)
    
    # Add Ethereum counts
    for coin, count in etherscan_buy_counts.items():
        aggregated_buy[coin] += count
    for coin, count in etherscan_sell_counts.items():
        aggregated_sell[coin] += count
    
    # Add Whale Alert counts
    for coin, count in whale_buy_counts.items():
        aggregated_buy[coin] += count
    for coin, count in whale_sell_counts.items():
        aggregated_sell[coin] += count
    
    # Add Solana counts
    for coin, count in solana_buy_counts.items():
        aggregated_buy[coin] += count
    for coin, count in solana_sell_counts.items():
        aggregated_sell[coin] += count
    
    aggregated_total = {}
    for coin in set(list(aggregated_buy.keys()) + list(aggregated_sell.keys())):
        total = aggregated_buy[coin] + aggregated_sell[coin]
        aggregated_total[coin] = total
    
    aggregated_total["XRP"] = xrp_payment_count

    sorted_coins = sorted(aggregated_total.items(), key=lambda x: x[1], reverse=True)[:20]

    print("\n\n=== FINAL AGGREGATED COIN SUMMARY ===")
    print(f"Time period: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("\nTransaction Statistics:")
    
    for coin, total in sorted_coins:
        if coin == "XRP":
            print(f"\n{coin}:")
            print(f"  • Total Transactions: {total}")
            print(f"  • Total Amount: {xrp_total_amount:,.2f} XRP")
        else:
            buy = aggregated_buy.get(coin, 0)
            sell = aggregated_sell.get(coin, 0)
            buy_pct = compute_buy_percentage(buy, sell)
            trend = "More Buys" if buy_pct > 0.55 else ("More Sells" if buy_pct < 0.45 else "Neutral")
            
            print(f"\n{coin}:")
            print(f"  • Buy Transactions: {buy}")
            print(f"  • Sell Transactions: {sell}")
            print(f"  • Total Transactions: {total}")
            print(f"  • Buy Percentage: {buy_pct*100:.2f}%")
            print(f"  • Trend: {trend}")
    
    print("\nFetching news for top coins...")
    top_coins = [coin for coin, _ in sorted_coins if coin != "XRP"]
    get_news_for_tokens(top_coins)
    print("======================================\n")

def print_final_xrp_summary():
    print("\n\n=== FINAL XRP TRANSACTION SUMMARY ===")
    print(f"Total XRP Payment Transactions: {xrp_payment_count}")
    print(f"Total XRP Amount Transferred (Payments): {xrp_total_amount:.6f} XRP")

# ----------------------
# MAIN LOOP
# ----------------------
def main():
    safe_print("Starting Combined Multi-Chain Coin Monitor...")
    safe_print("Press Ctrl+C to exit.\n")
    if not test_etherscan_connection():
        safe_print("Failed to connect to Etherscan API. ERC-20 monitoring will be disabled.")
        return
    
    # Initialize prices from CoinGecko
    global TOKEN_PRICES
    TOKEN_PRICES = initialize_prices()
    
    # Start all monitoring threads
    threads = []
    
    # Start price update thread
    price_thread = threading.Thread(target=update_token_prices, daemon=True)
    price_thread.start()
    threads.append(price_thread)
    
    # Start monitoring threads
    threads.append(start_whale_thread())
    threads.append(start_xrp_thread())
    threads.append(start_solana_thread())
    
    # Store threads globally
    global active_threads
    active_threads = threads
    
    # Main monitoring loop
    try:
        while not shutdown_flag.is_set():
            print_new_erc20_transfers()
            time.sleep(60)
    except Exception as e:
        print(f"Error in main loop: {e}")
        signal_handler(signal.SIGINT, None)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        signal_handler(signal.SIGINT, None)
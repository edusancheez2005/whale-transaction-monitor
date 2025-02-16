import time
import requests
from datetime import datetime, timedelta
from typing import Dict, Optional, List
from config.api_keys import NEWS_API_KEY, BITQUERY_API_BASE_URL, DUNE_API_BASE_URL, DUNE_API_KEY, BITQUERY_API_KEY
from data.tokens import TOKEN_PRICES
from utils.base_helpers import safe_print

# Define query IDs for important metrics
DUNE_QUERIES = {
    # DEX Metrics
    "uniswap_v3_volume": "2516339",  # 24h volume on Uniswap V3
    "uniswap_v2_volume": "2516340",  # 24h volume on Uniswap V2
    "total_dex_volume": "2516341",   # Total DEX volume across major platforms
    
    # Lending Protocols
    "aave_v3_tvl": "2516342",        # Aave V3 Total Value Locked
    "compound_tvl": "2516343",        # Compound Total Value Locked
    "lending_rates": "2516344",       # Major lending protocol rates
    
    # Network Metrics
    "gas_analytics": "2516345",       # Gas price analytics
    "eth_burn_rate": "2516346",       # ETH burn rate post-EIP-1559
    
    # Token Metrics
    "stablecoin_flows": "2516347",    # Major stablecoin flow analysis
    "token_bridges": "2516348",       # Cross-chain bridge volumes
}

def initialize_prices():
    """Initialize token prices from CoinGecko"""
    coingecko = CoinGeckoAPI()  # Create instance here
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
    
    # Add delay between requests to respect rate limits
    for symbol, coin_id in token_map.items():
        try:
            price = coingecko.get_price(coin_id)
            if price:
                updated_prices[symbol] = price
                print(f"Retrieved {symbol} price: ${price}")
            else:
                # Fallback to existing price
                if symbol in TOKEN_PRICES:
                    updated_prices[symbol] = TOKEN_PRICES[symbol]
                    print(f"Using fallback price for {symbol}: ${TOKEN_PRICES[symbol]}")
            time.sleep(1)  # Rate limiting delay
        except Exception as e:
            print(f"Error getting price for {symbol}: {e}")
            if symbol in TOKEN_PRICES:
                updated_prices[symbol] = TOKEN_PRICES[symbol]
    
    return updated_prices


class CoinGeckoAPI:
    def __init__(self):
        self.base_url = "https://api.coingecko.com/api/v3"
        self.api_key = "CG-KQUtZkRECi63h68Sv3YrEbsS"
        self.price_cache = {}
        self.cache_duration = 300  # 5 minutes
        self.last_request_time = 0
        self.min_request_interval = 1.5  # Rate limit protection

    def _rate_limit(self):
        """Ensure we don't exceed rate limits"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last_request)
        self.last_request_time = time.time()

    def get_price(self, coin_id: str) -> Optional[float]:
        """Get current price for a coin with caching"""
        # Check cache first
        if coin_id in self.price_cache:
            cache_time, price = self.price_cache[coin_id]
            if datetime.now() - cache_time < timedelta(seconds=self.cache_duration):
                return price

        self._rate_limit()
        try:
            headers = {"x-cg-api-key": self.api_key}
            response = requests.get(
                f"{self.base_url}/simple/price",
                params={
                    "ids": coin_id,
                    "vs_currencies": "usd"
                },
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if coin_id in data and "usd" in data[coin_id]:
                    price = data[coin_id]["usd"]
                    self.price_cache[coin_id] = (datetime.now(), price)
                    return price
            return None
        except Exception as e:
            print(f"CoinGecko API error for {coin_id}: {e}")
            return None

class DefiLlamaData:
    def __init__(self):
        self.base_url = "https://api.llama.fi"
        
    def get_protocol_tvl(self, protocol_slug):
        """Get TVL data for DeFi protocols"""
        try:
            url = f"{self.base_url}/protocol/{protocol_slug}"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                return response.json()
            print(f"Error fetching TVL data: {response.status_code}")
            return None
        except Exception as e:
            print(f"Error fetching DeFiLlama data: {e}")
            return None

    def get_dex_volume(self, dex_name):
        """Get DEX trading volume data"""
        try:
            url = f"{self.base_url}/dexs/daily/{dex_name}"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                return response.json()
            print(f"Error fetching DEX volume: {response.status_code}")
            return None
        except Exception as e:
            print(f"Error fetching DEX volume: {e}")
            return None
        
    def get_protocol_summary(self):
        """Get summary of major DeFi protocols"""
        protocols = ["uniswap", "aave", "compound", "curve", "balancer"]
        summary = {}
        
        for protocol in protocols:
            try:
                data = self.get_protocol_tvl(protocol)
                if data:
                    summary[protocol] = {
                        'tvl': data.get('tvl', 0),
                        'tvlChange24h': data.get('tvlChange24h', 0),
                        'volume24h': data.get('volume24h', 0),
                        'mcap': data.get('mcap', 0)
                    }
            except Exception as e:
                print(f"Error getting {protocol} data: {e}")
                
        return summary

    def get_chain_tvl(self):
        """Get TVL data for major chains"""
        try:
            url = f"{self.base_url}/chains"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Error getting chain TVL: {e}")
            return None
        
class DuneAnalytics:
    def __init__(self):
        self.base_url = DUNE_API_BASE_URL
        self.api_key = DUNE_API_KEY
        self.headers = {
            "x-dune-api-key": self.api_key
        }

    def execute_query(self, query_id: str) -> Dict:
        """Execute a saved Dune query by ID"""
        try:
            url = f"{self.base_url}/query/{query_id}/execute"
            response = requests.post(url, headers=self.headers)
            if response.status_code == 200:
                execution_id = response.json()['execution_id']
                return self.get_query_results(execution_id)
            return None
        except Exception as e:
            print(f"Error executing Dune query: {e}")
            return None

    def get_query_results(self, execution_id: str) -> Dict:
        """Get results of an executed query"""
        try:
            url = f"{self.base_url}/execution/{execution_id}/results"
            while True:
                response = requests.get(url, headers=self.headers)
                if response.status_code == 200:
                    data = response.json()
                    if data['state'] == 'QUERY_STATE_COMPLETED':
                        return data['result']
                time.sleep(1)
        except Exception as e:
            print(f"Error getting Dune results: {e}")
            return None
    
    def get_comprehensive_metrics(self):
        """Get comprehensive metrics from all tracked queries"""
        metrics = {}
        
        for metric_name, query_id in DUNE_QUERIES.items():
            try:
                results = self.execute_query(query_id)
                if results:
                    metrics[metric_name] = results
            except Exception as e:
                print(f"Error getting {metric_name}: {e}")
                
        return metrics

    def get_network_health(self):
        """Get network health metrics"""
        try:
            gas_data = self.execute_query(DUNE_QUERIES["gas_analytics"])
            burn_data = self.execute_query(DUNE_QUERIES["eth_burn_rate"])
            
            return {
                'gas': gas_data['data'][0] if gas_data else None,
                'burn_rate': burn_data['data'][0] if burn_data else None
            }
        except Exception as e:
            print(f"Error getting network health: {e}")
            return None

class BitQueryAPI:
    def __init__(self):
        self.base_url = BITQUERY_API_BASE_URL
        self.api_key = BITQUERY_API_KEY
        self.headers = {
            "X-API-KEY": self.api_key,
            "Content-Type": "application/json"
        }

    def execute_query(self, query: str, variables: Dict = None, timeout: int = 10) -> Dict:
        """Execute a GraphQL query with timeout"""
        try:
            payload = {
                "query": query,
                "variables": variables or {}
            }
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json=payload,
                timeout=timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            safe_print("BitQuery timeout error")
            return None
        except Exception as e:
            safe_print(f"BitQuery error: {e}")
            return None

    def get_cross_chain_summary(self) -> Dict:
        """Get summary of cross-chain activity"""
        query = """
        query {
            ethereum: ethereum {
                stats: dexTrades(options: {limit: 1}) {
                    totalVolume: tradeAmount(calculate: sum)
                    uniqueAddresses: count(distinct: {field: transaction{txFrom}})
                }
            }
            bsc: bsc {
                stats: dexTrades(options: {limit: 1}) {
                    totalVolume: tradeAmount(calculate: sum)
                    uniqueAddresses: count(distinct: {field: transaction{txFrom}})
                }
            }
        }
        """
        
        try:
            results = self.execute_query(query)
            if not results or 'data' not in results:
                return {}
                
            summary = {}
            for chain in ['ethereum', 'bsc']:
                if chain in results['data']:
                    chain_data = results['data'][chain]['stats'][0]
                    summary[chain.upper()] = {
                        'volume': float(chain_data['totalVolume']),
                        'addresses': int(chain_data['uniqueAddresses'])
                    }
            return summary
        except Exception as e:
            safe_print(f"Error getting cross-chain summary: {e}")
            return {}
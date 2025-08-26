"""
MarketDataProvider - Professional market data interface for CoinGecko API

Provides real-time and historical market data for any ERC-20 token with
comprehensive caching, error handling, and rate limiting.
"""

import time
import logging
import requests
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from config.api_keys import COINGECKO_API_KEY


class MarketDataProvider:
    """
    Professional-grade market data provider for the Opportunity Engine.
    
    Provides real-time and historical market data for any ERC-20 token or crypto asset,
    with comprehensive caching, error handling, and rate limiting.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        self.session = requests.Session()
        self.base_url = "https://api.coingecko.com/api/v3"
        self.api_key = api_key or COINGECKO_API_KEY
        
        # Enhanced caching for different data types
        self.price_cache = {}
        self.market_chart_cache = {}
        self.cache_expiry = {}
        
        # Rate limiting
        self.last_request_time = 0
        self.min_request_interval = 1.2  # Slightly more conservative
        
        self.logger = logging.getLogger(f"{__name__}.MarketDataProvider")
        
        # Chain ID mapping for CoinGecko
        self.chain_id_mapping = {
            'ethereum': 'ethereum',
            'polygon': 'polygon-pos',
            'bsc': 'binance-smart-chain',
            'arbitrum': 'arbitrum-one',
            'optimism': 'optimistic-ethereum',
            'avalanche': 'avalanche',
            'fantom': 'fantom',
            'solana': 'solana'
        }
        
        # Setup session headers
        if self.api_key:
            self.session.headers.update({'x-cg-api-key': self.api_key})
        self.session.headers.update({
            'User-Agent': 'WhaleOpportunityEngine/1.0',
            'Accept': 'application/json'
        })
        
        self.logger.info("MarketDataProvider initialized with enhanced CoinGecko integration")
    
    def _rate_limit(self):
        """Ensure we don't exceed rate limits."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last_request)
        self.last_request_time = time.time()
    
    def _get_cache_key(self, method: str, **kwargs) -> str:
        """Generate a consistent cache key."""
        key_parts = [method] + [f"{k}={v}" for k, v in sorted(kwargs.items())]
        return "|".join(key_parts)
    
    def _check_cache(self, cache_key: str, cache_ttl_minutes: int = 5) -> Optional[Any]:
        """Check if data exists in cache and is still valid."""
        if cache_key in self.market_chart_cache and cache_key in self.cache_expiry:
            if datetime.utcnow() < self.cache_expiry[cache_key]:
                return self.market_chart_cache[cache_key]
        return None
    
    def _set_cache(self, cache_key: str, data: Any, cache_ttl_minutes: int = 5):
        """Set data in cache with expiry."""
        self.market_chart_cache[cache_key] = data
        self.cache_expiry[cache_key] = datetime.utcnow() + timedelta(minutes=cache_ttl_minutes)
    
    def get_market_data_for_token(self, contract_address: str, chain: str) -> Optional[Dict[str, Any]]:
        """
        Fetch granular intra-day market data for a specific token contract.
        
        This is the main workhorse method for real-time heuristics (RSI, EMA, volume velocity).
        Returns 5-minute granularity data for the last 24 hours.
        
        Args:
            contract_address: The token contract address (e.g., '0x123...abc')
            chain: The blockchain name (e.g., 'ethereum', 'polygon')
            
        Returns:
            Dict containing price and volume data with timestamps, or None if not found
        """
        cache_key = self._get_cache_key("market_data", contract=contract_address, chain=chain)
        
        # Check cache first (short TTL for real-time data)
        cached_data = self._check_cache(cache_key, cache_ttl_minutes=2)
        if cached_data:
            self.logger.debug(f"Using cached market data for {contract_address}")
            return cached_data
        
        chain_id = self.chain_id_mapping.get(chain.lower())
        if not chain_id:
            self.logger.warning(f"Unsupported chain: {chain}")
            return None
        
        self._rate_limit()
        
        try:
            endpoint = f"/coins/{chain_id}/contract/{contract_address.lower()}/market_chart"
            url = f"{self.base_url}{endpoint}"
            
            params = {
                'vs_currency': 'usd',
                'days': '1'  # 5-minute granularity for last 24 hours
            }
            
            self.logger.debug(f"Fetching market data for {contract_address} on {chain}")
            response = self.session.get(url, params=params, timeout=15)
            
            if response.status_code == 404:
                self.logger.warning(f"Token {contract_address} not found on CoinGecko for chain {chain}")
                return None
            
            response.raise_for_status()
            data = response.json()
            
            # Validate the response structure
            if not all(key in data for key in ['prices', 'market_caps', 'total_volumes']):
                self.logger.warning(f"Invalid response structure for {contract_address}")
                return None
            
            # Transform data for easier consumption
            market_data = {
                'prices': data['prices'],  # [[timestamp, price], ...]
                'volumes': data['total_volumes'],  # [[timestamp, volume], ...]
                'market_caps': data['market_caps'],  # [[timestamp, market_cap], ...]
                'contract_address': contract_address,
                'chain': chain,
                'fetched_at': datetime.utcnow().isoformat(),
                'granularity': '5min',
                'data_points': len(data['prices'])
            }
            
            # Cache the result
            self._set_cache(cache_key, market_data, cache_ttl_minutes=2)
            
            self.logger.info(f"Successfully fetched {len(data['prices'])} data points for {contract_address}")
            return market_data
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Network error fetching market data for {contract_address}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error fetching market data for {contract_address}: {e}")
            return None
    
    def get_daily_historical_for_token(self, contract_address: str, chain: str, days: int = 30) -> Optional[Dict[str, Any]]:
        """
        Fetch longer-term historical data for calculating moving averages and trends.
        
        Args:
            contract_address: The token contract address
            chain: The blockchain name
            days: Number of days of historical data (default 30)
            
        Returns:
            Dict containing daily/hourly price and volume data, or None if not found
        """
        cache_key = self._get_cache_key("historical", contract=contract_address, chain=chain, days=days)
        
        # Check cache first (longer TTL for historical data)
        cached_data = self._check_cache(cache_key, cache_ttl_minutes=30)
        if cached_data:
            self.logger.debug(f"Using cached historical data for {contract_address}")
            return cached_data
        
        chain_id = self.chain_id_mapping.get(chain.lower())
        if not chain_id:
            self.logger.warning(f"Unsupported chain: {chain}")
            return None
        
        self._rate_limit()
        
        try:
            endpoint = f"/coins/{chain_id}/contract/{contract_address.lower()}/market_chart"
            url = f"{self.base_url}{endpoint}"
            
            params = {
                'vs_currency': 'usd',
                'days': str(days)
            }
            
            self.logger.debug(f"Fetching {days}-day historical data for {contract_address} on {chain}")
            response = self.session.get(url, params=params, timeout=15)
            
            if response.status_code == 404:
                self.logger.warning(f"Token {contract_address} not found on CoinGecko for chain {chain}")
                return None
            
            response.raise_for_status()
            data = response.json()
            
            # Validate the response structure
            if not all(key in data for key in ['prices', 'market_caps', 'total_volumes']):
                self.logger.warning(f"Invalid response structure for {contract_address}")
                return None
            
            # Transform data for easier consumption
            historical_data = {
                'prices': data['prices'],
                'volumes': data['total_volumes'],
                'market_caps': data['market_caps'],
                'contract_address': contract_address,
                'chain': chain,
                'days': days,
                'fetched_at': datetime.utcnow().isoformat(),
                'granularity': 'hourly' if days <= 90 else 'daily',
                'data_points': len(data['prices'])
            }
            
            # Cache the result
            self._set_cache(cache_key, historical_data, cache_ttl_minutes=30)
            
            self.logger.info(f"Successfully fetched {days}-day historical data for {contract_address}")
            return historical_data
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Network error fetching historical data for {contract_address}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error fetching historical data for {contract_address}: {e}")
            return None
    
    def get_price(self, symbol: str) -> float:
        """
        Legacy method for backward compatibility.
        Get current USD price for a cryptocurrency symbol.
        """
        # Check cache first
        cache_key = f"price_{symbol}"
        if cache_key in self.price_cache and cache_key in self.cache_expiry:
            if datetime.utcnow() < self.cache_expiry[cache_key]:
                return self.price_cache[cache_key]
        
        try:
            # Map common symbols to CoinGecko IDs
            symbol_map = {
                'ETH': 'ethereum',
                'BTC': 'bitcoin',
                'MATIC': 'matic-network',
                'SOL': 'solana',
                'AVAX': 'avalanche-2',
                'ARB': 'arbitrum',
                'OP': 'optimism'
            }
            
            coin_id = symbol_map.get(symbol, symbol.lower())
            
            self._rate_limit()
            response = self.session.get(
                f"{self.base_url}/simple/price",
                params={'ids': coin_id, 'vs_currencies': 'usd'},
                timeout=10
            )
            response.raise_for_status()
            
            data = response.json()
            price = data.get(coin_id, {}).get('usd', 0)
            
            # Cache the price
            self.price_cache[cache_key] = price
            self.cache_expiry[cache_key] = datetime.utcnow() + timedelta(minutes=5)
            
            return price
            
        except Exception as e:
            self.logger.warning(f"Failed to fetch price for {symbol}: {e}")
            # Return fallback prices
            fallback_prices = {
                'ETH': 3000, 'BTC': 45000, 'MATIC': 0.8, 'SOL': 100,
                'AVAX': 35, 'ARB': 1.2, 'OP': 2.5
            }
            return fallback_prices.get(symbol, 1.0) 
    
    def get_token_market_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get comprehensive market data for a token by symbol.
        
        Args:
            symbol: Token symbol (e.g., 'WETH', 'USDC')
            
        Returns:
            Dict containing market data including price, volume, market cap, etc.
        """
        try:
            # Normalize symbol for CoinGecko
            normalized_symbol = symbol.lower()
            if normalized_symbol == 'weth':
                normalized_symbol = 'ethereum'
            elif normalized_symbol == 'wbtc':
                normalized_symbol = 'bitcoin'
            
            self._rate_limit()
            
            # Get comprehensive coin data
            response = self.session.get(
                f"{self.base_url}/coins/{normalized_symbol}",
                params={
                    'localization': 'false',
                    'tickers': 'false',
                    'market_data': 'true',
                    'community_data': 'false',
                    'developer_data': 'false',
                    'sparkline': 'false'
                },
                timeout=10
            )
            response.raise_for_status()
            
            data = response.json()
            market_data = data.get('market_data', {})
            
            if not market_data:
                return None
            
            # Extract relevant market data
            result = {
                'current_price': market_data.get('current_price', {}).get('usd', 0),
                'price_change_24h': market_data.get('price_change_percentage_24h', 0),
                'price_change_7d': market_data.get('price_change_percentage_7d', 0),
                'market_cap': market_data.get('market_cap', {}).get('usd', 0),
                'total_volume': market_data.get('total_volume', {}).get('usd', 0),
                'circulating_supply': market_data.get('circulating_supply', 0),
                'max_supply': market_data.get('max_supply'),
                'market_cap_rank': market_data.get('market_cap_rank'),
                'high_24h': market_data.get('high_24h', {}).get('usd', 0),
                'low_24h': market_data.get('low_24h', {}).get('usd', 0),
                'ath': market_data.get('ath', {}).get('usd', 0),
                'ath_change_percentage': market_data.get('ath_change_percentage', {}).get('usd', 0),
                'atl': market_data.get('atl', {}).get('usd', 0),
                'atl_change_percentage': market_data.get('atl_change_percentage', {}).get('usd', 0),
                'last_updated': market_data.get('last_updated'),
                'symbol': symbol.upper(),
                'name': data.get('name', symbol)
            }
            
            return result
            
        except Exception as e:
            self.logger.warning(f"Failed to fetch market data for {symbol}: {e}")
            # Return basic fallback data
            fallback_price = self.get_current_price(symbol)
            return {
                'current_price': fallback_price,
                'price_change_24h': 0,
                'price_change_7d': 0,
                'market_cap': 0,
                'total_volume': 0,
                'symbol': symbol.upper(),
                'name': symbol
            } 
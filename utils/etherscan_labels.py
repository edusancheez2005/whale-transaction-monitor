#!/usr/bin/env python3
"""
Etherscan Address Label Provider
Fetches and caches address labels from Etherscan/Polygonscan APIs
"""

import aiohttp
import asyncio
import logging
import time
from typing import Optional, Dict, Tuple
from config import api_keys

logger = logging.getLogger(__name__)

class EtherscanLabelProvider:
    """
    Fetch and cache Etherscan address labels with conservative CEX detection.
    
    Only classifies as BUY/SELL when we have HIGH CONFIDENCE (verified CEX addresses).
    """
    
    def __init__(self):
        self.cache = {}
        self.cache_ttl = 3600  # 1 hour cache
        self.rate_limit_delay = 0.2  # 200ms between calls to avoid rate limits
        self.last_call_time = 0
    
    async def get_address_label(self, address: str, chain: str = 'ethereum') -> Optional[str]:
        """
        Get address label from Etherscan API.
        Returns contract name/label or None.
        
        Example returns:
        - "Binance 8"
        - "MEV Bot: 0x123"
        - "Uniswap V3: Router"
        - None (unlabeled address)
        """
        cache_key = f"{chain}:{address.lower()}"
        
        # Check cache
        if cache_key in self.cache:
            cached_data = self.cache[cache_key]
            if time.time() - cached_data['timestamp'] < self.cache_ttl:
                return cached_data['label']
        
        try:
            # Rate limiting
            current_time = time.time()
            time_since_last = current_time - self.last_call_time
            if time_since_last < self.rate_limit_delay:
                await asyncio.sleep(self.rate_limit_delay - time_since_last)
            
            # Map chain to explorer API
            api_config = {
                'ethereum': {
                    'url': 'https://api.etherscan.io/api',
                    'key': api_keys.ETHERSCAN_API_KEY
                },
                'polygon': {
                    'url': 'https://api.polygonscan.com/api',
                    'key': api_keys.POLYGONSCAN_API_KEY
                }
            }
            
            config = api_config.get(chain)
            if not config or not config['key']:
                return None
            
            params = {
                'module': 'contract',
                'action': 'getsourcecode',
                'address': address,
                'apikey': config['key']
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(config['url'], params=params, timeout=10) as response:
                    self.last_call_time = time.time()
                    
                    if response.status == 200:
                        data = await response.json()
                        if data.get('status') == '1' and data.get('result'):
                            result = data['result'][0]
                            contract_name = result.get('ContractName', '')
                            
                            # Cache the result
                            self.cache[cache_key] = {
                                'label': contract_name,
                                'timestamp': time.time()
                            }
                            
                            if contract_name:
                                logger.info(f"Label found for {address[:10]}...: {contract_name}")
                            
                            return contract_name
                    elif response.status == 429:
                        logger.warning(f"Etherscan rate limit hit for {chain}")
        
        except Exception as e:
            logger.warning(f"Failed to get label for {address}: {e}")
        
        # Cache null result to avoid repeated API calls
        self.cache[cache_key] = {
            'label': None,
            'timestamp': time.time()
        }
        
        return None
    
    def is_known_cex(self, label: str) -> Tuple[bool, Optional[str]]:
        """
        Check if label indicates a centralized exchange.
        Returns (is_cex, exchange_name)
        
        CONSERVATIVE: Only match well-known exchanges with high confidence.
        """
        if not label:
            return False, None
        
        label_lower = label.lower()
        
        # Known CEX patterns (must be very specific to avoid false positives)
        cex_patterns = {
            'binance': 'Binance',
            'coinbase': 'Coinbase',
            'kraken': 'Kraken',
            'okx': 'OKX',
            'ok ex': 'OKX',
            'bybit': 'Bybit',
            'kucoin': 'KuCoin',
            'huobi': 'Huobi',
            'gate.io': 'Gate.io',
            'gateio': 'Gate.io',
            'bitfinex': 'Bitfinex',
            'crypto.com': 'Crypto.com',
            'gemini': 'Gemini',
            'bitstamp': 'Bitstamp',
            'bittrex': 'Bittrex'
        }
        
        for pattern, exchange_name in cex_patterns.items():
            if pattern in label_lower:
                # Extra validation: ensure it's not a DEX or other protocol
                if any(dex in label_lower for dex in ['uniswap', 'sushiswap', 'curve', 'balancer', 'dex']):
                    continue
                
                logger.info(f"✅ CEX detected: {label} → {exchange_name}")
                return True, exchange_name
        
        return False, None
    
    def classify_transfer_by_label(
        self, 
        from_label: Optional[str], 
        to_label: Optional[str]
    ) -> Tuple[Optional[str], float, str]:
        """
        CONSERVATIVE classification based on Etherscan labels.
        
        Only returns BUY/SELL if we have HIGH CONFIDENCE (verified CEX).
        Otherwise returns None to keep as TRANSFER.
        
        Returns: (classification, confidence, reason)
        """
        # Check from_label
        if from_label:
            is_cex, cex_name = self.is_known_cex(from_label)
            if is_cex:
                return (
                    'BUY',
                    0.90,
                    f'CEX withdrawal from {cex_name}'
                )
        
        # Check to_label
        if to_label:
            is_cex, cex_name = self.is_known_cex(to_label)
            if is_cex:
                return (
                    'SELL',
                    0.90,
                    f'CEX deposit to {cex_name}'
                )
        
        # No CEX detected - keep as TRANSFER (safe default)
        return None, 0.0, 'No CEX detected'
    
    def get_address_type(self, label: str) -> Optional[str]:
        """
        Categorize address type from label.
        Useful for metadata/filtering.
        
        Returns: 'cex', 'dex', 'defi', 'bridge', 'mev', 'whale', or None
        """
        if not label:
            return None
        
        label_lower = label.lower()
        
        # CEX
        if self.is_known_cex(label)[0]:
            return 'cex'
        
        # DEX
        if any(dex in label_lower for dex in ['uniswap', 'sushiswap', 'curve', 'balancer', 'pancakeswap', '1inch']):
            return 'dex'
        
        # DeFi Protocols
        if any(proto in label_lower for proto in ['aave', 'compound', 'maker', 'lido', 'yearn']):
            return 'defi'
        
        # Bridges
        if any(bridge in label_lower for bridge in ['bridge', 'portal', 'wormhole', 'multichain', 'hop']):
            return 'bridge'
        
        # MEV/Arbitrage
        if any(mev in label_lower for mev in ['mev', 'flashbots', 'sandwich', 'arbitrage']):
            return 'mev'
        
        # Known whales/foundations
        if any(whale in label_lower for whale in ['whale', 'foundation', 'treasury']):
            return 'whale'
        
        return None


# Global instance
label_provider = EtherscanLabelProvider()



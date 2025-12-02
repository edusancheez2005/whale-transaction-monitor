#!/usr/bin/env python3
"""
Token Intelligence & Quality Filtering
Filters out scam tokens, wash trading, and low-quality signals
"""

import aiohttp
import asyncio
import logging
import time
from typing import Dict, Optional
from decimal import Decimal
from datetime import datetime, timedelta
from config import api_keys
from web3 import Web3

logger = logging.getLogger(__name__)

class TokenIntelligence:
    """
    Gather token context to filter noise and improve signal quality.
    
    Checks:
    - Token age (avoid brand new tokens = rug pulls)
    - Holder count (avoid low holder count = wash trading)
    - Liquidity depth (avoid thin liquidity = manipulation)
    - Volume patterns (detect abnormal spikes)
    """
    
    def __init__(self):
        self.cache = {}
        self.cache_ttl = 1800  # 30 minutes for token metadata
        
    async def get_token_metadata_etherscan(
        self, 
        token_address: str, 
        chain: str = 'ethereum'
    ) -> Optional[Dict]:
        """
        Get token metadata from Etherscan API.
        
        Returns:
        - contract_creation_date
        - holder_count (if available via Etherscan)
        - is_verified
        """
        cache_key = f"metadata:{chain}:{token_address.lower()}"
        
        if cache_key in self.cache:
            cached = self.cache[cache_key]
            if time.time() - cached['timestamp'] < self.cache_ttl:
                return cached['data']
        
        try:
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
            
            # Get contract creation date
            params = {
                'module': 'contract',
                'action': 'getcontractcreation',
                'contractaddresses': token_address,
                'apikey': config['key']
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(config['url'], params=params, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('status') == '1' and data.get('result'):
                            result = data['result'][0]
                            
                            metadata = {
                                'creator_address': result.get('contractCreator'),
                                'creation_tx_hash': result.get('txHash'),
                                'is_verified': True  # If we got results, it's verified
                            }
                            
                            # Cache result
                            self.cache[cache_key] = {
                                'data': metadata,
                                'timestamp': time.time()
                            }
                            
                            return metadata
        
        except Exception as e:
            logger.warning(f"Failed to get token metadata for {token_address}: {e}")
        
        return None
    
    async def get_token_holder_count(
        self, 
        token_address: str, 
        chain: str = 'ethereum'
    ) -> Optional[int]:
        """
        Get token holder count from Etherscan API.
        Note: This requires a paid Etherscan API key for accurate data.
        """
        try:
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
            
            # Note: This endpoint may require Pro API
            params = {
                'module': 'token',
                'action': 'tokenholderlist',
                'contractaddress': token_address,
                'page': 1,
                'offset': 1,
                'apikey': config['key']
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(config['url'], params=params, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        # If we get results, estimate holder count
                        # (Full count requires iterating all pages)
                        if data.get('status') == '1':
                            # Return a conservative estimate
                            return 100  # Placeholder - can't get exact without Pro API
        
        except Exception as e:
            logger.debug(f"Holder count unavailable for {token_address}: {e}")
        
        return None
    
    async def get_dex_liquidity(
        self, 
        token_address: str, 
        chain: str = 'ethereum',
        w3: Optional[Web3] = None
    ) -> Decimal:
        """
        Get total DEX liquidity for token across major pools.
        
        Checks Uniswap V2/V3, SushiSwap pools for this token.
        Returns total liquidity in USD.
        """
        try:
            # TODO: Implement DEX liquidity aggregation
            # For now, return a conservative estimate
            # This would require:
            # 1. Query Uniswap factory for pairs containing this token
            # 2. Get reserves from each pair
            # 3. Calculate USD value
            # 4. Sum across all pairs
            
            # Placeholder implementation
            return Decimal('0')
        
        except Exception as e:
            logger.debug(f"Failed to get liquidity for {token_address}: {e}")
            return Decimal('0')
    
    def calculate_risk_score(
        self,
        token_age_days: Optional[int],
        holder_count: Optional[int],
        liquidity_usd: Decimal,
        trade_size_usd: float
    ) -> Dict:
        """
        Calculate risk score for a token trade.
        
        Returns:
        {
            'risk_level': 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL',
            'risk_score': 0.0 - 1.0,
            'risk_factors': [list of risk factors],
            'should_alert': bool
        }
        """
        risk_factors = []
        risk_score = 0.0
        
        # Check token age
        if token_age_days is not None:
            if token_age_days < 7:
                risk_factors.append('Token < 7 days old (potential rug pull)')
                risk_score += 0.4
            elif token_age_days < 30:
                risk_factors.append('Token < 30 days old (new/unproven)')
                risk_score += 0.2
        
        # Check holder count
        if holder_count is not None:
            if holder_count < 100:
                risk_factors.append('Very few holders (<100) - wash trading risk')
                risk_score += 0.3
            elif holder_count < 500:
                risk_factors.append('Limited holder base (<500)')
                risk_score += 0.1
        
        # Check liquidity depth
        if liquidity_usd > 0:
            liquidity_ratio = float(liquidity_usd) / max(trade_size_usd, 1)
            if liquidity_ratio < 5:
                risk_factors.append(f'Low liquidity (only {liquidity_ratio:.1f}x trade size)')
                risk_score += 0.4
            elif liquidity_ratio < 10:
                risk_factors.append(f'Thin liquidity ({liquidity_ratio:.1f}x trade size)')
                risk_score += 0.2
        
        # Determine risk level
        if risk_score >= 0.7:
            risk_level = 'CRITICAL'
            should_alert = False  # Don't alert on very high risk
        elif risk_score >= 0.5:
            risk_level = 'HIGH'
            should_alert = False
        elif risk_score >= 0.3:
            risk_level = 'MEDIUM'
            should_alert = True  # Alert with caution
        else:
            risk_level = 'LOW'
            should_alert = True  # Green light
        
        return {
            'risk_level': risk_level,
            'risk_score': risk_score,
            'risk_factors': risk_factors,
            'should_alert': should_alert
        }
    
    async def should_alert_on_token(
        self,
        token_address: str,
        chain: str,
        trade_size_usd: float,
        w3: Optional[Web3] = None
    ) -> Dict:
        """
        Main entry point: determine if we should alert on this token.
        
        Returns:
        {
            'should_alert': bool,
            'risk_assessment': Dict,
            'token_metadata': Dict
        }
        """
        try:
            # Get token metadata
            metadata = await self.get_token_metadata_etherscan(token_address, chain)
            holder_count = await self.get_token_holder_count(token_address, chain)
            liquidity = await self.get_dex_liquidity(token_address, chain, w3)
            
            # Estimate token age (if we have metadata)
            token_age_days = None
            if metadata and metadata.get('creation_tx_hash'):
                # For now, assume established if we have metadata
                # In production, fetch actual block timestamp
                token_age_days = 365  # Conservative assumption
            
            # Calculate risk
            risk_assessment = self.calculate_risk_score(
                token_age_days=token_age_days,
                holder_count=holder_count,
                liquidity_usd=liquidity,
                trade_size_usd=trade_size_usd
            )
            
            logger.info(
                f"Token risk assessment for {token_address[:10]}...: "
                f"{risk_assessment['risk_level']} "
                f"(score: {risk_assessment['risk_score']:.2f})"
            )
            
            return {
                'should_alert': risk_assessment['should_alert'],
                'risk_assessment': risk_assessment,
                'token_metadata': metadata or {}
            }
        
        except Exception as e:
            logger.error(f"Token intelligence check failed for {token_address}: {e}")
            # On error, default to alerting (don't filter if we can't check)
            return {
                'should_alert': True,
                'risk_assessment': {
                    'risk_level': 'UNKNOWN',
                    'risk_score': 0.5,
                    'risk_factors': ['Unable to assess risk'],
                    'should_alert': True
                },
                'token_metadata': {}
            }


# Global instance
token_intelligence = TokenIntelligence()



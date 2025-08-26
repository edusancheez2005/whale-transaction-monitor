#!/usr/bin/env python3
"""
Real-Time Transaction Classification for Market Flow Engine

This module extends the existing classification system with real-time DEX swap analysis,
stablecoin-based BUY/SELL detection, and multi-source validation using Covalent and BigQuery.
"""

import sys
import os
import time
import json
import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import asyncio
import aiohttp

# Add config path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))

try:
    from web3 import Web3
    from web3.exceptions import Web3Exception
    import api_keys
    import settings
    from utils.classification_final import transaction_classifier, analyze_address_characteristics
except ImportError as e:
    logging.error(f"Missing required packages: {e}")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ClassifiedSwap:
    """Data model for classified swap transactions."""
    transaction_hash: str
    block_number: int
    block_timestamp: datetime
    chain: str
    dex: str
    
    # Token Information
    token_in_address: str
    token_out_address: str
    token_in_symbol: Optional[str] = None
    token_out_symbol: Optional[str] = None
    token_in_decimals: Optional[int] = None
    token_out_decimals: Optional[int] = None
    
    # Transaction Amounts
    amount_in: Decimal = Decimal('0')
    amount_out: Decimal = Decimal('0')
    amount_in_usd: Optional[Decimal] = None
    amount_out_usd: Optional[Decimal] = None
    
    # Classification
    classification: str = 'UNKNOWN'  # BUY, SELL, UNKNOWN
    confidence_score: float = 0.0
    
    # Enrichment Data
    sender_address: str = ''
    recipient_address: Optional[str] = None
    is_whale_transaction: bool = False
    whale_classification: Optional[str] = None
    
    # Price Data
    token_price_usd: Optional[Decimal] = None
    gas_used: Optional[int] = None
    gas_price: Optional[Decimal] = None
    transaction_fee_usd: Optional[Decimal] = None
    
    # Metadata
    raw_log_data: Optional[Dict] = None
    classification_method: str = 'stablecoin_heuristic'

class RealTimeClassifier:
    """Enhanced real-time transaction classifier with multi-source validation."""
    
    def __init__(self):
        """Initialize the classifier with Web3 connections and API clients."""
        self.w3_ethereum = Web3(Web3.HTTPProvider(api_keys.ETHEREUM_RPC_URL))
        self.w3_polygon = Web3(Web3.HTTPProvider(api_keys.POLYGON_RPC_URL))
        
        # Token and price caches
        self.token_cache = {}
        self.price_cache = {}
        self.cache_ttl = settings.CLASSIFICATION_CONFIG['price_cache_ttl_seconds']
        
        # Stablecoin addresses (normalized to lowercase)
        self.stablecoins = {
            chain: {symbol: addr.lower() for symbol, addr in tokens.items()}
            for chain, tokens in settings.STABLECOINS.items()
        }
        
        logger.info("✅ RealTimeClassifier initialized with Web3 connections")
    
    def get_web3_client(self, chain: str) -> Web3:
        """Get the appropriate Web3 client for the chain."""
        if chain == 'ethereum':
            return self.w3_ethereum
        elif chain == 'polygon':
            return self.w3_polygon
        else:
            raise ValueError(f"Unsupported chain: {chain}")
    
    async def get_token_info(self, token_address: str, chain: str) -> Dict[str, Any]:
        """Get token information (symbol, decimals) with caching."""
        cache_key = f"{chain}:{token_address.lower()}"
        
        if cache_key in self.token_cache:
            return self.token_cache[cache_key]
        
        try:
            w3 = self.get_web3_client(chain)
            
            # Standard ERC-20 ABI for symbol and decimals
            erc20_abi = [
                {
                    "constant": True,
                    "inputs": [],
                    "name": "symbol",
                    "outputs": [{"name": "", "type": "string"}],
                    "type": "function"
                },
                {
                    "constant": True,
                    "inputs": [],
                    "name": "decimals",
                    "outputs": [{"name": "", "type": "uint8"}],
                    "type": "function"
                }
            ]
            
            contract = w3.eth.contract(
                address=Web3.to_checksum_address(token_address),
                abi=erc20_abi
            )
            
            symbol = contract.functions.symbol().call()
            decimals = contract.functions.decimals().call()
            
            token_info = {
                'symbol': symbol,
                'decimals': decimals,
                'address': token_address.lower()
            }
            
            self.token_cache[cache_key] = token_info
            return token_info
            
        except Exception as e:
            logger.warning(f"Failed to get token info for {token_address} on {chain}: {e}")
            # Return default values
            default_info = {
                'symbol': f"TOKEN_{token_address[:8]}",
                'decimals': 18,
                'address': token_address.lower()
            }
            self.token_cache[cache_key] = default_info
            return default_info
    
    async def get_token_price_coingecko(self, token_address: str, chain: str) -> Optional[Decimal]:
        """Get token price from CoinGecko API with caching and rate limiting."""
        cache_key = f"price:{chain}:{token_address.lower()}"
        current_time = time.time()
        
        # Check cache
        if cache_key in self.price_cache:
            price_data = self.price_cache[cache_key]
            if current_time - price_data['timestamp'] < self.cache_ttl:
                return price_data['price']
        
        try:
            # Map chain names to CoinGecko platform IDs
            platform_map = {
                'ethereum': 'ethereum',
                'polygon': 'polygon-pos',
                'solana': 'solana'
            }
            
            platform = platform_map.get(chain)
            if not platform:
                return None
            
            url = f"https://api.coingecko.com/api/v3/simple/token_price/{platform}"
            params = {
                'contract_addresses': token_address,
                'vs_currencies': 'usd'
            }
            
            # Add API key if available
            if api_keys.COINGECKO_API_KEY:
                params['x_cg_pro_api_key'] = api_keys.COINGECKO_API_KEY
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        price = data.get(token_address.lower(), {}).get('usd')
                        
                        if price:
                            price_decimal = Decimal(str(price))
                            self.price_cache[cache_key] = {
                                'price': price_decimal,
                                'timestamp': current_time
                            }
                            return price_decimal
                    elif response.status == 429:
                        logger.warning("CoinGecko rate limit hit, using cached price if available")
                        
        except Exception as e:
            logger.warning(f"Failed to get price for {token_address} on {chain}: {e}")
        
        return None
    
    def is_stablecoin(self, token_address: str, chain: str) -> bool:
        """Check if a token is a known stablecoin."""
        token_addr = token_address.lower()
        chain_stablecoins = self.stablecoins.get(chain, {})
        return token_addr in chain_stablecoins.values()
    
    def classify_swap_direction(self, token_in: str, token_out: str, chain: str) -> Tuple[str, float]:
        """
        Classify swap direction based on stablecoin involvement.
        
        Returns:
            Tuple of (classification, confidence_score)
        """
        token_in_is_stable = self.is_stablecoin(token_in, chain)
        token_out_is_stable = self.is_stablecoin(token_out, chain)
        
        if token_in_is_stable and not token_out_is_stable:
            # Spending stablecoin to get token = BUY
            return ('BUY', 0.9)
        elif token_out_is_stable and not token_in_is_stable:
            # Selling token for stablecoin = SELL
            return ('SELL', 0.9)
        elif token_in_is_stable and token_out_is_stable:
            # Stablecoin to stablecoin = transfer/arbitrage
            return ('UNKNOWN', 0.3)
        else:
            # Token to token swap - need additional analysis
            return ('UNKNOWN', 0.5)
    
    async def enrich_with_covalent(self, transaction_hash: str, chain: str) -> Dict[str, Any]:
        """Enrich transaction data using Covalent API."""
        try:
            # Map chain names to Covalent chain IDs
            chain_id_map = {
                'ethereum': '1',
                'polygon': '137'
            }
            
            chain_id = chain_id_map.get(chain)
            if not chain_id:
                return {}
            
            url = f"{api_keys.COVALENT_API_BASE_URL}/{chain_id}/transaction_v2/{transaction_hash}/"
            headers = {'Authorization': f'Bearer {api_keys.COVALENT_API_KEY}'}
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get('data', {}).get('items', [{}])[0] if data.get('data', {}).get('items') else {}
                    else:
                        logger.warning(f"Covalent API error {response.status} for tx {transaction_hash}")
                        
        except Exception as e:
            logger.warning(f"Covalent enrichment failed for {transaction_hash}: {e}")
        
        return {}
    
    async def validate_with_bigquery(self, transaction_hash: str, chain: str) -> Dict[str, Any]:
        """Validate classification using BigQuery public datasets."""
        try:
            # This would require BigQuery client setup
            # For now, return empty dict as placeholder
            # In production, implement BigQuery queries for transaction validation
            logger.info(f"BigQuery validation for {transaction_hash} on {chain} - placeholder")
            return {}
            
        except Exception as e:
            logger.warning(f"BigQuery validation failed for {transaction_hash}: {e}")
            return {}
    
    async def classify_uniswap_v2_swap(self, log_data: Dict, chain: str) -> ClassifiedSwap:
        """Classify Uniswap V2 swap transaction."""
        try:
            # Extract basic transaction info
            tx_hash = log_data.get('transactionHash', '')
            block_number = int(log_data.get('blockNumber', '0'), 16)
            
            # Get transaction receipt for more details
            w3 = self.get_web3_client(chain)
            tx_receipt = w3.eth.get_transaction_receipt(tx_hash)
            tx = w3.eth.get_transaction(tx_hash)
            
            # Parse V2 swap event data
            # V2 Swap event: Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address indexed to)
            topics = log_data.get('topics', [])
            data = log_data.get('data', '0x')
            
            # Decode amounts from data field
            amounts = w3.codec.decode(['uint256', 'uint256', 'uint256', 'uint256'], bytes.fromhex(data[2:]))
            amount0_in, amount1_in, amount0_out, amount1_out = amounts
            
            # Get pair contract to determine token addresses
            pair_address = log_data.get('address', '')
            
            # For now, use placeholder token addresses - in production, query pair contract
            token_in_addr = "0x0000000000000000000000000000000000000000"  # Placeholder
            token_out_addr = "0x0000000000000000000000000000000000000001"  # Placeholder
            
            # Get token information
            token_in_info = await self.get_token_info(token_in_addr, chain)
            token_out_info = await self.get_token_info(token_out_addr, chain)
            
            # Classify swap direction
            classification, confidence = self.classify_swap_direction(token_in_addr, token_out_addr, chain)
            
            # Create classified swap object
            swap = ClassifiedSwap(
                transaction_hash=tx_hash,
                block_number=block_number,
                block_timestamp=datetime.fromtimestamp(tx_receipt.get('timestamp', 0), tz=timezone.utc),
                chain=chain,
                dex='uniswap_v2',
                token_in_address=token_in_addr,
                token_out_address=token_out_addr,
                token_in_symbol=token_in_info['symbol'],
                token_out_symbol=token_out_info['symbol'],
                token_in_decimals=token_in_info['decimals'],
                token_out_decimals=token_out_info['decimals'],
                amount_in=Decimal(str(amount0_in if amount0_in > 0 else amount1_in)),
                amount_out=Decimal(str(amount0_out if amount0_out > 0 else amount1_out)),
                classification=classification,
                confidence_score=confidence,
                sender_address=tx['from'],
                recipient_address=tx['to'],
                gas_used=tx_receipt.get('gasUsed'),
                gas_price=Decimal(str(tx.get('gasPrice', 0))),
                raw_log_data=log_data,
                classification_method='uniswap_v2_stablecoin_analysis'
            )
            
            return swap
            
        except Exception as e:
            logger.error(f"Failed to classify Uniswap V2 swap: {e}")
            # Return minimal swap object
            return ClassifiedSwap(
                transaction_hash=log_data.get('transactionHash', ''),
                block_number=0,
                block_timestamp=datetime.now(tz=timezone.utc),
                chain=chain,
                dex='uniswap_v2',
                token_in_address='',
                token_out_address='',
                classification='UNKNOWN',
                confidence_score=0.0,
                raw_log_data=log_data
            )
    
    async def classify_uniswap_v3_swap(self, log_data: Dict, chain: str) -> ClassifiedSwap:
        """Classify Uniswap V3 swap transaction."""
        # Similar implementation to V2 but with V3-specific event parsing
        # This is a placeholder - full implementation would decode V3 swap events
        logger.info(f"Classifying Uniswap V3 swap on {chain}")
        
        return ClassifiedSwap(
            transaction_hash=log_data.get('transactionHash', ''),
            block_number=0,
            block_timestamp=datetime.now(tz=timezone.utc),
            chain=chain,
            dex='uniswap_v3',
            token_in_address='',
            token_out_address='',
            classification='UNKNOWN',
            confidence_score=0.0,
            raw_log_data=log_data,
            classification_method='uniswap_v3_analysis'
        )
    
    async def classify_jupiter_swap(self, transaction_data: Dict) -> ClassifiedSwap:
        """Classify Jupiter swap on Solana."""
        # Placeholder for Jupiter swap classification
        logger.info("Classifying Jupiter swap on Solana")
        
        return ClassifiedSwap(
            transaction_hash=transaction_data.get('signature', ''),
            block_number=0,
            block_timestamp=datetime.now(tz=timezone.utc),
            chain='solana',
            dex='jupiter',
            token_in_address='',
            token_out_address='',
            classification='UNKNOWN',
            confidence_score=0.0,
            raw_log_data=transaction_data,
            classification_method='jupiter_analysis'
        )
    
    async def enrich_whale_classification(self, swap: ClassifiedSwap) -> ClassifiedSwap:
        """Enrich swap with whale classification using existing whale database."""
        try:
            # Use existing classification system
            classification, confidence = transaction_classifier(
                swap.sender_address,
                swap.recipient_address or '',
                swap.token_in_symbol,
                float(swap.amount_in_usd) if swap.amount_in_usd else None,
                swap.transaction_hash,
                'real_time_monitor'
            )
            
            # Check if this is a whale transaction based on USD value
            whale_threshold = settings.CLASSIFICATION_CONFIG['whale_threshold_usd']
            if swap.amount_in_usd and swap.amount_in_usd >= whale_threshold:
                swap.is_whale_transaction = True
                swap.whale_classification = f"high_value_{classification}"
            
            return swap
            
        except Exception as e:
            logger.warning(f"Whale enrichment failed for {swap.transaction_hash}: {e}")
            return swap

# Global classifier instance
classifier = RealTimeClassifier()

async def classify_swap_transaction(log_data: Dict, chain: str, dex: str) -> ClassifiedSwap:
    """
    Main entry point for classifying swap transactions.
    
    Args:
        log_data: Raw log data from blockchain
        chain: Chain name (ethereum, polygon, solana)
        dex: DEX name (uniswap_v2, uniswap_v3, jupiter)
    
    Returns:
        ClassifiedSwap object with full classification and enrichment
    """
    try:
        # Route to appropriate classifier based on DEX
        if dex == 'uniswap_v2':
            swap = await classifier.classify_uniswap_v2_swap(log_data, chain)
        elif dex == 'uniswap_v3':
            swap = await classifier.classify_uniswap_v3_swap(log_data, chain)
        elif dex == 'jupiter':
            swap = await classifier.classify_jupiter_swap(log_data)
        else:
            raise ValueError(f"Unsupported DEX: {dex}")
        
        # Enrich with price data
        if swap.token_in_address:
            swap.token_price_usd = await classifier.get_token_price_coingecko(
                swap.token_in_address, chain
            )
            
            if swap.token_price_usd and swap.amount_in:
                # Calculate USD value
                token_amount = swap.amount_in / (10 ** (swap.token_in_decimals or 18))
                swap.amount_in_usd = token_amount * swap.token_price_usd
        
        # Enrich with whale classification
        swap = await classifier.enrich_whale_classification(swap)
        
        # Enrich with external data sources
        covalent_data = await classifier.enrich_with_covalent(swap.transaction_hash, chain)
        bigquery_data = await classifier.validate_with_bigquery(swap.transaction_hash, chain)
        
        logger.info(f"✅ Classified {dex} swap: {swap.classification} ({swap.confidence_score:.2f} confidence)")
        return swap
        
    except Exception as e:
        logger.error(f"Failed to classify swap transaction: {e}")
        # Return minimal swap object
        return ClassifiedSwap(
            transaction_hash=log_data.get('transactionHash', ''),
            block_number=0,
            block_timestamp=datetime.now(tz=timezone.utc),
            chain=chain,
            dex=dex,
            token_in_address='',
            token_out_address='',
            classification='UNKNOWN',
            confidence_score=0.0,
            raw_log_data=log_data
        ) 
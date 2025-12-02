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

# BigQuery is optional; if unavailable, disable gracefully
try:
    from utils.bigquery_analyzer import bigquery_analyzer  # type: ignore
except Exception:
    bigquery_analyzer = None

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
            # Do not fabricate values; propagate so caller can record missing_fields
            raise
    
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
        Deterministic, correctness-first BUY/SELL policy for swaps:
        - If token_out is a stablecoin and token_in is not: SELL (exiting into stables)
        - Else if token_in is a stablecoin and token_out is not: BUY (deploying stables)
        - Else if both are stables: UNKNOWN
        - Else (token-token): BUY the token_out (coverage mode: token_out_rule)
        """
        token_in_is_stable = self.is_stablecoin(token_in, chain)
        token_out_is_stable = self.is_stablecoin(token_out, chain)
        
        if token_out_is_stable and not token_in_is_stable:
            return ('SELL', 0.95)
        if token_in_is_stable and not token_out_is_stable:
            return ('BUY', 0.95)
        if token_in_is_stable and token_out_is_stable:
            return ('UNKNOWN', 0.30)
        # token-token: user acquires token_out
        return ('BUY', 0.85)
    
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
        """Validate using BigQuery only if available at no cost, otherwise return disabled reason."""
        try:
            if not bigquery_analyzer or not getattr(bigquery_analyzer, 'client', None):
                return {'bigquery_disabled_reason': 'unavailable_or_cost'}
            return {'bigquery_enabled': True}
        except Exception as e:
            logger.warning(f"BigQuery validation failed for {transaction_hash}: {e}")
            return {'bigquery_disabled_reason': 'error'}
    
    async def classify_uniswap_v2_swap(self, log_data: Dict, chain: str) -> ClassifiedSwap:
        """Classify Uniswap V2 swap transaction with exact on-chain decoding and no placeholders."""
        try:
            # Extract basic transaction info
            tx_hash = log_data.get('transactionHash', '')
            block_number = int(log_data.get('blockNumber', '0'), 16) if isinstance(log_data.get('blockNumber'), str) else int(log_data.get('blockNumber') or 0)
            
            # Get transaction receipt for more details
            w3 = self.get_web3_client(chain)
            tx_receipt = w3.eth.get_transaction_receipt(tx_hash)
            tx = w3.eth.get_transaction(tx_hash)
            
            # Parse V2 swap event data
            # V2 Swap event: Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address indexed to)
            data = log_data.get('data', '0x')
            
            # Decode amounts from data field
            amounts = w3.codec.decode(['uint256', 'uint256', 'uint256', 'uint256'], bytes.fromhex(data[2:]))
            amount0_in, amount1_in, amount0_out, amount1_out = amounts
            
            # Get pair contract to determine token addresses
            pair_address = log_data.get('address', '')
            pair_abi = [
                {"constant": True, "inputs": [], "name": "token0", "outputs": [{"name": "", "type": "address"}], "type": "function"},
                {"constant": True, "inputs": [], "name": "token1", "outputs": [{"name": "", "type": "address"}], "type": "function"},
                {"constant": True, "inputs": [], "name": "getReserves", "outputs": [
                    {"name": "_reserve0", "type": "uint112"},
                    {"name": "_reserve1", "type": "uint112"},
                    {"name": "_blockTimestampLast", "type": "uint32"}
                ], "type": "function"}
            ]
            pair = w3.eth.contract(address=Web3.to_checksum_address(pair_address), abi=pair_abi)
            token0_addr = pair.functions.token0().call()
            token1_addr = pair.functions.token1().call()
            
            # Determine direction from non-zero legs
            if amount0_in > 0 and amount1_out > 0:
                token_in_addr = token0_addr
                token_out_addr = token1_addr
                raw_amount_in = Decimal(amount0_in)
                raw_amount_out = Decimal(amount1_out)
            elif amount1_in > 0 and amount0_out > 0:
                token_in_addr = token1_addr
                token_out_addr = token0_addr
                raw_amount_in = Decimal(amount1_in)
                raw_amount_out = Decimal(amount0_out)
            else:
                token_in_addr = token0_addr
                token_out_addr = token1_addr
                raw_amount_in = Decimal(0)
                raw_amount_out = Decimal(0)
            
            # Token metadata with no placeholders
            missing_fields: List[str] = []
            try:
                token_in_info = await self.get_token_info(token_in_addr, chain)
            except Exception:
                token_in_info = None
                missing_fields.append('token_in_metadata')
            try:
                token_out_info = await self.get_token_info(token_out_addr, chain)
            except Exception:
                token_out_info = None
                missing_fields.append('token_out_metadata')
            
            # Block timestamp via block lookup
            block = w3.eth.get_block(tx_receipt['blockNumber'])
            block_ts = datetime.fromtimestamp(block['timestamp'], tz=timezone.utc)
            
            token_in_decimals = token_in_info['decimals'] if token_in_info else None
            token_out_decimals = token_out_info['decimals'] if token_out_info else None
            if token_in_decimals is None:
                missing_fields.append('token_in_decimals')
            if token_out_decimals is None:
                missing_fields.append('token_out_decimals')
            
            # Pool reserves and price impact estimation
            price_impact_bps = None
            reserves = None
            try:
                r0, r1, _ = pair.functions.getReserves().call()
                reserves = {'reserve0': str(r0), 'reserve1': str(r1)}
                if token_in_decimals is not None and token_out_decimals is not None and raw_amount_in > 0 and raw_amount_out > 0 and r0 and r1:
                    if token_in_addr.lower() == token0_addr.lower():
                        mid = Decimal(r1) / Decimal(r0)
                    else:
                        mid = Decimal(r0) / Decimal(r1)
                    exec_price = (raw_amount_out / (10 ** token_out_decimals)) / (raw_amount_in / (10 ** token_in_decimals))
                    price_impact_bps = int(max(Decimal(0), (mid - exec_price) / mid * Decimal(10000)))
            except Exception as e:
                logger.warning(f"Failed to get reserves for {pair_address}: {e}")
                missing_fields.append('pool_reserves')
            
            # Classification after correct mapping
            classification, confidence = self.classify_swap_direction(token_in_addr, token_out_addr, chain)
            
            # Pricing: CoinGecko first, fallback to pool if stablecoin is involved
            price_source = None
            amount_in_usd: Optional[Decimal] = None
            try:
                token_in_price = await self.get_token_price_coingecko(token_in_addr, chain)
                if token_in_price and token_in_decimals is not None and raw_amount_in > 0:
                    amount_in_usd = (raw_amount_in / (10 ** token_in_decimals)) * token_in_price
                    price_source = 'coingecko'
                else:
                    if self.is_stablecoin(token_out_addr, chain) and token_out_decimals is not None:
                        amount_in_usd = (raw_amount_out / (10 ** token_out_decimals))
                        price_source = 'pool_midprice'
                    elif self.is_stablecoin(token_in_addr, chain) and token_in_decimals is not None:
                        amount_in_usd = (raw_amount_in / (10 ** token_in_decimals))
                        price_source = 'pool_midprice'
                    else:
                        missing_fields.append('usd_pricing')
            except Exception as e:
                logger.warning(f"Pricing failed for {tx_hash}: {e}")
                missing_fields.append('usd_pricing')
            
            raw_meta = {
                'pool_address': pair_address,
                'router_address': tx.get('to'),
                'price_source': price_source,
                'pool_reserves_or_sqrtPriceX96': reserves,
                'liquidity': None,
                'price_impact_bps': price_impact_bps,
                'mev_heuristics': {},
                'cex_flow_direction': None,
                'wallet_behavior_metrics': None,
                'missing_fields': list(set(missing_fields)),
                'direction_basis': 'stablecoin_rule' if (self.is_stablecoin(token_in_addr, chain) or self.is_stablecoin(token_out_addr, chain)) else 'token_out_rule',
                'provider_paths': ['web3']
            }
            
            swap = ClassifiedSwap(
                transaction_hash=tx_hash,
                block_number=block_number,
                block_timestamp=block_ts,
                chain=chain,
                dex='uniswap_v2',
                token_in_address=token_in_addr,
                token_out_address=token_out_addr,
                token_in_symbol=(token_in_info['symbol'] if token_in_info else None),
                token_out_symbol=(token_out_info['symbol'] if token_out_info else None),
                token_in_decimals=token_in_decimals,
                token_out_decimals=token_out_decimals,
                amount_in=raw_amount_in,
                amount_out=raw_amount_out,
                classification=classification,
                confidence_score=confidence,
                sender_address=tx['from'],
                recipient_address=tx['to'],
                gas_used=tx_receipt.get('gasUsed'),
                gas_price=Decimal(str(tx.get('gasPrice', 0)) if tx.get('gasPrice') is not None else '0'),
                raw_log_data=raw_meta,
                classification_method='uniswap_v2_exact_decode'
            )
            
            if amount_in_usd is not None:
                swap.amount_in_usd = amount_in_usd
            
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
                raw_log_data={'missing_fields': ['uniswap_v2_decode_failed'], 'provider_paths': ['web3']},
                classification_method='uniswap_v2_exact_decode'
            )
    
    async def classify_uniswap_v3_swap(self, log_data: Dict, chain: str) -> ClassifiedSwap:
        """Classify Uniswap V3 swap transaction with exact pool decoding and price context."""
        try:
            tx_hash = log_data.get('transactionHash', '')
            block_number = int(log_data.get('blockNumber', '0'), 16) if isinstance(log_data.get('blockNumber'), str) else int(log_data.get('blockNumber') or 0)
            w3 = self.get_web3_client(chain)
            tx_receipt = w3.eth.get_transaction_receipt(tx_hash)
            tx = w3.eth.get_transaction(tx_hash)

            pool_address = log_data.get('address', '')
            pool_abi = [
                {"constant": True, "inputs": [], "name": "token0", "outputs": [{"name": "", "type": "address"}], "type": "function"},
                {"constant": True, "inputs": [], "name": "token1", "outputs": [{"name": "", "type": "address"}], "type": "function"},
                {"constant": True, "inputs": [], "name": "slot0", "outputs": [
                    {"name": "sqrtPriceX96", "type": "uint160"},
                    {"name": "tick", "type": "int24"},
                    {"name": "observationIndex", "type": "uint16"},
                    {"name": "observationCardinality", "type": "uint16"},
                    {"name": "observationCardinalityNext", "type": "uint16"},
                    {"name": "feeProtocol", "type": "uint8"},
                    {"name": "unlocked", "type": "bool"}
                ], "type": "function"},
                {"constant": True, "inputs": [], "name": "liquidity", "outputs": [{"name": "", "type": "uint128"}], "type": "function"}
            ]
            pool = w3.eth.contract(address=Web3.to_checksum_address(pool_address), abi=pool_abi)
            token0_addr = pool.functions.token0().call()
            token1_addr = pool.functions.token1().call()

            data = log_data.get('data', '0x')
            amount0, amount1, sqrt_price_x96, liquidity_val, tick = w3.codec.decode(['int256', 'int256', 'uint160', 'uint128', 'int24'], bytes.fromhex(data[2:]))

            if amount0 < 0 and amount1 > 0:
                token_in_addr = token1_addr
                token_out_addr = token0_addr
                raw_amount_in = Decimal(amount1)
                raw_amount_out = Decimal(abs(amount0))
            elif amount1 < 0 and amount0 > 0:
                token_in_addr = token0_addr
                token_out_addr = token1_addr
                raw_amount_in = Decimal(amount0)
                raw_amount_out = Decimal(abs(amount1))
            else:
                token_in_addr = token0_addr
                token_out_addr = token1_addr
                raw_amount_in = Decimal(0)
                raw_amount_out = Decimal(0)

            missing_fields: List[str] = []
            try:
                token_in_info = await self.get_token_info(token_in_addr, chain)
            except Exception:
                token_in_info = None
                missing_fields.append('token_in_metadata')
            try:
                token_out_info = await self.get_token_info(token_out_addr, chain)
            except Exception:
                token_out_info = None
                missing_fields.append('token_out_metadata')

            token_in_decimals = token_in_info['decimals'] if token_in_info else None
            token_out_decimals = token_out_info['decimals'] if token_out_info else None
            if token_in_decimals is None:
                missing_fields.append('token_in_decimals')
            if token_out_decimals is None:
                missing_fields.append('token_out_decimals')

            block = w3.eth.get_block(tx_receipt['blockNumber'])
            block_ts = datetime.fromtimestamp(block['timestamp'], tz=timezone.utc)

            price_impact_bps = None
            pool_state = {
                'sqrtPriceX96': str(sqrt_price_x96),
                'liquidity': str(liquidity_val),
                'tick': int(tick)
            }
            try:
                mid_token1_per_token0 = (Decimal(sqrt_price_x96) ** 2) / Decimal(2 ** 192)
                exec_price = None
                if raw_amount_in > 0 and raw_amount_out > 0 and token_in_decimals is not None and token_out_decimals is not None:
                    exec_price = (raw_amount_out / (10 ** token_out_decimals)) / (raw_amount_in / (10 ** token_in_decimals))
                if exec_price:
                    if token_in_addr.lower() == token0_addr.lower():
                        mid = mid_token1_per_token0
                    else:
                        mid = (Decimal(1) / mid_token1_per_token0) if mid_token1_per_token0 > 0 else None
                    if mid:
                        price_impact_bps = int(max(Decimal(0), (mid - exec_price) / mid * Decimal(10000)))
            except Exception as e:
                logger.warning(f"Failed to compute v3 price impact for {pool_address}: {e}")
                missing_fields.append('sqrt_price_analysis')

            classification, confidence = self.classify_swap_direction(token_in_addr, token_out_addr, chain)

            amount_in_usd: Optional[Decimal] = None
            price_source = None
            try:
                token_in_price = await self.get_token_price_coingecko(token_in_addr, chain)
                if token_in_price and token_in_decimals is not None and raw_amount_in > 0:
                    amount_in_usd = (raw_amount_in / (10 ** token_in_decimals)) * token_in_price
                    price_source = 'coingecko'
                else:
                    if self.is_stablecoin(token_out_addr, chain) and token_out_decimals is not None:
                        amount_in_usd = (raw_amount_out / (10 ** token_out_decimals))
                        price_source = 'pool_midprice'
                    elif self.is_stablecoin(token_in_addr, chain) and token_in_decimals is not None:
                        amount_in_usd = (raw_amount_in / (10 ** token_in_decimals))
                        price_source = 'pool_midprice'
                    else:
                        missing_fields.append('usd_pricing')
            except Exception as e:
                logger.warning(f"Pricing failed for {tx_hash}: {e}")
                missing_fields.append('usd_pricing')

            raw_meta = {
                'pool_address': pool_address,
                'router_address': tx.get('to'),
                'price_source': price_source,
                'pool_reserves_or_sqrtPriceX96': pool_state,
                'liquidity': str(liquidity_val),
                'price_impact_bps': price_impact_bps,
                'mev_heuristics': {},
                'cex_flow_direction': None,
                'wallet_behavior_metrics': None,
                'missing_fields': list(set(missing_fields)),
                'direction_basis': 'stablecoin_rule' if (self.is_stablecoin(token_in_addr, chain) or self.is_stablecoin(token_out_addr, chain)) else 'token_out_rule',
                'provider_paths': ['web3']
            }

            swap = ClassifiedSwap(
                transaction_hash=tx_hash,
                block_number=block_number,
                block_timestamp=block_ts,
                chain=chain,
                dex='uniswap_v3',
                token_in_address=token_in_addr,
                token_out_address=token_out_addr,
                token_in_symbol=(token_in_info['symbol'] if token_in_info else None),
                token_out_symbol=(token_out_info['symbol'] if token_out_info else None),
                token_in_decimals=token_in_decimals,
                token_out_decimals=token_out_decimals,
                amount_in=raw_amount_in,
                amount_out=raw_amount_out,
                classification=classification,
                confidence_score=confidence,
                sender_address=tx['from'],
                recipient_address=tx['to'],
                gas_used=tx_receipt.get('gasUsed'),
                gas_price=Decimal(str(tx.get('gasPrice', 0)) if tx.get('gasPrice') is not None else '0'),
                raw_log_data=raw_meta,
                classification_method='uniswap_v3_exact_decode'
            )
            if amount_in_usd is not None:
                swap.amount_in_usd = amount_in_usd
            return swap
        except Exception as e:
            logger.error(f"Failed to classify Uniswap V3 swap: {e}")
            # Return minimal swap object
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
                raw_log_data={'missing_fields': ['uniswap_v3_decode_failed'], 'provider_paths': ['web3']},
                classification_method='uniswap_v3_exact_decode'
        )
    
    async def classify_balancer_v2_swap(self, log_data: Dict, chain: str) -> ClassifiedSwap:
        """Classify Balancer V2 swap with exact Vault decoding."""
        try:
            tx_hash = log_data.get('transactionHash', '')
            block_number = int(log_data.get('blockNumber', '0'), 16) if isinstance(log_data.get('blockNumber'), str) else int(log_data.get('blockNumber') or 0)
            w3 = self.get_web3_client(chain)
            tx_receipt = w3.eth.get_transaction_receipt(tx_hash)
            tx = w3.eth.get_transaction(tx_hash)

            # Balancer V2 Swap event: Swap(bytes32 indexed poolId, address indexed tokenIn, address indexed tokenOut, uint256 amountIn, uint256 amountOut)
            # Topics: [event_sig, poolId, tokenIn, tokenOut]
            topics = log_data.get('topics', [])
            data = log_data.get('data', '0x')
            
            if len(topics) < 4:
                raise ValueError("Insufficient topics for Balancer V2 Swap event")
            
            pool_id = topics[1] if isinstance(topics[1], str) else topics[1].hex()
            token_in_addr_raw = topics[2] if isinstance(topics[2], str) else topics[2].hex()
            token_out_addr_raw = topics[3] if isinstance(topics[3], str) else topics[3].hex()
            
            # Extract addresses from topics (last 20 bytes)
            token_in_addr = '0x' + token_in_addr_raw[-40:]
            token_out_addr = '0x' + token_out_addr_raw[-40:]
            
            # Decode amounts from data
            amounts = w3.codec.decode(['uint256', 'uint256'], bytes.fromhex(data[2:]))
            raw_amount_in, raw_amount_out = Decimal(amounts[0]), Decimal(amounts[1])

            missing_fields: List[str] = []
            try:
                token_in_info = await self.get_token_info(token_in_addr, chain)
            except Exception:
                token_in_info = None
                missing_fields.append('token_in_metadata')
            try:
                token_out_info = await self.get_token_info(token_out_addr, chain)
            except Exception:
                token_out_info = None
                missing_fields.append('token_out_metadata')

            token_in_decimals = token_in_info['decimals'] if token_in_info else None
            token_out_decimals = token_out_info['decimals'] if token_out_info else None
            if token_in_decimals is None:
                missing_fields.append('token_in_decimals')
            if token_out_decimals is None:
                missing_fields.append('token_out_decimals')

            block = w3.eth.get_block(tx_receipt['blockNumber'])
            block_ts = datetime.fromtimestamp(block['timestamp'], tz=timezone.utc)

            classification, confidence = self.classify_swap_direction(token_in_addr, token_out_addr, chain)

            amount_in_usd: Optional[Decimal] = None
            price_source = None
            try:
                token_in_price = await self.get_token_price_coingecko(token_in_addr, chain)
                if token_in_price and token_in_decimals is not None and raw_amount_in > 0:
                    amount_in_usd = (raw_amount_in / (10 ** token_in_decimals)) * token_in_price
                    price_source = 'coingecko'
                else:
                    if self.is_stablecoin(token_out_addr, chain) and token_out_decimals is not None:
                        amount_in_usd = (raw_amount_out / (10 ** token_out_decimals))
                        price_source = 'pool_midprice'
                    elif self.is_stablecoin(token_in_addr, chain) and token_in_decimals is not None:
                        amount_in_usd = (raw_amount_in / (10 ** token_in_decimals))
                        price_source = 'pool_midprice'
                    else:
                        missing_fields.append('usd_pricing')
            except Exception as e:
                logger.warning(f"Pricing failed for {tx_hash}: {e}")
                missing_fields.append('usd_pricing')

            raw_meta = {
                'pool_id': pool_id,
                'vault_address': log_data.get('address'),
                'router_address': tx.get('to'),
                'price_source': price_source,
                'missing_fields': list(set(missing_fields)),
                'direction_basis': 'stablecoin_rule' if (self.is_stablecoin(token_in_addr, chain) or self.is_stablecoin(token_out_addr, chain)) else 'token_out_rule',
                'provider_paths': ['web3']
            }

            swap = ClassifiedSwap(
                transaction_hash=tx_hash,
                block_number=block_number,
                block_timestamp=block_ts,
                chain=chain,
                dex='balancer_v2',
                token_in_address=token_in_addr,
                token_out_address=token_out_addr,
                token_in_symbol=(token_in_info['symbol'] if token_in_info else None),
                token_out_symbol=(token_out_info['symbol'] if token_out_info else None),
                token_in_decimals=token_in_decimals,
                token_out_decimals=token_out_decimals,
                amount_in=raw_amount_in,
                amount_out=raw_amount_out,
                classification=classification,
                confidence_score=confidence,
                sender_address=tx['from'],
                recipient_address=tx['to'],
                gas_used=tx_receipt.get('gasUsed'),
                gas_price=Decimal(str(tx.get('gasPrice', 0)) if tx.get('gasPrice') is not None else '0'),
                raw_log_data=raw_meta,
                classification_method='balancer_v2_exact_decode'
            )
            if amount_in_usd is not None:
                swap.amount_in_usd = amount_in_usd
            return swap
        except Exception as e:
            logger.error(f"Failed to classify Balancer V2 swap: {e}")
            return ClassifiedSwap(
                transaction_hash=log_data.get('transactionHash', ''),
                block_number=0,
                block_timestamp=datetime.now(tz=timezone.utc),
                chain=chain,
                dex='balancer_v2',
                token_in_address='',
                token_out_address='',
                classification='UNKNOWN',
                confidence_score=0.0,
                raw_log_data={'missing_fields': ['balancer_v2_decode_failed'], 'provider_paths': ['web3']},
                classification_method='balancer_v2_exact_decode'
            )

    async def classify_curve_swap(self, log_data: Dict, chain: str) -> ClassifiedSwap:
        """Classify Curve TokenExchange with exact pool coin resolution."""
        try:
            tx_hash = log_data.get('transactionHash', '')
            block_number = int(log_data.get('blockNumber', '0'), 16) if isinstance(log_data.get('blockNumber'), str) else int(log_data.get('blockNumber') or 0)
            w3 = self.get_web3_client(chain)
            tx_receipt = w3.eth.get_transaction_receipt(tx_hash)
            tx = w3.eth.get_transaction(tx_hash)

            # Curve TokenExchange: (address indexed buyer, int128 sold_id, uint256 tokens_sold, int128 bought_id, uint256 tokens_bought)
            # Topics: [event_sig, buyer]
            data = log_data.get('data', '0x')
            decoded = w3.codec.decode(['int128', 'uint256', 'int128', 'uint256'], bytes.fromhex(data[2:]))
            sold_id, tokens_sold, bought_id, tokens_bought = decoded
            
            pool_address = log_data.get('address', '')
            
            # Fetch coin addresses from pool
            pool_abi = [
                {"constant": True, "inputs": [{"name": "arg0", "type": "int128"}], "name": "coins", "outputs": [{"name": "", "type": "address"}], "type": "function"}
            ]
            pool = w3.eth.contract(address=Web3.to_checksum_address(pool_address), abi=pool_abi)
            
            missing_fields: List[str] = []
            try:
                token_in_addr = pool.functions.coins(sold_id).call()
                token_out_addr = pool.functions.coins(bought_id).call()
            except Exception as e:
                logger.warning(f"Failed to get Curve pool coins for {pool_address}: {e}")
                missing_fields.append('curve_pool_coins')
                raise

            raw_amount_in = Decimal(tokens_sold)
            raw_amount_out = Decimal(tokens_bought)

            try:
                token_in_info = await self.get_token_info(token_in_addr, chain)
            except Exception:
                token_in_info = None
                missing_fields.append('token_in_metadata')
            try:
                token_out_info = await self.get_token_info(token_out_addr, chain)
            except Exception:
                token_out_info = None
                missing_fields.append('token_out_metadata')

            token_in_decimals = token_in_info['decimals'] if token_in_info else None
            token_out_decimals = token_out_info['decimals'] if token_out_info else None
            if token_in_decimals is None:
                missing_fields.append('token_in_decimals')
            if token_out_decimals is None:
                missing_fields.append('token_out_decimals')

            block = w3.eth.get_block(tx_receipt['blockNumber'])
            block_ts = datetime.fromtimestamp(block['timestamp'], tz=timezone.utc)

            classification, confidence = self.classify_swap_direction(token_in_addr, token_out_addr, chain)

            amount_in_usd: Optional[Decimal] = None
            price_source = None
            try:
                token_in_price = await self.get_token_price_coingecko(token_in_addr, chain)
                if token_in_price and token_in_decimals is not None and raw_amount_in > 0:
                    amount_in_usd = (raw_amount_in / (10 ** token_in_decimals)) * token_in_price
                    price_source = 'coingecko'
                else:
                    if self.is_stablecoin(token_out_addr, chain) and token_out_decimals is not None:
                        amount_in_usd = (raw_amount_out / (10 ** token_out_decimals))
                        price_source = 'pool_midprice'
                    elif self.is_stablecoin(token_in_addr, chain) and token_in_decimals is not None:
                        amount_in_usd = (raw_amount_in / (10 ** token_in_decimals))
                        price_source = 'pool_midprice'
                    else:
                        missing_fields.append('usd_pricing')
            except Exception as e:
                logger.warning(f"Pricing failed for {tx_hash}: {e}")
                missing_fields.append('usd_pricing')

            raw_meta = {
                'pool_address': pool_address,
                'sold_id': int(sold_id),
                'bought_id': int(bought_id),
                'router_address': tx.get('to'),
                'price_source': price_source,
                'missing_fields': list(set(missing_fields)),
                'direction_basis': 'stablecoin_rule' if (self.is_stablecoin(token_in_addr, chain) or self.is_stablecoin(token_out_addr, chain)) else 'token_out_rule',
                'provider_paths': ['web3']
            }

            swap = ClassifiedSwap(
                transaction_hash=tx_hash,
                block_number=block_number,
                block_timestamp=block_ts,
                chain=chain,
                dex='curve',
                token_in_address=token_in_addr,
                token_out_address=token_out_addr,
                token_in_symbol=(token_in_info['symbol'] if token_in_info else None),
                token_out_symbol=(token_out_info['symbol'] if token_out_info else None),
                token_in_decimals=token_in_decimals,
                token_out_decimals=token_out_decimals,
                amount_in=raw_amount_in,
                amount_out=raw_amount_out,
                classification=classification,
                confidence_score=confidence,
                sender_address=tx['from'],
                recipient_address=tx['to'],
                gas_used=tx_receipt.get('gasUsed'),
                gas_price=Decimal(str(tx.get('gasPrice', 0)) if tx.get('gasPrice') is not None else '0'),
                raw_log_data=raw_meta,
                classification_method='curve_exact_decode'
            )
            if amount_in_usd is not None:
                swap.amount_in_usd = amount_in_usd
            return swap
        except Exception as e:
            logger.error(f"Failed to classify Curve swap: {e}")
            return ClassifiedSwap(
                transaction_hash=log_data.get('transactionHash', ''),
                block_number=0,
                block_timestamp=datetime.now(tz=timezone.utc),
                chain=chain,
                dex='curve',
                token_in_address='',
                token_out_address='',
                classification='UNKNOWN',
                confidence_score=0.0,
                raw_log_data={'missing_fields': ['curve_decode_failed'], 'provider_paths': ['web3']},
                classification_method='curve_exact_decode'
            )

    async def classify_1inch_swap(self, tx_hash: str, chain: str) -> ClassifiedSwap:
        """Classify 1inch aggregator swap using net ERC-20 Transfer delta analysis."""
        try:
            w3 = self.get_web3_client(chain)
            tx_receipt = w3.eth.get_transaction_receipt(tx_hash)
            tx = w3.eth.get_transaction(tx_hash)
            sender = tx['from']
            
            # Parse all ERC-20 Transfer events in the receipt
            transfer_topic = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
            transfers = []
            for log in tx_receipt['logs']:
                topics = log.get('topics', [])
                if len(topics) >= 3:
                    topic0 = topics[0].hex() if hasattr(topics[0], 'hex') else str(topics[0])
                    if topic0 == transfer_topic:
                        # ERC-20 Transfer(address indexed from, address indexed to, uint256 value)
                        from_addr_raw = topics[1].hex() if hasattr(topics[1], 'hex') else str(topics[1])
                        to_addr_raw = topics[2].hex() if hasattr(topics[2], 'hex') else str(topics[2])
                        from_addr = '0x' + from_addr_raw[-40:]
                        to_addr = '0x' + to_addr_raw[-40:]
                        token_addr = log['address']
                        data = log.get('data', '0x')
                        amount = int(data, 16) if data and data != '0x' else 0
                        transfers.append({
                            'token': token_addr.lower() if hasattr(token_addr, 'lower') else str(token_addr).lower(),
                            'from': from_addr.lower(),
                            'to': to_addr.lower(),
                            'amount': amount
                        })
            
            # Compute net delta per token for the sender
            net_delta = {}
            for t in transfers:
                token = t['token']
                if token not in net_delta:
                    net_delta[token] = 0
                if t['from'] == sender.lower():
                    net_delta[token] -= t['amount']
                if t['to'] == sender.lower():
                    net_delta[token] += t['amount']
            
            # Identify token_in (net negative) and token_out (net positive)
            token_in_addr = None
            token_out_addr = None
            raw_amount_in = Decimal(0)
            raw_amount_out = Decimal(0)
            for token, delta in net_delta.items():
                if delta < 0 and abs(delta) > abs(raw_amount_in):
                    token_in_addr = token
                    raw_amount_in = Decimal(abs(delta))
                if delta > 0 and delta > raw_amount_out:
                    token_out_addr = token
                    raw_amount_out = Decimal(delta)
            
            if not token_in_addr or not token_out_addr:
                raise ValueError("Could not determine token_in/token_out from Transfer deltas")
            
            missing_fields = []
            try:
                token_in_info = await self.get_token_info(token_in_addr, chain)
            except Exception:
                token_in_info = None
                missing_fields.append('token_in_metadata')
            try:
                token_out_info = await self.get_token_info(token_out_addr, chain)
            except Exception:
                token_out_info = None
                missing_fields.append('token_out_metadata')
            
            token_in_decimals = token_in_info['decimals'] if token_in_info else None
            token_out_decimals = token_out_info['decimals'] if token_out_info else None
            if token_in_decimals is None:
                missing_fields.append('token_in_decimals')
            if token_out_decimals is None:
                missing_fields.append('token_out_decimals')
            
            block = w3.eth.get_block(tx_receipt['blockNumber'])
            block_ts = datetime.fromtimestamp(block['timestamp'], tz=timezone.utc)
            
            classification, confidence = self.classify_swap_direction(token_in_addr, token_out_addr, chain)
            
            amount_in_usd: Optional[Decimal] = None
            price_source = None
            try:
                token_in_price = await self.get_token_price_coingecko(token_in_addr, chain)
                if token_in_price and token_in_decimals is not None and raw_amount_in > 0:
                    amount_in_usd = (raw_amount_in / (10 ** token_in_decimals)) * token_in_price
                    price_source = 'coingecko'
                else:
                    if self.is_stablecoin(token_out_addr, chain) and token_out_decimals is not None:
                        amount_in_usd = (raw_amount_out / (10 ** token_out_decimals))
                        price_source = 'net_transfer_stablecoin'
                    elif self.is_stablecoin(token_in_addr, chain) and token_in_decimals is not None:
                        amount_in_usd = (raw_amount_in / (10 ** token_in_decimals))
                        price_source = 'net_transfer_stablecoin'
                    else:
                        missing_fields.append('usd_pricing')
            except Exception as e:
                logger.warning(f"Pricing failed for 1inch tx {tx_hash}: {e}")
                missing_fields.append('usd_pricing')
            
            raw_meta = {
                'router_address': tx.get('to'),
                'price_source': price_source,
                'missing_fields': list(set(missing_fields)),
                'direction_basis': 'stablecoin_rule' if (self.is_stablecoin(token_in_addr, chain) or self.is_stablecoin(token_out_addr, chain)) else 'token_out_rule',
                'provider_paths': ['web3_transfer_delta'],
                'transfer_count': len(transfers)
            }
            
            swap = ClassifiedSwap(
                transaction_hash=tx_hash,
                block_number=tx_receipt['blockNumber'],
                block_timestamp=block_ts,
                chain=chain,
                dex='1inch_v6',
                token_in_address=token_in_addr,
                token_out_address=token_out_addr,
                token_in_symbol=(token_in_info['symbol'] if token_in_info else None),
                token_out_symbol=(token_out_info['symbol'] if token_out_info else None),
                token_in_decimals=token_in_decimals,
                token_out_decimals=token_out_decimals,
                amount_in=raw_amount_in,
                amount_out=raw_amount_out,
                classification=classification,
                confidence_score=confidence,
                sender_address=sender,
                recipient_address=tx.get('to'),
                gas_used=tx_receipt.get('gasUsed'),
                gas_price=Decimal(str(tx.get('gasPrice', 0)) if tx.get('gasPrice') is not None else '0'),
                raw_log_data=raw_meta,
                classification_method='1inch_transfer_delta'
            )
            if amount_in_usd is not None:
                swap.amount_in_usd = amount_in_usd
            return swap
        except Exception as e:
            logger.error(f"Failed to classify 1inch swap: {e}")
            return ClassifiedSwap(
                transaction_hash=tx_hash,
                block_number=0,
                block_timestamp=datetime.now(tz=timezone.utc),
                chain=chain,
                dex='1inch_v6',
                token_in_address='',
                token_out_address='',
                classification='UNKNOWN',
                confidence_score=0.0,
                raw_log_data={'missing_fields': ['1inch_decode_failed'], 'provider_paths': ['web3_transfer_delta']},
                classification_method='1inch_transfer_delta'
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
                float(swap.amount_in_usd) if swap.amount_in_usd else 0.0,
                swap.chain
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
        # Enforce scope: ethereum and polygon only
        if chain not in ('ethereum', 'polygon'):
            raise ValueError(f"Unsupported chain for this pipeline: {chain}")
        if dex == 'jupiter':
            raise ValueError("Unsupported DEX in scope: jupiter")
        
        # Route to appropriate classifier based on DEX
        if dex == 'uniswap_v2':
            swap = await classifier.classify_uniswap_v2_swap(log_data, chain)
        elif dex == 'uniswap_v3':
            swap = await classifier.classify_uniswap_v3_swap(log_data, chain)
        elif dex == 'balancer_v2':
            swap = await classifier.classify_balancer_v2_swap(log_data, chain)
        elif dex == 'curve':
            swap = await classifier.classify_curve_swap(log_data, chain)
        else:
            raise ValueError(f"Unsupported DEX: {dex}")
        
        # Enrich with price data only if token_in_decimals known and price present
        if swap.token_in_address and swap.token_in_decimals is not None:
            token_price = await classifier.get_token_price_coingecko(
                swap.token_in_address, chain
            )
            if token_price and swap.amount_in:
                token_amount = swap.amount_in / (10 ** swap.token_in_decimals)
                swap.amount_in_usd = token_amount * token_price
        
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
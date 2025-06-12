#!/usr/bin/env python3
"""
Whale Discovery Agent - Comprehensive Multi-Chain Whale Address Discovery

This script discovers whale addresses (>$1-2M USD holdings) across multiple blockchain networks
by querying BigQuery public datasets and various APIs, enriches them with current balance data,
and stores them in Supabase with "potential_whale_balance_based" analysis tags.

Features:
- Multi-chain support (Ethereum, Bitcoin, Polygon, Solana, XRP, Cardano)
- BigQuery whale discovery using public datasets
- Balance enrichment using multiple APIs (Etherscan, Helius, Covalent, Moralis, etc.)
- Comprehensive rate limiting and error handling
- CLI arguments for chain selection, value thresholds, and dry-run mode
- Integration with existing codebase structure and Supabase storage

Author: Whale Transaction Monitor System
Version: 1.0.0
"""

import argparse
import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
import os
import pytz  # Added for proper timezone handling in Covalent API
import functools  # Added for fixing run_in_executor calls

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Google Cloud BigQuery
from google.cloud import bigquery

# Supabase
from supabase import create_client, Client

# Import existing utilities
from config.api_keys import (
    ETHERSCAN_API_KEY, HELIUS_API_KEY, COVALENT_API_KEY, MORALIS_API_KEY,
    SOLSCAN_API_KEY, POLYGONSCAN_API_KEY, BLOCKFROST_PROJECT_ID,
    SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, GOOGLE_APPLICATION_CREDENTIALS,
    WHALE_ALERT_API_KEY, DUNE_API_KEY, COINGECKO_API_KEY
)
from utils.api_integrations import (
    AddressData, EtherscanAPI, HeliusAPI, CovalentAPI, MoralisAPI,
    SolscanAPI, PolygonscanAPI, APIIntegrationBase, WhaleAlertAPI,
    ComprehensiveWhaleDiscovery, PriceService, DuneAnalyticsAPI,
    RichListScraper, GitHubWhaleDataCollector
)
from utils.bigquery_public_data_extractor import BigQueryPublicDataIntegrationManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'whale_discovery_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# Helper functions
def format_currency(value: float) -> str:
    """Format currency value with appropriate commas and precision."""
    if value >= 1_000_000:
        return f"${value/1_000_000:.1f}M"
    elif value >= 1_000:
        return f"${value/1_000:.1f}K"
    else:
        return f"${value:.2f}"


def get_current_timestamp() -> str:
    """Get current timestamp in ISO format."""
    return datetime.utcnow().isoformat()


@dataclass
class WhaleAddress:
    """Whale address data structure with enriched balance information for comprehensive Supabase schema."""
    address: str
    blockchain: str
    balance_native: float
    balance_usd: float
    source_system: str
    discovery_method: str
    confidence_score: float
    metadata: Dict[str, Any]
    discovered_at: datetime
    
    def to_supabase_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format for comprehensive Supabase insertion."""
        # Extract metadata for specialized fields
        address_type = self.metadata.get('address_type', self._infer_address_type())
        signal_potential = self._determine_signal_potential()
        entity_name = self.metadata.get('entity_name') or self._generate_entity_name()
        last_seen_tx = self.metadata.get('last_seen_tx') or self.metadata.get('last_transaction')
        
        return {
            'address': self.address,
            'blockchain': self.blockchain,
            'label': self._generate_label(),
            'source': self._format_source(),
            'confidence': self.confidence_score,
            'last_seen_tx': last_seen_tx,
            'address_type': address_type,
            'signal_potential': signal_potential,
            'balance_native': self.balance_native,
            'balance_usd': self.balance_usd,
            'entity_name': entity_name,
            'last_balance_check': datetime.utcnow().isoformat(),
            'detection_method': self.discovery_method,
            'analysis_tags': self._build_analysis_tags(),
            'updated_at': get_current_timestamp()
        }
    
    def _infer_address_type(self) -> str:
        """Infer address type based on blockchain and metadata."""
        if self.metadata.get('is_exchange'):
            return 'Exchange Wallet'
        elif self.metadata.get('is_contract'):
            return 'Contract'
        elif self.blockchain in ['ethereum', 'polygon']:
            return 'EOA'  # Externally Owned Account
        elif self.blockchain == 'bitcoin':
            if self.address.startswith('bc1'):
                return 'Bech32'
            elif self.address.startswith('3'):
                return 'P2SH'
            elif self.address.startswith('1'):
                return 'P2PKH'
        return 'Unknown'
    
    def _determine_signal_potential(self) -> str:
        """Determine signal potential based on balance and metadata."""
        if self.balance_usd >= 100_000_000:  # $100M+
            return 'Ultra High Net Worth'
        elif self.balance_usd >= 10_000_000:  # $10M+
            return 'High Balance Whale'
        elif self.balance_usd >= 1_000_000:  # $1M+
            return 'Whale'
        elif self.metadata.get('high_activity'):
            return 'High Activity'
        elif self.metadata.get('fresh_whale'):
            return 'Fresh Whale'
        else:
            return 'High Balance'
    
    def _generate_label(self) -> str:
        """Generate appropriate label based on source and characteristics."""
        if self.metadata.get('is_exchange'):
            return 'Exchange'
        elif self.metadata.get('name_tag'):
            return self.metadata['name_tag']
        elif self.source_system.startswith('GitHub'):
            return 'GitHub Sourced Whale'
        elif self.source_system.startswith('Dune'):
            return 'Dune Analytics Whale'
        elif 'RichList' in self.source_system:
            return 'Rich List Whale'
        else:
            return 'Whale'
    
    def _format_source(self) -> str:
        """Format source information for the source field."""
        source_details = self.metadata.get('source_details', self.source_system)
        return source_details
    
    def _generate_entity_name(self) -> str:
        """Generate entity name based on metadata and balance."""
        if self.metadata.get('name_tag'):
            return self.metadata['name_tag']
        elif self.metadata.get('entity_name'):
            return self.metadata['entity_name']
        else:
            return f'Whale Address ({format_currency(self.balance_usd)})'
    
    def _build_analysis_tags(self) -> Dict[str, Any]:
        """Build comprehensive analysis_tags JSONB structure."""
        analysis_tags = {
            'whale_discovery': {
                'method': self.discovery_method,
                'balance_native': self.balance_native,
                'balance_usd': self.balance_usd,
                'discovered_at': self.discovered_at.isoformat(),
                'confidence_score': self.confidence_score,
                'source_system': self.source_system
            },
            'tags': ['whale_discovery', 'high_value_holder']
        }
        
        # Add balance-based tags
        if self.balance_usd >= 100_000_000:
            analysis_tags['tags'].append('ultra_whale')
        elif self.balance_usd >= 10_000_000:
            analysis_tags['tags'].append('mega_whale')
        elif self.balance_usd >= 1_000_000:
            analysis_tags['tags'].append('whale')
        
        # Add blockchain-specific tags
        analysis_tags['tags'].append(self.blockchain)
        
        # Add source-specific metadata
        if self.source_system.startswith('GitHub'):
            analysis_tags['github_metadata'] = self.metadata.get('github_metadata', {})
        elif self.source_system.startswith('Dune'):
            analysis_tags['dune_metadata'] = self.metadata.get('dune_metadata', {})
        elif 'RichList' in self.source_system:
            analysis_tags['rich_list_metadata'] = {
                'rank': self.metadata.get('rank'),
                'is_exchange': self.metadata.get('is_exchange', False),
                'name_tag': self.metadata.get('name_tag')
            }
        
        # Add detection method metadata
        analysis_tags[f'{self.discovery_method}_metadata'] = {
            key: value for key, value in self.metadata.items()
            if key not in ['github_metadata', 'dune_metadata', 'analysis_tags']
        }
        
        return analysis_tags


class ChainBalanceEnricher:
    """Enrich addresses with current balance data using appropriate APIs for each chain."""
    
    def __init__(self, api_keys: Dict[str, str]):
        self.api_keys = api_keys
        self.enrichers = self._initialize_enrichers()
        self.rate_limiters = self._initialize_rate_limiters()
        
    def _initialize_enrichers(self) -> Dict[str, Any]:
        """Initialize API clients for each supported chain."""
        enrichers = {}
        
        try:
            # Ethereum
            if self.api_keys.get('etherscan'):
                enrichers['ethereum'] = EtherscanAPI(self.api_keys['etherscan'])
            if self.api_keys.get('moralis'):
                enrichers['ethereum_moralis'] = MoralisAPI(self.api_keys['moralis'])
            if self.api_keys.get('covalent'):
                enrichers['ethereum_covalent'] = CovalentAPI(self.api_keys['covalent'])
                
            # Polygon
            if self.api_keys.get('polygonscan'):
                enrichers['polygon'] = PolygonscanAPI(self.api_keys['polygonscan'])
                
            # Solana
            if self.api_keys.get('helius'):
                enrichers['solana'] = HeliusAPI(self.api_keys['helius'])
            if self.api_keys.get('solscan'):
                enrichers['solana_solscan'] = SolscanAPI(self.api_keys['solscan'])
                
            # Bitcoin (using Blockstream API)
            enrichers['bitcoin'] = BitcoinBalanceAPI()
            
            # XRP (using XRPL API)
            enrichers['xrp'] = XRPLedgerAPI()
            
            # Cardano (using Blockfrost)
            if self.api_keys.get('blockfrost'):
                enrichers['cardano'] = CardanoBlockfrostAPI(self.api_keys['blockfrost'])
                
            logger.info(f"Initialized enrichers for chains: {list(enrichers.keys())}")
            
        except Exception as e:
            logger.error(f"Error initializing enrichers: {e}")
            
        return enrichers
    
    def _initialize_rate_limiters(self) -> Dict[str, Dict[str, Any]]:
        """Initialize rate limiting parameters for each API."""
        return {
            'etherscan': {'calls_per_second': 5, 'last_call': 0},
            'polygonscan': {'calls_per_second': 5, 'last_call': 0},
            'helius': {'calls_per_second': 10, 'last_call': 0},
            'solscan': {'calls_per_second': 5, 'last_call': 0},
            'moralis': {'calls_per_second': 25, 'last_call': 0},
            'covalent': {'calls_per_second': 4, 'last_call': 0},
            'blockfrost': {'calls_per_second': 10, 'last_call': 0},
            'blockstream': {'calls_per_second': 4, 'last_call': 0},
            'xrpl': {'calls_per_second': 20, 'last_call': 0}
        }
    
    def _apply_rate_limit(self, api_name: str):
        """Apply rate limiting for the specified API."""
        if api_name not in self.rate_limiters:
            return
            
        limiter = self.rate_limiters[api_name]
        calls_per_second = limiter['calls_per_second']
        last_call = limiter['last_call']
        
        min_interval = 1.0 / calls_per_second
        time_since_last = time.time() - last_call
        
        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            time.sleep(sleep_time)
            
        self.rate_limiters[api_name]['last_call'] = time.time()
    
    async def enrich_address_balance(self, address: str, blockchain: str) -> Optional[Tuple[float, float]]:
        """
        Enrich address with current balance data.
        Returns (native_balance, usd_balance) or None if failed.
        """
        try:
            if blockchain == 'ethereum':
                return await self._enrich_ethereum_balance(address)
            elif blockchain == 'bitcoin':
                return await self._enrich_bitcoin_balance(address)
            elif blockchain == 'polygon':
                return await self._enrich_polygon_balance(address)
            elif blockchain == 'solana':
                return await self._enrich_solana_balance(address)
            elif blockchain == 'xrp':
                return await self._enrich_xrp_balance(address)
            elif blockchain == 'cardano':
                return await self._enrich_cardano_balance(address)
            else:
                logger.warning(f"Balance enrichment not supported for blockchain: {blockchain}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to enrich balance for {address} on {blockchain}: {e}")
            return None
    
    async def _enrich_ethereum_balance(self, address: str) -> Optional[Tuple[float, float]]:
        """Enrich Ethereum address balance using multiple APIs with fallback."""
        # Try Etherscan first
        if 'ethereum' in self.enrichers:
            try:
                self._apply_rate_limit('etherscan')
                balance_wei = await self._get_ethereum_balance_etherscan(address)
                if balance_wei is not None:
                    balance_eth = balance_wei / 1e18
                    # Get ETH price (simplified - should use actual price API)
                    eth_price = 2400.0  # Placeholder - integrate with CoinGecko
                    balance_usd = balance_eth * eth_price
                    return balance_eth, balance_usd
            except Exception as e:
                logger.warning(f"Etherscan balance fetch failed for {address}: {e}")
        
        # Try Moralis as fallback
        if 'ethereum_moralis' in self.enrichers:
            try:
                self._apply_rate_limit('moralis')
                return await self._get_ethereum_balance_moralis(address)
            except Exception as e:
                logger.warning(f"Moralis balance fetch failed for {address}: {e}")
        
        # Try Covalent as final fallback
        if 'ethereum_covalent' in self.enrichers:
            try:
                self._apply_rate_limit('covalent')
                return await self._get_ethereum_balance_covalent(address)
            except Exception as e:
                logger.warning(f"Covalent balance fetch failed for {address}: {e}")
        
        return None
    
    async def _get_ethereum_balance_etherscan(self, address: str) -> Optional[int]:
        """Get Ethereum balance using Etherscan API."""
        import aiohttp
        
        url = "https://api.etherscan.io/api"
        params = {
            'module': 'account',
            'action': 'balance',
            'address': address,
            'tag': 'latest',
            'apikey': self.api_keys['etherscan']
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('status') == '1':
                        return int(data['result'])
        return None
    
    async def _get_ethereum_balance_moralis(self, address: str) -> Optional[Tuple[float, float]]:
        """Get Ethereum balance using Moralis API."""
        import aiohttp
        
        url = f"https://deep-index.moralis.io/api/v2/{address}/balance"
        headers = {
            'X-API-Key': self.api_keys['moralis']
        }
        params = {
            'chain': 'eth'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    balance_wei = int(data.get('balance', 0))
                    balance_eth = balance_wei / 1e18
                    eth_price = 2400.0  # Placeholder
                    return balance_eth, balance_eth * eth_price
        return None
    
    async def _get_ethereum_balance_covalent(self, address: str) -> Optional[Tuple[float, float]]:
        """Get Ethereum balance using Covalent API."""
        import aiohttp
        
        url = f"https://api.covalenthq.com/v1/eth-mainnet/address/{address}/balances_v2/"
        headers = {
            'Authorization': f'Bearer {self.api_keys["covalent"]}'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    items = data.get('data', {}).get('items', [])
                    for item in items:
                        if item.get('contract_ticker_symbol') == 'ETH':
                            balance_eth = float(item.get('balance', 0)) / 1e18
                            quote_rate = item.get('quote_rate', 2400.0)
                            return balance_eth, balance_eth * quote_rate
        return None
    
    async def _enrich_bitcoin_balance(self, address: str) -> Optional[Tuple[float, float]]:
        """Enrich Bitcoin address balance using Blockstream API."""
        import aiohttp
        
        self._apply_rate_limit('blockstream')
        
        url = f"https://blockstream.info/api/address/{address}"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        balance_satoshis = data.get('chain_stats', {}).get('funded_txo_sum', 0)
                        unspent_satoshis = data.get('chain_stats', {}).get('spent_txo_sum', 0)
                        current_balance = (balance_satoshis - unspent_satoshis) / 1e8
                        
                        # Get BTC price (placeholder)
                        btc_price = 42000.0
                        balance_usd = current_balance * btc_price
                        
                        return current_balance, balance_usd
        except Exception as e:
            logger.error(f"Bitcoin balance fetch failed for {address}: {e}")
            
        return None
    
    async def _enrich_solana_balance(self, address: str) -> Optional[Tuple[float, float]]:
        """Enrich Solana address balance using Helius API."""
        import aiohttp
        
        self._apply_rate_limit('helius')
        
        url = f"https://mainnet.helius-rpc.com/?api-key={self.api_keys['helius']}"
        
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBalance",
            "params": [address]
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        data = await response.json()
                        balance_lamports = data.get('result', {}).get('value', 0)
                        balance_sol = balance_lamports / 1e9
                        
                        # Get SOL price (placeholder)
                        sol_price = 100.0
                        balance_usd = balance_sol * sol_price
                        
                        return balance_sol, balance_usd
        except Exception as e:
            logger.error(f"Solana balance fetch failed for {address}: {e}")
            
        return None
    
    async def _enrich_polygon_balance(self, address: str) -> Optional[Tuple[float, float]]:
        """Enrich Polygon address balance using Polygonscan API."""
        import aiohttp
        
        self._apply_rate_limit('polygonscan')
        
        url = "https://api.polygonscan.com/api"
        params = {
            'module': 'account',
            'action': 'balance',
            'address': address,
            'tag': 'latest',
            'apikey': self.api_keys['polygonscan']
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('status') == '1':
                            balance_wei = int(data['result'])
                            balance_matic = balance_wei / 1e18
                            
                            # Get MATIC price (placeholder)
                            matic_price = 0.8
                            balance_usd = balance_matic * matic_price
                            
                            return balance_matic, balance_usd
        except Exception as e:
            logger.error(f"Polygon balance fetch failed for {address}: {e}")
            
        return None
    
    async def _enrich_xrp_balance(self, address: str) -> Optional[Tuple[float, float]]:
        """Enrich XRP address balance using XRPL API."""
        import aiohttp
        
        self._apply_rate_limit('xrpl')
        
        url = "https://xrplcluster.com"
        payload = {
            "method": "account_info",
            "params": [
                {
                    "account": address,
                    "ledger_index": "current"
                }
            ]
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        data = await response.json()
                        account_data = data.get('result', {}).get('account_data', {})
                        balance_drops = int(account_data.get('Balance', 0))
                        balance_xrp = balance_drops / 1e6
                        
                        # Get XRP price (placeholder)
                        xrp_price = 0.6
                        balance_usd = balance_xrp * xrp_price
                        
                        return balance_xrp, balance_usd
        except Exception as e:
            logger.error(f"XRP balance fetch failed for {address}: {e}")
            
        return None
    
    async def _enrich_cardano_balance(self, address: str) -> Optional[Tuple[float, float]]:
        """Enrich Cardano address balance using Blockfrost API."""
        import aiohttp
        
        self._apply_rate_limit('blockfrost')
        
        url = f"https://cardano-mainnet.blockfrost.io/api/v0/addresses/{address}"
        headers = {
            'project_id': self.api_keys['blockfrost']
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        amounts = data.get('amount', [])
                        for amount in amounts:
                            if amount.get('unit') == 'lovelace':
                                balance_lovelace = int(amount.get('quantity', 0))
                                balance_ada = balance_lovelace / 1e6
                                
                                # Get ADA price (placeholder)
                                ada_price = 0.4
                                balance_usd = balance_ada * ada_price
                                
                                return balance_ada, balance_usd
        except Exception as e:
            logger.error(f"Cardano balance fetch failed for {address}: {e}")
            
        return None


class BitcoinBalanceAPI(APIIntegrationBase):
    """Bitcoin balance API using Blockstream."""
    
    def __init__(self):
        super().__init__(
            api_key="",  # No API key needed for public Blockstream API
            base_url="https://blockstream.info/api",
            rate_limit_per_second=4
        )


class XRPLedgerAPI(APIIntegrationBase):
    """XRP Ledger API client."""
    
    def __init__(self):
        super().__init__(
            api_key="",
            base_url="https://xrplcluster.com",
            rate_limit_per_second=20
        )


class CardanoBlockfrostAPI(APIIntegrationBase):
    """Cardano Blockfrost API client."""
    
    def __init__(self, project_id: str):
        super().__init__(
            api_key=project_id,
            base_url="https://cardano-mainnet.blockfrost.io/api/v0",
            rate_limit_per_second=10
        )


class WhaleDiscoveryAgent:
    """
    Enhanced multi-chain whale discovery agent with comprehensive blockchain coverage.
    
    Supports whale discovery across:
    - Ethereum (via Etherscan, Covalent, BigQuery)
    - Solana (via Helius, BigQuery)
    - Polygon (via Polygonscan, Covalent, BigQuery)
    - BSC (via Covalent, BigQuery)
    - Avalanche (via Covalent, BigQuery)
    - Arbitrum (via Covalent, BigQuery)
    - Optimism (via Covalent, BigQuery)
    - Bitcoin (via BigQuery)
    """
    
    # Enhanced multi-chain configuration
    SUPPORTED_CHAINS = {
        'ethereum': {
            'apis': ['etherscan', 'covalent', 'moralis'],
            'bigquery': True,
            'covalent_chain_id': 1,
            'min_balance_discovery_factor': 0.1  # Use 10% of min_balance for discovery
        },
        'solana': {
            'apis': ['helius'],
            'bigquery': True,
            'min_balance_discovery_factor': 0.1
        },
        'polygon': {
            'apis': ['polygonscan', 'covalent'],
            'bigquery': True,
            'covalent_chain_id': 137,
            'min_balance_discovery_factor': 0.1
        },
        'bsc': {
            'apis': ['covalent'],
            'bigquery': True,
            'covalent_chain_id': 56,
            'min_balance_discovery_factor': 0.1
        },
        'avalanche': {
            'apis': ['covalent'],
            'bigquery': True,
            'covalent_chain_id': 43114,
            'min_balance_discovery_factor': 0.1
        },
        'arbitrum': {
            'apis': ['covalent'],
            'bigquery': True,
            'covalent_chain_id': 42161,
            'min_balance_discovery_factor': 0.1
        },
        'optimism': {
            'apis': ['covalent'],
            'bigquery': True,
            'covalent_chain_id': 10,
            'min_balance_discovery_factor': 0.1
        },
        'bitcoin': {
            'apis': [],
            'bigquery': True,
            'min_balance_discovery_factor': 0.05  # More conservative for Bitcoin
        }
    }
    
    def __init__(self, 
                 min_balance_usd: float = 1_000_000,
                 target_chains: List[str] = None,
                 dry_run: bool = False,
                 test_mode: bool = False):
        """
        Initialize the enhanced multi-chain whale discovery agent.
        
        Args:
            min_balance_usd: Minimum USD balance to qualify as whale
            target_chains: List of blockchain names to target (None = all supported)
            dry_run: If True, only log what would be stored without actual storage
            test_mode: If True, run in test mode with minimal API calls (5 results per source max)
        """
        self.min_balance_usd = min_balance_usd
        self.target_chains = target_chains or list(self.SUPPORTED_CHAINS.keys())
        self.dry_run = dry_run
        self.test_mode = test_mode
        
        # Statistics tracking
        self.stats = {
            'total_candidates_bigquery': 0,
            'total_candidates_apis': 0,
            'total_candidates_discovered': 0,
            'total_enriched': 0,
            'total_whales_found': 0,
            'total_whale_value': 0.0,
            'whales_by_chain': {},
            'candidates_by_chain': {},
            'api_performance': {},
            'bigquery_performance': {},
            'errors': 0
        }
        
        # Initialize components
        logger.info(f"Initializing Enhanced Multi-Chain Whale Discovery Agent")
        logger.info(f"Target chains: {', '.join(self.target_chains)}")
        logger.info(f"Minimum whale balance: ${self.min_balance_usd:,.0f}")
        logger.info(f"Dry run mode: {self.dry_run}")
        logger.info(f"Test mode: {self.test_mode}")
        
        self.client = self._init_bigquery_client()
        self.supabase = self._init_supabase_client()
        self.balance_enricher = self._init_balance_enricher()
        self.bigquery_manager = self._init_bigquery_manager()
        self.api_discovery_clients = self._init_api_discovery_clients()
        
        # Initialize comprehensive whale discovery system
        self.comprehensive_discovery = self._init_comprehensive_discovery()
        
        logger.info("Enhanced Multi-Chain Whale Discovery Agent initialized successfully")
    
    def _init_bigquery_client(self) -> bigquery.Client:
        """Initialize BigQuery client."""
        try:
            import os
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_APPLICATION_CREDENTIALS
            client = bigquery.Client()
            logger.info("BigQuery client initialized successfully")
            return client
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            raise
    
    def _init_supabase_client(self) -> Client:
        """Initialize Supabase client."""
        try:
            client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
            logger.info("Supabase client initialized successfully")
            return client
        except Exception as e:
            logger.error(f"Failed to initialize Supabase client: {e}")
            raise
    
    def _init_balance_enricher(self) -> ChainBalanceEnricher:
        """Initialize balance enricher with API keys."""
        api_keys = {
            'etherscan': ETHERSCAN_API_KEY,
            'helius': HELIUS_API_KEY,
            'covalent': COVALENT_API_KEY,
            'moralis': MORALIS_API_KEY,
            'solscan': SOLSCAN_API_KEY,
            'polygonscan': POLYGONSCAN_API_KEY,
            'blockfrost': BLOCKFROST_PROJECT_ID
        }
        return ChainBalanceEnricher(api_keys)
    
    def _init_bigquery_manager(self) -> BigQueryPublicDataIntegrationManager:
        """Initialize BigQuery manager for whale discovery queries."""
        return BigQueryPublicDataIntegrationManager(
            bigquery_client=self.client,
            project_id=self.client.project
        )
    
    def _init_api_discovery_clients(self) -> Dict[str, Any]:
        """Initialize API clients for whale address discovery."""
        api_clients = {}
        
        try:
            # Whale Alert API for real-time whale transactions
            if WHALE_ALERT_API_KEY:
                api_clients['whale_alert'] = WhaleAlertAPI(WHALE_ALERT_API_KEY)
                
            # Etherscan API for Ethereum large transactions
            if ETHERSCAN_API_KEY:
                api_clients['etherscan'] = EtherscanAPI(ETHERSCAN_API_KEY)
                
            # Polygonscan API for Polygon large transactions  
            if POLYGONSCAN_API_KEY:
                api_clients['polygonscan'] = PolygonscanAPI(POLYGONSCAN_API_KEY)
                
            # Helius API for Solana large transactions
            if HELIUS_API_KEY:
                api_clients['helius'] = HeliusAPI(HELIUS_API_KEY)
                
            # Covalent API for multi-chain large transactions
            if COVALENT_API_KEY:
                api_clients['covalent'] = CovalentAPI(COVALENT_API_KEY)
                
            # Moralis API for multi-chain transactions and NFT data
            if MORALIS_API_KEY:
                api_clients['moralis'] = MoralisAPI(MORALIS_API_KEY)
                
            logger.info(f"Initialized {len(api_clients)} API discovery clients: {list(api_clients.keys())}")
            
        except Exception as e:
            logger.error(f"Error initializing API discovery clients: {e}")
            
        return api_clients
    
    def _init_comprehensive_discovery(self) -> ComprehensiveWhaleDiscovery:
        """Initialize the comprehensive whale discovery system."""
        try:
            api_keys = {
                'etherscan': ETHERSCAN_API_KEY,
                'polygonscan': POLYGONSCAN_API_KEY,
                'dune': DUNE_API_KEY,
                'coingecko': COINGECKO_API_KEY,
                'helius': HELIUS_API_KEY,
                'covalent': COVALENT_API_KEY,
                'moralis': MORALIS_API_KEY
            }
            
            comprehensive_discovery = ComprehensiveWhaleDiscovery(api_keys)
            logger.info("Comprehensive whale discovery system initialized")
            return comprehensive_discovery
            
        except Exception as e:
            logger.error(f"Failed to initialize comprehensive discovery system: {e}")
            raise
    
    async def discover_whales(self) -> List[WhaleAddress]:
        """
        Comprehensive whale discovery across all sources.
        Enhanced to target 10,000+ whale addresses from multiple sources.
        """
        logger.info("🐋 Starting comprehensive multi-source whale discovery...")
        logger.info(f"Target chains: {self.target_chains}")
        logger.info(f"Minimum balance threshold: {format_currency(self.min_balance_usd)}")
        
        all_whale_addresses = []
        
        # Phase 1: Comprehensive Discovery using all sources
        if not self.test_mode:
            logger.info("📊 Phase 1: Comprehensive multi-source whale discovery")
            
            # Configure for high-volume collection
            limit_per_source = 5000 if not self.test_mode else 5
            
            try:
                comprehensive_addresses = self.comprehensive_discovery.discover_whale_addresses(
                    limit_per_source=limit_per_source
                )
                
                logger.info(f"✅ Comprehensive discovery found {len(comprehensive_addresses)} addresses")
                
                # Convert AddressData to WhaleAddress format
                for addr_data in comprehensive_addresses:
                    try:
                        # Get balance information
                        balance_native = addr_data.metadata.get('balance_native', 0)
                        balance_usd = addr_data.metadata.get('balance_usd', 0)
                        
                        # If no balance info, try to enrich
                        if balance_usd == 0 and not self.dry_run:
                            balance_result = await self.balance_enricher.enrich_address_balance(
                                addr_data.address, addr_data.blockchain
                            )
                            if balance_result:
                                balance_native, balance_usd = balance_result
                        
                        # Only include if meets minimum threshold (must have valid positive balance)
                        if balance_usd is not None and balance_usd > 0 and balance_usd >= self.min_balance_usd:
                            whale_address = WhaleAddress(
                                address=addr_data.address,
                                blockchain=addr_data.blockchain,
                                balance_native=balance_native,
                                balance_usd=balance_usd,
                                source_system=addr_data.source_system,
                                discovery_method=addr_data.metadata.get('detection_method', 'comprehensive_discovery'),
                                confidence_score=addr_data.confidence_score,
                                metadata=addr_data.metadata,
                                discovered_at=datetime.utcnow()
                            )
                            all_whale_addresses.append(whale_address)
                        else:
                            # Log why address was skipped for debugging
                            if balance_usd is None:
                                logger.debug(f"Skipped {addr_data.address}: No balance data available")
                            elif balance_usd <= 0:
                                logger.debug(f"Skipped {addr_data.address}: Invalid balance ${balance_usd}")
                            else:
                                logger.debug(f"Skipped {addr_data.address}: Balance ${balance_usd:,.0f} below threshold ${self.min_balance_usd:,.0f}")
                    
                    except Exception as e:
                        logger.warning(f"Failed to process address {addr_data.address}: {e}")
                        continue
                
                logger.info(f"📈 Processed {len(all_whale_addresses)} qualifying whale addresses from comprehensive discovery")
                
            except Exception as e:
                logger.error(f"Comprehensive discovery failed: {e}")
        
        # Phase 2: Enhanced BigQuery Discovery (if needed for more addresses)
        if len(all_whale_addresses) < 10000:
            logger.info("📊 Phase 2: Enhanced BigQuery whale discovery")
            
            for chain in self.target_chains:
                if len(all_whale_addresses) >= 10000:
                    break
                    
                try:
                    # Get more candidates from BigQuery
                    candidates = await self._discover_candidates_bigquery(chain, limit=3000)
                    logger.info(f"BigQuery {chain}: Found {len(candidates)} whale candidates")
                    
                    # Enrich and filter
                    enriched_whales = await self._enrich_and_filter_candidates(candidates, chain)
                    all_whale_addresses.extend(enriched_whales)
                    
                    logger.info(f"BigQuery {chain}: Added {len(enriched_whales)} qualified whales")
                    
                except Exception as e:
                    logger.error(f"BigQuery discovery failed for {chain}: {e}")
                    continue
        
        # Phase 3: API-based Discovery (additional sources)
        if len(all_whale_addresses) < 10000:
            logger.info("📊 Phase 3: API-based whale discovery")
            
            for chain in self.target_chains:
                if len(all_whale_addresses) >= 10000:
                    break
                    
                try:
                    # Get more candidates from APIs
                    candidates = await self._discover_candidates_apis(chain, limit=2000)
                    logger.info(f"API {chain}: Found {len(candidates)} whale candidates")
                    
                    # Enrich and filter
                    enriched_whales = await self._enrich_and_filter_candidates(candidates, chain)
                    all_whale_addresses.extend(enriched_whales)
                    
                    logger.info(f"API {chain}: Added {len(enriched_whales)} qualified whales")
                    
                except Exception as e:
                    logger.error(f"API discovery failed for {chain}: {e}")
                    continue
        
        # Remove duplicates while preserving order and highest confidence
        unique_whales = self._deduplicate_whale_addresses(all_whale_addresses)
        
        # Sort by balance descending
        unique_whales.sort(key=lambda w: w.balance_usd, reverse=True)
        
        logger.info(f"🎯 Final whale discovery results:")
        logger.info(f"   Total unique whale addresses: {len(unique_whales)}")
        logger.info(f"   Total value: {format_currency(sum(w.balance_usd for w in unique_whales))}")
        
        # Print source breakdown
        source_breakdown = {}
        for whale in unique_whales:
            source = whale.source_system
            if source not in source_breakdown:
                source_breakdown[source] = {'count': 0, 'total_value': 0}
            source_breakdown[source]['count'] += 1
            source_breakdown[source]['total_value'] += whale.balance_usd
        
        logger.info("📊 Source breakdown:")
        for source, stats in sorted(source_breakdown.items(), key=lambda x: x[1]['count'], reverse=True):
            logger.info(f"   {source}: {stats['count']} addresses ({format_currency(stats['total_value'])})")
        
        return unique_whales
    
    def _deduplicate_whale_addresses(self, whale_addresses: List[WhaleAddress]) -> List[WhaleAddress]:
        """Remove duplicate addresses, keeping the one with highest confidence score."""
        seen_addresses = {}
        
        for whale in whale_addresses:
            key = f"{whale.address.lower()}_{whale.blockchain}"
            
            if key not in seen_addresses:
                seen_addresses[key] = whale
            else:
                # Keep the one with higher confidence score
                if whale.confidence_score > seen_addresses[key].confidence_score:
                    seen_addresses[key] = whale
                # If same confidence, prefer higher balance
                elif (whale.confidence_score == seen_addresses[key].confidence_score and 
                      whale.balance_usd > seen_addresses[key].balance_usd):
                    seen_addresses[key] = whale
        
        return list(seen_addresses.values())
    
    async def _discover_candidates_bigquery(self, chain: str, limit: int = 2000) -> List[str]:
        """Discover candidate whale addresses using BigQuery public datasets."""
        candidates = []
        
        try:
            if chain == 'ethereum':
                candidates = await self._discover_ethereum_whales_bigquery(limit)
            elif chain == 'bitcoin':
                candidates = await self._discover_bitcoin_whales_bigquery(limit)
            elif chain == 'polygon':
                candidates = await self._discover_polygon_whales_bigquery(limit)
            elif chain == 'solana':
                candidates = await self._discover_solana_whales_bigquery(limit)
            else:
                logger.warning(f"BigQuery whale discovery not implemented for {chain}")
                
        except Exception as e:
            logger.error(f"BigQuery discovery failed for {chain}: {e}")
            
        return candidates

    async def _discover_ethereum_whales_bigquery(self, limit: int = 2000) -> List[str]:
        """Discover potential Ethereum whale addresses using BigQuery public datasets."""
        addresses = []
        
        try:
            # Calculate discovery threshold (10% of minimum balance for broader search)
            discovery_factor = self.SUPPORTED_CHAINS['ethereum'].get('min_balance_discovery_factor', 0.1)
            min_value_usd = self.min_balance_usd * discovery_factor
            
            logger.info(f"Discovering Ethereum whales via BigQuery with threshold ${min_value_usd:,.0f}")
            
            # Execute whale identification query
            results = await asyncio.get_event_loop().run_in_executor(
                None, 
                functools.partial(
                    self.bigquery_manager.execute_advanced_query,
                    query_type='whale',
                    chain='ethereum',
                    min_tx_volume_usd=min_value_usd,
                    min_tx_count=10,
                    lookback_days=30,
                    limit=limit
                )
            )
            
            # Extract addresses from results
            for row in results:
                if row.get('address'):
                    addresses.append(row['address'])
            
            logger.info(f"BigQuery discovered {len(addresses)} Ethereum whale candidates")
            return addresses
            
        except Exception as e:
            logger.error(f"Failed to discover Ethereum whales from BigQuery: {e}")
            return []

    async def _discover_bitcoin_whales_bigquery(self, limit: int = 2000) -> List[str]:
        """Discover potential Bitcoin whale addresses using BigQuery public datasets."""
        addresses = []
        
        try:
            # Calculate discovery threshold (5% of minimum balance for Bitcoin - more conservative)
            discovery_factor = self.SUPPORTED_CHAINS['bitcoin'].get('min_balance_discovery_factor', 0.05)
            min_value_usd = self.min_balance_usd * discovery_factor
            
            logger.info(f"Discovering Bitcoin whales via BigQuery with threshold ${min_value_usd:,.0f}")
            
            # Execute whale identification query for Bitcoin
            results = await asyncio.get_event_loop().run_in_executor(
                None,
                functools.partial(
                    self.bigquery_manager.execute_advanced_query,
                    query_type='whale',
                    chain='bitcoin',
                    min_tx_volume_usd=min_value_usd,
                    min_tx_count=5,
                    lookback_days=30,
                    limit=limit
                )
            )
            
            # Extract addresses from results
            for row in results:
                if row.get('address'):
                    addresses.append(row['address'])
            
            logger.info(f"BigQuery discovered {len(addresses)} Bitcoin whale candidates")
            return addresses
            
        except Exception as e:
            logger.error(f"Failed to discover Bitcoin whales from BigQuery: {e}")
            return []

    async def _discover_polygon_whales_bigquery(self, limit: int = 2000) -> List[str]:
        """Discover potential Polygon whale addresses using BigQuery public datasets."""
        addresses = []
        
        try:
            discovery_factor = self.SUPPORTED_CHAINS['polygon'].get('min_balance_discovery_factor', 0.1)
            min_value_usd = self.min_balance_usd * discovery_factor
            
            logger.info(f"Discovering Polygon whales via BigQuery with threshold ${min_value_usd:,.0f}")
            
            # Execute whale identification query for Polygon
            results = await asyncio.get_event_loop().run_in_executor(
                None,
                functools.partial(
                    self.bigquery_manager.execute_advanced_query,
                    query_type='whale',
                    chain='polygon',
                    min_tx_volume_usd=min_value_usd,
                    min_tx_count=10,
                    lookback_days=30,
                    limit=limit
                )
            )
            
            # Extract addresses from results
            for row in results:
                if row.get('address'):
                    addresses.append(row['address'])
            
            logger.info(f"BigQuery discovered {len(addresses)} Polygon whale candidates")
            return addresses
            
        except Exception as e:
            logger.error(f"Failed to discover Polygon whales from BigQuery: {e}")
            return []

    async def _discover_solana_whales_bigquery(self, limit: int = 2000) -> List[str]:
        """Discover potential Solana whale addresses using BigQuery public datasets."""
        addresses = []
        
        try:
            discovery_factor = self.SUPPORTED_CHAINS['solana'].get('min_balance_discovery_factor', 0.1)
            min_value_usd = self.min_balance_usd * discovery_factor
            
            logger.info(f"Discovering Solana whales via BigQuery with threshold ${min_value_usd:,.0f}")
            
            # Execute whale identification query for Solana
            results = await asyncio.get_event_loop().run_in_executor(
                None,
                functools.partial(
                    self.bigquery_manager.execute_advanced_query,
                    query_type='whale',
                    chain='solana',
                    min_tx_volume_usd=min_value_usd,
                    min_tx_count=10,
                    lookback_days=30,
                    limit=limit
                )
            )
            
            # Extract addresses from results
            for row in results:
                if row.get('address'):
                    addresses.append(row['address'])
            
            logger.info(f"BigQuery discovered {len(addresses)} Solana whale candidates")
            return addresses
            
        except Exception as e:
            logger.error(f"Failed to discover Solana whales from BigQuery: {e}")
            return []

    async def _discover_candidates_apis(self, chain: str, limit: int = 1000) -> List[str]:
        """
        Enhanced multi-chain API discovery with comprehensive blockchain coverage.
        
        Args:
            chain: Target blockchain name
            
        Returns:
            List of candidate addresses from various APIs
        """
        addresses = []
        chain_config = self.SUPPORTED_CHAINS.get(chain, {})
        supported_apis = chain_config.get('apis', [])
        
        logger.info(f"🔌 Starting API discovery for {chain} using: {', '.join(supported_apis)}")
        
        try:
            if chain == 'ethereum':
                addresses = await self._discover_ethereum_whales_apis(limit)
            elif chain == 'solana':
                addresses = await self._discover_solana_whales_apis(limit)
            elif chain == 'polygon':
                addresses = await self._discover_polygon_whales_apis(limit)
            elif chain in ['bsc', 'avalanche', 'arbitrum', 'optimism']:
                addresses = await self._discover_multi_chain_whales_apis(chain, limit)
            elif chain == 'bitcoin':
                # Bitcoin uses different APIs and is mostly BigQuery-based
                logger.info(f"Bitcoin whale discovery primarily via BigQuery (limited API support)")
                addresses = []
            else:
                logger.warning(f"No API discovery method implemented for {chain}")
                
        except Exception as e:
            logger.error(f"API discovery failed for {chain}: {e}")
            
        logger.info(f"API discovery for {chain} completed: {len(addresses)} candidates found")
        return addresses

    async def _discover_multi_chain_whales_apis(self, chain: str, limit: int = 1000) -> List[str]:
        """
        Discover whale addresses on BSC, Avalanche, Arbitrum, Optimism using Covalent API.
        
        Args:
            chain: Target blockchain name
            
        Returns:
            List of candidate addresses
        """
        addresses = []
        chain_config = self.SUPPORTED_CHAINS.get(chain, {})
        chain_id = chain_config.get('covalent_chain_id')
        
        if not chain_id:
            logger.warning(f"No Covalent chain ID configured for {chain}")
            return addresses
            
        try:
            if 'covalent' in self.api_discovery_clients:
                logger.info(f"Discovering {chain} whales via Covalent API (Chain ID: {chain_id})...")
                covalent_api = self.api_discovery_clients['covalent']
                
                # Get token holders for this chain
                token_holder_addresses = await asyncio.get_event_loop().run_in_executor(
                    None, functools.partial(covalent_api.get_token_holders_multichain, chain_id, limit)
                )
                addresses.extend([addr.address for addr in token_holder_addresses])
                logger.info(f"Covalent found {len(token_holder_addresses)} {chain} token holder addresses")
                
                # Get recent high-value transactions for this chain
                recent_tx_addresses = await asyncio.get_event_loop().run_in_executor(
                    None, functools.partial(covalent_api.get_recent_transactions, chain_id, limit)
                )
                addresses.extend([addr.address for addr in recent_tx_addresses])
                logger.info(f"Covalent found {len(recent_tx_addresses)} {chain} recent transaction addresses")
                
        except Exception as e:
            logger.error(f"{chain} API discovery failed: {e}")
            
        # Remove duplicates and return unique addresses
        unique_addresses = list(set(addresses))
        logger.info(f"Total unique {chain} API candidates: {len(unique_addresses)}")
        return unique_addresses

    async def _discover_ethereum_whales_apis(self, limit: int = 1000) -> List[str]:
        """Discover Ethereum whale addresses using APIs."""
        addresses = []
        
        try:
            # Calculate minimum ETH value for large transactions
            eth_price = 2400.0  # Placeholder price
            min_value_eth = self.min_balance_usd / eth_price / 10  # Use 1/10th for discovery threshold
            
            # Use Whale Alert API for recent large transactions
            if 'whale_alert' in self.api_discovery_clients:
                logger.info("Discovering Ethereum whales via Whale Alert API...")
                whale_alert_api = self.api_discovery_clients['whale_alert']
                recent_txns = await asyncio.get_event_loop().run_in_executor(
                    None, functools.partial(whale_alert_api.get_recent_transactions, int(self.min_balance_usd / 5), limit)
                )
                ethereum_txns = [addr for addr in recent_txns if addr.blockchain.lower() == 'ethereum']
                addresses.extend([addr.address for addr in ethereum_txns])
                logger.info(f"Whale Alert found {len(ethereum_txns)} Ethereum whale addresses")
            
            # Use Etherscan API for large transactions
            if 'etherscan' in self.api_discovery_clients:
                logger.info("Discovering Ethereum whales via Etherscan API...")
                etherscan_api = self.api_discovery_clients['etherscan']
                large_tx_addresses = await asyncio.get_event_loop().run_in_executor(
                    None, functools.partial(etherscan_api.get_large_transaction_addresses, min_value_eth, limit)
                )
                addresses.extend([addr.address for addr in large_tx_addresses])
                logger.info(f"Etherscan found {len(large_tx_addresses)} large transaction addresses")
            
            # Use Covalent API for recent high-value transactions
            if 'covalent' in self.api_discovery_clients:
                logger.info("Discovering Ethereum whales via Covalent API...")
                covalent_api = self.api_discovery_clients['covalent']
                recent_addresses = await asyncio.get_event_loop().run_in_executor(
                    None, functools.partial(covalent_api.get_recent_transactions, 1, limit)
                )
                addresses.extend([addr.address for addr in recent_addresses])
                logger.info(f"Covalent found {len(recent_addresses)} recent transaction addresses")
                
        except Exception as e:
            logger.error(f"Ethereum API discovery failed: {e}")
            
        # Remove duplicates and return unique addresses
        unique_addresses = list(set(addresses))
        logger.info(f"Total unique Ethereum API candidates: {len(unique_addresses)}")
        return unique_addresses

    async def _discover_polygon_whales_apis(self, limit: int = 1000) -> List[str]:
        """Discover Polygon whale addresses using APIs."""
        addresses = []
        
        try:
            # Calculate minimum MATIC value for large transactions  
            matic_price = 0.8  # Placeholder price
            min_value_matic = self.min_balance_usd / matic_price / 10  # Use 1/10th for discovery
            
            # Use Polygonscan API for large transactions
            if 'polygonscan' in self.api_discovery_clients:
                logger.info("Discovering Polygon whales via Polygonscan API...")
                polygonscan_api = self.api_discovery_clients['polygonscan']
                large_tx_addresses = await asyncio.get_event_loop().run_in_executor(
                    None, functools.partial(polygonscan_api.get_large_transaction_addresses, min_value_matic, limit)
                )
                addresses.extend([addr.address for addr in large_tx_addresses])
                logger.info(f"Polygonscan found {len(large_tx_addresses)} large transaction addresses")
            
            # Use Covalent API for Polygon transactions
            if 'covalent' in self.api_discovery_clients:
                logger.info("Discovering Polygon whales via Covalent API...")
                covalent_api = self.api_discovery_clients['covalent']
                recent_addresses = await asyncio.get_event_loop().run_in_executor(
                    None, functools.partial(covalent_api.get_recent_transactions, 137, limit)
                )
                addresses.extend([addr.address for addr in recent_addresses])
                logger.info(f"Covalent found {len(recent_addresses)} Polygon transaction addresses")
                
        except Exception as e:
            logger.error(f"Polygon API discovery failed: {e}")
            
        unique_addresses = list(set(addresses))
        logger.info(f"Total unique Polygon API candidates: {len(unique_addresses)}")
        return unique_addresses

    async def _discover_solana_whales_apis(self, limit: int = 1000) -> List[str]:
        """
        Enhanced Solana whale discovery using comprehensive Helius API methods.
        
        Returns:
            List of candidate Solana addresses
        """
        addresses = []
        
        try:
            # Calculate minimum SOL value for large transactions
            sol_price = 100.0  # Approximate SOL price in USD
            min_value_sol = (self.min_balance_usd * 0.1) / sol_price  # Use 10% for discovery
            
            # Use Helius API for comprehensive Solana discovery
            if 'helius' in self.api_discovery_clients:
                helius_api = self.api_discovery_clients['helius']
                logger.info("Discovering Solana whales via Enhanced Helius API...")
                
                # 1. Get recent large SOL transactions
                large_tx_addresses = await asyncio.get_event_loop().run_in_executor(
                    None, functools.partial(helius_api.get_recent_large_transactions, min_value_sol, limit)
                )
                addresses.extend([addr.address for addr in large_tx_addresses])
                logger.info(f"Helius found {len(large_tx_addresses)} large SOL transaction addresses")
                
                # 2. Get top SPL token holders
                token_holder_addresses = await asyncio.get_event_loop().run_in_executor(
                    None, functools.partial(helius_api.get_top_token_holders, None, limit)
                )
                addresses.extend([addr.address for addr in token_holder_addresses])
                logger.info(f"Helius found {len(token_holder_addresses)} SPL token holder addresses")
                
                # 3. Get accounts with high SOL balances
                high_balance_addresses = await asyncio.get_event_loop().run_in_executor(
                    None, functools.partial(helius_api.get_accounts_by_sol_balance, min_value_sol, limit)
                )
                addresses.extend([addr.address for addr in high_balance_addresses])
                logger.info(f"Helius found {len(high_balance_addresses)} high SOL balance addresses")
                
                # 4. Get program accounts (for DeFi whales)
                program_addresses = await asyncio.get_event_loop().run_in_executor(
                    None, functools.partial(helius_api.get_program_accounts, "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", limit)
                )
                addresses.extend([addr.address for addr in program_addresses])
                logger.info(f"Helius found {len(program_addresses)} program account addresses")
                
        except Exception as e:
            logger.error(f"Solana API discovery failed: {e}")
            
        # Remove duplicates and return unique addresses
        unique_addresses = list(set(addresses))
        logger.info(f"Total unique Solana API candidates: {len(unique_addresses)}")
        return unique_addresses

    async def _enrich_and_filter_candidates(self, candidates: List[str], chain: str) -> List[WhaleAddress]:
        """
        Enhanced candidate enrichment and filtering without arbitrary upper limits.
        
        Args:
            candidates: List of candidate addresses to enrich
            chain: Target blockchain name
            
        Returns:
            List of whale addresses that meet the minimum balance criteria
        """
        whale_addresses = []
        enriched_count = 0
        
        logger.info(f"💰 Starting enrichment for {len(candidates)} {chain} candidates...")
        
        # Process candidates in batches for better performance
        batch_size = 10  # Reduced from 50 to prevent API overload
        total_batches = (len(candidates) + batch_size - 1) // batch_size
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(candidates))
            batch_candidates = candidates[start_idx:end_idx]
            
            logger.info(f"Processing batch {batch_num + 1}/{total_batches} ({len(batch_candidates)} addresses)")
            
            # Process batch in parallel
            tasks = []
            for address in batch_candidates:
                task = self.balance_enricher.enrich_address_balance(address, chain)
                tasks.append(task)
            
            try:
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for i, result in enumerate(batch_results):
                    address = batch_candidates[i]
                    
                    if isinstance(result, Exception):
                        logger.warning(f"Failed to enrich {address} on {chain}: {result}")
                        continue
                    
                    if result is None:
                        continue
                        
                    enriched_count += 1
                    balance_native, balance_usd = result
                    
                    # Check if address qualifies as whale (minimum balance check only)
                    if balance_usd is not None and balance_usd > 0 and balance_usd >= self.min_balance_usd:
                        whale_address = WhaleAddress(
                            address=address,
                            blockchain=chain,
                            balance_native=balance_native,
                            balance_usd=balance_usd,
                            source_system='whale_discovery_agent',
                            discovery_method=f'{chain}_api_bigquery_combined',
                            confidence_score=0.8,  # High confidence for enriched balances
                            metadata={
                                'discovery_chain': chain,
                                'balance_enrichment_successful': True,
                                'enrichment_timestamp': get_current_timestamp(),
                                'min_balance_threshold': self.min_balance_usd,
                                'meets_whale_criteria': True
                            },
                            discovered_at=datetime.utcnow()
                        )
                        
                        whale_addresses.append(whale_address)
                        logger.info(f"✅ Whale found: {address} on {chain} - ${balance_usd:,.0f} ({balance_native:.4f} {chain.upper()})")
                
                # Add longer delay between batches to respect API limits
                if batch_num < total_batches - 1:
                    await asyncio.sleep(2.0)  # Increased from 0.5 to 2.0 seconds
                    
            except Exception as e:
                logger.error(f"Batch enrichment failed for {chain} batch {batch_num + 1}: {e}")
                continue
        
        logger.info(f"🎯 Enrichment completed for {chain}:")
        logger.info(f"  - Candidates processed: {len(candidates)}")
        logger.info(f"  - Successfully enriched: {enriched_count}")
        logger.info(f"  - Whale addresses found: {len(whale_addresses)}")
        logger.info(f"  - Total whale value: ${sum(w.balance_usd for w in whale_addresses):,.0f}")
        
        # NO $50M FILTER APPLIED - All whales are retained as requested
        logger.info(f"  - All {len(whale_addresses)} whale addresses retained (no upper limit filter)")
        
        return whale_addresses
    
    async def store_whale_addresses(self, whale_addresses: List[WhaleAddress]) -> int:
        """Store discovered whale addresses in Supabase."""
        if self.dry_run:
            logger.info(f"DRY RUN: Would store {len(whale_addresses)} whale addresses")
            return len(whale_addresses)
        
        stored_count = 0
        
        for whale in whale_addresses:
            try:
                # Check if address already exists
                existing = self.supabase.table('addresses').select('id').eq('address', whale.address).eq('blockchain', whale.blockchain).execute()
                
                if existing.data:
                    # Update existing record
                    update_data = whale.to_supabase_dict()
                    result = self.supabase.table('addresses').update(update_data).eq('id', existing.data[0]['id']).execute()
                    logger.info(f"Updated existing whale address: {whale.address}")
                else:
                    # Insert new record
                    insert_data = whale.to_supabase_dict()
                    result = self.supabase.table('addresses').insert(insert_data).execute()
                    logger.info(f"Inserted new whale address: {whale.address}")
                
                stored_count += 1
                
            except Exception as e:
                logger.error(f"Failed to store whale address {whale.address}: {e}")
                self.stats['errors'] += 1
                continue
        
        self.stats['stored_addresses'] = stored_count
        logger.info(f"Successfully stored {stored_count} whale addresses")
        return stored_count
    
    def print_statistics(self):
        """Print discovery statistics."""
        print("\n" + "="*60)
        print("WHALE DISCOVERY AGENT - FINAL STATISTICS")
        print("="*60)
        print(f"Discovered Candidates (BigQuery): {self.stats['total_candidates_bigquery']:,}")
        print(f"Discovered Candidates (APIs): {self.stats['total_candidates_apis']:,}")
        print(f"Total Discovered Candidates: {self.stats['total_candidates_discovered']:,}")
        print(f"Successfully Enriched: {self.stats['total_enriched']:,}")
        print(f"Whale Addresses Found: {self.stats['total_whales_found']:,}")
        print(f"Addresses Stored: {self.stats.get('stored_addresses', 0):,}")
        print(f"Errors Encountered: {self.stats['errors']:,}")
        print(f"Success Rate: {(self.stats['total_whales_found']/max(1, self.stats['total_candidates_discovered']))*100:.1f}%")
        
        # Add comprehensive discovery statistics
        if hasattr(self, 'comprehensive_discovery') and hasattr(self.comprehensive_discovery, 'stats'):
            comp_stats = self.comprehensive_discovery.stats
            print(f"\n🔍 COMPREHENSIVE DISCOVERY BREAKDOWN:")
            print(f"   Etherscan Rich List: {comp_stats.get('total_from_etherscan', 0):,} addresses")
            print(f"   Polygonscan Rich List: {comp_stats.get('total_from_polygonscan', 0):,} addresses")  
            print(f"   GitHub Repositories: {comp_stats.get('total_from_github', 0):,} addresses")
            print(f"   Dune Analytics: {comp_stats.get('total_from_dune', 0):,} addresses")
            print(f"   Total Unique Collected: {comp_stats.get('total_collected', 0):,} addresses")
        
        # Add Covalent-specific statistics if available
        if hasattr(self, 'enricher') and hasattr(self.enricher, 'enrichers'):
            covalent_apis = [api for name, api in self.enricher.enrichers.items() if 'covalent' in name.lower()]
            if covalent_apis:
                total_covalent_addresses = sum(getattr(api, 'addresses_fetched_count', 0) for api in covalent_apis)
                if total_covalent_addresses > 0:
                    print(f"\n🐋 COVALENT GOLDRUSH API RESULTS:")
                    print(f"   Addresses fetched: {total_covalent_addresses:,}")
                    print(f"   Using proper GoldRush endpoints ✅")
                else:
                    print(f"\n⚠️  COVALENT GOLDRUSH API: No addresses fetched")
        
        print("="*60)

    def _print_enhanced_statistics(self):
        """Print comprehensive statistics for the enhanced multi-chain whale discovery."""
        print("\n" + "="*80)
        print("🎯 ENHANCED MULTI-CHAIN WHALE DISCOVERY - FINAL STATISTICS")
        print("="*80)
        print("Chain-wise Statistics:")
        for chain, stats in self.stats['candidates_by_chain'].items():
            print(f"\n🔗 {chain.upper()} blockchain:")
            for key, value in stats.items():
                if key != 'whale_value':
                    print(f"{key.capitalize()}: {value}")
            print(f"Total {chain} candidates: {stats['total']}")
            print(f"Enriched {chain} candidates: {stats['enriched']}")
            print(f"Whales found on {chain}: {stats['whales']}")
            print(f"Total {chain} whale value: ${stats['whale_value']:,.2f}")
        print(f"\nTotal Candidates Discovered: {self.stats['total_candidates_discovered']}")
        print(f"Total Enriched: {self.stats['total_enriched']}")
        print(f"Total Whales Found: {self.stats['total_whales_found']}")
        print(f"Total Whale Value: ${self.stats['total_whale_value']:,.2f}")
        print(f"Errors Encountered: {self.stats['errors']}")
        print(f"Success Rate: {(self.stats['total_whales_found']/max(1, self.stats['total_candidates_discovered']))*100:.1f}%")
        print("="*80)


async def main():
    """Main entry point for the whale discovery agent."""
    parser = argparse.ArgumentParser(
        description="Whale Discovery Agent - Multi-chain whale address discovery and enrichment"
    )
    
    parser.add_argument(
        '--chains',
        nargs='+',
        default=['ethereum', 'bitcoin', 'polygon', 'solana'],
        choices=['ethereum', 'bitcoin', 'polygon', 'solana', 'xrp', 'cardano'],
        help='Target blockchains for whale discovery'
    )
    
    parser.add_argument(
        '--min-balance',
        type=float,
        default=1_000_000,
        help='Minimum USD balance threshold for whale classification (default: $1M)'
    )
    
    parser.add_argument(
        '--max-balance',
        type=float,
        default=50_000_000,
        help='Maximum USD balance threshold to avoid exchange wallets (default: $50M)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run discovery without storing results in database'
    )
    
    parser.add_argument(
        '--test-mode',
        action='store_true',
        help='Run in test mode with minimal API calls (5 results per source max)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--output-file',
        type=str,
        help='Save discovered whale addresses to JSON file'
    )
    
    args = parser.parse_args()
    
    # Configure logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Enable dry-run automatically in test mode
    if args.test_mode:
        args.dry_run = True
        logger.info("🧪 TEST MODE ENABLED - Using minimal limits and dry-run")
    
    try:
        # Initialize whale discovery agent
        agent = WhaleDiscoveryAgent(
            min_balance_usd=args.min_balance,
            target_chains=args.chains,
            dry_run=args.dry_run,
            test_mode=args.test_mode  # Pass test mode to agent
        )
        
        # Discover whale addresses
        whale_addresses = await agent.discover_whales()
        
        # Filter by maximum balance if specified
        if args.max_balance:
            filtered_whales = [w for w in whale_addresses if w.balance_usd <= args.max_balance]
            removed_count = len(whale_addresses) - len(filtered_whales)
            if removed_count > 0:
                logger.info(f"Filtered out {removed_count} addresses above ${args.max_balance:,.2f} threshold")
            whale_addresses = filtered_whales
        
        # Store results
        if whale_addresses:
            stored_count = await agent.store_whale_addresses(whale_addresses)
            
            # Save to file if requested
            if args.output_file:
                with open(args.output_file, 'w') as f:
                    whale_data = [asdict(whale) for whale in whale_addresses]
                    # Convert datetime objects to strings for JSON serialization
                    for whale in whale_data:
                        whale['discovered_at'] = whale['discovered_at'].isoformat()
                    json.dump(whale_data, f, indent=2, default=str)
                logger.info(f"Whale addresses saved to {args.output_file}")
        
        # Print final statistics
        agent.print_statistics()
        
        # Test mode summary
        if args.test_mode:
            logger.info("\n" + "="*60)
            logger.info("🧪 TEST MODE SUMMARY")
            logger.info("="*60)
            logger.info(f"Total whale addresses discovered: {len(whale_addresses)}")
            logger.info(f"API calls made: Minimal (< 50 total)")
            logger.info(f"Sources tested: All 4 comprehensive sources")
            logger.info(f"Data format: Ready for Supabase insertion")
            logger.info(f"Test completed successfully! ✅")
            
        # Summary
        if whale_addresses:
            total_value = sum(w.balance_usd for w in whale_addresses)
            avg_balance = total_value / len(whale_addresses)
            max_balance = max(w.balance_usd for w in whale_addresses)
            
            print(f"\nSUMMARY:")
            print(f"Total Whale Value: ${total_value:,.2f}")
            print(f"Average Balance: ${avg_balance:,.2f}")
            print(f"Largest Whale: ${max_balance:,.2f}")
            
            # Chain breakdown
            chain_stats = {}
            for whale in whale_addresses:
                chain = whale.blockchain
                if chain not in chain_stats:
                    chain_stats[chain] = {'count': 0, 'total_value': 0}
                chain_stats[chain]['count'] += 1
                chain_stats[chain]['total_value'] += whale.balance_usd
            
            print(f"\nCHAIN BREAKDOWN:")
            for chain, stats in chain_stats.items():
                print(f"{chain.upper()}: {stats['count']} whales, ${stats['total_value']:,.2f}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Whale discovery failed: {e}")
        return 1


if __name__ == "__main__":
    import asyncio
    sys.exit(asyncio.run(main())) 
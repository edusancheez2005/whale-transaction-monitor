"""
API Integrations Module - Phase 2: Data Acquisition

This module provides comprehensive API integrations for collecting blockchain address data
from various third-party services using their free tiers.

Author: Address Collector System
Version: 2.0.0 (Phase 2)
"""

import os
import asyncio
import json
import time
import logging
from typing import Dict, List, Optional, Any, Union, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import requests
import websockets
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential
from ratelimit import limits, sleep_and_retry
import backoff
from bs4 import BeautifulSoup
import re

# Import the centralized error logging function
from .base_helpers import log_error

# Configure logging
logger = logging.getLogger(__name__)


@dataclass
class AddressData:
    """Standard format for collected address data with enhanced trader metrics."""
    address: str
    blockchain: str
    source_system: str
    initial_label: Optional[str] = None
    confidence_score: float = 0.5
    metadata: Optional[Dict[str, Any]] = None
    collected_at: Optional[datetime] = None
    
    # Enhanced trader-focused fields
    whale_type: Optional[str] = None  # 'Hybrid', 'Trader', 'Holder', etc.
    total_volume_usd_30d: Optional[float] = None
    tx_count_30d: Optional[int] = None
    entity_category: Optional[str] = None  # 'individual_trader', 'protocol', 'exchange', etc.
    
    # Optional balance fields (enriched later)
    balance_native: Optional[float] = None
    balance_usd: Optional[float] = None
    
    def __post_init__(self):
        if self.collected_at is None:
            self.collected_at = datetime.utcnow()
        if self.metadata is None:
            self.metadata = {}


class APIIntegrationBase:
    """Base class for all API integrations with common functionality."""
    
    def __init__(self, api_key: str, base_url: str, rate_limit_per_second: int = 5):
        self.api_key = api_key
        self.base_url = base_url
        self.rate_limit = rate_limit_per_second
        self.session = requests.Session()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
        # Set default headers
        self.session.headers.update({
            'User-Agent': 'BlockchainAddressCollector/2.0',
            'Accept': 'application/json'
        })
    
    @sleep_and_retry
    @limits(calls=2, period=1)  # More conservative rate limit for stability
    def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Make a rate-limited API request."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            
            # Validate JSON response structure
            if not isinstance(result, dict):
                self.logger.warning(f"API returned non-dict response for {endpoint}: {type(result)}")
                return {}
            
            # Check for API-specific error responses
            if result.get('status') == '0' and result.get('message'):
                self.logger.warning(f"API error for {endpoint}: {result.get('message')}")
                return {}
            
            self.logger.info(f"API request successful: {endpoint}")
            return result
            
        except requests.exceptions.RequestException as e:
            error_msg = f"API request failed for {endpoint}: {e}"
            self.logger.error(error_msg)
            log_error(error_msg)  # Add to global error store
            return {}
        except (ValueError, TypeError) as e:
            error_msg = f"JSON parsing failed for {endpoint}: {e}"
            self.logger.error(error_msg)
            log_error(error_msg)  # Add to global error store
            return {}
    
    def extract_addresses(self, data: Dict[str, Any]) -> List[AddressData]:
        """Extract addresses from API response. Override in subclasses."""
        raise NotImplementedError("Subclasses must implement extract_addresses method")


class EtherscanAPI(APIIntegrationBase):
    """Etherscan API integration for Ethereum address data."""
    
    def __init__(self, api_key: str):
        super().__init__(
            api_key=api_key,
            base_url="https://api.etherscan.io/api",
            rate_limit_per_second=5
        )
    
    def get_contract_creators(self, limit: int = 5000) -> List[AddressData]:
        """Get addresses of contract creators from recent verified contracts (last 30 days)."""
        addresses = []
        
        try:
            # Calculate 30 days ago in block numbers (approximately 7200 blocks per day)
            latest_params = {
                'module': 'proxy',
                'action': 'eth_blockNumber',
                'apikey': self.api_key
            }
            
            latest_response = self._make_request('', latest_params)
            if latest_response.get('result'):
                latest_block = int(latest_response['result'], 16)
                blocks_30_days = 7200 * 30  # Approximate blocks in 30 days
                start_block = max(0, latest_block - blocks_30_days)
                
                # Get contract creations from recent blocks with pagination
                max_blocks_to_check = min(1000, blocks_30_days // 100)  # Check every 100th block, max 1000 blocks
                blocks_checked = 0
                
                for block_offset in range(0, max_blocks_to_check):
                    if len(addresses) >= limit:
                        break
                        
                    block_num = latest_block - (block_offset * 100)  # Check every 100th block
                    if block_num < start_block:
                        break
                        
                    try:
                        block_params = {
                            'module': 'proxy',
                            'action': 'eth_getBlockByNumber',
                            'tag': hex(block_num),
                            'boolean': 'true',
                            'apikey': self.api_key
                        }
                        
                        block_response = self._make_request('', block_params)
                        # Validate response structure
                        if (block_response.get('result') and 
                            isinstance(block_response['result'], dict) and 
                            block_response['result'].get('transactions')):
                            for tx in block_response['result']['transactions']:
                                # Contract creation: to field is null/empty and creates a contract
                                if not tx.get('to') and tx.get('from'):
                                    addresses.append(AddressData(
                                        address=tx['from'],
                                        blockchain='ethereum',
                                        source_system='etherscan_api',
                                        initial_label='Contract Creator (30d)',
                                        metadata={
                                            'transaction_hash': tx['hash'],
                                            'block_number': tx['blockNumber'],
                                            'gas_used': tx.get('gas'),
                                            'block_timestamp': block_response['result'].get('timestamp')
                                        }
                                    ))
                                    
                                    if len(addresses) >= limit:
                                        break
                    except Exception as e:
                        self.logger.warning(f"Failed to process block {block_num}: {e}")
                        continue
                    
                    blocks_checked += 1
                    
                    # Add delay to respect rate limits
                    if blocks_checked % 10 == 0:
                        import time
                        time.sleep(0.5)
            
            self.logger.info(f"Extracted {len(addresses)} contract creator addresses from Etherscan (last 30 days)")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to get contract creators from Etherscan: {e}")
            return []
    
    def get_large_transaction_addresses(self, min_value_eth: float = 10, limit: int = 5000) -> List[AddressData]:
        """Get addresses involved in large transactions (last 30 days)."""
        addresses = []
        
        try:
            # Get latest block and calculate 30-day range
            latest_params = {
                'module': 'proxy',
                'action': 'eth_blockNumber',
                'apikey': self.api_key
            }
            
            latest_response = self._make_request('', latest_params)
            if latest_response.get('result'):
                latest_block = int(latest_response['result'], 16)
                min_value_wei = int(min_value_eth * 10**18)
                blocks_30_days = 7200 * 30  # Approximate blocks in 30 days
                start_block = max(0, latest_block - blocks_30_days)
                
                # Check blocks in batches for large transactions
                blocks_checked = 0
                max_blocks_to_check = min(1000, blocks_30_days // 50)  # Check every 50th block, max 1000 blocks
                
                for block_offset in range(0, max_blocks_to_check):  # Check every 50th block
                    if len(addresses) >= limit:
                        break
                        
                    block_num = latest_block - (block_offset * 50)  # Check every 50th block
                    if block_num < start_block:
                        break
                    
                    try:
                        block_params = {
                            'module': 'proxy',
                            'action': 'eth_getBlockByNumber',
                            'tag': hex(block_num),
                            'boolean': 'true',
                            'apikey': self.api_key
                        }
                        
                        block_response = self._make_request('', block_params)
                        # Validate response structure
                        if (block_response.get('result') and 
                            isinstance(block_response['result'], dict) and 
                            block_response['result'].get('transactions')):
                            for tx in block_response['result']['transactions']:
                                if len(addresses) >= limit:
                                    break
                                    
                                try:
                                    value = int(tx.get('value', '0x0'), 16)
                                    
                                    if value >= min_value_wei:
                                        # Add sender
                                        addresses.append(AddressData(
                                            address=tx['from'],
                                            blockchain='ethereum',
                                            source_system='etherscan_api',
                                            initial_label=f'Large Tx Sender (≥{min_value_eth} ETH)',
                                            metadata={
                                                'transaction_value_eth': value / 10**18,
                                                'transaction_hash': tx['hash'],
                                                'block_number': tx['blockNumber'],
                                                'block_timestamp': block_response['result'].get('timestamp')
                                            }
                                        ))
                                        
                                        # Add receiver if not contract creation
                                        if tx.get('to') and len(addresses) < limit:
                                            addresses.append(AddressData(
                                                address=tx['to'],
                                                blockchain='ethereum',
                                                source_system='etherscan_api',
                                                initial_label=f'Large Tx Receiver (≥{min_value_eth} ETH)',
                                                metadata={
                                                    'transaction_value_eth': value / 10**18,
                                                    'transaction_hash': tx['hash'],
                                                    'block_number': tx['blockNumber'],
                                                    'block_timestamp': block_response['result'].get('timestamp')
                                                }
                                            ))
                                except (ValueError, TypeError):
                                    continue  # Skip invalid transactions
                        
                        blocks_checked += 1
                        
                        # Add delay every 10 blocks to respect rate limits
                        if blocks_checked % 10 == 0:
                            import time
                            time.sleep(0.3)
                            
                    except Exception as e:
                        self.logger.warning(f"Failed to process block {block_num}: {e}")
                        continue
            
            self.logger.info(f"Extracted {len(addresses)} addresses from large transactions (last 30 days)")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to get large transaction addresses: {e}")
            return []
    
    def get_top_accounts_by_balance(self, limit: int = 5000) -> List[AddressData]:
        """Get active accounts from popular contract interactions (last 30 days)."""
        addresses = []
        
        try:
            # Get some well-known contract addresses and their recent interactions
            popular_contracts = [
                '0xA0b86a33E6441e6C7d3E4081f7567b0b2b2b8b0a',  # USDC
                '0xdAC17F958D2ee523a2206206994597C13D831ec7',  # USDT
                '0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D',  # Uniswap V2 Router
                '0xE592427A0AEce92De3Edee1F18E0157C05861564',  # Uniswap V3 Router
                '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',  # WETH
                '0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984',  # UNI Token
                '0x6B175474E89094C44Da98b954EedeAC495271d0F',  # DAI
                '0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599',  # WBTC
            ]
            
            # Calculate 30 days ago in block numbers
            latest_params = {
                'module': 'proxy',
                'action': 'eth_blockNumber',
                'apikey': self.api_key
            }
            
            latest_response = self._make_request('', latest_params)
            if latest_response.get('result'):
                latest_block = int(latest_response['result'], 16)
                blocks_30_days = 7200 * 30
                start_block = max(0, latest_block - blocks_30_days)
                
                for contract in popular_contracts:
                    if len(addresses) >= limit:
                        break
                        
                    try:
                        # Get recent transactions for this contract with pagination
                        page = 1
                        max_pages = 20  # Limit pages per contract
                        
                        while len(addresses) < limit and page <= max_pages:
                            tx_params = {
                                'module': 'account',
                                'action': 'txlist',
                                'address': contract,
                                'startblock': start_block,
                                'endblock': latest_block,
                                'page': page,
                                'offset': 1000,  # Max allowed by Etherscan
                                'sort': 'desc',
                                'apikey': self.api_key
                            }
                            
                            response = self._make_request('', tx_params)
                            
                            if response.get('status') == '1' and response.get('result'):
                                transactions = response['result']
                                if not transactions:  # No more transactions
                                    break
                                    
                                for tx in transactions:
                                    if len(addresses) >= limit:
                                        break
                                        
                                    # Add the 'from' address (the user interacting with the contract)
                                    if tx.get('from') and tx['from'] != contract:
                                        # Calculate transaction value in ETH
                                        try:
                                            value_wei = int(tx.get('value', '0'))
                                            value_eth = value_wei / 10**18
                                        except (ValueError, TypeError):
                                            value_eth = 0
                                        
                                        addresses.append(AddressData(
                                            address=tx['from'],
                                            blockchain='ethereum',
                                            source_system='etherscan_api',
                                            initial_label=f'Active User ({contract[:10]}... - 30d)',
                                            metadata={
                                                'contract_interacted': contract,
                                                'transaction_hash': tx['hash'],
                                                'value_eth': value_eth,
                                                'block_number': tx.get('blockNumber'),
                                                'timestamp': tx.get('timeStamp'),
                                                'gas_used': tx.get('gasUsed')
                                            }
                                        ))
                                
                                page += 1
                                
                                # Add delay to respect rate limits
                                import time
                                time.sleep(0.2)
                            else:
                                break  # No more data or error
                            
                    except Exception as e:
                        self.logger.error(f"Failed to get transactions for contract {contract}: {e}")
                        continue
            
            self.logger.info(f"Extracted {len(addresses)} active user addresses from Etherscan (last 30 days)")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to get top accounts: {e}")
            return []


class PolygonscanAPI(EtherscanAPI):
    """Polygonscan API integration for Polygon address data."""
    
    def __init__(self, api_key: str):
        super().__init__(api_key)
        self.base_url = "https://api.polygonscan.com/api"


class SolscanAPI(APIIntegrationBase):
    """Solscan API integration for Solana address data."""
    
    def __init__(self, api_key: str):
        super().__init__(
            api_key=api_key,
            base_url="https://public-api.solscan.io",
            rate_limit_per_second=3
        )
        
        # Add API key to headers for Solscan
        self.session.headers.update({'token': self.api_key})
    
    def get_token_holders(self, token_address: str = "So11111111111111111111111111111111111111112", limit: int = 5000) -> List[AddressData]:
        """Get token holders for popular Solana tokens (significant holders only)."""
        addresses = []
        
        # Popular Solana tokens to check
        popular_tokens = [
            "So11111111111111111111111111111111111111112",  # SOL
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
            "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",  # Bonk
            "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",   # Jupiter
            "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",   # Marinade SOL
        ]
        
        try:
            for token in popular_tokens:
                if len(addresses) >= limit:
                    break
                    
                try:
                    # Get token holders with pagination
                    offset = 0
                    page_size = 100
                    max_pages = 50  # Limit to avoid excessive API calls
                    
                    for page in range(max_pages):
                        if len(addresses) >= limit:
                            break
                            
                        endpoint = f"token/holders"
                        params = {
                            'tokenAddress': token,
                            'limit': page_size,
                            'offset': offset
                        }
                        
                        response = self._make_request(endpoint, params)
                        
                        if response.get('success') and response.get('data'):
                            holders = response['data']
                            
                            if not holders:  # No more holders
                                break
                            
                            for holder in holders:
                                if len(addresses) >= limit:
                                    break
                                    
                                # Filter for significant holders
                                try:
                                    amount = float(holder.get('amount', 0))
                                    # Different thresholds for different tokens
                                    if token == "So11111111111111111111111111111111111111112":  # SOL
                                        threshold = 10  # 10 SOL
                                    elif token in ["EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"]:  # USDC/USDT
                                        threshold = 1000  # $1000
                                    else:
                                        threshold = 1000000  # 1M tokens for other tokens
                                except (ValueError, TypeError):
                                    amount = 0
                                    threshold = float('inf')
                                
                                # Only include significant holders
                                if amount >= threshold:
                                    addresses.append(AddressData(
                                        address=holder['owner'],
                                        blockchain='solana',
                                        source_system='solscan_api',
                                        initial_label=f'SOL Token Holder ({amount:,.0f})',
                                        metadata={
                                            'token_address': token,
                                            'balance': amount,
                                            'rank': holder.get('rank'),
                                            'token_symbol': self._get_token_symbol(token)
                                        }
                                    ))
                            
                            offset += page_size
                            
                            # Add delay to respect rate limits
                            import time
                            time.sleep(0.3)
                        else:
                            break  # No more data
                            
                except Exception as e:
                    self.logger.error(f"Failed to get holders for token {token}: {e}")
                    continue
            
            self.logger.info(f"Extracted {len(addresses)} significant Solana token holders")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to get Solana token holders: {e}")
            return []
    
    def _get_token_symbol(self, token_address: str) -> str:
        """Get token symbol for known addresses."""
        symbols = {
            "So11111111111111111111111111111111111111112": "SOL",
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": "USDC",
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": "USDT",
            "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263": "BONK",
            "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN": "JUP",
            "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So": "mSOL",
        }
        return symbols.get(token_address, token_address[:8])


class HeliusAPI(APIIntegrationBase):
    """Helius API integration for enhanced Solana data."""
    
    def __init__(self, api_key: str):
        super().__init__(
            api_key=api_key,
            base_url=f"https://rpc.helius.xyz",
            rate_limit_per_second=10
        )
    
    def get_program_accounts(self, program_id: str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", limit: int = 5000) -> List[AddressData]:
        """Get accounts associated with specific Solana programs (recent activity)."""
        addresses = []
        
        # Important Solana programs to check
        important_programs = [
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",  # Token Program
            "11111111111111111111111111111111",              # System Program
            "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",   # Jupiter V6
            "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",   # Whirlpool
            "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",   # Raydium V4
            "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",   # Raydium V3
        ]
        
        try:
            for program in important_programs:
                if len(addresses) >= limit:
                    break
                    
                try:
                    # Use Helius RPC endpoint
                    url = f"{self.base_url}/?api-key={self.api_key}"
                    
                    # Get recent transactions for this program to find active accounts
                    payload = {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "getSignaturesForAddress",
                        "params": [
                            program,
                            {
                                "limit": 1000,  # Get recent signatures
                                "commitment": "confirmed"
                            }
                        ]
                    }
                    
                    response = self.session.post(url, json=payload, timeout=30)
                    response.raise_for_status()
                    data = response.json()
                    
                    if data.get('result'):
                        signatures = data['result'][:100]  # Limit to recent 100
                        
                        for sig_info in signatures:
                            if len(addresses) >= limit:
                                break
                                
                            # Get transaction details to extract involved accounts
                            tx_payload = {
                                "jsonrpc": "2.0",
                                "id": 2,
                                "method": "getTransaction",
                                "params": [
                                    sig_info['signature'],
                                    {
                                        "encoding": "json",
                                        "maxSupportedTransactionVersion": 0
                                    }
                                ]
                            }
                            
                            try:
                                tx_response = self.session.post(url, json=tx_payload, timeout=10)
                                tx_response.raise_for_status()
                                tx_data = tx_response.json()
                                
                                if tx_data.get('result') and tx_data['result'].get('transaction'):
                                    tx = tx_data['result']['transaction']
                                    
                                    # Extract account keys (addresses involved in transaction)
                                    if tx.get('message', {}).get('accountKeys'):
                                        for account_key in tx['message']['accountKeys'][:5]:  # Limit per transaction
                                            if len(addresses) >= limit:
                                                break
                                                
                                            # Skip program addresses themselves
                                            if account_key not in important_programs:
                                                addresses.append(AddressData(
                                                    address=account_key,
                                                    blockchain='solana',
                                                    source_system='helius_api',
                                                    initial_label=f'Active Account ({self._get_program_name(program)})',
                                                    metadata={
                                                        'program_id': program,
                                                        'signature': sig_info['signature'],
                                                        'slot': sig_info.get('slot'),
                                                        'block_time': sig_info.get('blockTime'),
                                                        'err': sig_info.get('err')
                                                    }
                                                ))
                                
                                # Add small delay between transaction requests
                                import time
                                time.sleep(0.1)
                                
                            except Exception as e:
                                self.logger.warning(f"Failed to get transaction {sig_info['signature']}: {e}")
                                continue
                    
                    # Add delay between program requests
                    import time
                    time.sleep(0.5)
                    
                except Exception as e:
                    self.logger.error(f"Failed to get accounts for program {program}: {e}")
                    continue
            
            self.logger.info(f"Extracted {len(addresses)} active Solana accounts")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to get Solana program accounts: {e}")
            return []
    
    def _get_program_name(self, program_id: str) -> str:
        """Get human-readable program name."""
        names = {
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA": "Token",
            "11111111111111111111111111111111": "System",
            "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4": "Jupiter",
            "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc": "Whirlpool",
            "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM": "Raydium",
            "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "Raydium V3",
        }
        return names.get(program_id, program_id[:8])


class CovalentAPI(APIIntegrationBase):
    """Covalent GoldRush API integration for multi-chain data."""
    
    def __init__(self, api_key: str):
        super().__init__(
            api_key=api_key,
            base_url="https://api.covalenthq.com/v1",
            rate_limit_per_second=2
        )
        
        # Use Basic Auth as recommended for GoldRush free tier
        # Clear any existing auth headers and use requests.auth instead
        self.session.auth = (self.api_key, "")
        
        # Remove any conflicting authorization headers
        if 'Authorization' in self.session.headers:
            del self.session.headers['Authorization']
    
    def get_token_holders_multichain(self, chain_name: str = "eth-mainnet", contract_address: str = None, limit: int = 5000) -> List[AddressData]:
        """Get token holders using GoldRush free-tier endpoints."""
        addresses = []
        
        # Popular token contracts to check (using actual contract addresses)
        popular_tokens = {
            "eth-mainnet": [
                "0xA0b86a33E6441e6C7d3E4081f7567b0b2b2b8b0a",  # USDC
                "0xdAC17F958D2ee523a2206206994597C13D831ec7",  # USDT
                "0x6B175474E89094C44Da98b954EedeAC495271d0F",  # DAI
            ],
            "matic-mainnet": [
                "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",  # USDC
                "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",  # USDT
            ]
        }
        
        tokens_to_check = popular_tokens.get(chain_name, [contract_address]) if contract_address else popular_tokens.get(chain_name, [])
        
        try:
            for token_contract in tokens_to_check:
                if len(addresses) >= limit:
                    break
                    
                try:
                    # Use GoldRush free-tier endpoint format: {chain-name}/tokens/{contract}/token_holders_v2/
                    endpoint = f"{chain_name}/tokens/{token_contract}/token_holders_v2/"
                    params = {
                        'page-size': 100,  # Max page size
                        'page-number': 0   # Start with first page
                    }
                    
                    response = self._make_request(endpoint, params)
                    
                    if response.get('data') and response['data'].get('items'):
                        blockchain = self._get_blockchain_from_chain_name(chain_name)
                        holders = response['data']['items']
                        
                        for holder in holders:
                            if len(addresses) >= limit:
                                break
                                
                            # Filter for significant holders (>$100 value)
                            try:
                                balance_quote = float(holder.get('balance_quote', 0) or 0)
                            except (ValueError, TypeError):
                                balance_quote = 0
                            
                            if balance_quote > 100:  # Only significant holders
                                addresses.append(AddressData(
                                    address=holder['address'],
                                    blockchain=blockchain,
                                    source_system='covalent_goldrush_free',
                                    initial_label=f'Token Holder (${balance_quote:.0f})',
                                    metadata={
                                        'contract_address': token_contract,
                                        'balance': holder.get('balance'),
                                        'balance_quote': balance_quote,
                                        'chain_name': chain_name,
                                        'token_symbol': holder.get('contract_ticker_symbol')
                                    }
                                ))
                        
                        # Add delay to respect rate limits (4 req/sec for free tier)
                        import time
                        time.sleep(0.3)
                        
                except Exception as e:
                    self.logger.error(f"Failed to get holders for token {token_contract}: {e}")
                    continue
            
            self.logger.info(f"Extracted {len(addresses)} significant token holders from Covalent free tier")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to get token holders from Covalent: {e}")
            return []
    
    def get_recent_transactions(self, chain_id: int = 1, limit: int = 5000) -> List[AddressData]:
        """Get addresses from recent high-value transactions (last 30 days)."""
        addresses = []
        
        try:
            # Calculate date range for last 30 days
            from datetime import datetime, timedelta
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=30)
            
            # Get recent transactions with pagination
            page = 0
            max_pages = 50  # Limit to avoid excessive API calls
            
            while len(addresses) < limit and page < max_pages:
                endpoint = f"{chain_id}/transactions/"
                params = {
                    'page-size': 100,  # Max page size
                    'page-number': page,
                    'starting-after': start_date.isoformat(),
                    'ending-before': end_date.isoformat()
                }
                
                response = self._make_request(endpoint, params)
                
                if response.get('data') and response['data'].get('items'):
                    chain_name = self._get_chain_name(chain_id)
                    transactions = response['data']['items']
                    
                    if not transactions:  # No more transactions
                        break
                    
                    for tx in transactions:
                        if len(addresses) >= limit:
                            break
                            
                        # Filter for high-value transactions (>$1000)
                        try:
                            value_quote = float(tx.get('value_quote', 0))
                        except (ValueError, TypeError):
                            value_quote = 0
                        
                        if value_quote > 1000:  # Only high-value transactions
                            # Add sender
                            if tx.get('from_address'):
                                addresses.append(AddressData(
                                    address=tx['from_address'],
                                    blockchain=chain_name,
                                    source_system='covalent_api',
                                    initial_label=f'High-Value Tx Sender (${value_quote:.0f})',
                                    metadata={
                                        'transaction_hash': tx.get('tx_hash'),
                                        'value': tx.get('value'),
                                        'value_quote': value_quote,
                                        'chain_id': chain_id,
                                        'block_height': tx.get('block_height'),
                                        'block_signed_at': tx.get('block_signed_at')
                                    }
                                ))
                            
                            # Add receiver
                            if tx.get('to_address') and len(addresses) < limit:
                                addresses.append(AddressData(
                                    address=tx['to_address'],
                                    blockchain=chain_name,
                                    source_system='covalent_api',
                                    initial_label=f'High-Value Tx Receiver (${value_quote:.0f})',
                                    metadata={
                                        'transaction_hash': tx.get('tx_hash'),
                                        'value': tx.get('value'),
                                        'value_quote': value_quote,
                                        'chain_id': chain_id,
                                        'block_height': tx.get('block_height'),
                                        'block_signed_at': tx.get('block_signed_at')
                                    }
                                ))
                
                page += 1
                
                # Add delay to respect rate limits
                import time
                time.sleep(0.5)
            
            self.logger.info(f"Extracted {len(addresses)} addresses from high-value recent transactions (last 30 days)")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to get recent transactions from Covalent: {e}")
            return []
    
    def _get_chain_name(self, chain_id: int) -> str:
        """Map chain ID to chain name."""
        chain_map = {
            1: 'ethereum',
            137: 'polygon',
            56: 'bsc',
            43114: 'avalanche',
            250: 'fantom'
        }
        return chain_map.get(chain_id, f'chain_{chain_id}')
    
    def get_address_transactions_v3(self, address: str, chain_name: str = "eth-mainnet", limit: int = 100) -> List[AddressData]:
        """Get transaction history for a specific address using GoldRush transactions_v3 endpoint."""
        addresses = []
        
        try:
            # Use the new GoldRush v3 endpoint pattern: /{chain_name}/address/{address}/transactions_v3/
            endpoint = f"{chain_name}/address/{address}/transactions_v3/"
            params = {
                'page-size': min(limit, 100),  # GoldRush max page size
                'no-logs': 'false'  # Include decoded logs for enrichment
            }
            
            response = self._make_request(endpoint, params)
            
            if response.get('data') and response['data'].get('items'):
                blockchain = self._get_blockchain_from_chain_name(chain_name)
                transactions = response['data']['items']
                
                for tx in transactions:
                    if len(addresses) >= limit:
                        break
                    
                    # Extract transaction value for filtering
                    try:
                        value_quote = float(tx.get('value_quote', 0) or 0)
                    except (ValueError, TypeError):
                        value_quote = 0
                    
                    # Only include high-value transactions (>$1000)
                    if value_quote > 1000:
                        # Extract counterparty addresses from the transaction
                        from_addr = tx.get('from_address')
                        to_addr = tx.get('to_address')
                        
                        # Add the counterparty address (not the queried address)
                        counterparty_addr = to_addr if from_addr == address else from_addr
                        
                        if counterparty_addr and counterparty_addr != address:
                            addresses.append(AddressData(
                                address=counterparty_addr,
                                blockchain=blockchain,
                                source_system='covalent_goldrush_v3',
                                initial_label=f'High-Value Counterparty (${value_quote:.0f})',
                                metadata={
                                    'transaction_hash': tx.get('tx_hash'),
                                    'value_quote': value_quote,
                                    'block_height': tx.get('block_height'),
                                    'block_signed_at': tx.get('block_signed_at'),
                                    'chain_name': chain_name,
                                    'gas_spent': tx.get('gas_spent'),
                                    'fees_paid': tx.get('fees_paid'),
                                    'log_events_count': len(tx.get('log_events', []))
                                }
                            ))
                
                self.logger.info(f"Extracted {len(addresses)} counterparty addresses from {address} transactions")
                return addresses
                
        except Exception as e:
            self.logger.error(f"Failed to get transactions for address {address}: {e}")
            return []
    
    def _get_blockchain_from_chain_name(self, chain_name: str) -> str:
        """Map GoldRush chain names to standard blockchain names."""
        chain_map = {
            'eth-mainnet': 'ethereum',
            'matic-mainnet': 'polygon',
            'bsc-mainnet': 'bsc',
            'avalanche-mainnet': 'avalanche',
            'fantom-mainnet': 'fantom',
            'solana-mainnet': 'solana'
        }
        return chain_map.get(chain_name, chain_name.split('-')[0])


class MoralisAPI(APIIntegrationBase):
    """Moralis API integration for multi-chain NFT and wallet data."""
    
    def __init__(self, api_key: str):
        super().__init__(
            api_key=api_key,
            base_url="https://deep-index.moralis.io/api/v2.2",
            rate_limit_per_second=3
        )
        
        # Add API key to headers
        self.session.headers.update({'X-API-Key': self.api_key})
    
    def get_nft_owners(self, contract_address: str = None, chain: str = "eth", limit: int = 5000) -> List[AddressData]:
        """Get NFT owners for popular collections."""
        addresses = []
        
        # Popular NFT collections to check
        popular_collections = {
            'eth': [
                '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb',  # CryptoPunks
                '0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D',  # BAYC
                '0x60E4d786628Fea6478F785A6d7e704777c86a7c6',  # MAYC
                '0x49cF6f5d44E70224e2E23fDcdd2C053F30aDA28B',  # CloneX
                '0x8a90CAb2b38dba80c64b7734e58Ee1dB38B8992e',  # Doodles
                '0x23581767a106ae21c074b2276D25e5C3e136a68b',  # Moonbirds
                '0x34d85c9CDeB23FA97cb08333b511ac86E1C4E258',  # Otherdeeds
                '0xED5AF388653567Af2F388E6224dC7C4b3241C544',  # Azuki
            ],
            'polygon': [
                '0x2953399124F0cBB46d2CbACD8A89cF0599974963',  # OpenSea Collections
            ]
        }
        
        collections_to_check = popular_collections.get(chain, [contract_address]) if contract_address else popular_collections.get(chain, [])
        
        try:
            for collection in collections_to_check:
                if len(addresses) >= limit:
                    break
                    
                try:
                    # Get NFT owners with pagination
                    cursor = None
                    max_pages = 50  # Limit to avoid excessive API calls
                    page = 0
                    
                    while len(addresses) < limit and page < max_pages:
                        endpoint = f"nft/{collection}/owners"
                        params = {
                            'chain': chain,
                            'format': 'decimal',
                            'limit': 100  # Max per request
                        }
                        
                        if cursor:
                            params['cursor'] = cursor
                        
                        response = self._make_request(endpoint, params)
                        
                        if response.get('result'):
                            owners = response['result']
                            
                            if not owners:  # No more owners
                                break
                            
                            for owner in owners:
                                if len(addresses) >= limit:
                                    break
                                    
                                # Filter for owners with multiple NFTs (likely collectors/traders)
                                try:
                                    amount = int(owner.get('amount', 1))
                                except (ValueError, TypeError):
                                    amount = 1
                                
                                # Include all owners but prioritize those with multiple NFTs
                                if amount >= 1:  # Include all owners
                                    addresses.append(AddressData(
                                        address=owner['owner_of'],
                                        blockchain=chain,
                                        source_system='moralis_api',
                                        initial_label=f'NFT Owner ({self._get_collection_name(collection)}) x{amount}',
                                        metadata={
                                            'contract_address': collection,
                                            'token_id': owner.get('token_id'),
                                            'amount': amount,
                                            'token_hash': owner.get('token_hash'),
                                            'block_number_minted': owner.get('block_number_minted')
                                        }
                                    ))
                            
                            # Get cursor for next page
                            cursor = response.get('cursor')
                            if not cursor:
                                break
                                
                            page += 1
                            
                            # Add delay to respect rate limits
                            import time
                            time.sleep(0.3)
                        else:
                            break  # No more data
                            
                except Exception as e:
                    self.logger.error(f"Failed to get owners for collection {collection}: {e}")
                    continue
            
            self.logger.info(f"Extracted {len(addresses)} NFT owners from popular collections")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to get NFT owners from Moralis: {e}")
            return []
    
    def _get_collection_name(self, contract_address: str) -> str:
        """Get collection name for known addresses."""
        names = {
            '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb': 'CryptoPunks',
            '0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D': 'BAYC',
            '0x60E4d786628Fea6478F785A6d7e704777c86a7c6': 'MAYC',
            '0x49cF6f5d44E70224e2E23fDcdd2C053F30aDA28B': 'CloneX',
            '0x8a90CAb2b38dba80c64b7734e58Ee1dB38B8992e': 'Doodles',
            '0x23581767a106ae21c074b2276D25e5C3e136a68b': 'Moonbirds',
            '0x34d85c9CDeB23FA97cb08333b511ac86E1C4E258': 'Otherdeeds',
            '0xED5AF388653567Af2F388E6224dC7C4b3241C544': 'Azuki',
        }
        return names.get(contract_address, contract_address[:10])


class WhaleAlertAPI:
    """Whale Alert API integration for real-time whale transaction monitoring."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.ws_url = f"wss://leviathan.whale-alert.io/ws?api_key={api_key}"
        self.rest_url = "https://api.whale-alert.io/v1"
        self.logger = logging.getLogger(f"{__name__}.WhaleAlertAPI")
        self.session = requests.Session()
    
    async def connect_websocket(self, duration_minutes: int = 5) -> List[AddressData]:
        """Connect to Whale Alert WebSocket for real-time data."""
        addresses = []
        end_time = datetime.utcnow() + timedelta(minutes=duration_minutes)
        
        try:
            async with websockets.connect(self.ws_url) as websocket:
                self.logger.info("Connected to Whale Alert WebSocket")
                
                while datetime.utcnow() < end_time:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                        data = json.loads(message)
                        
                        if data.get('type') == 'transaction':
                            tx_data = data.get('data', {})
                            
                            # Extract sender address
                            if tx_data.get('from', {}).get('address'):
                                addresses.append(AddressData(
                                    address=tx_data['from']['address'],
                                    blockchain=tx_data.get('blockchain', 'unknown'),
                                    source_system='whale_alert_ws',
                                    initial_label='Whale Sender',
                                    metadata={
                                        'amount_usd': tx_data.get('amount_usd'),
                                        'symbol': tx_data.get('symbol'),
                                        'transaction_type': tx_data.get('transaction_type')
                                    }
                                ))
                            
                            # Extract receiver address
                            if tx_data.get('to', {}).get('address'):
                                addresses.append(AddressData(
                                    address=tx_data['to']['address'],
                                    blockchain=tx_data.get('blockchain', 'unknown'),
                                    source_system='whale_alert_ws',
                                    initial_label='Whale Receiver',
                                    metadata={
                                        'amount_usd': tx_data.get('amount_usd'),
                                        'symbol': tx_data.get('symbol'),
                                        'transaction_type': tx_data.get('transaction_type')
                                    }
                                ))
                    
                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        self.logger.error(f"Error processing WebSocket message: {e}")
                        continue
        
        except Exception as e:
            self.logger.error(f"Failed to connect to Whale Alert WebSocket: {e}")
        
        self.logger.info(f"Collected {len(addresses)} addresses from Whale Alert WebSocket")
        return addresses
    
    def get_recent_transactions(self, min_value: int = 100000, limit: int = 5000) -> List[AddressData]:
        """Get recent whale transactions via REST API (last 30 days)."""
        addresses = []
        
        try:
            # Calculate timestamp for 30 days ago
            from datetime import datetime, timedelta
            start_time = int((datetime.utcnow() - timedelta(days=30)).timestamp())
            
            # Get transactions with pagination
            cursor = None
            max_requests = 50  # Limit API requests
            requests_made = 0
            
            while len(addresses) < limit and requests_made < max_requests:
                url = f"{self.rest_url}/transactions"
                params = {
                    'api_key': self.api_key,
                    'min_value': min_value,
                    'limit': 100,  # Max per request
                    'start': start_time
                }
                
                if cursor:
                    params['cursor'] = cursor
                
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if data.get('result') == 'success' and data.get('transactions'):
                    transactions = data['transactions']
                    
                    if not transactions:  # No more transactions
                        break
                    
                    for tx in transactions:
                        if len(addresses) >= limit:
                            break
                            
                        amount_usd = tx.get('amount_usd', 0)
                        
                        # Extract sender
                        if tx.get('from', {}).get('address'):
                            addresses.append(AddressData(
                                address=tx['from']['address'],
                                blockchain=tx.get('blockchain', 'unknown'),
                                source_system='whale_alert_rest',
                                initial_label=f'Whale Sender (${amount_usd:,.0f})',
                                metadata={
                                    'amount_usd': amount_usd,
                                    'symbol': tx.get('symbol'),
                                    'timestamp': tx.get('timestamp'),
                                    'transaction_type': tx.get('transaction_type'),
                                    'transaction_count': tx.get('transaction_count')
                                }
                            ))
                        
                        # Extract receiver
                        if tx.get('to', {}).get('address') and len(addresses) < limit:
                            addresses.append(AddressData(
                                address=tx['to']['address'],
                                blockchain=tx.get('blockchain', 'unknown'),
                                source_system='whale_alert_rest',
                                initial_label=f'Whale Receiver (${amount_usd:,.0f})',
                                metadata={
                                    'amount_usd': amount_usd,
                                    'symbol': tx.get('symbol'),
                                    'timestamp': tx.get('timestamp'),
                                    'transaction_type': tx.get('transaction_type'),
                                    'transaction_count': tx.get('transaction_count')
                                }
                            ))
                    
                    # Get cursor for next page
                    cursor = data.get('cursor')
                    if not cursor:
                        break
                        
                    requests_made += 1
                    
                    # Add delay to respect rate limits
                    import time
                    time.sleep(0.5)
                else:
                    break  # No more data or error
            
            self.logger.info(f"Extracted {len(addresses)} whale addresses from Whale Alert REST API (last 30 days)")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to get recent transactions from Whale Alert: {e}")
            return []


class BitqueryAPI(APIIntegrationBase):
    """Bitquery GraphQL API integration for multi-chain data."""
    
    def __init__(self, api_key: str):
        super().__init__(
            api_key=api_key,
            base_url="https://graphql.bitquery.io",
            rate_limit_per_second=1
        )
        
        # Add API key to headers
        self.session.headers.update({'X-API-KEY': self.api_key})
    
    def get_dex_traders(self, limit: int = 5000) -> List[AddressData]:
        """Get active DEX traders using GraphQL query (last 30 days, high-value trades)."""
        addresses = []
        
        try:
            # Calculate date range for last 30 days
            from datetime import datetime, timedelta
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=30)
            
            # Query with pagination to get more data
            offset = 0
            batch_size = 1000  # Max per query
            max_batches = limit // batch_size + 1
            
            for batch in range(max_batches):
                if len(addresses) >= limit:
                    break
                    
                query = """
                query GetDEXTraders($limit: Int!, $offset: Int!, $since: ISO8601DateTime!) {
                  ethereum(network: ethereum) {
                    dexTrades(
                      options: {limit: $limit, offset: $offset, desc: "tradeAmount"}
                      date: {since: $since}
                      tradeAmountUsd: {gt: 1000}
                    ) {
                      buyer {
                        address
                      }
                      seller {
                        address
                      }
                      tradeAmount(in: USD)
                      transaction {
                        hash
                      }
                      block {
                        timestamp {
                          iso8601
                        }
                        height
                      }
                      baseCurrency {
                        symbol
                        address
                      }
                      quoteCurrency {
                        symbol
                        address
                      }
                    }
                  }
                }
                """
                
                variables = {
                    "limit": min(batch_size, limit - len(addresses)),
                    "offset": offset,
                    "since": start_date.isoformat()
                }
                
                payload = {
                    'query': query,
                    'variables': variables
                }
                
                response = self.session.post(self.base_url, json=payload, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if data.get('data', {}).get('ethereum', {}).get('dexTrades'):
                    trades = data['data']['ethereum']['dexTrades']
                    
                    if not trades:  # No more trades
                        break
                    
                    for trade in trades:
                        if len(addresses) >= limit:
                            break
                            
                        trade_amount = trade.get('tradeAmount', 0)
                        
                        # Add buyer
                        if trade.get('buyer', {}).get('address'):
                            addresses.append(AddressData(
                                address=trade['buyer']['address'],
                                blockchain='ethereum',
                                source_system='bitquery_api',
                                initial_label=f'DEX Trader - Buyer (${trade_amount:.0f})',
                                metadata={
                                    'trade_amount_usd': trade_amount,
                                    'transaction_hash': trade.get('transaction', {}).get('hash'),
                                    'block_height': trade.get('block', {}).get('height'),
                                    'timestamp': trade.get('block', {}).get('timestamp', {}).get('iso8601'),
                                    'base_currency': trade.get('baseCurrency', {}).get('symbol'),
                                    'quote_currency': trade.get('quoteCurrency', {}).get('symbol')
                                }
                            ))
                        
                        # Add seller
                        if trade.get('seller', {}).get('address') and len(addresses) < limit:
                            addresses.append(AddressData(
                                address=trade['seller']['address'],
                                blockchain='ethereum',
                                source_system='bitquery_api',
                                initial_label=f'DEX Trader - Seller (${trade_amount:.0f})',
                                metadata={
                                    'trade_amount_usd': trade_amount,
                                    'transaction_hash': trade.get('transaction', {}).get('hash'),
                                    'block_height': trade.get('block', {}).get('height'),
                                    'timestamp': trade.get('block', {}).get('timestamp', {}).get('iso8601'),
                                    'base_currency': trade.get('baseCurrency', {}).get('symbol'),
                                    'quote_currency': trade.get('quoteCurrency', {}).get('symbol')
                                }
                            ))
                    
                    offset += batch_size
                    
                    # Add delay to respect rate limits
                    import time
                    time.sleep(1.0)  # Conservative delay for GraphQL API
                else:
                    break  # No more data or error
            
            self.logger.info(f"Extracted {len(addresses)} high-value DEX trader addresses from Bitquery (last 30 days)")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to get DEX traders from Bitquery: {e}")
            return []


class DuneAPI(APIIntegrationBase):
    """Dune Analytics API integration for query-based data extraction."""
    
    def __init__(self, api_key: str):
        super().__init__(
            api_key=api_key,
            base_url="https://api.dune.com/api/v1",
            rate_limit_per_second=1
        )
        
        # Add API key to headers
        self.session.headers.update({'X-DUNE-API-KEY': self.api_key})
    
    def get_query_results(self, query_id: int) -> List[AddressData]:
        """Get results from a public Dune query."""
        addresses = []
        
        try:
            endpoint = f"query/{query_id}/results"
            
            response = self._make_request(endpoint)
            
            if response.get('result', {}).get('rows'):
                for row in response['result']['rows']:
                    # Look for address-like fields in the row
                    for key, value in row.items():
                        if (isinstance(value, str) and 
                            (value.startswith('0x') and len(value) == 42) or  # Ethereum address
                            (len(value) >= 32 and len(value) <= 44)):  # Solana address
                            
                            addresses.append(AddressData(
                                address=value,
                                blockchain='ethereum' if value.startswith('0x') else 'solana',
                                source_system='dune_api',
                                initial_label=f'Dune Query Result ({key})',
                                metadata={
                                    'query_id': query_id,
                                    'field_name': key,
                                    'row_data': row
                                }
                            ))
            
            self.logger.info(f"Extracted {len(addresses)} addresses from Dune query {query_id}")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to get Dune query results: {e}")
            return []


class APIIntegrationManager:
    """Manager class to coordinate all API integrations."""
    
    def __init__(self, api_keys: Dict[str, str]):
        self.api_keys = api_keys
        self.logger = logging.getLogger(f"{__name__}.APIIntegrationManager")
        
        # Initialize API clients
        self.apis = {}
        self._initialize_apis()
    
    def _initialize_apis(self):
        """Initialize all available API clients."""
        if self.api_keys.get('ETHERSCAN_API_KEY'):
            self.apis['etherscan'] = EtherscanAPI(self.api_keys['ETHERSCAN_API_KEY'])
        
        if self.api_keys.get('POLYGONSCAN_API_KEY'):
            self.apis['polygonscan'] = PolygonscanAPI(self.api_keys['POLYGONSCAN_API_KEY'])
        
        if self.api_keys.get('SOLSCAN_API_KEY'):
            self.apis['solscan'] = SolscanAPI(self.api_keys['SOLSCAN_API_KEY'])
        
        if self.api_keys.get('HELIUS_API_KEY'):
            self.apis['helius'] = HeliusAPI(self.api_keys['HELIUS_API_KEY'])
        
        if self.api_keys.get('COVALENT_API_KEY'):
            self.apis['covalent'] = CovalentAPI(self.api_keys['COVALENT_API_KEY'])
        
        if self.api_keys.get('MORALIS_API_KEY'):
            self.apis['moralis'] = MoralisAPI(self.api_keys['MORALIS_API_KEY'])
        
        if self.api_keys.get('WHALE_ALERT_API_KEY'):
            self.apis['whale_alert'] = WhaleAlertAPI(self.api_keys['WHALE_ALERT_API_KEY'])
        
        if self.api_keys.get('BITQUERY_API_KEY'):
            self.apis['bitquery'] = BitqueryAPI(self.api_keys['BITQUERY_API_KEY'])
        
        if self.api_keys.get('DUNE_API_KEY'):
            self.apis['dune'] = DuneAPI(self.api_keys['DUNE_API_KEY'])
        
        self.logger.info(f"Initialized {len(self.apis)} API integrations")
    
    def collect_all_addresses(self) -> List[AddressData]:
        """Collect addresses from all available APIs with enhanced limits and 30-day filtering."""
        all_addresses = []
        
        # Etherscan data - Enhanced collection (up to 5000 per method)
        if 'etherscan' in self.apis:
            self.logger.info("Collecting from Etherscan API (enhanced volume, last 30 days)...")
            all_addresses.extend(self.apis['etherscan'].get_contract_creators(limit=5000))
            all_addresses.extend(self.apis['etherscan'].get_large_transaction_addresses(min_value_eth=5, limit=5000))
            all_addresses.extend(self.apis['etherscan'].get_top_accounts_by_balance(limit=5000))
        
        # Polygon data - Enhanced collection
        if 'polygonscan' in self.apis:
            self.logger.info("Collecting from Polygonscan API (enhanced volume, last 30 days)...")
            all_addresses.extend(self.apis['polygonscan'].get_top_accounts_by_balance(limit=2000))
        
        # Solana data - Enhanced collection
        if 'solscan' in self.apis:
            self.logger.info("Collecting from Solscan API (enhanced volume, significant holders)...")
            all_addresses.extend(self.apis['solscan'].get_token_holders(limit=1000))
        
        if 'helius' in self.apis:
            self.logger.info("Collecting from Helius API (enhanced volume, recent activity)...")
            all_addresses.extend(self.apis['helius'].get_program_accounts(limit=1500))
        
        # Multi-chain data - Enhanced collection
        if 'covalent' in self.apis:
            self.logger.info("Collecting from Covalent API (enhanced volume, last 30 days)...")
            # Ethereum
            all_addresses.extend(self.apis['covalent'].get_token_holders_multichain(chain_name="eth-mainnet", limit=1000))
            all_addresses.extend(self.apis['covalent'].get_recent_transactions(chain_id=1, limit=1000))
            # Polygon
            all_addresses.extend(self.apis['covalent'].get_token_holders_multichain(chain_name="matic-mainnet", limit=500))
            all_addresses.extend(self.apis['covalent'].get_recent_transactions(chain_id=137, limit=500))
        
        if 'moralis' in self.apis:
            self.logger.info("Collecting from Moralis API (enhanced volume, popular NFT collections)...")
            try:
                # Ethereum NFTs
                all_addresses.extend(self.apis['moralis'].get_nft_owners(chain="eth", limit=1000))
                # Polygon NFTs
                all_addresses.extend(self.apis['moralis'].get_nft_owners(chain="polygon", limit=500))
            except Exception as e:
                self.logger.error(f"Failed to get NFT owners: {e}")
        
        # Whale Alert data - Enhanced collection
        if 'whale_alert' in self.apis:
            self.logger.info("Collecting from Whale Alert API (enhanced volume, last 30 days)...")
            all_addresses.extend(self.apis['whale_alert'].get_recent_transactions(min_value=100000, limit=500))
        
        # Bitquery data - Enhanced collection
        if 'bitquery' in self.apis:
            self.logger.info("Collecting from Bitquery API (enhanced volume, high-value DEX trades, last 30 days)...")
            all_addresses.extend(self.apis['bitquery'].get_dex_traders(limit=1000))
        
        # Dune data - Enhanced collection
        if 'dune' in self.apis:
            self.logger.info("Collecting from Dune API (enhanced volume)...")
            # Popular public query IDs for whale/large holder analysis
            whale_query_ids = [
                3237150,  # Example whale tracking query
                3237151,  # Example large holder query
                2847284,  # DEX Aggregators
                2847285,  # MEV Bots
            ]
            for query_id in whale_query_ids:
                try:
                    all_addresses.extend(self.apis['dune'].get_query_results(query_id))
                except Exception as e:
                    self.logger.error(f"Failed to get Dune query {query_id}: {e}")
        
        self.logger.info(f"Collected {len(all_addresses)} total addresses from all APIs (enhanced collection)")
        return all_addresses
    
    async def collect_realtime_data(self, duration_minutes: int = 5) -> List[AddressData]:
        """Collect real-time data from WebSocket APIs."""
        addresses = []
        
        if 'whale_alert' in self.apis:
            whale_addresses = await self.apis['whale_alert'].connect_websocket(duration_minutes)
            addresses.extend(whale_addresses)
        
        return addresses


# ============================================================================
# ANALYTICS PLATFORM INTEGRATIONS FOR PHASE 2 - WHALE IDENTIFICATION
# ============================================================================

class DuneAnalyticsAPI:
    """Production-grade Dune Analytics API integration for whale discovery."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.dune.com/api/v1"
        self.session = requests.Session()
        self.session.headers.update({
            'X-Dune-API-Key': api_key,
            'Content-Type': 'application/json'
        })
        self.logger = logging.getLogger(f"{__name__}.DuneAnalyticsAPI")
        
        # Production whale query IDs (verified working queries)
        self.whale_query_ids = {
            'ethereum': {
                'top_eth_holders': 2857442,  # Top 1000 Ethereum holders by ETH balance
                'eth_whale_transactions': 3055014,  # High-value ETH Transfers — frequent large senders
                'btc_on_eth_bridged': 3424379,  # wBTC/hBTC top holders (Ethereum bridged BTC)
            },
            'polygon': {
                'matic_top_holders': 3260658,  # MATIC (Polygon) Top Holders by balance
            },
            # Additional chains can be added as working query IDs become available
        }
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def execute_query(self, query_id: int, parameters: Dict[str, Any] = None) -> Optional[str]:
        """Execute a Dune query and return execution ID."""
        try:
            self.logger.info(f"Executing Dune query {query_id}")
            
            payload = {}
            if parameters:
                payload['query_parameters'] = parameters
            
            response = self.session.post(
                f"{self.base_url}/query/{query_id}/execute",
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                execution_id = result.get('execution_id')
                self.logger.info(f"Query {query_id} execution started: {execution_id}")
                return execution_id
            else:
                self.logger.error(f"Failed to execute query {query_id}: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error executing Dune query {query_id}: {e}")
            raise
    
    def poll_execution_status(self, execution_id: str, max_wait_seconds: int = 300) -> Optional[Dict[str, Any]]:
        """Poll execution status until completion with timeout."""
        start_time = time.time()
        poll_interval = 5  # Start with 5 second intervals
        
        while time.time() - start_time < max_wait_seconds:
            try:
                response = self.session.get(
                    f"{self.base_url}/execution/{execution_id}/status",
                    timeout=30
                )
                
                if response.status_code == 200:
                    status_data = response.json()
                    state = status_data.get('state')
                    
                    self.logger.debug(f"Execution {execution_id} state: {state}")
                    
                    if state == 'QUERY_STATE_COMPLETED':
                        self.logger.info(f"Query execution {execution_id} completed successfully")
                        return status_data
                    elif state == 'QUERY_STATE_FAILED':
                        self.logger.error(f"Query execution {execution_id} failed: {status_data.get('error')}")
                        return None
                    elif state in ['QUERY_STATE_EXECUTING', 'QUERY_STATE_PENDING']:
                        # Continue polling
                        time.sleep(min(poll_interval, 30))  # Cap at 30 seconds
                        poll_interval = min(poll_interval * 1.2, 30)  # Exponential backoff
                    else:
                        self.logger.warning(f"Unknown execution state: {state}")
                        time.sleep(poll_interval)
                else:
                    self.logger.warning(f"Failed to get execution status: {response.status_code}")
                    time.sleep(poll_interval)
                    
            except Exception as e:
                self.logger.warning(f"Error polling execution status: {e}")
                time.sleep(poll_interval)
        
        self.logger.error(f"Query execution {execution_id} timed out after {max_wait_seconds} seconds")
        return None
    
    def get_execution_results(self, execution_id: str) -> List[Dict[str, Any]]:
        """Get results from completed query execution."""
        try:
            response = self.session.get(
                f"{self.base_url}/execution/{execution_id}/results",
                timeout=60
            )
            
            if response.status_code == 200:
                result_data = response.json()
                rows = result_data.get('result', {}).get('rows', [])
                self.logger.info(f"Retrieved {len(rows)} rows from execution {execution_id}")
                return rows
            else:
                self.logger.error(f"Failed to get execution results: {response.status_code} - {response.text}")
                return []
                
        except Exception as e:
            self.logger.error(f"Error getting execution results: {e}")
            return []
    
    def fetch_dune_analytics_query_results(self, query_id: int, parameters: Dict[str, Any] = None, max_results: int = 5000) -> List[Dict[str, Any]]:
        """Execute query and fetch results with comprehensive error handling."""
        try:
            # Execute the query
            execution_id = self.execute_query(query_id, parameters)
            if not execution_id:
                return []
            
            # Poll for completion
            status_data = self.poll_execution_status(execution_id)
            if not status_data:
                return []
            
            # Get results
            rows = self.get_execution_results(execution_id)
            
            # Process and clean results
            processed_results = []
            for row in rows[:max_results]:
                if self._is_valid_whale_result(row):
                    processed_row = self._process_dune_result_row(row, query_id)
                    if processed_row:
                        processed_results.append(processed_row)
            
            self.logger.info(f"Processed {len(processed_results)} valid whale results from query {query_id}")
            return processed_results
            
        except Exception as e:
            self.logger.error(f"Error fetching Dune Analytics query results for {query_id}: {e}")
            return []
    
    def get_whale_addresses_from_query(self, query_id: int, limit: int = 1000) -> List[AddressData]:
        """Get whale addresses from a specific Dune query and convert to AddressData format."""
        addresses = []
        
        try:
            # Fetch data using the existing method
            whale_data_list = self.fetch_dune_analytics_query_results(query_id, max_results=limit)
            
            for whale_data in whale_data_list:
                if whale_data.get('address'):
                    addresses.append(AddressData(
                        address=whale_data['address'],
                        blockchain=whale_data.get('blockchain', 'ethereum'),
                        source_system='Dune-Analytics',
                        initial_label=whale_data.get('label', 'whale'),
                        metadata={
                            'balance_native': whale_data.get('balance_native'),
                            'balance_usd': whale_data.get('balance_usd'),
                            'transaction_count': whale_data.get('transaction_count'),
                            'query_id': query_id,
                            'dune_metadata': whale_data,
                            'detection_method': 'dune_analytics_query',
                            'source_details': f'Dune-Query-{query_id}',
                            'discovered_at': datetime.utcnow().isoformat()
                        },
                        confidence_score=whale_data.get('confidence_score', 0.9)
                    ))
            
            self.logger.info(f"Converted {len(addresses)} Dune results to AddressData format")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Error converting Dune query results to addresses: {e}")
            return []
    
    def discover_whales_by_blockchain(self, blockchain: str, limit_per_query: int = 1000) -> List[AddressData]:
        """Discover whales for a specific blockchain using multiple queries."""
        addresses = []
        
        if blockchain not in self.whale_query_ids:
            self.logger.warning(f"No whale queries configured for blockchain: {blockchain}")
            return addresses
        
        queries = self.whale_query_ids[blockchain]
        self.logger.info(f"Running {len(queries)} whale discovery queries for {blockchain}")
        
        for query_name, query_id in queries.items():
            try:
                query_addresses = self.get_whale_addresses_from_query(query_id, limit_per_query)
                addresses.extend(query_addresses)
                
                self.logger.info(f"Query '{query_name}' ({query_id}): Found {len(query_addresses)} addresses")
                
                # Rate limiting between queries
                time.sleep(2)
                
            except Exception as e:
                self.logger.error(f"Failed to run query '{query_name}' ({query_id}): {e}")
                continue
        
        # Remove duplicates while preserving order
        unique_addresses = []
        seen_addresses = set()
        
        for addr in addresses:
            addr_key = f"{addr.address}_{addr.blockchain}"
            if addr_key not in seen_addresses:
                unique_addresses.append(addr)
                seen_addresses.add(addr_key)
        
        self.logger.info(f"Discovered {len(unique_addresses)} unique whale addresses for {blockchain}")
        return unique_addresses
    
    def _is_valid_whale_result(self, row: Dict[str, Any]) -> bool:
        """Validate if a Dune result row contains valid whale data."""
        # Must have an address
        address = row.get('address') or row.get('wallet') or row.get('user_address')
        if not address:
            return False
        
        # Address format validation
        address_str = str(address)
        if not (address_str.startswith('0x') and len(address_str) == 42):
            # For Bitcoin/other chains, add other validations
            if not any(address_str.startswith(prefix) for prefix in ['1', '3', 'bc1', 'D', 'L', 'M', 'X', 'T']):
                return False
        
        # Should have some meaningful whale indicators
        balance = row.get('balance') or row.get('balance_usd') or row.get('value_usd')
        tx_count = row.get('transaction_count') or row.get('tx_count') or row.get('activity_count')
        
        if balance:
            try:
                balance_value = float(balance)
                if balance_value < 10000:  # Minimum $10k for whale consideration
                    return False
            except (ValueError, TypeError):
                pass
        
        return True
    
    def _process_dune_result_row(self, row: Dict[str, Any], query_id: int) -> Optional[Dict[str, Any]]:
        """Process a Dune result row into standardized whale data."""
        try:
            # Extract address with multiple field name attempts
            address = (row.get('address') or 
                      row.get('wallet') or 
                      row.get('user_address') or 
                      row.get('from_address') or 
                      row.get('to_address'))
            
            if not address:
                return None
            
            # Standardize address format
            address = str(address).strip()
            
            # Extract blockchain info
            blockchain = (row.get('blockchain') or 
                         row.get('chain') or 
                         row.get('network') or 
                         self._detect_blockchain_from_query(query_id))
            
            # Extract balance information
            balance_native = self._extract_numeric_value(row, [
                'balance', 'balance_native', 'amount', 'native_balance'
            ])
            
            balance_usd = self._extract_numeric_value(row, [
                'balance_usd', 'value_usd', 'usd_value', 'amount_usd'
            ])
            
            # Extract transaction/activity data
            transaction_count = self._extract_numeric_value(row, [
                'transaction_count', 'tx_count', 'activity_count', 'num_transactions'
            ])
            
            # Extract labels/tags
            label = (row.get('label') or 
                    row.get('tag') or 
                    row.get('entity_name') or 
                    row.get('name') or 
                    'whale')
            
            # Calculate confidence score based on data quality
            confidence_score = self._calculate_confidence_score(row, balance_usd, transaction_count)
            
            return {
                'address': address,
                'blockchain': blockchain,
                'balance_native': balance_native,
                'balance_usd': balance_usd,
                'transaction_count': transaction_count,
                'label': label,
                'confidence_score': confidence_score,
                'raw_data': row
            }
            
        except Exception as e:
            self.logger.debug(f"Error processing Dune result row: {e}")
            return None
    
    def _extract_numeric_value(self, row: Dict[str, Any], field_names: List[str]) -> Optional[float]:
        """Extract numeric value from row using multiple field name attempts."""
        for field_name in field_names:
            if field_name in row:
                try:
                    value = row[field_name]
                    if value is not None:
                        return float(value)
                except (ValueError, TypeError):
                    continue
        return None
    
    def _detect_blockchain_from_query(self, query_id: int) -> str:
        """Detect blockchain from query ID mapping."""
        for blockchain, queries in self.whale_query_ids.items():
            if query_id in queries.values():
                return blockchain
        return 'ethereum'  # Default fallback
    
    def _calculate_confidence_score(self, row: Dict[str, Any], balance_usd: Optional[float], 
                                  transaction_count: Optional[float]) -> float:
        """Calculate confidence score based on data completeness and whale indicators."""
        score = 0.7  # Base score
        
        # Boost for high USD balance
        if balance_usd:
            if balance_usd > 10_000_000:  # $10M+
                score += 0.2
            elif balance_usd > 1_000_000:  # $1M+
                score += 0.15
            elif balance_usd > 100_000:  # $100K+
                score += 0.1
        
        # Boost for high transaction activity
        if transaction_count:
            if transaction_count > 1000:
                score += 0.1
            elif transaction_count > 100:
                score += 0.05
        
        # Boost for additional metadata
        if row.get('label') or row.get('entity_name'):
            score += 0.05
        
        if row.get('first_seen') or row.get('last_seen'):
            score += 0.05
        
        return min(score, 1.0)  # Cap at 1.0


class MarketDataProvider:
    """
    Professional-grade market data provider for the Opportunity Engine.
    
    Provides real-time and historical market data for any ERC-20 token or crypto asset,
    with comprehensive caching, error handling, and rate limiting.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        self.session = requests.Session()
        self.base_url = "https://api.coingecko.com/api/v3"
        self.api_key = api_key
        
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
                'granularity': '5min'
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
                'granularity': 'hourly' if days <= 90 else 'daily'
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


# Create an alias for backward compatibility
PriceService = MarketDataProvider


class RichListScraper:
    """Enhanced scraper for rich lists from block explorers with robust HTML parsing."""
    
    def __init__(self, price_service: PriceService):
        self.price_service = price_service
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        self.logger = logging.getLogger(f"{__name__}.RichListScraper")
    
    def scrape_etherscan_rich_list(self, limit: int = 10000) -> List[AddressData]:
        """Scrape Etherscan rich list with robust parsing for top ETH holders."""
        addresses = []
        
        try:
            self.logger.info(f"Scraping Etherscan rich list (target: {limit} addresses)...")
            
            # Get ETH price for USD conversion
            eth_price = self.price_service.get_price('ETH')
            self.logger.info(f"Current ETH price: ${eth_price:,.2f}")
            
            # Calculate pages needed (approximately 100 addresses per page)
            pages_to_scrape = min(100, (limit // 100) + 1)
            self.logger.info(f"Scraping {pages_to_scrape} pages to target {limit} addresses")
            
            for page in range(1, pages_to_scrape + 1):
                if len(addresses) >= limit:
                    break
                    
                try:
                    page_addresses = self._scrape_etherscan_page(page, eth_price)
                    addresses.extend(page_addresses)
                    
                    self.logger.info(f"Page {page}: Found {len(page_addresses)} addresses (Total: {len(addresses)})")
                    
                    # Respectful rate limiting
                    time.sleep(1.5)
                    
                except Exception as e:
                    self.logger.warning(f"Failed to scrape Etherscan page {page}: {e}")
                    continue
            
            self.logger.info(f"Successfully scraped {len(addresses)} addresses from Etherscan rich list")
            return addresses[:limit]  # Ensure we don't exceed the limit
            
        except Exception as e:
            self.logger.error(f"Failed to scrape Etherscan rich list: {e}")
            return []
    
    def _scrape_etherscan_page(self, page: int, eth_price: float) -> List[AddressData]:
        """Scrape a single Etherscan accounts page with multiple parsing strategies."""
        addresses = []
        
        try:
            url = f"https://etherscan.io/accounts/{page}"
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Strategy 1: Try to find the main table
            table = None
            table_selectors = [
                'table.table',
                'table',
                'div.table-responsive table',
                '.table-responsive table'
            ]
            
            for selector in table_selectors:
                table = soup.select_one(selector)
                if table:
                    break
            
            if not table:
                self.logger.warning(f"No table found on Etherscan page {page}")
                return addresses
            
            # Strategy 2: Find all rows with flexible detection
            rows = table.find_all('tr')
            if len(rows) <= 1:
                self.logger.warning(f"No data rows found on Etherscan page {page}")
                return addresses
            
            # Skip header row
            data_rows = rows[1:]
            
            for i, row in enumerate(data_rows):
                try:
                    address_data = self._parse_etherscan_row(row, page, i + 1, eth_price)
                    if address_data:
                        addresses.append(address_data)
                except Exception as e:
                    self.logger.debug(f"Failed to parse row {i+1} on page {page}: {e}")
                    continue
            
            return addresses
            
        except Exception as e:
            self.logger.error(f"Error scraping Etherscan page {page}: {e}")
            return []
    
    def _parse_etherscan_row(self, row, page: int, row_index: int, eth_price: float) -> Optional[AddressData]:
        """Parse a single row from Etherscan with multiple fallback strategies."""
        try:
            # Get all cells
            cells = row.find_all(['td', 'th'])
            if len(cells) < 2:
                return None
            
            # Strategy 1: Extract address from links
            address = None
            name_tag = None
            
            for cell in cells:
                # Look for address links
                links = cell.find_all('a')
                for link in links:
                    href = link.get('href', '')
                    if '/address/' in href:
                        addr_candidate = href.split('/address/')[-1].split('?')[0]
                        if self._is_valid_ethereum_address(addr_candidate):
                            address = addr_candidate
                            
                            # Look for name tag in the same cell
                            name_spans = cell.find_all('span')
                            for span in name_spans:
                                span_class = span.get('class', [])
                                if any('text-muted' in str(cls) for cls in span_class):
                                    name_tag = span.get_text(strip=True)
                            break
                
                if address:
                    break
            
            if not address:
                # Fallback: Use regex to find addresses in row text
                row_text = row.get_text()
                import re
                addr_matches = re.findall(r'0x[a-fA-F0-9]{40}', row_text)
                if addr_matches:
                    address = addr_matches[0]
            
            if not address:
                return None
            
            # Strategy 2: Extract balance with multiple approaches
            balance_eth = None
            
            # Look for balance in cells
            for cell in cells:
                cell_text = cell.get_text(strip=True)
                
                # Try to find ETH balance
                if 'ETH' in cell_text or any(char.isdigit() for char in cell_text):
                    # Extract numbers from the cell
                    import re
                    
                    # Look for patterns like "123,456.78" or "123456.78"
                    balance_patterns = [
                        r'([\d,]+\.?\d*)\s*ETH',  # "123,456.78 ETH"
                        r'([\d,]+\.?\d*)',        # Just numbers
                    ]
                    
                    for pattern in balance_patterns:
                        matches = re.findall(pattern, cell_text)
                        if matches:
                            try:
                                balance_str = matches[0].replace(',', '')
                                balance_eth = float(balance_str)
                                break
                            except (ValueError, IndexError):
                                continue
                    
                    if balance_eth is not None:
                        break
            
            # If still no balance found, try to extract from anywhere in the row
            if balance_eth is None:
                row_text = row.get_text()
                import re
                # Look for any number that could be a balance
                numbers = re.findall(r'[\d,]+\.?\d*', row_text)
                for num_str in numbers:
                    try:
                        num = float(num_str.replace(',', ''))
                        # ETH balances for rich list should be substantial
                        if num > 100:  # Minimum 100 ETH for rich list
                            balance_eth = num
                            break
                    except ValueError:
                        continue
            
            if balance_eth is None or balance_eth <= 0:
                return None
            
            # Calculate USD value
            balance_usd = balance_eth * eth_price
            
            # Determine if this is likely an exchange
            is_exchange = False
            if name_tag:
                exchange_keywords = ['exchange', 'binance', 'coinbase', 'kraken', 'gate.io', 'huobi', 'okx', 'upbit', 'bitfinex']
                is_exchange = any(keyword in name_tag.lower() for keyword in exchange_keywords)
            
            # Calculate rank (approximate)
            rank = (page - 1) * 100 + row_index
            
            return AddressData(
                address=address,
                blockchain='ethereum',
                source_system='Etherscan-RichList',
                initial_label='exchange' if is_exchange else 'whale',
                metadata={
                    'rank': rank,
                    'balance_eth': balance_eth,
                    'balance_usd': balance_usd,
                    'name_tag': name_tag,
                    'is_exchange': is_exchange,
                    'detection_method': 'rich_list_scraping',
                    'source_page': page,
                    'source_details': f'Etherscan Rich List Page {page}',
                    'discovered_at': datetime.utcnow().isoformat()
                },
                confidence_score=0.95 if not is_exchange else 0.4
            )
            
        except Exception as e:
            self.logger.debug(f"Error parsing Etherscan row: {e}")
            return None
    
    def scrape_polygonscan_rich_list(self, limit: int = 10000) -> List[AddressData]:
        """Scrape Polygonscan rich list with robust parsing for top MATIC holders."""
        addresses = []
        
        try:
            self.logger.info(f"Scraping Polygonscan rich list (target: {limit} addresses)...")
            
            # Get MATIC price for USD conversion
            matic_price = self.price_service.get_price('MATIC')
            self.logger.info(f"Current MATIC price: ${matic_price:.4f}")
            
            # Calculate pages needed
            pages_to_scrape = min(100, (limit // 100) + 1)
            self.logger.info(f"Scraping {pages_to_scrape} pages to target {limit} addresses")
            
            for page in range(1, pages_to_scrape + 1):
                if len(addresses) >= limit:
                    break
                    
                try:
                    page_addresses = self._scrape_polygonscan_page(page, matic_price)
                    addresses.extend(page_addresses)
                    
                    self.logger.info(f"Page {page}: Found {len(page_addresses)} addresses (Total: {len(addresses)})")
                    
                    # Respectful rate limiting
                    time.sleep(1.5)
                    
                except Exception as e:
                    self.logger.warning(f"Failed to scrape Polygonscan page {page}: {e}")
                    continue
            
            self.logger.info(f"Successfully scraped {len(addresses)} addresses from Polygonscan rich list")
            return addresses[:limit]
            
        except Exception as e:
            self.logger.error(f"Failed to scrape Polygonscan rich list: {e}")
            return []
    
    def _scrape_polygonscan_page(self, page: int, matic_price: float) -> List[AddressData]:
        """Scrape a single Polygonscan accounts page."""
        addresses = []
        
        try:
            url = f"https://polygonscan.com/accounts/{page}"
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find table using multiple strategies
            table = None
            table_selectors = [
                'table.table',
                'table',
                'div.table-responsive table',
                '.table-responsive table'
            ]
            
            for selector in table_selectors:
                table = soup.select_one(selector)
                if table:
                    break
            
            if not table:
                self.logger.warning(f"No table found on Polygonscan page {page}")
                return addresses
            
            rows = table.find_all('tr')
            if len(rows) <= 1:
                return addresses
            
            data_rows = rows[1:]
            
            for i, row in enumerate(data_rows):
                try:
                    address_data = self._parse_polygonscan_row(row, page, i + 1, matic_price)
                    if address_data:
                        addresses.append(address_data)
                except Exception as e:
                    self.logger.debug(f"Failed to parse Polygonscan row {i+1} on page {page}: {e}")
                    continue
            
            return addresses
            
        except Exception as e:
            self.logger.error(f"Error scraping Polygonscan page {page}: {e}")
            return []
    
    def _parse_polygonscan_row(self, row, page: int, row_index: int, matic_price: float) -> Optional[AddressData]:
        """Parse a single row from Polygonscan with fallback strategies."""
        try:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 2:
                return None
            
            # Extract address
            address = None
            name_tag = None
            
            for cell in cells:
                links = cell.find_all('a')
                for link in links:
                    href = link.get('href', '')
                    if '/address/' in href:
                        addr_candidate = href.split('/address/')[-1].split('?')[0]
                        if self._is_valid_ethereum_address(addr_candidate):
                            address = addr_candidate
                            
                            # Look for name tag
                            name_spans = cell.find_all('span')
                            for span in name_spans:
                                span_class = span.get('class', [])
                                if any('text-muted' in str(cls) for cls in span_class):
                                    name_tag = span.get_text(strip=True)
                            break
                
                if address:
                    break
            
            if not address:
                # Fallback regex
                row_text = row.get_text()
                import re
                addr_matches = re.findall(r'0x[a-fA-F0-9]{40}', row_text)
                if addr_matches:
                    address = addr_matches[0]
            
            if not address:
                return None
            
            # Extract balance
            balance_matic = None
            
            for cell in cells:
                cell_text = cell.get_text(strip=True)
                
                if 'MATIC' in cell_text or any(char.isdigit() for char in cell_text):
                    import re
                    balance_patterns = [
                        r'([\d,]+\.?\d*)\s*MATIC',
                        r'([\d,]+\.?\d*)',
                    ]
                    
                    for pattern in balance_patterns:
                        matches = re.findall(pattern, cell_text)
                        if matches:
                            try:
                                balance_str = matches[0].replace(',', '')
                                balance_matic = float(balance_str)
                                break
                            except (ValueError, IndexError):
                                continue
                    
                    if balance_matic is not None:
                        break
            
            if balance_matic is None:
                # Try to find any substantial number in the row
                row_text = row.get_text()
                import re
                numbers = re.findall(r'[\d,]+\.?\d*', row_text)
                for num_str in numbers:
                    try:
                        num = float(num_str.replace(',', ''))
                        if num > 1000:  # Minimum 1000 MATIC for rich list
                            balance_matic = num
                            break
                    except ValueError:
                        continue
            
            if balance_matic is None or balance_matic <= 0:
                return None
            
            balance_usd = balance_matic * matic_price
            
            # Check if exchange
            is_exchange = False
            if name_tag:
                exchange_keywords = ['exchange', 'binance', 'coinbase', 'kraken', 'gate.io', 'huobi', 'okx']
                is_exchange = any(keyword in name_tag.lower() for keyword in exchange_keywords)
            
            rank = (page - 1) * 100 + row_index
            
            return AddressData(
                address=address,
                blockchain='polygon',
                source_system='Polygonscan-RichList',
                initial_label='exchange' if is_exchange else 'whale',
                metadata={
                    'rank': rank,
                    'balance_matic': balance_matic,
                    'balance_usd': balance_usd,
                    'name_tag': name_tag,
                    'is_exchange': is_exchange,
                    'detection_method': 'rich_list_scraping',
                    'source_page': page,
                    'source_details': f'Polygonscan Rich List Page {page}',
                    'discovered_at': datetime.utcnow().isoformat()
                },
                confidence_score=0.95 if not is_exchange else 0.4
            )
            
        except Exception as e:
            self.logger.debug(f"Error parsing Polygonscan row: {e}")
            return None
    
    def _is_valid_ethereum_address(self, address: str) -> bool:
        """Validate Ethereum address format."""
        import re
        return bool(re.match(r'^0x[a-fA-F0-9]{40}$', address))


class GitHubWhaleDataCollector:
    """Advanced GitHub collector for whale addresses from specific repositories."""
    
    def __init__(self, github_token: Optional[str] = None):
        self.github_token = github_token
        self.session = requests.Session()
        if github_token:
            self.session.headers['Authorization'] = f'token {github_token}'
        self.session.headers['User-Agent'] = 'Whale-Discovery-Agent/1.0'
        self.logger = logging.getLogger(f"{__name__}.GitHubWhaleDataCollector")
    
    def collect_from_repositories(self, repo_targets: List[Dict[str, str]], limit: int = 5000) -> List[AddressData]:
        """Collect whale addresses from specified GitHub repositories with advanced parsing."""
        addresses = []
        
        # Production repositories with comprehensive whale address data
        production_targets = [
            {
                'repo': 'Pymmdrza/Rich-Address-Wallet',
                'files': [
                    # Bitcoin addresses
                    'BITCOIN/P2PKH.txt.gz',
                    'BITCOIN/P2SH.txt.gz', 
                    'BITCOIN/BECH32.txt.gz',
                    'Bitcoin/ALL.txt',
                    '30000BTCRichWalletAdd.txt',
                    '10000BitcoinRichWalletAdd.txt',
                    
                    # Ethereum addresses
                    'ETHEREUM/ALL.txt',
                    'ETHEREUM/P2PKH.txt.gz',
                    '10000ETHRichAddress.md',
                    '10000richAddressETH.txt',
                    
                    # Other cryptocurrencies
                    'LITECOIN/ALL.txt',
                    'DOGECOIN/ALL.txt',
                    'DASH/ALL.txt',
                    'BITCOIN-CASH/ALL.txt',
                    'TRON/ALL.txt',
                    'ZCASH/ALL.txt'
                ],
                'format': 'mixed'
            },
            {
                'repo': 'Parms-Crypto/SolanaWhaleWatcher', 
                'files': [
                    'Holders_Balances_Master.json',
                    'Holders_Master.json',
                    'data/whale_addresses.json',
                    'data/top_holders.json'
                ],
                'format': 'json'
            }
        ]
        
        # Use provided targets or production defaults
        targets = repo_targets if repo_targets else production_targets
        
        self.logger.info(f"Collecting from {len(targets)} GitHub repositories (target: {limit} addresses)")
        
        for target in targets:
            if len(addresses) >= limit:
                break
                
            try:
                repo_addresses = self._collect_from_single_repo(target, limit - len(addresses))
                addresses.extend(repo_addresses)
                self.logger.info(f"Collected {len(repo_addresses)} addresses from {target['repo']} (Total: {len(addresses)})")
                
                # Rate limiting
                time.sleep(1)
                
            except Exception as e:
                self.logger.error(f"Failed to collect from repository {target['repo']}: {e}")
                continue
        
        self.logger.info(f"Successfully collected {len(addresses)} addresses from GitHub repositories")
        return addresses[:limit]
    
    def _collect_from_single_repo(self, repo_target: Dict[str, str], limit: int) -> List[AddressData]:
        """Collect addresses from a single GitHub repository."""
        addresses = []
        repo_name = repo_target['repo']
        files = repo_target['files']
        format_type = repo_target.get('format', 'text_list')
        
        self.logger.info(f"Processing repository: {repo_name}")
        
        for file_path in files:
            if len(addresses) >= limit:
                break
                
            try:
                file_addresses = self._process_github_file(repo_name, file_path, format_type, limit - len(addresses))
                addresses.extend(file_addresses)
                
                if file_addresses:
                    self.logger.info(f"  {file_path}: {len(file_addresses)} addresses")
                
                # Rate limiting between file requests
                time.sleep(0.5)
                
            except Exception as e:
                self.logger.warning(f"Failed to process {repo_name}/{file_path}: {e}")
                continue
        
        return addresses
    
    def _process_github_file(self, repo_name: str, file_path: str, format_type: str, limit: int) -> List[AddressData]:
        """Process a single file from GitHub repository with format detection."""
        addresses = []
        
        try:
            # Get file content
            content = self._download_github_file(repo_name, file_path)
            if not content:
                return addresses
            
            # Determine blockchain from file path/name
            blockchain = self._detect_blockchain_from_path(file_path)
            
            # Parse based on file format
            if file_path.endswith('.gz'):
                addresses = self._parse_gzipped_file(content, blockchain, file_path, limit)
            elif file_path.endswith('.json'):
                addresses = self._parse_json_file(content, blockchain, file_path, limit)
            elif file_path.endswith('.md'):
                addresses = self._parse_markdown_file(content, blockchain, file_path, limit)
            else:
                addresses = self._parse_text_file(content, blockchain, file_path, limit)
            
            # Add metadata to all addresses
            for addr in addresses:
                addr.metadata.update({
                    'github_metadata': {
                        'repo': repo_name,
                        'file': file_path,
                        'format': format_type
                    },
                    'source_details': f'GitHub-{repo_name}/{file_path}',
                    'discovered_at': datetime.utcnow().isoformat()
                })
            
            return addresses
            
        except Exception as e:
            self.logger.error(f"Error processing GitHub file {repo_name}/{file_path}: {e}")
            return []
    
    def _download_github_file(self, repo_name: str, file_path: str) -> Optional[bytes]:
        """Download file content from GitHub repository."""
        try:
            # Try multiple URL formats
            urls = [
                f"https://raw.githubusercontent.com/{repo_name}/main/{file_path}",
                f"https://raw.githubusercontent.com/{repo_name}/master/{file_path}",
                f"https://api.github.com/repos/{repo_name}/contents/{file_path}"
            ]
            
            for url in urls:
                try:
                    response = self.session.get(url, timeout=30)
                    if response.status_code == 200:
                        # If it's API response, decode base64 content
                        if 'api.github.com' in url:
                            import base64
                            content_data = response.json()
                            if content_data.get('content'):
                                return base64.b64decode(content_data['content'])
                        else:
                            return response.content
                except Exception as e:
                    self.logger.debug(f"Failed to download from {url}: {e}")
                    continue
            
            self.logger.warning(f"Could not download {repo_name}/{file_path} from any URL")
            return None
            
        except Exception as e:
            self.logger.error(f"Error downloading {repo_name}/{file_path}: {e}")
            return None
    
    def _detect_blockchain_from_path(self, file_path: str) -> str:
        """Detect blockchain type from file path."""
        file_path_lower = file_path.lower()
        
        if 'bitcoin' in file_path_lower or 'btc' in file_path_lower:
            return 'bitcoin'
        elif 'ethereum' in file_path_lower or 'eth' in file_path_lower:
            return 'ethereum'
        elif 'litecoin' in file_path_lower or 'ltc' in file_path_lower:
            return 'litecoin'
        elif 'dogecoin' in file_path_lower or 'doge' in file_path_lower:
            return 'dogecoin'
        elif 'dash' in file_path_lower:
            return 'dash'
        elif 'bitcoin-cash' in file_path_lower or 'bch' in file_path_lower:
            return 'bitcoin-cash'
        elif 'tron' in file_path_lower or 'trx' in file_path_lower:
            return 'tron'
        elif 'zcash' in file_path_lower or 'zec' in file_path_lower:
            return 'zcash'
        elif 'solana' in file_path_lower or 'sol' in file_path_lower:
            return 'solana'
        elif 'polygon' in file_path_lower or 'matic' in file_path_lower:
            return 'polygon'
        else:
            return 'unknown'
    
    def _parse_gzipped_file(self, content: bytes, blockchain: str, file_path: str, limit: int) -> List[AddressData]:
        """Parse gzipped text files containing addresses."""
        addresses = []
        
        try:
            import gzip
            # Decompress the content
            decompressed = gzip.decompress(content)
            text_content = decompressed.decode('utf-8', errors='ignore')
            
            # Parse as text file
            addresses = self._parse_text_content(text_content, blockchain, file_path, limit)
            
        except Exception as e:
            self.logger.error(f"Error parsing gzipped file {file_path}: {e}")
        
        return addresses
    
    def _parse_text_file(self, content: bytes, blockchain: str, file_path: str, limit: int) -> List[AddressData]:
        """Parse plain text files containing addresses."""
        try:
            text_content = content.decode('utf-8', errors='ignore')
            return self._parse_text_content(text_content, blockchain, file_path, limit)
        except Exception as e:
            self.logger.error(f"Error parsing text file {file_path}: {e}")
            return []
    
    def _parse_text_content(self, text_content: str, blockchain: str, file_path: str, limit: int) -> List[AddressData]:
        """Parse text content for cryptocurrency addresses."""
        addresses = []
        
        try:
            lines = text_content.split('\n')
            
            for i, line in enumerate(lines):
                if len(addresses) >= limit:
                    break
                
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                # Extract address using patterns
                address = self._extract_address_from_line(line, blockchain)
                
                if address:
                    # Try to extract balance if present in the line
                    balance_native, balance_usd = self._extract_balance_from_line(line, blockchain)
                    
                    addresses.append(AddressData(
                        address=address,
                        blockchain=blockchain,
                        source_system='GitHub-Repository',
                        initial_label='whale',
                        metadata={
                            'line_number': i + 1,
                            'balance_native': balance_native,
                            'balance_usd': balance_usd,
                            'detection_method': 'github_repository_parsing',
                            'address_type': self._get_address_type(address, blockchain, file_path)
                        },
                        confidence_score=0.8
                    ))
            
        except Exception as e:
            self.logger.error(f"Error parsing text content from {file_path}: {e}")
        
        return addresses
    
    def _parse_json_file(self, content: bytes, blockchain: str, file_path: str, limit: int) -> List[AddressData]:
        """Parse JSON files containing address data."""
        addresses = []
        
        try:
            text_content = content.decode('utf-8', errors='ignore')
            data = json.loads(text_content)
            
            # Handle different JSON structures
            if isinstance(data, dict):
                # Handle nested JSON structures
                for key, value in data.items():
                    if len(addresses) >= limit:
                        break
                    
                    if isinstance(value, dict):
                        address = self._extract_address_from_dict(value, blockchain)
                        if address:
                            addresses.append(self._create_address_data_from_dict(address, value, blockchain, file_path))
                    elif isinstance(value, str):
                        address = self._extract_address_from_line(value, blockchain)
                        if address:
                            addresses.append(AddressData(
                                address=address,
                                blockchain=blockchain,
                                source_system='GitHub-Repository',
                                initial_label='whale',
                                metadata={
                                    'json_key': key,
                                    'detection_method': 'github_repository_parsing'
                                },
                                confidence_score=0.8
                            ))
            
            elif isinstance(data, list):
                for i, item in enumerate(data):
                    if len(addresses) >= limit:
                        break
                    
                    if isinstance(item, dict):
                        address = self._extract_address_from_dict(item, blockchain)
                        if address:
                            addresses.append(self._create_address_data_from_dict(address, item, blockchain, file_path))
                    elif isinstance(item, str):
                        address = self._extract_address_from_line(item, blockchain)
                        if address:
                            addresses.append(AddressData(
                                address=address,
                                blockchain=blockchain,
                                source_system='GitHub-Repository',
                                initial_label='whale',
                                metadata={
                                    'array_index': i,
                                    'detection_method': 'github_repository_parsing'
                                },
                                confidence_score=0.8
                            ))
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in {file_path}: {e}")
        except Exception as e:
            self.logger.error(f"Error parsing JSON file {file_path}: {e}")
        
        return addresses
    
    def _parse_markdown_file(self, content: bytes, blockchain: str, file_path: str, limit: int) -> List[AddressData]:
        """Parse markdown files that may contain addresses in code blocks or lists."""
        addresses = []
        
        try:
            text_content = content.decode('utf-8', errors='ignore')
            lines = text_content.split('\n')
            
            for i, line in enumerate(lines):
                if len(addresses) >= limit:
                    break
                
                line = line.strip()
                if not line:
                    continue
                
                # Extract address from markdown content
                address = self._extract_address_from_line(line, blockchain)
                
                if address:
                    addresses.append(AddressData(
                        address=address,
                        blockchain=blockchain,
                        source_system='GitHub-Repository',
                        initial_label='whale',
                        metadata={
                            'line_number': i + 1,
                            'detection_method': 'github_repository_parsing',
                            'source_format': 'markdown'
                        },
                        confidence_score=0.75
                    ))
            
        except Exception as e:
            self.logger.error(f"Error parsing markdown file {file_path}: {e}")
        
        return addresses
    
    def _extract_address_from_line(self, line: str, blockchain: str) -> Optional[str]:
        """Extract cryptocurrency address from a text line."""
        import re
        
        # Define address patterns for different blockchains
        patterns = {
            'bitcoin': [
                r'\b[13][a-km-zA-HJ-NP-Z1-9]{25,34}\b',  # Legacy (P2PKH, P2SH)
                r'\bbc1[a-zA-HJ-NP-Z0-9]{39,59}\b'       # Bech32
            ],
            'ethereum': [r'\b0x[a-fA-F0-9]{40}\b'],
            'litecoin': [r'\b[LM3][a-km-zA-HJ-NP-Z1-9]{26,33}\b'],
            'dogecoin': [r'\bD[5-9A-HJ-NP-U][1-9A-HJ-NP-Za-km-z]{32}\b'],
            'dash': [r'\bX[1-9A-HJ-NP-Za-km-z]{33}\b'],
            'bitcoin-cash': [r'\b[13][a-km-zA-HJ-NP-Z1-9]{25,34}\b'],
            'tron': [r'\bT[A-Za-z1-9]{33}\b'],
            'zcash': [r'\bt1[a-zA-Z0-9]{62}\b'],
            'solana': [r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b'],
            'polygon': [r'\b0x[a-fA-F0-9]{40}\b']
        }
        
        # Try blockchain-specific patterns first
        if blockchain in patterns:
            for pattern in patterns[blockchain]:
                matches = re.findall(pattern, line)
                if matches:
                    return matches[0]
        
        # Fallback: try all patterns
        for blockchain_type, blockchain_patterns in patterns.items():
            for pattern in blockchain_patterns:
                matches = re.findall(pattern, line)
                if matches:
                    return matches[0]
        
        return None
    
    def _extract_address_from_dict(self, data: dict, blockchain: str) -> Optional[str]:
        """Extract address from a dictionary object."""
        # Common field names for addresses
        address_fields = ['address', 'wallet', 'account', 'pubkey', 'owner', 'holder']
        
        for field in address_fields:
            if field in data:
                address_candidate = str(data[field])
                # Validate the address
                address = self._extract_address_from_line(address_candidate, blockchain)
                if address:
                    return address
        
        # If no direct field, search all string values
        for key, value in data.items():
            if isinstance(value, str):
                address = self._extract_address_from_line(value, blockchain)
                if address:
                    return address
        
        return None
    
    def _create_address_data_from_dict(self, address: str, data: dict, blockchain: str, file_path: str) -> AddressData:
        """Create AddressData from dictionary with extracted metadata."""
        
        # Extract balance information
        balance_native = None
        balance_usd = None
        
        balance_fields = ['balance', 'amount', 'value', 'holdings', 'tokens']
        for field in balance_fields:
            if field in data:
                try:
                    balance_native = float(data[field])
                    break
                except (ValueError, TypeError):
                    continue
        
        # Extract label/name information
        label = 'whale'
        name_fields = ['name', 'label', 'tag', 'entity']
        for field in name_fields:
            if field in data and data[field]:
                label = str(data[field])
                break
        
        return AddressData(
            address=address,
            blockchain=blockchain,
            source_system='GitHub-Repository',
            initial_label=label,
            metadata={
                'balance_native': balance_native,
                'balance_usd': balance_usd,
                'detection_method': 'github_repository_parsing',
                'json_data': data,
                'source_format': 'json'
            },
            confidence_score=0.85
        )
    
    def _extract_balance_from_line(self, line: str, blockchain: str) -> tuple:
        """Extract balance information from a text line."""
        import re
        
        balance_native = None
        balance_usd = None
        
        # Look for balance patterns
        balance_patterns = [
            r'(\d+\.?\d*)\s*BTC',
            r'(\d+\.?\d*)\s*ETH', 
            r'(\d+\.?\d*)\s*MATIC',
            r'(\d+\.?\d*)\s*SOL',
            r'\$(\d+\.?\d*)',
            r'(\d+,?\d*\.?\d*)\s*USD'
        ]
        
        for pattern in balance_patterns:
            matches = re.findall(pattern, line)
            if matches:
                try:
                    value = float(matches[0].replace(',', ''))
                    if '$' in pattern or 'USD' in pattern:
                        balance_usd = value
                    else:
                        balance_native = value
                    break
                except ValueError:
                    continue
        
        return balance_native, balance_usd
    
    def _get_address_type(self, address: str, blockchain: str, file_path: str) -> str:
        """Determine address type from patterns and file path."""
        if 'P2PKH' in file_path:
            return 'P2PKH'
        elif 'P2SH' in file_path:
            return 'P2SH'
        elif 'BECH32' in file_path:
            return 'BECH32'
        elif blockchain == 'bitcoin':
            if address.startswith('1'):
                return 'P2PKH'
            elif address.startswith('3'):
                return 'P2SH'
            elif address.startswith('bc1'):
                return 'BECH32'
        elif blockchain in ['ethereum', 'polygon']:
            return 'EOA'  # Assume EOA unless determined otherwise
        
        return 'unknown'


class ComprehensiveWhaleDiscovery:
    """Comprehensive whale discovery system using multiple sources."""
    
    def __init__(self, api_keys: Dict[str, str], test_mode: bool = False):
        self.api_keys = api_keys
        self.test_mode = test_mode
        self.price_service = PriceService()
        self.rich_list_scraper = RichListScraper(self.price_service)
        self.github_collector = GitHubWhaleDataCollector(api_keys.get('github_token'))
        
        # Initialize Dune Analytics if API key available
        self.dune_analytics = None
        dune_key = api_keys.get('dune') or api_keys.get('dune_api_key') or api_keys.get('DUNE_API_KEY')
        if dune_key:
            try:
                self.dune_analytics = DuneAnalyticsAPI(dune_key)
                logger.info("Dune Analytics API initialized successfully")
            except Exception as e:
                logger.warning(f"Failed to initialize Dune Analytics: {e}")
        else:
            logger.info("Dune Analytics API key not found - will use mock data")
        
        self.stats = {
            'total_from_etherscan': 0,
            'total_from_polygonscan': 0,
            'total_from_github': 0,
            'total_from_dune': 0,
            'total_collected': 0
        }
        
        logger.info("🔍 Comprehensive whale discovery system initialized")
    
    def discover_whale_addresses(self, limit_per_source: int = 5000) -> List[AddressData]:
        """Discover whale addresses from all available sources."""
        all_addresses = []
        
        # Adjust limits for test mode
        if self.test_mode:
            limit_per_source = 5
            logger.info("🧪 Test mode enabled - using minimal limits")
        
        try:
            # 1. Scrape Etherscan rich list
            logger.info("📊 Scraping Etherscan rich list...")
            etherscan_addresses = self.rich_list_scraper.scrape_etherscan_rich_list(limit_per_source)
            all_addresses.extend(etherscan_addresses)
            self.stats['total_from_etherscan'] = len(etherscan_addresses)
            
            # 2. Scrape Polygonscan rich list
            logger.info("📊 Scraping Polygonscan rich list...")
            polygonscan_addresses = self.rich_list_scraper.scrape_polygonscan_rich_list(limit_per_source)
            all_addresses.extend(polygonscan_addresses)
            self.stats['total_from_polygonscan'] = len(polygonscan_addresses)
            
            # 3. Collect from GitHub repositories
            logger.info("📁 Collecting from GitHub repositories...")
            github_addresses = self.github_collector.collect_from_repositories([], limit_per_source)
            all_addresses.extend(github_addresses)
            self.stats['total_from_github'] = len(github_addresses)
            
            # 4. Fetch from Dune Analytics
            if self.dune_analytics:
                logger.info("📈 Fetching from Dune Analytics...")
                # Common whale query IDs (example IDs, replace with actual productive queries)
                whale_query_ids = [1234567, 2345678, 3456789] if not self.test_mode else [1234567]
                dune_addresses = []
                
                for query_id in whale_query_ids:
                    try:
                        query_addresses = self.dune_analytics.get_whale_addresses_from_query(query_id, limit_per_source)
                        dune_addresses.extend(query_addresses)
                    except Exception as e:
                        logger.warning(f"Failed to fetch from Dune query {query_id}: {e}")
                
                all_addresses.extend(dune_addresses)
                self.stats['total_from_dune'] = len(dune_addresses)
            else:
                logger.info("📈 Dune Analytics not available - generating mock whale data...")
                # Generate mock Dune data for testing
                mock_dune_addresses = []
                mock_count = 5 if self.test_mode else 20
                
                for i in range(mock_count):
                    mock_dune_addresses.append(AddressData(
                        address=f"0xddddddddddddddddddddddddddddddddddddddd{i:02d}",
                        blockchain='ethereum',
                        source_system='Dune-Query-TestMode',
                        initial_label='whale',
                        metadata={
                            'balance_usd': 2000000 + i * 300000,
                            'dune_query_id': 'mock',
                            'whale_score': 0.85,
                            'transaction_count': 1000 + i * 100,
                            'detection_method': 'dune_analytics',
                            'mock_data': True
                        },
                        confidence_score=0.85
                    ))
                
                all_addresses.extend(mock_dune_addresses)
                self.stats['total_from_dune'] = len(mock_dune_addresses)
            
            # Remove duplicates while preserving order
            unique_addresses = []
            seen_addresses = set()
            
            for addr in all_addresses:
                if addr.address.lower() not in seen_addresses:
                    unique_addresses.append(addr)
                    seen_addresses.add(addr.address.lower())
            
            self.stats['total_collected'] = len(unique_addresses)
            
            logger.info(f"🎯 Comprehensive discovery complete: {len(unique_addresses)} unique whale addresses")
            self._print_discovery_stats()
            
            return unique_addresses
            
        except Exception as e:
            logger.error(f"Comprehensive whale discovery failed: {e}")
            return []
    
    def _print_discovery_stats(self):
        """Print detailed discovery statistics."""
        logger.info("📊 Whale Discovery Statistics:")
        logger.info(f"  • Etherscan: {self.stats['total_from_etherscan']} addresses")
        logger.info(f"  • Polygonscan: {self.stats['total_from_polygonscan']} addresses")
        logger.info(f"  • GitHub: {self.stats['total_from_github']} addresses")
        logger.info(f"  • Dune Analytics: {self.stats['total_from_dune']} addresses")
        logger.info(f"  • Total Unique: {self.stats['total_collected']} addresses")


def get_moralis_token_metadata(token_address: str, chain: str = "eth") -> Dict[str, Any]:
    """
    Placeholder function for Moralis token metadata (not implemented).
    Returns empty dict to prevent import errors.
    """
    return {}

def get_zerion_portfolio_analysis(address: str) -> Dict[str, Any]:
    """
    Placeholder function for Zerion portfolio analysis (not implemented).
    Returns empty dict to prevent import errors.
    """
    return {}

def enhanced_cex_address_matching(from_addr: str, to_addr: str, blockchain: str = "ethereum") -> Tuple[Optional[str], float, List[str]]:
    """
    Enhanced CEX address matching with improved logic.
    Returns match result, confidence, and evidence.
    """
    from data.addresses import known_exchange_addresses
    
    # Normalize addresses
    from_addr = from_addr.lower() if from_addr else ""
    to_addr = to_addr.lower() if to_addr else ""
    
    evidence = []
    cex_exchange = None
    confidence = 0.0
    
    # Check if from_addr is a known CEX
    if from_addr in known_exchange_addresses:
        cex_exchange = known_exchange_addresses[from_addr]
        evidence.append(f"From address matches {cex_exchange}")
        confidence = 0.90
    
    # Check if to_addr is a known CEX
    elif to_addr in known_exchange_addresses:
        cex_exchange = known_exchange_addresses[to_addr]
        evidence.append(f"To address matches {cex_exchange}")
        confidence = 0.90
    
    return cex_exchange, confidence, evidence
def get_moralis_token_metadata(token_address: str, chain: str = "eth"):
    """Placeholder function for Moralis token metadata (not implemented)."""
    return {}

def get_zerion_portfolio_analysis(address: str):
    """Placeholder function for Zerion portfolio analysis (not implemented)."""
    return {}

def enhanced_cex_address_matching(from_addr: str, to_addr: str, blockchain: str = "ethereum"):
    """Enhanced CEX address matching with improved logic."""
    from data.addresses import known_exchange_addresses
    
    # Normalize addresses
    from_addr = from_addr.lower() if from_addr else ""
    to_addr = to_addr.lower() if to_addr else ""
    
    evidence = []
    cex_exchange = None
    confidence = 0.0
    
    # Check if from_addr is a known CEX
    if from_addr in known_exchange_addresses:
        cex_exchange = known_exchange_addresses[from_addr]
        evidence.append(f"From address matches {cex_exchange}")
        confidence = 0.90
    
    # Check if to_addr is a known CEX
    elif to_addr in known_exchange_addresses:
        cex_exchange = known_exchange_addresses[to_addr]
        evidence.append(f"To address matches {cex_exchange}")
        confidence = 0.90
    
    return cex_exchange, confidence, evidence

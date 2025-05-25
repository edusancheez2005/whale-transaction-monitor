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
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
from datetime import datetime, timedelta
import requests
import websockets
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential
from ratelimit import limits, sleep_and_retry
import backoff

# Configure logging
logger = logging.getLogger(__name__)


@dataclass
class AddressData:
    """Standard format for collected address data."""
    address: str
    blockchain: str
    source_system: str
    initial_label: Optional[str] = None
    confidence_score: float = 0.5
    metadata: Optional[Dict[str, Any]] = None
    collected_at: Optional[datetime] = None
    
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
    @limits(calls=5, period=1)  # Default rate limit
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
            self.logger.error(f"API request failed for {endpoint}: {e}")
            return {}
        except (ValueError, TypeError) as e:
            self.logger.error(f"JSON parsing failed for {endpoint}: {e}")
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
    """Covalent API integration for multi-chain data."""
    
    def __init__(self, api_key: str):
        super().__init__(
            api_key=api_key,
            base_url="https://api.covalenthq.com/v1",
            rate_limit_per_second=2
        )
        
        # Add API key to headers
        self.session.headers.update({'Authorization': f'Bearer {self.api_key}'})
    
    def get_token_holders_multichain(self, chain_id: int = 1, contract_address: str = None, limit: int = 5000) -> List[AddressData]:
        """Get token holders across multiple chains (significant holders only)."""
        addresses = []
        
        # Popular token contracts to check
        popular_tokens = {
            1: [  # Ethereum
                "0xA0b86a33E6441e6C7d3E4081f7567b0b2b2b8b0a",  # USDC
                "0xdAC17F958D2ee523a2206206994597C13D831ec7",  # USDT
                "0x6B175474E89094C44Da98b954EedeAC495271d0F",  # DAI
                "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",  # WBTC
                "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984",  # UNI
            ],
            137: [  # Polygon
                "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",  # USDC
                "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",  # USDT
            ]
        }
        
        tokens_to_check = popular_tokens.get(chain_id, [contract_address]) if contract_address else popular_tokens.get(chain_id, [])
        
        try:
            for token_contract in tokens_to_check:
                if len(addresses) >= limit:
                    break
                    
                try:
                    # Get token holders with pagination
                    page = 0
                    max_pages = 20  # Limit to avoid excessive API calls
                    
                    while len(addresses) < limit and page < max_pages:
                        endpoint = f"{chain_id}/tokens/{token_contract}/token_holders/"
                        params = {
                            'page-size': 100,  # Max page size
                            'page-number': page
                        }
                        
                        response = self._make_request(endpoint, params)
                        
                        if response.get('data') and response['data'].get('items'):
                            chain_name = self._get_chain_name(chain_id)
                            holders = response['data']['items']
                            
                            if not holders:  # No more holders
                                break
                            
                            for holder in holders:
                                if len(addresses) >= limit:
                                    break
                                    
                                # Filter for significant holders (>$100 value)
                                try:
                                    balance_quote = float(holder.get('balance_quote', 0))
                                except (ValueError, TypeError):
                                    balance_quote = 0
                                
                                if balance_quote > 100:  # Only significant holders
                                    addresses.append(AddressData(
                                        address=holder['address'],
                                        blockchain=chain_name,
                                        source_system='covalent_api',
                                        initial_label=f'Token Holder (${balance_quote:.0f})',
                                        metadata={
                                            'contract_address': token_contract,
                                            'balance': holder.get('balance'),
                                            'balance_quote': balance_quote,
                                            'chain_id': chain_id,
                                            'token_symbol': holder.get('contract_ticker_symbol')
                                        }
                                    ))
                            
                            page += 1
                            
                            # Add delay to respect rate limits
                            import time
                            time.sleep(0.5)  # Conservative delay for free tier
                        else:
                            break  # No more data
                            
                except Exception as e:
                    self.logger.error(f"Failed to get holders for token {token_contract}: {e}")
                    continue
            
            self.logger.info(f"Extracted {len(addresses)} significant token holders from Covalent")
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
        """Collect addresses from all available APIs with enhanced volume and 30-day filtering."""
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
            all_addresses.extend(self.apis['polygonscan'].get_top_accounts_by_balance(limit=5000))
        
        # Solana data - Enhanced collection
        if 'solscan' in self.apis:
            self.logger.info("Collecting from Solscan API (enhanced volume, significant holders)...")
            all_addresses.extend(self.apis['solscan'].get_token_holders(limit=5000))
        
        if 'helius' in self.apis:
            self.logger.info("Collecting from Helius API (enhanced volume, recent activity)...")
            all_addresses.extend(self.apis['helius'].get_program_accounts(limit=5000))
        
        # Multi-chain data - Enhanced collection
        if 'covalent' in self.apis:
            self.logger.info("Collecting from Covalent API (enhanced volume, last 30 days)...")
            # Ethereum
            all_addresses.extend(self.apis['covalent'].get_token_holders_multichain(chain_id=1, limit=5000))
            all_addresses.extend(self.apis['covalent'].get_recent_transactions(chain_id=1, limit=5000))
            # Polygon
            all_addresses.extend(self.apis['covalent'].get_token_holders_multichain(chain_id=137, limit=2500))
            all_addresses.extend(self.apis['covalent'].get_recent_transactions(chain_id=137, limit=2500))
        
        if 'moralis' in self.apis:
            self.logger.info("Collecting from Moralis API (enhanced volume, popular NFT collections)...")
            try:
                # Ethereum NFTs
                all_addresses.extend(self.apis['moralis'].get_nft_owners(chain="eth", limit=5000))
                # Polygon NFTs
                all_addresses.extend(self.apis['moralis'].get_nft_owners(chain="polygon", limit=2500))
            except Exception as e:
                self.logger.error(f"Failed to get NFT owners: {e}")
        
        # Whale Alert data - Enhanced collection
        if 'whale_alert' in self.apis:
            self.logger.info("Collecting from Whale Alert API (enhanced volume, last 30 days)...")
            all_addresses.extend(self.apis['whale_alert'].get_recent_transactions(min_value=100000, limit=5000))
        
        # Bitquery data - Enhanced collection
        if 'bitquery' in self.apis:
            self.logger.info("Collecting from Bitquery API (enhanced volume, high-value DEX trades, last 30 days)...")
            all_addresses.extend(self.apis['bitquery'].get_dex_traders(limit=5000))
        
        # Dune data - Enhanced collection
        if 'dune' in self.apis:
            self.logger.info("Collecting from Dune API (enhanced volume)...")
            # Popular public query IDs for whale/large holder analysis
            whale_query_ids = [
                3237150,  # Example whale tracking query
                3237151,  # Example large holder query
                2030469,  # Example DEX volume query
                1746140,  # Example token holder query
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
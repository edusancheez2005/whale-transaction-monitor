#!/usr/bin/env python3
"""
Real Etherscan/Polygonscan Polling Implementation

This module implements actual blockchain polling to fetch Uniswap swap events
from Etherscan and Polygonscan APIs, then processes them through the classification engine.
"""

import sys
import os
import time
import asyncio
import logging
from typing import Dict, List, Optional
import requests
from datetime import datetime, timezone

# Add config path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))

try:
    import api_keys
    import settings
    from utils.real_time_classification import classify_swap_transaction
except ImportError as e:
    logging.error(f"Missing required imports: {e}")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EtherscanPoller:
    """Real implementation of Etherscan/Polygonscan polling for Uniswap swaps."""
    
    def __init__(self, chain: str):
        """Initialize the poller for a specific chain."""
        self.chain = chain
        self.last_processed_block = None
        
        # Set up API configuration
        if chain == 'ethereum':
            self.api_key = api_keys.ETHERSCAN_API_KEY
            self.base_url = "https://api.etherscan.io/api"
            self.contracts = settings.DEX_CONTRACTS['ethereum']
            self.event_signatures = settings.EVENT_SIGNATURES
        elif chain == 'polygon':
            self.api_key = api_keys.POLYGONSCAN_API_KEY
            self.base_url = "https://api.polygonscan.com/api"
            self.contracts = settings.DEX_CONTRACTS['polygon']
            self.event_signatures = settings.EVENT_SIGNATURES
        else:
            raise ValueError(f"Unsupported chain: {chain}")
        
        logger.info(f"✅ EtherscanPoller initialized for {chain}")
    
    def get_latest_block(self) -> int:
        """Get the latest block number from the blockchain."""
        try:
            params = {
                'module': 'proxy',
                'action': 'eth_blockNumber',
                'apikey': self.api_key
            }
            
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if 'result' in data:
                # Convert hex to int
                block_number = int(data['result'], 16)
                return block_number
            else:
                logger.error(f"Invalid response from {self.chain}: {data}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to get latest block for {self.chain}: {e}")
            return None
    
    def get_uniswap_swap_logs(self, from_block: int, to_block: int) -> List[Dict]:
        """Get Uniswap swap logs from a block range."""
        try:
            # Get Uniswap V2 swaps
            v2_logs = self._get_logs_for_event(
                from_block, to_block, 
                self.event_signatures['uniswap_v2_swap']
            )
            
            # Get Uniswap V3 swaps  
            v3_logs = self._get_logs_for_event(
                from_block, to_block,
                self.event_signatures['uniswap_v3_swap']
            )
            
            # Combine and tag with DEX type
            all_logs = []
            for log in v2_logs:
                log['dex'] = 'uniswap_v2'
                all_logs.append(log)
            
            for log in v3_logs:
                log['dex'] = 'uniswap_v3'
                all_logs.append(log)
            
            logger.info(f"📊 Found {len(all_logs)} swap events in blocks {from_block}-{to_block}")
            return all_logs
            
        except Exception as e:
            logger.error(f"Failed to get swap logs: {e}")
            return []
    
    def _get_logs_for_event(self, from_block: int, to_block: int, topic0: str) -> List[Dict]:
        """Get logs for a specific event signature."""
        try:
            params = {
                'module': 'logs',
                'action': 'getLogs',
                'fromBlock': hex(from_block),
                'toBlock': hex(to_block),
                'topic0': topic0,
                'apikey': self.api_key
            }
            
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if data.get('status') == '1' and 'result' in data:
                return data['result']
            elif data.get('status') == '0':
                # No results found (normal)
                return []
            else:
                logger.warning(f"Unexpected response: {data}")
                return []
                
        except Exception as e:
            logger.error(f"Failed to get logs for event {topic0}: {e}")
            return []
    
    async def process_swap_logs(self, logs: List[Dict], storage_callback=None) -> int:
        """Process swap logs through the classification engine."""
        processed_count = 0
        
        for log in logs:
            try:
                # Classify the swap
                classified_swap = await classify_swap_transaction(
                    log, self.chain, log.get('dex', 'unknown')
                )
                
                # Store if callback provided
                if storage_callback and classified_swap:
                    stored = storage_callback(classified_swap)
                    if stored:
                        processed_count += 1
                        logger.info(f"✅ Processed {classified_swap.classification} swap: {classified_swap.transaction_hash[:16]}...")
                
            except Exception as e:
                logger.error(f"Failed to process swap log: {e}")
                continue
        
        return processed_count
    
    async def poll_for_swaps(self, storage_callback=None, poll_interval: int = 15) -> None:
        """Main polling loop for swap events."""
        logger.info(f"🔄 Starting {self.chain} swap polling (every {poll_interval}s)")
        
        # Get initial block
        if self.last_processed_block is None:
            latest_block = self.get_latest_block()
            if latest_block:
                # Start from 10 blocks ago to catch recent swaps
                self.last_processed_block = latest_block - 10
                logger.info(f"📍 Starting from block {self.last_processed_block}")
        
        while True:
            try:
                # Get current latest block
                latest_block = self.get_latest_block()
                if not latest_block:
                    logger.warning("Failed to get latest block, retrying...")
                    await asyncio.sleep(poll_interval)
                    continue
                
                # Check if there are new blocks to process
                if latest_block > self.last_processed_block:
                    from_block = self.last_processed_block + 1
                    to_block = min(latest_block, from_block + 100)  # Process max 100 blocks at once
                    
                    logger.info(f"🔍 Scanning blocks {from_block} to {to_block} on {self.chain}")
                    
                    # Get swap logs
                    swap_logs = self.get_uniswap_swap_logs(from_block, to_block)
                    
                    if swap_logs:
                        # Process the swaps
                        processed = await self.process_swap_logs(swap_logs, storage_callback)
                        logger.info(f"✅ Processed {processed}/{len(swap_logs)} swaps from {self.chain}")
                    
                    # Update last processed block
                    self.last_processed_block = to_block
                
                # Wait before next poll
                await asyncio.sleep(poll_interval)
                
            except Exception as e:
                logger.error(f"Error in {self.chain} polling loop: {e}")
                await asyncio.sleep(poll_interval)

# Global poller instances
ethereum_poller = None
polygon_poller = None

def get_ethereum_poller() -> EtherscanPoller:
    """Get or create Ethereum poller instance."""
    global ethereum_poller
    if ethereum_poller is None:
        ethereum_poller = EtherscanPoller('ethereum')
    return ethereum_poller

def get_polygon_poller() -> EtherscanPoller:
    """Get or create Polygon poller instance."""
    global polygon_poller
    if polygon_poller is None:
        polygon_poller = EtherscanPoller('polygon')
    return polygon_poller

async def start_ethereum_polling(storage_callback=None):
    """Start Ethereum swap polling."""
    poller = get_ethereum_poller()
    await poller.poll_for_swaps(storage_callback)

async def start_polygon_polling(storage_callback=None):
    """Start Polygon swap polling."""
    poller = get_polygon_poller()
    await poller.poll_for_swaps(storage_callback)

if __name__ == "__main__":
    # Test the poller
    async def test_poller():
        poller = EtherscanPoller('ethereum')
        latest_block = poller.get_latest_block()
        print(f"Latest Ethereum block: {latest_block}")
        
        if latest_block:
            # Test getting logs from recent blocks
            logs = poller.get_uniswap_swap_logs(latest_block - 5, latest_block)
            print(f"Found {len(logs)} swap logs in last 5 blocks")
    
    asyncio.run(test_poller()) 
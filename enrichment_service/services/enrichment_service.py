"""
Address Enrichment Service Module

This module implements the core functionality for enriching crypto wallet addresses
with metadata from various sources like Nansen, Arkham, and fallback sources.
"""
import aiohttp
import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Union, Any
import redis
import time

from ..models.address import (
    AddressLabelType, 
    ChainType, 
    LabelSource, 
    EnrichedAddressResponse,
    AddressLabel
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cache TTL in seconds (24 hours)
CACHE_TTL = 86400  

class AddressEnrichmentService:
    """
    Service for enriching crypto wallet addresses with metadata from various sources.
    
    This service handles:
    1. Caching of enrichment results (using Redis)
    2. Integration with multiple label providers (Nansen, Arkham, etc.)
    3. Aggregation of labels from multiple sources
    4. Background fetching of labels for slow APIs
    """
    
    def __init__(self, redis_url: Optional[str] = None):
        """
        Initialize the address enrichment service.
        
        Args:
            redis_url: Redis connection URL (defaults to localhost if not provided)
        """
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.redis = redis.from_url(self.redis_url)
        
        # API keys for various services
        self.nansen_api_key = os.getenv("NANSEN_API_KEY", "")
        self.arkham_api_key = os.getenv("ARKHAM_API_KEY", "")
        self.chainalysis_api_key = os.getenv("CHAINALYSIS_API_KEY", "")
        
        # Session for API requests
        self.session = None
    
    async def get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp session"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def get_address_enrichment(
        self, 
        address: str, 
        chain: Union[str, ChainType],
        force_refresh: bool = False
    ) -> EnrichedAddressResponse:
        """
        Get enriched data for a blockchain address.
        
        Args:
            address: The blockchain address to enrich
            chain: The blockchain the address belongs to
            force_refresh: Whether to force refresh cached data
            
        Returns:
            EnrichedAddressResponse: The enriched address data
        """
        # Normalize inputs
        address = address.lower() if isinstance(chain, str) and chain.lower() in ["ethereum", "polygon"] else address
        if isinstance(chain, str):
            chain = ChainType(chain.lower())
        
        # Generate cache key
        cache_key = f"address:{chain.value}:{address}"
        
        # Check cache first unless forcing refresh
        if not force_refresh:
            cached_data = self.redis.get(cache_key)
            if cached_data:
                try:
                    data = json.loads(cached_data)
                    # Convert from dict to model
                    response = EnrichedAddressResponse(**data)
                    response.cached = True
                    return response
                except Exception as e:
                    logger.error(f"Error parsing cached data: {e}")
        
        # If not in cache or refresh needed, fetch from external sources
        all_labels = await self._get_address_labels(address, chain)
        
        # Aggregate labels to determine the primary label
        primary_label, source, confidence = self._aggregate_labels(all_labels)
        
        # Prepare response
        response = EnrichedAddressResponse(
            address=address,
            chain=chain,
            primary_label=primary_label,
            source=source,
            confidence=confidence,
            all_labels=all_labels,
            cached=False,
            last_updated=datetime.utcnow().isoformat()
        )
        
        # Cache the result
        self.redis.setex(
            cache_key, 
            CACHE_TTL, 
            json.dumps(response.dict())
        )
        
        return response
    
    async def _get_address_labels(
        self, 
        address: str, 
        chain: ChainType
    ) -> List[AddressLabel]:
        """
        Fetch labels for an address from multiple sources.
        
        Args:
            address: The blockchain address
            chain: The blockchain type
            
        Returns:
            List[AddressLabel]: List of labels from various sources
        """
        # Create tasks for all label sources
        tasks = []
        
        # Only add tasks for APIs we have keys for or fallbacks
        if self.nansen_api_key:
            tasks.append(self._get_nansen_label(address, chain))
        
        if self.arkham_api_key:
            tasks.append(self._get_arkham_label(address, chain))
            
        if self.chainalysis_api_key:
            tasks.append(self._get_chainalysis_label(address, chain))
        
        # Always add fallback sources
        tasks.append(self._get_explorer_label(address, chain))
        
        # Run all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and None results
        labels = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Error fetching label: {result}")
            elif result is not None:
                labels.append(result)
        
        return labels
    
    def _aggregate_labels(
        self, 
        labels: List[AddressLabel]
    ) -> tuple[AddressLabelType, LabelSource, float]:
        """
        Aggregate multiple labels to determine the primary label.
        
        Current strategy: Choose the label with highest confidence.
        Future enhancement: Use weighted voting based on source reliability.
        
        Args:
            labels: List of labels from various sources
            
        Returns:
            tuple: (primary_label, source, confidence)
        """
        if not labels:
            return AddressLabelType.UNKNOWN, LabelSource.UNKNOWN, 0.5
        
        # Sort by confidence (highest first)
        sorted_labels = sorted(labels, key=lambda x: x.confidence, reverse=True)
        
        # Return the highest confidence label
        top_label = sorted_labels[0]
        return top_label.label_type, top_label.source, top_label.confidence
    
    # Integration with label providers
    
    async def _get_nansen_label(
        self, 
        address: str, 
        chain: ChainType
    ) -> Optional[AddressLabel]:
        """
        Get address label from Nansen API.
        
        Args:
            address: The blockchain address
            chain: The blockchain type
            
        Returns:
            Optional[AddressLabel]: Label from Nansen, or None if unavailable
        """
        # In a real implementation, this would call the Nansen API
        # For this example, we'll simulate a response
        
        # Skip chains not supported by Nansen
        if chain not in [ChainType.ETHEREUM, ChainType.POLYGON]:
            return None
            
        try:
            # Simulated API call - in real implementation, this would be an HTTP request
            # with proper error handling, retries, etc.
            
            # For demo purposes, classify some example addresses
            # In production, this would be a real API call
            if address.startswith("0x1"):
                return AddressLabel(
                    label_type=AddressLabelType.EXCHANGE,
                    source=LabelSource.NANSEN,
                    confidence=0.95,
                    additional_info={"exchange_name": "Binance"}
                )
            elif address.startswith("0x2"):
                return AddressLabel(
                    label_type=AddressLabelType.DEX,
                    source=LabelSource.NANSEN,
                    confidence=0.92,
                    additional_info={"dex_name": "Uniswap"}
                )
            
            # Simulate API limitation/error for other addresses
            return None
            
        except Exception as e:
            logger.error(f"Error fetching Nansen label: {e}")
            return None
    
    async def _get_arkham_label(
        self, 
        address: str, 
        chain: ChainType
    ) -> Optional[AddressLabel]:
        """
        Get address label from Arkham Intelligence API.
        
        Args:
            address: The blockchain address
            chain: The blockchain type
            
        Returns:
            Optional[AddressLabel]: Label from Arkham, or None if unavailable
        """
        # Similar to Nansen implementation, but for Arkham
        # This is a simulated implementation
        
        try:
            # Simulated response
            if address.startswith("0x3"):
                return AddressLabel(
                    label_type=AddressLabelType.MARKET_MAKER,
                    source=LabelSource.ARKHAM,
                    confidence=0.88,
                    additional_info={"market_maker_name": "Jump Trading"}
                )
            elif address.startswith("0x4"):
                return AddressLabel(
                    label_type=AddressLabelType.BRIDGE,
                    source=LabelSource.ARKHAM,
                    confidence=0.96,
                    additional_info={"bridge_name": "Wormhole"}
                )
                
            return None
            
        except Exception as e:
            logger.error(f"Error fetching Arkham label: {e}")
            return None
    
    async def _get_chainalysis_label(
        self, 
        address: str, 
        chain: ChainType
    ) -> Optional[AddressLabel]:
        """
        Get address label from Chainalysis API.
        
        Args:
            address: The blockchain address
            chain: The blockchain type
            
        Returns:
            Optional[AddressLabel]: Label from Chainalysis, or None if unavailable
        """
        # Simulated implementation for Chainalysis
        try:
            if address.startswith("0x5"):
                return AddressLabel(
                    label_type=AddressLabelType.EXCHANGE,
                    source=LabelSource.CHAINALYSIS,
                    confidence=0.91,
                    additional_info={"exchange_name": "Coinbase"}
                )
                
            return None
            
        except Exception as e:
            logger.error(f"Error fetching Chainalysis label: {e}")
            return None
    
    async def _get_explorer_label(
        self, 
        address: str, 
        chain: ChainType
    ) -> Optional[AddressLabel]:
        """
        Get address label from blockchain explorers (fallback).
        
        Args:
            address: The blockchain address
            chain: The blockchain type
            
        Returns:
            Optional[AddressLabel]: Label from explorer, or None if unavailable
        """
        # For Ethereum addresses, check Etherscan
        if chain == ChainType.ETHEREUM:
            return await self._get_etherscan_label(address)
        # For Solana addresses, check Solscan
        elif chain == ChainType.SOLANA:
            return await self._get_solscan_label(address)
            
        # Add more explorers for other chains as needed
        
        return None
    
    async def _get_etherscan_label(self, address: str) -> Optional[AddressLabel]:
        """Get label from Etherscan"""
        # In a real implementation, this would scrape or call Etherscan API
        # Simulated implementation
        try:
            if address.startswith("0x7"):
                return AddressLabel(
                    label_type=AddressLabelType.EXCHANGE,
                    source=LabelSource.ETHERSCAN,
                    confidence=0.85,
                    additional_info={"exchange_name": "Kraken"}
                )
            return None
        except Exception as e:
            logger.error(f"Error fetching Etherscan label: {e}")
            return None
    
    async def _get_solscan_label(self, address: str) -> Optional[AddressLabel]:
        """Get label from Solscan"""
        # Similar implementation for Solana addresses
        try:
            # Simulated response
            if address.startswith("A"):
                return AddressLabel(
                    label_type=AddressLabelType.DEX,
                    source=LabelSource.SOLSCAN,
                    confidence=0.82,
                    additional_info={"dex_name": "Raydium"}
                )
            return None
        except Exception as e:
            logger.error(f"Error fetching Solscan label: {e}")
            return None 
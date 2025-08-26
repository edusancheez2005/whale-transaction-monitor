"""
Address Enrichment Module

This module provides functionality to enrich blockchain addresses with metadata
from various sources like Nansen, Arkham, and blockchain explorers.

It handles caching results in Redis to improve performance and reduce API calls.
"""
import os
import json
import logging
import time
import asyncio
import aiohttp
from enum import Enum
from typing import Dict, List, Optional, Union, Any
import redis
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("address_enrichment")

class ChainType(str, Enum):
    """Supported blockchain types"""
    ETHEREUM = "ethereum"
    SOLANA = "solana"
    XRP = "xrp"
    POLYGON = "polygon"
    BITCOIN = "bitcoin"
    BINANCE = "binance"

class LabelSource(str, Enum):
    """Sources of address labels"""
    NANSEN = "nansen"
    ARKHAM = "arkham"
    ETHERSCAN = "etherscan"
    SOLSCAN = "solscan"
    CHAINALYSIS = "chainalysis"
    WHALE_ALERT = "whale_alert"
    FLIPSIDE = "flipside"
    EXPLORER = "explorer"
    UNKNOWN = "unknown"

class AddressLabelType(str, Enum):
    """Types of address labels"""
    EXCHANGE = "exchange"  # Centralized exchanges
    DEX = "dex"  # Decentralized exchanges
    BRIDGE = "bridge"  # Cross-chain bridges
    MARKET_MAKER = "market_maker"  # Market makers, liquidity providers
    LENDING_PROTOCOL = "lending_protocol"  # Lending/borrowing platforms
    VALIDATOR = "validator"  # Blockchain validators
    SCAMMER = "scammer"  # Known scam addresses
    MEV_BOT = "mev_bot"  # MEV bots and extractors
    CONTRACT = "contract"  # Smart contracts
    PERSONAL = "personal"  # Individual wallets
    WHALE = "whale"  # Large holders
    UNKNOWN = "unknown"  # Unknown or unclassified

class AddressLabel:
    """Label for an address from a specific source"""
    def __init__(
        self, 
        label_type: AddressLabelType, 
        source: LabelSource, 
        confidence: float,
        additional_info: Optional[Dict[str, Any]] = None
    ):
        self.label_type = label_type
        self.source = source
        self.confidence = min(max(confidence, 0.0), 1.0)  # Ensure between 0 and 1
        self.additional_info = additional_info or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "label_type": self.label_type.value,
            "source": self.source.value,
            "confidence": self.confidence,
            "additional_info": self.additional_info
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AddressLabel':
        """Create from dictionary"""
        return cls(
            label_type=AddressLabelType(data["label_type"]),
            source=LabelSource(data["source"]),
            confidence=data["confidence"],
            additional_info=data.get("additional_info")
        )

class EnrichedAddress:
    """Enriched address data with labels from various sources"""
    def __init__(
        self,
        address: str,
        chain: ChainType,
        primary_label: AddressLabelType = AddressLabelType.UNKNOWN,
        source: LabelSource = LabelSource.UNKNOWN,
        confidence: float = 0.0,
        all_labels: List[AddressLabel] = None,
        cached: bool = False,
        last_updated: Optional[str] = None
    ):
        self.address = address
        self.chain = chain
        self.primary_label = primary_label
        self.source = source
        self.confidence = confidence
        self.all_labels = all_labels or []
        self.cached = cached
        self.last_updated = last_updated or datetime.utcnow().isoformat()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "address": self.address,
            "chain": self.chain.value,
            "primary_label": self.primary_label.value,
            "source": self.source.value,
            "confidence": self.confidence,
            "all_labels": [label.to_dict() for label in self.all_labels],
            "cached": self.cached,
            "last_updated": self.last_updated
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EnrichedAddress':
        """Create from dictionary"""
        return cls(
            address=data["address"],
            chain=ChainType(data["chain"]),
            primary_label=AddressLabelType(data["primary_label"]),
            source=LabelSource(data["source"]),
            confidence=data["confidence"],
            all_labels=[AddressLabel.from_dict(label) for label in data.get("all_labels", [])],
            cached=data.get("cached", False),
            last_updated=data.get("last_updated")
        )

class AddressEnrichmentService:
    """
    Service for enriching addresses with metadata from various sources.
    
    This service:
    1. Checks a local cache (Redis) before making external API calls
    2. Queries multiple label sources (Nansen, Arkham, explorers)
    3. Aggregates labels and selects the best one as primary
    4. Caches results for future use
    """
    
    def __init__(self, redis_url: Optional[str] = None):
        """
        Initialize the address enrichment service.
        
        Args:
            redis_url: Redis connection URL (defaults to env var REDIS_URL or localhost)
        """
        # Initialize Redis for caching
        self.redis_url = redis_url or os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        try:
            self.redis = redis.from_url(self.redis_url)
            logger.info(f"Connected to Redis at {self.redis_url}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}. Will proceed without caching.")
            self.redis = None
        
        # Cache TTL in seconds (24 hours by default)
        self.cache_ttl = int(os.environ.get("CACHE_TTL", 86400))
        
        # API configuration
        self.nansen_api_key = os.environ.get("NANSEN_API_KEY", "")
        self.nansen_api_url = os.environ.get("NANSEN_API_URL", "https://api.nansen.ai/v1")
        
        self.arkham_api_key = os.environ.get("ARKHAM_API_KEY", "")
        self.arkham_api_url = os.environ.get("ARKHAM_API_URL", "https://api.arkhamintelligence.com/v1")
        
        self.chainalysis_api_key = os.environ.get("CHAINALYSIS_API_KEY", "")
        self.chainalysis_api_url = os.environ.get("CHAINALYSIS_API_URL", "https://api.chainalysis.com/kyt/v1")
        
        # HTTP session for API requests
        self.session = None
    
    async def get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp session"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def enrich_address(
        self,
        address: str,
        chain: Union[str, ChainType],
        force_refresh: bool = False
    ) -> EnrichedAddress:
        """
        Enrich an address with metadata from various sources.
        
        Args:
            address: Blockchain address to enrich
            chain: Blockchain type (e.g., ethereum, solana)
            force_refresh: Whether to bypass cache and force refresh
            
        Returns:
            EnrichedAddress: Enriched address data
        """
        # Normalize inputs
        address = address.lower() if isinstance(chain, str) and chain.lower() in ["ethereum", "polygon"] else address
        if isinstance(chain, str):
            try:
                chain = ChainType(chain.lower())
            except ValueError:
                logger.warning(f"Invalid chain type: {chain}. Defaulting to ethereum.")
                chain = ChainType.ETHEREUM
        
        # Generate cache key
        cache_key = f"address:{chain.value}:{address}"
        
        # Check cache unless forcing refresh
        if not force_refresh and self.redis:
            cached_data = self.redis.get(cache_key)
            if cached_data:
                try:
                    data = json.loads(cached_data)
                    enriched = EnrichedAddress.from_dict(data)
                    enriched.cached = True
                    logger.info(f"Cache hit for {address} on {chain.value}")
                    return enriched
                except Exception as e:
                    logger.error(f"Error parsing cached data: {e}")
        
        # Fetch from external sources
        logger.info(f"Enriching address {address} on {chain.value}")
        all_labels = await self._fetch_address_labels(address, chain)
        
        # Aggregate labels to determine the primary label
        primary_label, source, confidence = self._aggregate_labels(all_labels)
        
        # Create enriched address object
        enriched = EnrichedAddress(
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
        if self.redis:
            try:
                self.redis.setex(
                    cache_key,
                    self.cache_ttl,
                    json.dumps(enriched.to_dict())
                )
                logger.info(f"Cached enrichment for {address} on {chain.value}")
            except Exception as e:
                logger.error(f"Failed to cache address data: {e}")
        
        return enriched
    
    async def _fetch_address_labels(
        self,
        address: str,
        chain: ChainType
    ) -> List[AddressLabel]:
        """
        Fetch address labels from all available sources.
        
        Args:
            address: Blockchain address
            chain: Blockchain type
            
        Returns:
            List[AddressLabel]: Labels from various sources
        """
        # Create tasks for all label sources
        tasks = []
        
        # Only add tasks for APIs we have keys for
        if self.nansen_api_key:
            tasks.append(self._get_nansen_label(address, chain))
        
        if self.arkham_api_key:
            tasks.append(self._get_arkham_label(address, chain))
        
        if self.chainalysis_api_key:
            tasks.append(self._get_chainalysis_label(address, chain))
        
        # Always add explorer tasks (these don't typically need API keys)
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
        
        Args:
            labels: List of labels from various sources
            
        Returns:
            tuple: (primary_label, source, confidence)
        """
        if not labels:
            return AddressLabelType.UNKNOWN, LabelSource.UNKNOWN, 0.5
        
        # Source priority weights (higher is more trusted)
        source_weights = {
            LabelSource.NANSEN: 5.0,
            LabelSource.ARKHAM: 4.5,
            LabelSource.CHAINALYSIS: 4.0,
            LabelSource.ETHERSCAN: 3.5,
            LabelSource.SOLSCAN: 3.5,
            LabelSource.WHALE_ALERT: 3.0,
            LabelSource.FLIPSIDE: 3.0,
            LabelSource.EXPLORER: 2.5,
            LabelSource.UNKNOWN: 1.0
        }
        
        # Calculate weighted scores for each unique label type
        label_scores = {}
        for label in labels:
            weight = source_weights.get(label.source, 1.0)
            weighted_score = label.confidence * weight
            
            if label.label_type not in label_scores:
                label_scores[label.label_type] = {
                    "total_score": 0,
                    "count": 0,
                    "max_single_score": 0,
                    "best_source": None
                }
            
            label_scores[label.label_type]["total_score"] += weighted_score
            label_scores[label.label_type]["count"] += 1
            
            if weighted_score > label_scores[label.label_type]["max_single_score"]:
                label_scores[label.label_type]["max_single_score"] = weighted_score
                label_scores[label.label_type]["best_source"] = label.source
        
        # Find the label type with the highest average weighted score
        best_label_type = AddressLabelType.UNKNOWN
        best_source = LabelSource.UNKNOWN
        best_score = 0
        
        for label_type, data in label_scores.items():
            avg_score = data["total_score"] / data["count"]
            if avg_score > best_score:
                best_score = avg_score
                best_label_type = label_type
                best_source = data["best_source"]
        
        # Normalize confidence to 0-1 range
        normalized_confidence = min(best_score / 5.0, 1.0)
        
        return best_label_type, best_source, normalized_confidence
    
    async def _get_nansen_label(
        self, 
        address: str, 
        chain: ChainType
    ) -> Optional[AddressLabel]:
        """
        Get address label from Nansen API.
        
        Args:
            address: Blockchain address
            chain: Blockchain type
            
        Returns:
            Optional[AddressLabel]: Label from Nansen, or None if unavailable
        """
        if not self.nansen_api_key:
            return None
        
        # Skip chains not supported by Nansen
        supported_chains = [ChainType.ETHEREUM, ChainType.POLYGON, ChainType.BINANCE]
        if chain not in supported_chains:
            return None
        
        try:
            # Get HTTP session
            session = await self.get_session()
            
            # Make API request to Nansen
            nansen_chain = chain.value
            if chain == ChainType.BINANCE:
                nansen_chain = "bsc"  # Nansen uses bsc instead of binance
                
            url = f"{self.nansen_api_url}/address/label"
            headers = {
                "X-API-KEY": self.nansen_api_key,
                "Accept": "application/json"
            }
            params = {
                "address": address,
                "chain": nansen_chain
            }
            
            async with session.get(url, headers=headers, params=params) as response:
                if response.status != 200:
                    logger.warning(f"Nansen API error: {response.status} - {await response.text()}")
                    return None
                
                data = await response.json()
                
                # Extract the label information
                if "labels" not in data or not data["labels"]:
                    return None
                
                # Map Nansen's label to our format
                nansen_label = data["labels"][0]["label"]  # Use the first/primary label
                confidence = data["labels"][0].get("confidence", 0.8)  # Default to 0.8 if not provided
                
                # Map Nansen label to our type
                label_type = self._map_nansen_label_to_type(nansen_label)
                
                # Create additional info from all available data
                additional_info = {
                    "nansen_label": nansen_label,
                    "all_labels": [l["label"] for l in data["labels"]],
                    "entity": data.get("entity")
                }
                
                return AddressLabel(
                    label_type=label_type,
                    source=LabelSource.NANSEN,
                    confidence=confidence,
                    additional_info=additional_info
                )
                
        except Exception as e:
            logger.error(f"Error getting Nansen label: {e}")
            return None
    
    def _map_nansen_label_to_type(self, nansen_label: str) -> AddressLabelType:
        """Map Nansen's label to our label type"""
        nansen_label = nansen_label.lower()
        
        if any(term in nansen_label for term in ["exchange", "cex"]):
            return AddressLabelType.EXCHANGE
        elif any(term in nansen_label for term in ["dex", "uniswap", "sushiswap", "pancakeswap"]):
            return AddressLabelType.DEX
        elif any(term in nansen_label for term in ["bridge", "portal", "wormhole"]):
            return AddressLabelType.BRIDGE
        elif any(term in nansen_label for term in ["market maker", "mm", "liquidity provider"]):
            return AddressLabelType.MARKET_MAKER
        elif any(term in nansen_label for term in ["aave", "compound", "lending", "lend"]):
            return AddressLabelType.LENDING_PROTOCOL
        elif any(term in nansen_label for term in ["validator", "staking"]):
            return AddressLabelType.VALIDATOR
        elif any(term in nansen_label for term in ["scam", "phish", "hack"]):
            return AddressLabelType.SCAMMER
        elif any(term in nansen_label for term in ["mev", "frontrun", "arbitrage bot"]):
            return AddressLabelType.MEV_BOT
        elif any(term in nansen_label for term in ["contract", "token", "erc20", "erc721"]):
            return AddressLabelType.CONTRACT
        elif any(term in nansen_label for term in ["whale", "large holder"]):
            return AddressLabelType.WHALE
        elif any(term in nansen_label for term in ["individual", "personal", "user", "wallet"]):
            return AddressLabelType.PERSONAL
        else:
            return AddressLabelType.UNKNOWN
    
    async def _get_arkham_label(
        self, 
        address: str, 
        chain: ChainType
    ) -> Optional[AddressLabel]:
        """
        Get address label from Arkham Intelligence API.
        
        Args:
            address: Blockchain address
            chain: Blockchain type
            
        Returns:
            Optional[AddressLabel]: Label from Arkham, or None if unavailable
        """
        if not self.arkham_api_key:
            return None
        
        # Arkham primarily supports Ethereum and some EVM chains
        supported_chains = [ChainType.ETHEREUM, ChainType.POLYGON, ChainType.BINANCE]
        if chain not in supported_chains:
            return None
        
        try:
            # Get HTTP session
            session = await self.get_session()
            
            # Make API request to Arkham
            url = f"{self.arkham_api_url}/entities/address/{address}"
            headers = {
                "API-Key": self.arkham_api_key,
                "Accept": "application/json"
            }
            
            async with session.get(url, headers=headers) as response:
                if response.status != 200:
                    logger.warning(f"Arkham API error: {response.status} - {await response.text()}")
                    return None
                
                data = await response.json()
                
                # Extract the entity information
                entity_name = data.get("name", "")
                entity_type = data.get("type", "")
                confidence = data.get("confidence", 0.8)  # Default to 0.8 if not provided
                
                if not entity_type:
                    return None
                
                # Map Arkham's entity type to our format
                label_type = self._map_arkham_entity_to_type(entity_type)
                
                # Create additional info from all available data
                additional_info = {
                    "arkham_entity_name": entity_name,
                    "arkham_entity_type": entity_type,
                    "entity_id": data.get("id"),
                    "description": data.get("description")
                }
                
                return AddressLabel(
                    label_type=label_type,
                    source=LabelSource.ARKHAM,
                    confidence=confidence,
                    additional_info=additional_info
                )
                
        except Exception as e:
            logger.error(f"Error getting Arkham label: {e}")
            return None
    
    def _map_arkham_entity_to_type(self, entity_type: str) -> AddressLabelType:
        """Map Arkham's entity type to our label type"""
        entity_type = entity_type.lower()
        
        if any(term in entity_type for term in ["exchange", "cex"]):
            return AddressLabelType.EXCHANGE
        elif any(term in entity_type for term in ["dex", "amm"]):
            return AddressLabelType.DEX
        elif any(term in entity_type for term in ["bridge", "cross-chain"]):
            return AddressLabelType.BRIDGE
        elif any(term in entity_type for term in ["market maker", "mm", "liquidity provider"]):
            return AddressLabelType.MARKET_MAKER
        elif any(term in entity_type for term in ["lending", "borrowing", "loan"]):
            return AddressLabelType.LENDING_PROTOCOL
        elif any(term in entity_type for term in ["validator", "staking"]):
            return AddressLabelType.VALIDATOR
        elif any(term in entity_type for term in ["scam", "phish", "hack"]):
            return AddressLabelType.SCAMMER
        elif any(term in entity_type for term in ["mev", "arbitrage"]):
            return AddressLabelType.MEV_BOT
        elif any(term in entity_type for term in ["contract", "protocol"]):
            return AddressLabelType.CONTRACT
        elif any(term in entity_type for term in ["whale", "large holder"]):
            return AddressLabelType.WHALE
        elif any(term in entity_type for term in ["individual", "personal", "user"]):
            return AddressLabelType.PERSONAL
        else:
            return AddressLabelType.UNKNOWN
    
    async def _get_chainalysis_label(
        self, 
        address: str, 
        chain: ChainType
    ) -> Optional[AddressLabel]:
        """
        Get address label from Chainalysis API.
        
        Args:
            address: Blockchain address
            chain: Blockchain type
            
        Returns:
            Optional[AddressLabel]: Label from Chainalysis, or None if unavailable
        """
        if not self.chainalysis_api_key:
            return None
        
        try:
            # Get HTTP session
            session = await self.get_session()
            
            # Make API request to Chainalysis
            url = f"{self.chainalysis_api_url}/address-screening"
            headers = {
                "Token": self.chainalysis_api_key,
                "Accept": "application/json"
            }
            params = {
                "address": address,
                "chain": chain.value
            }
            
            async with session.get(url, headers=headers, params=params) as response:
                if response.status != 200:
                    logger.warning(f"Chainalysis API error: {response.status} - {await response.text()}")
                    return None
                
                data = await response.json()
                
                # Extract the entity information
                if "entity" not in data or not data["entity"]:
                    return None
                
                entity = data["entity"]
                entity_type = entity.get("category", "")
                entity_name = entity.get("name", "")
                risk_score = entity.get("risk_score", 0.5)
                
                # Convert risk score to confidence (inverse relationship)
                # Lower risk means higher confidence in label
                confidence = max(0.0, 1.0 - risk_score)
                
                # Map Chainalysis entity to our format
                label_type = self._map_chainalysis_entity_to_type(entity_type)
                
                # Create additional info from all available data
                additional_info = {
                    "chainalysis_entity_name": entity_name,
                    "chainalysis_entity_type": entity_type,
                    "risk_score": risk_score,
                    "is_sanctioned": entity.get("is_sanctioned", False)
                }
                
                return AddressLabel(
                    label_type=label_type,
                    source=LabelSource.CHAINALYSIS,
                    confidence=confidence,
                    additional_info=additional_info
                )
                
        except Exception as e:
            logger.error(f"Error getting Chainalysis label: {e}")
            return None
    
    def _map_chainalysis_entity_to_type(self, entity_type: str) -> AddressLabelType:
        """Map Chainalysis entity type to our label type"""
        entity_type = entity_type.lower()
        
        if any(term in entity_type for term in ["exchange", "cex"]):
            return AddressLabelType.EXCHANGE
        elif any(term in entity_type for term in ["dex", "decentralized exchange"]):
            return AddressLabelType.DEX
        elif "bridge" in entity_type:
            return AddressLabelType.BRIDGE
        elif any(term in entity_type for term in ["market maker", "otc"]):
            return AddressLabelType.MARKET_MAKER
        elif any(term in entity_type for term in ["lending", "defi"]):
            return AddressLabelType.LENDING_PROTOCOL
        elif "staking" in entity_type:
            return AddressLabelType.VALIDATOR
        elif any(term in entity_type for term in ["scam", "darknet", "ransomware", "terrorist"]):
            return AddressLabelType.SCAMMER
        elif "smart contract" in entity_type:
            return AddressLabelType.CONTRACT
        elif any(term in entity_type for term in ["private wallet", "user"]):
            return AddressLabelType.PERSONAL
        else:
            return AddressLabelType.UNKNOWN
    
    async def _get_explorer_label(
        self, 
        address: str, 
        chain: ChainType
    ) -> Optional[AddressLabel]:
        """
        Get address label from blockchain explorers.
        
        Args:
            address: Blockchain address
            chain: Blockchain type
            
        Returns:
            Optional[AddressLabel]: Label from explorer, or None if unavailable
        """
        # Route to the appropriate explorer based on chain
        if chain == ChainType.ETHEREUM:
            return await self._get_etherscan_label(address)
        elif chain == ChainType.SOLANA:
            return await self._get_solscan_label(address)
        elif chain == ChainType.XRP:
            return await self._get_xrpscan_label(address)
        elif chain == ChainType.POLYGON:
            return await self._get_polygonscan_label(address)
        else:
            return None
    
    async def _get_etherscan_label(self, address: str) -> Optional[AddressLabel]:
        """Get label from Etherscan"""
        try:
            # Get HTTP session
            session = await self.get_session()
            
            # Etherscan API key is optional but recommended
            etherscan_api_key = os.environ.get("ETHERSCAN_API_KEY", "")
            
            # Make request to Etherscan
            url = "https://api.etherscan.io/api"
            params = {
                "module": "account",
                "action": "tokenbalance",
                "address": address,
                "tag": "latest"
            }
            
            if etherscan_api_key:
                params["apikey"] = etherscan_api_key
            
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    return None
                
                # We're not directly using the API response here, but
                # checking if the address is a contract
                
                # Make request to get account type
                url = f"https://etherscan.io/address/{address}"
                async with session.get(url) as response:
                    if response.status != 200:
                        return None
                    
                    html = await response.text()
                    
                    # Check for indicators in the HTML response
                    is_contract = "Contract" in html and "Creation" in html
                    label = None
                    
                    # Check for labels in the HTML
                    if "id='contractCopy'>" in html:
                        # Extract label from HTML - this is simplified and might need adjustment
                        start_idx = html.find("id='contractCopy'>") + len("id='contractCopy'>")
                        end_idx = html.find("</span>", start_idx)
                        if start_idx > 0 and end_idx > start_idx:
                            label = html[start_idx:end_idx].strip()
                    
                    if not label and not is_contract:
                        return None
                    
                    # Determine label type
                    if is_contract:
                        label_type = AddressLabelType.CONTRACT
                    elif label:
                        # Map etherscan label to our type
                        if any(term.lower() in label.lower() for term in ["exchange", "binance", "kraken", "coinbase"]):
                            label_type = AddressLabelType.EXCHANGE
                        elif any(term.lower() in label.lower() for term in ["uniswap", "sushiswap", "dex"]):
                            label_type = AddressLabelType.DEX
                        elif any(term.lower() in label.lower() for term in ["bridge", "wormhole", "portal"]):
                            label_type = AddressLabelType.BRIDGE
                        else:
                            label_type = AddressLabelType.UNKNOWN
                    else:
                        label_type = AddressLabelType.PERSONAL
                    
                    return AddressLabel(
                        label_type=label_type,
                        source=LabelSource.ETHERSCAN,
                        confidence=0.7,  # Conservative confidence for explorer-derived labels
                        additional_info={"label": label, "is_contract": is_contract}
                    )
                
        except Exception as e:
            logger.error(f"Error getting Etherscan label: {e}")
            return None
    
    async def _get_solscan_label(self, address: str) -> Optional[AddressLabel]:
        """Get label from Solscan"""
        try:
            # Get HTTP session
            session = await self.get_session()
            
            # Make request to Solscan
            url = f"https://api.solscan.io/account?address={address}"
            
            async with session.get(url) as response:
                if response.status != 200:
                    return None
                
                data = await response.json()
                
                # Check if it's a program (contract)
                is_program = data.get("type") == "program"
                label = data.get("name", "")
                
                if not label and not is_program:
                    return None
                
                # Determine label type
                if is_program:
                    label_type = AddressLabelType.CONTRACT
                elif label:
                    # Map solscan label to our type
                    label = label.lower()
                    if any(term in label for term in ["exchange", "binance", "ftx"]):
                        label_type = AddressLabelType.EXCHANGE
                    elif any(term in label for term in ["raydium", "serum", "dex"]):
                        label_type = AddressLabelType.DEX
                    elif any(term in label for term in ["bridge", "wormhole", "portal"]):
                        label_type = AddressLabelType.BRIDGE
                    else:
                        label_type = AddressLabelType.UNKNOWN
                else:
                    label_type = AddressLabelType.PERSONAL
                
                return AddressLabel(
                    label_type=label_type,
                    source=LabelSource.SOLSCAN,
                    confidence=0.7,
                    additional_info={"label": label, "is_program": is_program}
                )
                
        except Exception as e:
            logger.error(f"Error getting Solscan label: {e}")
            return None
    
    async def _get_xrpscan_label(self, address: str) -> Optional[AddressLabel]:
        """Get label from XRP Scan"""
        try:
            # Get HTTP session
            session = await self.get_session()
            
            # Make request to XRP Scan
            url = f"https://api.xrpscan.com/api/v1/account/{address}"
            
            async with session.get(url) as response:
                if response.status != 200:
                    return None
                
                data = await response.json()
                
                # Check for domain or label
                domain = data.get("domain", "")
                label = data.get("accountName", "")
                
                if not domain and not label:
                    return None
                
                # Use domain or label to determine type
                entity_name = domain or label
                
                # Determine label type
                if any(term in entity_name.lower() for term in ["exchange", "binance", "kraken", "coinbase"]):
                    label_type = AddressLabelType.EXCHANGE
                elif "dex" in entity_name.lower():
                    label_type = AddressLabelType.DEX
                else:
                    label_type = AddressLabelType.UNKNOWN
                
                return AddressLabel(
                    label_type=label_type,
                    source=LabelSource.EXPLORER,
                    confidence=0.7,
                    additional_info={"domain": domain, "account_name": label}
                )
                
        except Exception as e:
            logger.error(f"Error getting XRP Scan label: {e}")
            return None
    
    async def _get_polygonscan_label(self, address: str) -> Optional[AddressLabel]:
        """Get label from Polygonscan"""
        try:
            # Get HTTP session
            session = await self.get_session()
            
            # Polygonscan API key is optional but recommended
            polygonscan_api_key = os.environ.get("POLYGONSCAN_API_KEY", "")
            
            # Make request to Polygonscan
            url = "https://api.polygonscan.com/api"
            params = {
                "module": "account",
                "action": "tokenbalance",
                "address": address,
                "tag": "latest"
            }
            
            if polygonscan_api_key:
                params["apikey"] = polygonscan_api_key
            
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    return None
                
                # We're not directly using the API response here, but
                # checking if the address is a contract
                
                # Make request to get account type
                url = f"https://polygonscan.com/address/{address}"
                async with session.get(url) as response:
                    if response.status != 200:
                        return None
                    
                    html = await response.text()
                    
                    # Check for indicators in the HTML response
                    is_contract = "Contract" in html and "Creation" in html
                    label = None
                    
                    # Check for labels in the HTML
                    if "id='contractCopy'>" in html:
                        # Extract label from HTML - this is simplified and might need adjustment
                        start_idx = html.find("id='contractCopy'>") + len("id='contractCopy'>")
                        end_idx = html.find("</span>", start_idx)
                        if start_idx > 0 and end_idx > start_idx:
                            label = html[start_idx:end_idx].strip()
                    
                    if not label and not is_contract:
                        return None
                    
                    # Determine label type
                    if is_contract:
                        label_type = AddressLabelType.CONTRACT
                    elif label:
                        # Map polygonscan label to our type
                        if any(term.lower() in label.lower() for term in ["exchange", "binance", "kraken"]):
                            label_type = AddressLabelType.EXCHANGE
                        elif any(term.lower() in label.lower() for term in ["quickswap", "sushiswap", "dex"]):
                            label_type = AddressLabelType.DEX
                        elif any(term.lower() in label.lower() for term in ["bridge", "polygon bridge"]):
                            label_type = AddressLabelType.BRIDGE
                        else:
                            label_type = AddressLabelType.UNKNOWN
                    else:
                        label_type = AddressLabelType.PERSONAL
                    
                    return AddressLabel(
                        label_type=label_type,
                        source=LabelSource.EXPLORER,
                        confidence=0.7,
                        additional_info={"label": label, "is_contract": is_contract}
                    )
                
        except Exception as e:
            logger.error(f"Error getting Polygonscan label: {e}")
            return None
    
    async def close(self):
        """Close the HTTP session when done"""
        if self.session and not self.session.closed:
            await self.session.close()


# Usage example
async def main():
    """Example usage of the address enrichment service"""
    # Create the service
    service = AddressEnrichmentService()
    
    # Enrich an address
    address = "0x28c6c06298d514db089934071355e5743bf21d60"  # Binance hot wallet
    chain = ChainType.ETHEREUM
    
    enriched = await service.enrich_address(address, chain)
    print(f"Address: {enriched.address}")
    print(f"Primary label: {enriched.primary_label}")
    print(f"Source: {enriched.source}")
    print(f"Confidence: {enriched.confidence}")
    
    # Close the service when done
    await service.close()

# Run the example if executed directly
if __name__ == "__main__":
    asyncio.run(main()) 
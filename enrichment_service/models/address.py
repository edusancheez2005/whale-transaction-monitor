"""
Address Models Module

This module defines the Pydantic models for the address enrichment service.
"""
from enum import Enum
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime


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
    UNKNOWN = "unknown"


class AddressLabelType(str, Enum):
    """Types of address labels"""
    EXCHANGE = "exchange"
    DEX = "dex"
    BRIDGE = "bridge"
    MARKET_MAKER = "market_maker"
    LENDING_PROTOCOL = "lending_protocol"
    VALIDATOR = "validator"
    SCAMMER = "scammer"
    MEV_BOT = "mev_bot"
    CONTRACT = "contract"
    PERSONAL = "personal"
    WHALE = "whale"
    UNKNOWN = "unknown"


class EnrichmentRequest(BaseModel):
    """Request model for address enrichment"""
    address: str = Field(..., description="The blockchain address to enrich")
    chain: ChainType = Field(..., description="The blockchain the address belongs to")
    force_refresh: bool = Field(False, description="Whether to force refresh the data")


class AddressLabel(BaseModel):
    """Label for an address from a specific source"""
    label_type: AddressLabelType
    source: LabelSource
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence score (0-1)")
    additional_info: Optional[Dict[str, Any]] = Field(None, description="Additional information about the label")


class EnrichedAddressResponse(BaseModel):
    """Response model for enriched address data"""
    address: str
    chain: ChainType
    primary_label: AddressLabelType
    source: LabelSource
    confidence: float
    all_labels: List[AddressLabel] = []
    cached: bool = Field(False, description="Whether this response was served from cache")
    last_updated: Optional[str] = None  # ISO format timestamp 
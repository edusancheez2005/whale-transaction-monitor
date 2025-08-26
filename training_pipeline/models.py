"""
Training Data Pipeline Models

This module defines the data models for the training data generation pipeline.
"""
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List, Union
from enum import Enum
from datetime import datetime


class ChainType(str, Enum):
    """Supported blockchain types"""
    ETHEREUM = "ethereum"
    SOLANA = "solana"
    XRP = "xrp"
    POLYGON = "polygon"
    BITCOIN = "bitcoin"
    BINANCE = "binance"


class LabelType(str, Enum):
    """Classification label types"""
    BUY = "buy"
    SELL = "sell"
    TRANSFER = "transfer"
    UNKNOWN = "unknown"


class LabelSource(str, Enum):
    """Sources for transaction labels"""
    RULE_ENGINE = "rule_engine"
    WHALE_ALERT = "whale_alert"
    MANUAL = "manual"
    FLIPSIDE = "flipside"
    CROWDSOURCE = "crowdsource"
    NANSEN = "nansen"
    ETHERSCAN = "etherscan"


class RawTransaction(BaseModel):
    """Raw transaction data from various sources"""
    tx_hash: str
    from_address: str
    to_address: str
    chain: ChainType
    token: str = Field(..., description="Token symbol (e.g., ETH, SOL)")
    amount: float = Field(..., ge=0.0, description="Amount of tokens in the transaction")
    usd_value: Optional[float] = Field(None, ge=0.0, description="USD value of the transaction")
    timestamp: Optional[datetime] = None
    block_number: Optional[int] = None
    
    # Source information
    source: str = Field(..., description="Source of the transaction data")
    
    # Additional data that might help with classification
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Additional metadata")
    
    class Config:
        extra = "allow"  # Allow additional fields for flexibility


class LabeledTransaction(BaseModel):
    """Transaction with classification label"""
    tx_hash: str
    from_address: str
    to_address: str
    chain: ChainType
    token: str
    amount: float
    usd_value: Optional[float] = None
    timestamp: Optional[datetime] = None
    block_number: Optional[int] = None
    
    # Classification information
    label: LabelType
    label_confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence in the label (0-1)")
    label_source: LabelSource
    
    # Processing information
    processed_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Additional features
    features: Optional[Dict[str, Any]] = Field(default=None, description="Extracted features for ML")
    
    # Validation information
    is_validated: bool = Field(default=False, description="Whether the label has been manually validated")
    validator_notes: Optional[str] = None
    
    class Config:
        extra = "allow"  # Allow additional fields 
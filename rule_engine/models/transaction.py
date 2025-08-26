"""
Transaction Models Module

This module defines the Pydantic models for transaction classification.
"""
from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, Any, List
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


class ClassificationType(str, Enum):
    """Transaction classification types"""
    BUY = "buy"
    SELL = "sell"
    TRANSFER = "transfer"
    DEPOSIT = "deposit"  # Special case of potential sell prep
    WITHDRAWAL = "withdrawal"  # Special case of post-buy
    UNKNOWN = "unknown"


class ConfidenceLevel(str, Enum):
    """Confidence levels for classifications"""
    VERY_HIGH = "very_high"  # 90-100%
    HIGH = "high"  # 70-90%
    MEDIUM = "medium"  # 50-70%
    LOW = "low"  # 30-50%
    VERY_LOW = "very_low"  # 0-30%


class AddressMetadata(BaseModel):
    """Metadata for an address"""
    address: str
    label: str = Field(default="unknown")
    entity_type: str = Field(default="unknown")
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    
    class Config:
        extra = "allow"  # Allow additional fields for flexibility


class TransactionRequest(BaseModel):
    """Request model for transaction classification"""
    from_address: str
    to_address: str
    chain: ChainType
    token: str = Field(..., description="Token symbol (e.g., ETH, SOL)")
    amount: float = Field(..., ge=0.0, description="Amount of tokens in the transaction")
    usd_value: Optional[float] = Field(None, ge=0.0, description="USD value of the transaction")
    timestamp: Optional[datetime] = None
    tx_hash: Optional[str] = None
    block_number: Optional[int] = None
    
    # Add optional fields to help with classification
    from_address_metadata: Optional[AddressMetadata] = None
    to_address_metadata: Optional[AddressMetadata] = None
    
    # For specific transaction types
    input_data: Optional[str] = Field(None, description="Transaction input data (for ETH)")
    log_events: Optional[List[Dict[str, Any]]] = Field(None, description="Log events")
    
    class Config:
        extra = "allow"  # Allow additional fields for flexibility


class ClassificationResult(BaseModel):
    """Response model for transaction classification"""
    classification: ClassificationType
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence score (0-1)")
    confidence_level: ConfidenceLevel
    triggered_rule: str
    explanation: str = Field(..., description="Human-readable explanation of the classification")
    
    # Include the original transaction details
    transaction: TransactionRequest
    
    # Debug info
    rule_processing_time_ms: Optional[float] = None
    processed_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        extra = "allow"  # Allow additional fields 
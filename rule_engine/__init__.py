"""
rule engine package

exports the rule engine and pydantic models for convenient imports.
"""

from .rules.base import RuleEngine, BaseRule
from .models.transaction import (
    TransactionRequest,
    ClassificationResult,
    ClassificationType,
    ConfidenceLevel,
    AddressMetadata,
)

# Backwards-compatibility alias expected by some modules
Transaction = TransactionRequest

__all__ = [
    "RuleEngine",
    "BaseRule",
    "Transaction",
    "TransactionRequest",
    "ClassificationResult",
    "ClassificationType",
    "ConfidenceLevel",
    "AddressMetadata",
]

__version__ = "1.0.0"
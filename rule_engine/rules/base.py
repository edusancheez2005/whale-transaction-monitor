"""
Base Rule Engine Module

This module provides the base rule engine functionality for crypto transaction classification.
"""
import time
import logging
from typing import Dict, List, Type, Any, Optional
from abc import ABC, abstractmethod

from ..models.transaction import (
    TransactionRequest,
    ClassificationResult,
    ClassificationType,
    ConfidenceLevel
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BaseRule(ABC):
    """
    Base class for all classification rules
    
    All chain-specific or specialized rules should inherit from this class
    and implement the apply method.
    """
    name = "base_rule"
    description = "Base rule class"
    chain = None  # None means all chains
    
    @abstractmethod
    def apply(self, transaction: TransactionRequest) -> Optional[ClassificationResult]:
        """
        Apply the rule to a transaction
        
        Args:
            transaction: The transaction to classify
            
        Returns:
            ClassificationResult if the rule matches, None otherwise
        """
        pass
    
    def _map_confidence_to_level(self, confidence: float) -> ConfidenceLevel:
        """
        Map a confidence score to a confidence level
        
        Args:
            confidence: Confidence score (0-1)
            
        Returns:
            ConfidenceLevel: Corresponding confidence level
        """
        if confidence >= 0.9:
            return ConfidenceLevel.VERY_HIGH
        elif confidence >= 0.7:
            return ConfidenceLevel.HIGH
        elif confidence >= 0.5:
            return ConfidenceLevel.MEDIUM
        elif confidence >= 0.3:
            return ConfidenceLevel.LOW
        else:
            return ConfidenceLevel.VERY_LOW
    
    def create_result(
        self, 
        transaction: TransactionRequest, 
        classification: ClassificationType,
        confidence: float,
        explanation: str
    ) -> ClassificationResult:
        """
        Create a classification result
        
        Args:
            transaction: The classified transaction
            classification: The classification type
            confidence: Confidence score (0-1)
            explanation: Human-readable explanation of the classification
            
        Returns:
            ClassificationResult: The classification result
        """
        return ClassificationResult(
            classification=classification,
            confidence=confidence,
            confidence_level=self._map_confidence_to_level(confidence),
            triggered_rule=self.name,
            explanation=explanation,
            transaction=transaction
        )


class RuleEngine:
    """
    Rule engine for classifying crypto transactions
    
    This class manages a collection of rules and applies them in order
    to classify transactions.
    """
    
    def __init__(self):
        """Initialize the rule engine with an empty rule list"""
        self.rules: List[BaseRule] = []
        
    def register_rule(self, rule: BaseRule) -> None:
        """
        Register a rule with the engine
        
        Args:
            rule: The rule to register
        """
        self.rules.append(rule)
        logger.info(f"Registered rule: {rule.name}")
        
    def register_rules(self, rules: List[BaseRule]) -> None:
        """
        Register multiple rules with the engine
        
        Args:
            rules: List of rules to register
        """
        for rule in rules:
            self.register_rule(rule)
    
    def classify(self, transaction: TransactionRequest) -> ClassificationResult:
        """
        Classify a transaction using registered rules
        
        Rules are applied in order of registration. The first rule that returns
        a non-None result is used for classification.
        
        Args:
            transaction: The transaction to classify
            
        Returns:
            ClassificationResult: The classification result
        """
        start_time = time.time()
        
        # Apply each rule in order
        for rule in self.rules:
            # Skip rules that are chain-specific and don't match the transaction chain
            if rule.chain is not None and rule.chain != transaction.chain:
                continue
                
            try:
                result = rule.apply(transaction)
                if result is not None:
                    # Calculate processing time
                    processing_time_ms = (time.time() - start_time) * 1000
                    result.rule_processing_time_ms = processing_time_ms
                    logger.info(f"Transaction classified as {result.classification} by rule {rule.name} in {processing_time_ms:.2f}ms")
                    return result
            except Exception as e:
                logger.error(f"Error applying rule {rule.name}: {e}")
        
        # If no rule matched, use fallback classification
        processing_time_ms = (time.time() - start_time) * 1000
        result = ClassificationResult(
            classification=ClassificationType.UNKNOWN,
            confidence=0.3,
            confidence_level=ConfidenceLevel.LOW,
            triggered_rule="fallback",
            explanation="No classification rules matched this transaction",
            transaction=transaction,
            rule_processing_time_ms=processing_time_ms
        )
        logger.info(f"Transaction classified as UNKNOWN (fallback) in {processing_time_ms:.2f}ms")
        return result 
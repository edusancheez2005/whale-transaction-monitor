"""
Rule-Based Transaction Classification Engine

This module provides a rule-based engine for classifying cryptocurrency transactions
based on enriched address metadata, transaction patterns, and heuristics.
"""
import logging
import time
from enum import Enum
from typing import Dict, List, Optional, Any, Set, Union
from abc import ABC, abstractmethod
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("rule_engine")

# Import address enrichment types
from address_enrichment import (
    AddressLabelType, 
    LabelSource, 
    ChainType,
    EnrichedAddress
)

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

class AddressMetadata:
    """Metadata for an address in a transaction"""
    def __init__(
        self,
        address: str,
        label: str = "unknown",
        entity_type: str = "unknown",
        confidence: float = 0.5,
        additional_info: Optional[Dict[str, Any]] = None
    ):
        self.address = address
        self.label = label
        self.entity_type = entity_type
        self.confidence = min(max(confidence, 0.0), 1.0)  # Ensure between 0 and 1
        self.additional_info = additional_info or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "address": self.address,
            "label": self.label,
            "entity_type": self.entity_type,
            "confidence": self.confidence,
            "additional_info": self.additional_info
        }
    
    @classmethod
    def from_enriched_address(cls, enriched: EnrichedAddress) -> 'AddressMetadata':
        """Create from EnrichedAddress object"""
        return cls(
            address=enriched.address,
            label=enriched.primary_label.value,
            entity_type=enriched.primary_label.value,
            confidence=enriched.confidence,
            additional_info={"source": enriched.source.value}
        )

class Transaction:
    """Transaction data for classification"""
    def __init__(
        self,
        tx_hash: str,
        from_address: str,
        to_address: str,
        chain: Union[str, ChainType],
        token: str,
        amount: float,
        usd_value: Optional[float] = None,
        timestamp: Optional[datetime] = None,
        block_number: Optional[int] = None,
        from_address_metadata: Optional[AddressMetadata] = None,
        to_address_metadata: Optional[AddressMetadata] = None,
        input_data: Optional[str] = None,
        log_events: Optional[List[Dict[str, Any]]] = None
    ):
        self.tx_hash = tx_hash
        self.from_address = from_address
        self.to_address = to_address
        
        # Handle chain type
        if isinstance(chain, str):
            try:
                self.chain = ChainType(chain.lower())
            except ValueError:
                logger.warning(f"Invalid chain type: {chain}. Defaulting to ethereum.")
                self.chain = ChainType.ETHEREUM
        else:
            self.chain = chain
            
        self.token = token
        self.amount = amount
        self.usd_value = usd_value
        self.timestamp = timestamp
        self.block_number = block_number
        self.from_address_metadata = from_address_metadata
        self.to_address_metadata = to_address_metadata
        self.input_data = input_data
        self.log_events = log_events or []
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "tx_hash": self.tx_hash,
            "from_address": self.from_address,
            "to_address": self.to_address,
            "chain": self.chain.value,
            "token": self.token,
            "amount": self.amount,
            "usd_value": self.usd_value,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "block_number": self.block_number,
            "from_address_metadata": self.from_address_metadata.to_dict() if self.from_address_metadata else None,
            "to_address_metadata": self.to_address_metadata.to_dict() if self.to_address_metadata else None
        }

class ClassificationResult:
    """Result of transaction classification"""
    def __init__(
        self,
        classification: ClassificationType,
        confidence: float,
        triggered_rule: str,
        explanation: str,
        transaction: Transaction,
        rule_processing_time_ms: Optional[float] = None
    ):
        self.classification = classification
        self.confidence = min(max(confidence, 0.0), 1.0)  # Ensure between 0 and 1
        self.confidence_level = self._map_confidence_to_level(self.confidence)
        self.triggered_rule = triggered_rule
        self.explanation = explanation
        self.transaction = transaction
        self.rule_processing_time_ms = rule_processing_time_ms
        self.processed_at = datetime.utcnow()
    
    def _map_confidence_to_level(self, confidence: float) -> ConfidenceLevel:
        """Map confidence score to confidence level"""
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
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "classification": self.classification.value,
            "confidence": self.confidence,
            "confidence_level": self.confidence_level.value,
            "triggered_rule": self.triggered_rule,
            "explanation": self.explanation,
            "transaction": self.transaction.to_dict(),
            "rule_processing_time_ms": self.rule_processing_time_ms,
            "processed_at": self.processed_at.isoformat()
        }

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
    def apply(self, transaction: Transaction) -> Optional[ClassificationResult]:
        """
        Apply the rule to a transaction
        
        Args:
            transaction: The transaction to classify
            
        Returns:
            ClassificationResult if the rule matches, None otherwise
        """
        pass
    
    def create_result(
        self, 
        transaction: Transaction, 
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
            triggered_rule=self.name,
            explanation=explanation,
            transaction=transaction
        )

class ExchangeDepositRule(BaseRule):
    """
    Rule A: Exchange Deposits → Sell
    
    IF from_owner_type IN ["Personal", "Unknown"] AND to_owner_type == "Exchange"
    THEN classification = "sell"
    """
    name = "exchange_deposit_rule"
    description = "Classify transactions to exchanges as sells"
    
    # Labels that indicate exchanges
    EXCHANGE_LABELS: Set[str] = {
        "exchange", "cex", "centralized exchange"
    }
    
    # Labels that indicate personal wallets
    PERSONAL_LABELS: Set[str] = {
        "personal", "unknown", "individual", "user", "wallet"
    }
    
    def apply(self, transaction: Transaction) -> Optional[ClassificationResult]:
        # Skip if we don't have metadata for both addresses
        if not transaction.from_address_metadata or not transaction.to_address_metadata:
            return None
            
        from_label = transaction.from_address_metadata.label.lower()
        to_label = transaction.to_address_metadata.label.lower()
        
        # Check if this is a deposit to an exchange
        is_from_personal = any(term in from_label for term in self.PERSONAL_LABELS)
        is_to_exchange = any(term in to_label for term in self.EXCHANGE_LABELS)
        
        if is_from_personal and is_to_exchange:
            # Confidence is influenced by the metadata confidence
            from_confidence = transaction.from_address_metadata.confidence
            to_confidence = transaction.to_address_metadata.confidence
            
            # Combined confidence (average, but weighted more towards the destination)
            confidence = (from_confidence * 0.4) + (to_confidence * 0.6)
            
            explanation = (
                f"Transaction classified as SELL because it's a deposit from a " 
                f"Personal wallet ({from_label}) to an Exchange ({to_label})"
            )
            
            return self.create_result(
                transaction=transaction,
                classification=ClassificationType.SELL,
                confidence=confidence,
                explanation=explanation
            )
            
        return None

class ExchangeWithdrawalRule(BaseRule):
    """
    Rule B: Exchange Withdrawals → Buy
    
    IF from_owner_type == "Exchange" AND to_owner_type IN ["Personal", "Unknown"]
    THEN classification = "buy"
    """
    name = "exchange_withdrawal_rule"
    description = "Classify withdrawals from exchanges as buys"
    
    # Labels that indicate exchanges
    EXCHANGE_LABELS: Set[str] = {
        "exchange", "cex", "centralized exchange"
    }
    
    # Labels that indicate personal wallets
    PERSONAL_LABELS: Set[str] = {
        "personal", "unknown", "individual", "user", "wallet"
    }
    
    def apply(self, transaction: Transaction) -> Optional[ClassificationResult]:
        # Skip if we don't have metadata for both addresses
        if not transaction.from_address_metadata or not transaction.to_address_metadata:
            return None
            
        from_label = transaction.from_address_metadata.label.lower()
        to_label = transaction.to_address_metadata.label.lower()
        
        # Check if this is a withdrawal from an exchange
        is_from_exchange = any(term in from_label for term in self.EXCHANGE_LABELS)
        is_to_personal = any(term in to_label for term in self.PERSONAL_LABELS)
        
        if is_from_exchange and is_to_personal:
            # Confidence is influenced by the metadata confidence
            from_confidence = transaction.from_address_metadata.confidence
            to_confidence = transaction.to_address_metadata.confidence
            
            # Combined confidence (average, but weighted more towards the source)
            confidence = (from_confidence * 0.6) + (to_confidence * 0.4)
            
            explanation = (
                f"Transaction classified as BUY because it's a withdrawal from an "
                f"Exchange ({from_label}) to a Personal wallet ({to_label})"
            )
            
            return self.create_result(
                transaction=transaction,
                classification=ClassificationType.BUY,
                confidence=confidence,
                explanation=explanation
            )
            
        return None

class DexSwapRule(BaseRule):
    """
    Rule C: DEX Swaps
    
    IF from_owner_type == "Personal" AND to_owner_type == "DEX"
    THEN if token_out is Stablecoin → buy else → sell
    
    IF from_owner_type == "DEX" AND to_owner_type == "Personal"
    THEN if token_in is Stablecoin → buy else → sell
    """
    name = "dex_swap_rule"
    description = "Classify DEX swaps as buys or sells based on token types"
    
    # Labels that indicate DEXes
    DEX_LABELS: Set[str] = {
        "dex", "decentralized exchange", "amm", "automated market maker",
        "uniswap", "sushiswap", "pancakeswap", "curve", "balancer"
    }
    
    # Labels that indicate personal wallets
    PERSONAL_LABELS: Set[str] = {
        "personal", "unknown", "individual", "user", "wallet"
    }
    
    # Stablecoin tokens
    STABLECOINS: Set[str] = {
        "usdt", "usdc", "busd", "dai", "tusd", "usdp", "usdn", "frax", "lusd",
        "gusd", "husd", "susd", "ust", "usdd", "musd", "usdj", "usdx"
    }
    
    def apply(self, transaction: Transaction) -> Optional[ClassificationResult]:
        # Skip if we don't have metadata for both addresses
        if not transaction.from_address_metadata or not transaction.to_address_metadata:
            return None
            
        from_label = transaction.from_address_metadata.label.lower()
        to_label = transaction.to_address_metadata.label.lower()
        token = transaction.token.lower()
        
        # Check if either address is a DEX
        is_from_personal = any(term in from_label for term in self.PERSONAL_LABELS)
        is_to_personal = any(term in to_label for term in self.PERSONAL_LABELS)
        is_from_dex = any(term in from_label for term in self.DEX_LABELS)
        is_to_dex = any(term in to_label for term in self.DEX_LABELS)
        is_stablecoin = token in self.STABLECOINS
        
        # Case 1: Personal to DEX
        if is_from_personal and is_to_dex:
            # Confidence factors
            from_confidence = transaction.from_address_metadata.confidence
            to_confidence = transaction.to_address_metadata.confidence
            
            # Combined confidence 
            confidence = (from_confidence * 0.5) + (to_confidence * 0.5)
            
            if is_stablecoin:
                # Sending stablecoin to DEX = buying something else
                classification = ClassificationType.BUY
                explanation = (
                    f"Transaction classified as BUY because it's a transfer of stablecoin "
                    f"({token.upper()}) from a Personal wallet to a DEX, indicating a buy of another asset"
                )
            else:
                # Sending non-stablecoin to DEX = selling it
                classification = ClassificationType.SELL
                explanation = (
                    f"Transaction classified as SELL because it's a transfer of non-stablecoin "
                    f"({token.upper()}) from a Personal wallet to a DEX, indicating a sell"
                )
            
            return self.create_result(
                transaction=transaction,
                classification=classification,
                confidence=confidence,
                explanation=explanation
            )
        
        # Case 2: DEX to Personal
        elif is_from_dex and is_to_personal:
            # Confidence factors
            from_confidence = transaction.from_address_metadata.confidence
            to_confidence = transaction.to_address_metadata.confidence
            
            # Combined confidence 
            confidence = (from_confidence * 0.5) + (to_confidence * 0.5)
            
            if is_stablecoin:
                # Receiving stablecoin from DEX = sold something
                classification = ClassificationType.SELL
                explanation = (
                    f"Transaction classified as SELL because it's a transfer of stablecoin "
                    f"({token.upper()}) from a DEX to a Personal wallet, indicating proceeds from a sell"
                )
            else:
                # Receiving non-stablecoin from DEX = bought it
                classification = ClassificationType.BUY
                explanation = (
                    f"Transaction classified as BUY because it's a transfer of non-stablecoin "
                    f"({token.upper()}) from a DEX to a Personal wallet, indicating a completed buy"
                )
            
            return self.create_result(
                transaction=transaction,
                classification=classification,
                confidence=confidence,
                explanation=explanation
            )
            
        return None

class BridgeTransactionRule(BaseRule):
    """
    Rule D: Bridge Transactions → Transfer
    
    IF from_owner_type == "Bridge" OR to_owner_type == "Bridge"
    THEN classification = "transfer"
    """
    name = "bridge_transaction_rule"
    description = "Classify transactions involving bridges as transfers"
    
    # Labels that indicate bridges
    BRIDGE_LABELS: Set[str] = {
        "bridge", "cross-chain", "cross chain", "multichain", "wormhole", 
        "portal", "wrapped", "anyswap", "orbit", "hop", "synapse"
    }
    
    def apply(self, transaction: Transaction) -> Optional[ClassificationResult]:
        # Skip if we don't have metadata for both addresses
        if not transaction.from_address_metadata or not transaction.to_address_metadata:
            return None
            
        from_label = transaction.from_address_metadata.label.lower()
        to_label = transaction.to_address_metadata.label.lower()
        
        # Check if this is a transaction involving a bridge
        is_bridge_involved = (
            any(bridge in from_label for bridge in self.BRIDGE_LABELS) or
            any(bridge in to_label for bridge in self.BRIDGE_LABELS)
        )
        
        if is_bridge_involved:
            # Confidence is influenced by the metadata confidence
            from_confidence = transaction.from_address_metadata.confidence
            to_confidence = transaction.to_address_metadata.confidence
            
            # Combined confidence - take the higher one since either side being a bridge is sufficient
            confidence = max(from_confidence, to_confidence)
            
            explanation = (
                f"Transaction classified as TRANSFER because it involves a bridge "
                f"(from: {from_label}, to: {to_label}), indicating cross-chain movement"
            )
            
            return self.create_result(
                transaction=transaction,
                classification=ClassificationType.TRANSFER,
                confidence=confidence,
                explanation=explanation
            )
            
        return None

class MarketMakerTransferRule(BaseRule):
    """
    Rule E: Same-owner or Market Maker Transfers → Transfer
    
    IF from_owner_type == to_owner_type == "Market Maker"
    OR from_owner == to_owner
    THEN classification = "transfer"
    """
    name = "market_maker_transfer_rule"
    description = "Classify transactions between market makers or same owner as transfers"
    
    # Labels that indicate market makers
    MARKET_MAKER_LABELS: Set[str] = {
        "market maker", "market_maker", "liquidity provider", "market making",
        "mm", "otc desk", "otc", "prop trading", "proprietary"
    }
    
    def apply(self, transaction: Transaction) -> Optional[ClassificationResult]:
        # Skip if we don't have metadata for both addresses
        if not transaction.from_address_metadata or not transaction.to_address_metadata:
            return None
            
        from_label = transaction.from_address_metadata.label.lower()
        to_label = transaction.to_address_metadata.label.lower()
        
        # Check for market maker to market maker transfers
        is_market_maker_transfer = (
            any(mm in from_label for mm in self.MARKET_MAKER_LABELS) and
            any(mm in to_label for mm in self.MARKET_MAKER_LABELS)
        )
        
        # Check for same entity transfers
        # In a real implementation, you'd have more sophisticated entity matching
        from_entity = transaction.from_address_metadata.additional_info.get("entity_name", "").lower()
        to_entity = transaction.to_address_metadata.additional_info.get("entity_name", "").lower()
        same_entity = from_entity and to_entity and from_entity == to_entity
        
        if is_market_maker_transfer or same_entity:
            # Confidence is influenced by the metadata confidence
            from_confidence = transaction.from_address_metadata.confidence
            to_confidence = transaction.to_address_metadata.confidence
            
            # Combined confidence 
            confidence = (from_confidence * 0.5) + (to_confidence * 0.5)
            
            if is_market_maker_transfer:
                explanation = (
                    "Transaction classified as TRANSFER because it's between market makers, "
                    "likely for liquidity management purposes"
                )
            else:
                explanation = (
                    "Transaction classified as TRANSFER because it's between addresses "
                    "belonging to the same entity (internal transfer)"
                )
            
            return self.create_result(
                transaction=transaction,
                classification=ClassificationType.TRANSFER,
                confidence=confidence,
                explanation=explanation
            )
            
        return None

class SameOwnerTransferRule(BaseRule):
    """
    Rule F: Transfers between wallets owned by the same entity
    
    This rule uses additional entity information when available to detect
    transfers between wallets owned by the same user or entity.
    """
    name = "same_owner_transfer_rule"
    description = "Classify transfers between wallets owned by the same entity"
    
    def apply(self, transaction: Transaction) -> Optional[ClassificationResult]:
        # Need metadata for both sides
        if not transaction.from_address_metadata or not transaction.to_address_metadata:
            return None
        
        # Check for address clustering information in metadata
        from_cluster = transaction.from_address_metadata.additional_info.get("cluster_id")
        to_cluster = transaction.to_address_metadata.additional_info.get("cluster_id")
        
        # If we have cluster IDs and they match
        if from_cluster and to_cluster and from_cluster == to_cluster:
            confidence = 0.9  # High confidence when cluster IDs match
            
            explanation = (
                f"Transaction classified as TRANSFER because source and destination "
                f"addresses belong to the same cluster (ID: {from_cluster}), "
                f"indicating they are controlled by the same entity"
            )
            
            return self.create_result(
                transaction=transaction,
                classification=ClassificationType.TRANSFER,
                confidence=confidence,
                explanation=explanation
            )
        
        # Check entity names if available
        from_entity = transaction.from_address_metadata.additional_info.get("entity_name", "").lower()
        to_entity = transaction.to_address_metadata.additional_info.get("entity_name", "").lower()
        
        if from_entity and to_entity and from_entity == to_entity and from_entity != "unknown":
            confidence = 0.85  # Good confidence when entity names match
            
            explanation = (
                f"Transaction classified as TRANSFER because source and destination "
                f"addresses belong to the same entity ({from_entity})"
            )
            
            return self.create_result(
                transaction=transaction,
                classification=ClassificationType.TRANSFER,
                confidence=confidence,
                explanation=explanation
            )
            
        return None

class RuleEngine:
    """
    Rule engine for classifying crypto transactions
    
    This class manages a collection of rules and applies them in order
    to classify transactions.
    """
    
    def __init__(self):
        """Initialize the rule engine with default rules"""
        self.rules: List[BaseRule] = []
        
        # Register default rules
        self.register_rules([
            ExchangeDepositRule(),
            ExchangeWithdrawalRule(),
            DexSwapRule(),
            BridgeTransactionRule(),
            MarketMakerTransferRule(),
            SameOwnerTransferRule()
        ])
        
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
    
    def classify_transaction(self, transaction: Transaction) -> ClassificationResult:
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
                    logger.info(f"Transaction {transaction.tx_hash} classified as {result.classification} by rule {rule.name} in {processing_time_ms:.2f}ms")
                    return result
            except Exception as e:
                logger.error(f"Error applying rule {rule.name}: {e}")
        
        # If no rule matched, use fallback classification
        processing_time_ms = (time.time() - start_time) * 1000
        result = ClassificationResult(
            classification=ClassificationType.UNKNOWN,
            confidence=0.3,
            triggered_rule="fallback",
            explanation="No classification rules matched this transaction",
            transaction=transaction,
            rule_processing_time_ms=processing_time_ms
        )
        logger.info(f"Transaction {transaction.tx_hash} classified as UNKNOWN (fallback) in {processing_time_ms:.2f}ms")
        return result

# Usage example
def main():
    """Example usage of the rule engine"""
    # Create a transaction with address metadata
    transaction = Transaction(
        tx_hash="0x123456789abcdef",
        from_address="0x1234",
        to_address="0x5678",
        chain=ChainType.ETHEREUM,
        token="ETH",
        amount=1.0,
        usd_value=1800.0,
        from_address_metadata=AddressMetadata(
            address="0x1234",
            label="personal wallet",
            entity_type="individual",
            confidence=0.8
        ),
        to_address_metadata=AddressMetadata(
            address="0x5678",
            label="binance hot wallet",
            entity_type="exchange",
            confidence=0.95
        )
    )
    
    # Create the rule engine
    engine = RuleEngine()
    
    # Classify the transaction
    result = engine.classify_transaction(transaction)
    
    # Print the result
    print(f"Classification: {result.classification}")
    print(f"Confidence: {result.confidence} ({result.confidence_level})")
    print(f"Triggered rule: {result.triggered_rule}")
    print(f"Explanation: {result.explanation}")
    print(f"Processing time: {result.rule_processing_time_ms:.2f}ms")

if __name__ == "__main__":
    main() 
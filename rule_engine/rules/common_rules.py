"""
Common Classification Rules Module

This module implements common classification rules that apply to all chains.
These rules use enriched address metadata to improve classification accuracy.
"""
from typing import Optional, List, Set

from .base import BaseRule
from ..models.transaction import (
    TransactionRequest,
    ClassificationResult,
    ClassificationType
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
    
    def apply(self, transaction: TransactionRequest) -> Optional[ClassificationResult]:
        # Skip if we don't have metadata for both addresses
        if not transaction.from_address_metadata or not transaction.to_address_metadata:
            return None
            
        from_label = transaction.from_address_metadata.label.lower()
        to_label = transaction.to_address_metadata.label.lower()
        
        # Check if this is a deposit to an exchange
        if (from_label in self.PERSONAL_LABELS and 
            to_label in self.EXCHANGE_LABELS):
            
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
    
    def apply(self, transaction: TransactionRequest) -> Optional[ClassificationResult]:
        # Skip if we don't have metadata for both addresses
        if not transaction.from_address_metadata or not transaction.to_address_metadata:
            return None
            
        from_label = transaction.from_address_metadata.label.lower()
        to_label = transaction.to_address_metadata.label.lower()
        
        # Check if this is a withdrawal from an exchange
        if (from_label in self.EXCHANGE_LABELS and 
            to_label in self.PERSONAL_LABELS):
            
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
    THEN if token_out is Stablecoin → sell else → buy
    
    IF from_owner_type == "DEX" AND to_owner_type == "Personal"
    THEN if token_in is Stablecoin → buy else → sell
    """
    name = "dex_swap_rule"
    description = "Classify DEX swaps as buys or sells based on token types"
    
    # Labels that indicate DEXes
    DEX_LABELS: Set[str] = {
        "dex", "decentralized exchange", "amm", "automated market maker"
    }
    
    # Labels that indicate personal wallets
    PERSONAL_LABELS: Set[str] = {
        "personal", "unknown", "individual", "user", "wallet"
    }
    
    # Stablecoin tokens
    STABLECOINS: Set[str] = {
        "usdt", "usdc", "busd", "dai", "tusd", "usdp", "usdn", "frax", "lusd"
    }
    
    def apply(self, transaction: TransactionRequest) -> Optional[ClassificationResult]:
        # Skip if we don't have metadata for both addresses
        if not transaction.from_address_metadata or not transaction.to_address_metadata:
            return None
            
        from_label = transaction.from_address_metadata.label.lower()
        to_label = transaction.to_address_metadata.label.lower()
        token = transaction.token.lower()
        
        # Check if this is a transaction to a DEX (first part of the rule)
        if (from_label in self.PERSONAL_LABELS and 
            to_label in self.DEX_LABELS):
            
            # Confidence is influenced by the metadata confidence
            from_confidence = transaction.from_address_metadata.confidence
            to_confidence = transaction.to_address_metadata.confidence
            
            # Combined confidence 
            confidence = (from_confidence * 0.5) + (to_confidence * 0.5)
            
            if token in self.STABLECOINS:
                classification = ClassificationType.BUY
                explanation = (
                    f"Transaction classified as BUY because it's a transfer of stablecoin "
                    f"({token.upper()}) from a Personal wallet to a DEX, indicating a buy of another asset"
                )
            else:
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
        
        # Check if this is a transaction from a DEX (second part of the rule)
        elif (from_label in self.DEX_LABELS and 
              to_label in self.PERSONAL_LABELS):
            
            # Confidence is influenced by the metadata confidence
            from_confidence = transaction.from_address_metadata.confidence
            to_confidence = transaction.to_address_metadata.confidence
            
            # Combined confidence 
            confidence = (from_confidence * 0.5) + (to_confidence * 0.5)
            
            if token in self.STABLECOINS:
                classification = ClassificationType.SELL
                explanation = (
                    f"Transaction classified as SELL because it's a transfer of stablecoin "
                    f"({token.upper()}) from a DEX to a Personal wallet, indicating proceeds from a sell"
                )
            else:
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
        "bridge", "cross-chain", "cross chain", "multichain"
    }
    
    def apply(self, transaction: TransactionRequest) -> Optional[ClassificationResult]:
        # Skip if we don't have metadata for both addresses
        if not transaction.from_address_metadata or not transaction.to_address_metadata:
            return None
            
        from_label = transaction.from_address_metadata.label.lower()
        to_label = transaction.to_address_metadata.label.lower()
        
        # Check if this is a transaction involving a bridge
        if (any(bridge in from_label for bridge in self.BRIDGE_LABELS) or
            any(bridge in to_label for bridge in self.BRIDGE_LABELS)):
            
            # Confidence is influenced by the metadata confidence
            from_confidence = transaction.from_address_metadata.confidence
            to_confidence = transaction.to_address_metadata.confidence
            
            # Combined confidence 
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
        "market maker", "market_maker", "liquidity provider", "market making"
    }
    
    def apply(self, transaction: TransactionRequest) -> Optional[ClassificationResult]:
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
        same_entity = False
        if hasattr(transaction.from_address_metadata, 'entity_name') and hasattr(transaction.to_address_metadata, 'entity_name'):
            from_entity = getattr(transaction.from_address_metadata, 'entity_name', '').lower()
            to_entity = getattr(transaction.to_address_metadata, 'entity_name', '').lower()
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
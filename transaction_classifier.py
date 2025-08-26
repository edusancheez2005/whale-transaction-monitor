"""
Transaction Classifier Module

This module provides a unified interface for classifying cryptocurrency transactions,
integrating address enrichment and rule-based classification.
"""
import logging
import asyncio
import json
from typing import Dict, Any, Optional, List, Union
from datetime import datetime

from address_enrichment import (
    AddressEnrichmentService,
    EnrichedAddress,
    ChainType,
    AddressLabelType
)

from rule_engine import (
    RuleEngine,
    Transaction,
    AddressMetadata,
    ClassificationResult,
    ClassificationType
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("transaction_classifier")

class TransactionClassifier:
    """
    Unified transaction classifier that integrates address enrichment
    and rule-based classification.
    
    This class:
    1. Enriches transaction addresses with metadata
    2. Classifies transactions using the rule engine
    3. Provides confidence scores and explanations
    """
    
    def __init__(
        self,
        redis_url: Optional[str] = None,
        cache_ttl: Optional[int] = None
    ):
        """
        Initialize the transaction classifier
        
        Args:
            redis_url: Redis connection URL for caching
            cache_ttl: Cache TTL in seconds (defaults to 24 hours)
        """
        # Initialize address enrichment service
        self.enrichment_service = AddressEnrichmentService(redis_url=redis_url)
        if cache_ttl:
            self.enrichment_service.cache_ttl = cache_ttl
            
        # Initialize rule engine
        self.rule_engine = RuleEngine()
        
        logger.info(f"Transaction classifier initialized with {len(self.rule_engine.rules)} rules")
    
    async def classify_transaction(
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
        input_data: Optional[str] = None,
        force_refresh_metadata: bool = False
    ) -> ClassificationResult:
        """
        Classify a cryptocurrency transaction
        
        This method:
        1. Enriches both addresses with metadata
        2. Passes the transaction with enriched data to the rule engine
        3. Returns the classification result
        
        Args:
            tx_hash: Transaction hash
            from_address: Source address
            to_address: Destination address
            chain: Blockchain (e.g., ethereum, solana)
            token: Token symbol (e.g., ETH, USDC)
            amount: Token amount
            usd_value: USD value (optional)
            timestamp: Transaction timestamp (optional)
            block_number: Block number (optional)
            input_data: Transaction input data (optional)
            force_refresh_metadata: Whether to force refresh address metadata
            
        Returns:
            ClassificationResult: Transaction classification with confidence
        """
        # Normalize chain
        if isinstance(chain, str):
            try:
                chain_type = ChainType(chain.lower())
            except ValueError:
                logger.warning(f"Invalid chain type: {chain}. Defaulting to ethereum.")
                chain_type = ChainType.ETHEREUM
        else:
            chain_type = chain
        
        # Enrich both addresses
        try:
            # Create tasks for both addresses
            from_task = self.enrichment_service.enrich_address(
                from_address, 
                chain_type,
                force_refresh=force_refresh_metadata
            )
            
            to_task = self.enrichment_service.enrich_address(
                to_address, 
                chain_type,
                force_refresh=force_refresh_metadata
            )
            
            # Await both tasks concurrently
            from_enriched, to_enriched = await asyncio.gather(from_task, to_task)
            
            # Convert to address metadata format
            from_metadata = AddressMetadata.from_enriched_address(from_enriched)
            to_metadata = AddressMetadata.from_enriched_address(to_enriched)
            
            logger.info(f"Enriched addresses for tx {tx_hash}: "
                       f"from={from_enriched.primary_label.value} ({from_enriched.confidence:.2f}), "
                       f"to={to_enriched.primary_label.value} ({to_enriched.confidence:.2f})")
            
        except Exception as e:
            logger.error(f"Error enriching addresses for tx {tx_hash}: {e}")
            # Continue with unenriched addresses
            from_metadata = None
            to_metadata = None
        
        # Create transaction object
        transaction = Transaction(
            tx_hash=tx_hash,
            from_address=from_address,
            to_address=to_address,
            chain=chain_type,
            token=token,
            amount=amount,
            usd_value=usd_value,
            timestamp=timestamp,
            block_number=block_number,
            from_address_metadata=from_metadata,
            to_address_metadata=to_metadata,
            input_data=input_data
        )
        
        # Classify using rule engine
        result = self.rule_engine.classify_transaction(transaction)
        
        return result
    
    async def classify_transactions(
        self,
        transactions: List[Dict[str, Any]],
        force_refresh_metadata: bool = False
    ) -> List[ClassificationResult]:
        """
        Classify multiple transactions in batch
        
        Args:
            transactions: List of transaction dictionaries
            force_refresh_metadata: Whether to force refresh address metadata
            
        Returns:
            List[ClassificationResult]: Classification results
        """
        # Create tasks for each transaction
        tasks = []
        for tx in transactions:
            # Extract required fields
            tx_hash = tx.get("tx_hash", "")
            from_address = tx.get("from_address", "")
            to_address = tx.get("to_address", "")
            chain = tx.get("chain", "ethereum")
            token = tx.get("token", "")
            amount = float(tx.get("amount", 0))
            
            # Extract optional fields
            usd_value = float(tx.get("usd_value", 0)) if tx.get("usd_value") else None
            
            # Handle timestamp (string or datetime)
            timestamp = None
            if "timestamp" in tx:
                if isinstance(tx["timestamp"], str):
                    try:
                        timestamp = datetime.fromisoformat(tx["timestamp"].replace("Z", "+00:00"))
                    except ValueError:
                        pass
                elif isinstance(tx["timestamp"], datetime):
                    timestamp = tx["timestamp"]
            
            block_number = int(tx.get("block_number", 0)) if tx.get("block_number") else None
            input_data = tx.get("input_data")
            
            # Create classification task
            task = self.classify_transaction(
                tx_hash=tx_hash,
                from_address=from_address,
                to_address=to_address,
                chain=chain,
                token=token,
                amount=amount,
                usd_value=usd_value,
                timestamp=timestamp,
                block_number=block_number,
                input_data=input_data,
                force_refresh_metadata=force_refresh_metadata
            )
            
            tasks.append(task)
        
        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks)
        
        return results
    
    async def close(self):
        """Close the classifier and its components"""
        await self.enrichment_service.close()
        
    def generate_classification_summary(self, result: ClassificationResult) -> Dict[str, Any]:
        """
        Generate a human-readable summary of the classification
        
        Args:
            result: Classification result
            
        Returns:
            Dict: Summary information
        """
        # Get entity names from address metadata
        from_entity = "Unknown"
        to_entity = "Unknown"
        
        if result.transaction.from_address_metadata:
            from_entity = result.transaction.from_address_metadata.label.capitalize()
            
        if result.transaction.to_address_metadata:
            to_entity = result.transaction.to_address_metadata.label.capitalize()
        
        # Create summary based on classification
        if result.classification == ClassificationType.BUY:
            action = "purchased"
            confidence_text = self._get_confidence_text(result.confidence)
            summary = f"This transaction {confidence_text} represents a BUY: "
            summary += f"Tokens were {action} from {from_entity} by {to_entity}."
            
        elif result.classification == ClassificationType.SELL:
            action = "sold"
            confidence_text = self._get_confidence_text(result.confidence)
            summary = f"This transaction {confidence_text} represents a SELL: "
            summary += f"Tokens were {action} by {from_entity} to {to_entity}."
            
        elif result.classification == ClassificationType.TRANSFER:
            confidence_text = self._get_confidence_text(result.confidence)
            summary = f"This transaction {confidence_text} represents a TRANSFER: "
            summary += f"Tokens were moved between wallets, not bought or sold."
            
        else:
            summary = "This transaction's purpose could not be determined with confidence."
        
        # Add explanation
        summary += f"\n\nRationale: {result.explanation}"
        
        return {
            "classification": result.classification.value,
            "confidence": result.confidence,
            "confidence_level": result.confidence_level.value,
            "summary": summary,
            "from_entity": from_entity,
            "to_entity": to_entity,
            "triggered_rule": result.triggered_rule
        }
    
    def _get_confidence_text(self, confidence: float) -> str:
        """Get text describing confidence level"""
        if confidence >= 0.9:
            return "very likely"
        elif confidence >= 0.7:
            return "likely"
        elif confidence >= 0.5:
            return "possibly"
        else:
            return "may"

# Usage example
async def main():
    """Example usage of the transaction classifier"""
    # Create the classifier
    classifier = TransactionClassifier()
    
    # Example transaction (Ethereum transfer to Binance)
    result = await classifier.classify_transaction(
        tx_hash="0x123456789abcdef",
        from_address="0x28c6c06298d514db089934071355e5743bf21d60",  # Example address
        to_address="0x3f5CE5FBFe3E9af3971dD833D26bA9b5C936f0bE",  # Binance hot wallet
        chain="ethereum",
        token="ETH",
        amount=2.5,
        usd_value=4500.0
    )
    
    # Get summary
    summary = classifier.generate_classification_summary(result)
    
    # Print results
    print(f"Classification: {result.classification}")
    print(f"Confidence: {result.confidence} ({result.confidence_level})")
    print(f"Rule: {result.triggered_rule}")
    print("\nSummary:")
    print(summary["summary"])
    
    # Close the classifier
    await classifier.close()

if __name__ == "__main__":
    asyncio.run(main()) 
"""
Rule-Based Transaction Labeling Module

This module provides functionality for labeling transactions using the rule engine.
"""
import logging
import json
import os
import time
import asyncio
import httpx
from typing import List, Dict, Any, Optional
from datetime import datetime

from ..models import RawTransaction, LabeledTransaction, LabelType, LabelSource, ChainType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RuleBasedLabeler:
    """
    Label transactions using the rule engine
    
    This class handles:
    1. Sending transactions to the rule engine API
    2. Converting rule engine classifications to labels
    3. Tracking confidence and metadata
    """
    
    def __init__(self, rule_engine_url: str = "http://localhost:8001"):
        """
        Initialize the rule-based labeler
        
        Args:
            rule_engine_url: URL of the rule engine API
        """
        self.rule_engine_url = rule_engine_url
    
    async def label_transaction(self, transaction: RawTransaction) -> Optional[LabeledTransaction]:
        """
        Label a single transaction using the rule engine
        
        Args:
            transaction: The transaction to label
            
        Returns:
            Optional[LabeledTransaction]: The labeled transaction, or None if labeling failed
        """
        try:
            # Convert to rule engine input format
            rule_engine_input = {
                "from_address": transaction.from_address,
                "to_address": transaction.to_address,
                "chain": transaction.chain.value,
                "token": transaction.token,
                "amount": transaction.amount,
                "usd_value": transaction.usd_value,
                "timestamp": transaction.timestamp.isoformat() if transaction.timestamp else None,
                "tx_hash": transaction.tx_hash,
                "block_number": transaction.block_number
            }
            
            # If the transaction has metadata for owner types (e.g., from Whale Alert),
            # add address metadata to help the rule engine
            if transaction.metadata:
                from_owner_type = transaction.metadata.get("from_owner_type", "unknown")
                to_owner_type = transaction.metadata.get("to_owner_type", "unknown")
                
                if from_owner_type != "unknown":
                    rule_engine_input["from_address_metadata"] = {
                        "address": transaction.from_address,
                        "label": from_owner_type,
                        "entity_type": from_owner_type,
                        "confidence": 0.9  # High confidence for Whale Alert data
                    }
                
                if to_owner_type != "unknown":
                    rule_engine_input["to_address_metadata"] = {
                        "address": transaction.to_address,
                        "label": to_owner_type,
                        "entity_type": to_owner_type,
                        "confidence": 0.9  # High confidence for Whale Alert data
                    }
            
            # Call the rule engine API
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.rule_engine_url}/classify-transaction",
                    json=rule_engine_input,
                    timeout=10.0
                )
                
                # Check response
                if response.status_code != 200:
                    logger.error(f"Error from rule engine: {response.status_code} {response.text}")
                    return None
                
                # Parse response
                result = response.json()
                
                # Map rule engine classification to label
                rule_classification = result.get("classification", "unknown")
                if rule_classification == "buy":
                    label = LabelType.BUY
                elif rule_classification == "sell":
                    label = LabelType.SELL
                elif rule_classification == "transfer":
                    label = LabelType.TRANSFER
                else:
                    label = LabelType.UNKNOWN
                
                # Create labeled transaction
                labeled_tx = LabeledTransaction(
                    tx_hash=transaction.tx_hash,
                    from_address=transaction.from_address,
                    to_address=transaction.to_address,
                    chain=transaction.chain,
                    token=transaction.token,
                    amount=transaction.amount,
                    usd_value=transaction.usd_value,
                    timestamp=transaction.timestamp,
                    block_number=transaction.block_number,
                    
                    label=label,
                    label_confidence=result.get("confidence", 0.5),
                    label_source=LabelSource.RULE_ENGINE,
                    
                    # Store additional information for later analysis
                    features={
                        "triggered_rule": result.get("triggered_rule", "unknown"),
                        "explanation": result.get("explanation", ""),
                        "confidence_level": result.get("confidence_level", "medium"),
                        "from_label": result.get("transaction", {}).get("from_address_metadata", {}).get("label", "unknown"),
                        "to_label": result.get("transaction", {}).get("to_address_metadata", {}).get("label", "unknown"),
                        "original_source": transaction.source
                    }
                )
                
                return labeled_tx
                
        except Exception as e:
            logger.error(f"Error labeling transaction {transaction.tx_hash}: {e}")
            return None
    
    async def label_transactions(self, transactions: List[RawTransaction]) -> List[LabeledTransaction]:
        """
        Label multiple transactions using the rule engine
        
        Args:
            transactions: Transactions to label
            
        Returns:
            List[LabeledTransaction]: Labeled transactions
        """
        logger.info(f"Labeling {len(transactions)} transactions using rule engine")
        
        labeled_transactions = []
        
        # Process in batches to avoid overwhelming the rule engine
        batch_size = 50
        for i in range(0, len(transactions), batch_size):
            batch = transactions[i:i+batch_size]
            
            # Create tasks for parallel processing
            tasks = [self.label_transaction(tx) for tx in batch]
            
            # Wait for all tasks to complete
            results = await asyncio.gather(*tasks)
            
            # Filter out None results and add to the list
            batch_results = [tx for tx in results if tx is not None]
            labeled_transactions.extend(batch_results)
            
            logger.info(f"Labeled batch {i//batch_size + 1}/{(len(transactions) + batch_size - 1)//batch_size}: "
                       f"{len(batch_results)}/{len(batch)} transactions successfully labeled")
        
        logger.info(f"Labeled {len(labeled_transactions)}/{len(transactions)} transactions using rule engine")
        return labeled_transactions
    
    def save_labeled_transactions(self, transactions: List[LabeledTransaction], output_file: str) -> None:
        """
        Save labeled transactions to a JSON file
        
        Args:
            transactions: Labeled transactions to save
            output_file: Output file path
        """
        logger.info(f"Saving {len(transactions)} labeled transactions to {output_file}")
        
        try:
            # Convert to dictionaries
            data = [tx.dict() for tx in transactions]
            
            # Ensure the directory exists
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # Save to file
            with open(output_file, "w") as f:
                json.dump(data, f, indent=2, default=str)
                
            logger.info(f"Successfully saved labeled transactions to {output_file}")
            
        except Exception as e:
            logger.error(f"Error saving labeled transactions: {e}")


class WhaleAlertLabeler:
    """
    Label transactions using Whale Alert metadata
    
    This class handles:
    1. Extracting owner type information from Whale Alert data
    2. Converting to buy/sell/transfer labels
    3. Assigning confidence scores
    """
    
    def __init__(self):
        """Initialize the Whale Alert labeler"""
        pass
    
    def label_transactions(self, transactions: List[RawTransaction]) -> List[LabeledTransaction]:
        """
        Label transactions based on Whale Alert metadata
        
        Args:
            transactions: Transactions to label
            
        Returns:
            List[LabeledTransaction]: Labeled transactions
        """
        logger.info(f"Labeling {len(transactions)} transactions using Whale Alert metadata")
        
        labeled_transactions = []
        
        for tx in transactions:
            # Skip transactions without Whale Alert metadata
            if tx.source != "whale_alert" or not tx.metadata:
                continue
                
            try:
                # Extract owner types
                from_owner_type = tx.metadata.get("from_owner_type", "unknown").lower()
                to_owner_type = tx.metadata.get("to_owner_type", "unknown").lower()
                
                # Determine label based on owner types
                label = LabelType.UNKNOWN
                confidence = 0.5
                
                if from_owner_type == "unknown" and to_owner_type == "exchange":
                    # Unknown to Exchange = SELL
                    label = LabelType.SELL
                    confidence = 0.85
                elif from_owner_type == "exchange" and to_owner_type == "unknown":
                    # Exchange to Unknown = BUY
                    label = LabelType.BUY
                    confidence = 0.85
                elif from_owner_type == to_owner_type:
                    # Same owner type = TRANSFER
                    label = LabelType.TRANSFER
                    confidence = 0.7
                else:
                    # Other cases - provide lower confidence
                    if "exchange" in from_owner_type or "exchange" in to_owner_type:
                        if "exchange" in from_owner_type:
                            label = LabelType.BUY
                        else:
                            label = LabelType.SELL
                        confidence = 0.6
                    else:
                        label = LabelType.TRANSFER
                        confidence = 0.5
                
                # Create labeled transaction
                labeled_tx = LabeledTransaction(
                    tx_hash=tx.tx_hash,
                    from_address=tx.from_address,
                    to_address=tx.to_address,
                    chain=tx.chain,
                    token=tx.token,
                    amount=tx.amount,
                    usd_value=tx.usd_value,
                    timestamp=tx.timestamp,
                    block_number=tx.block_number,
                    
                    label=label,
                    label_confidence=confidence,
                    label_source=LabelSource.WHALE_ALERT,
                    
                    # Store additional information for later analysis
                    features={
                        "from_owner_type": from_owner_type,
                        "to_owner_type": to_owner_type,
                        "from_owner": tx.metadata.get("from_owner", ""),
                        "to_owner": tx.metadata.get("to_owner", ""),
                        "original_source": tx.source,
                        "labeling_method": "whale_alert_heuristic"
                    }
                )
                
                labeled_transactions.append(labeled_tx)
                
            except Exception as e:
                logger.error(f"Error labeling Whale Alert transaction {tx.tx_hash}: {e}")
        
        logger.info(f"Labeled {len(labeled_transactions)} transactions using Whale Alert metadata")
        return labeled_transactions 
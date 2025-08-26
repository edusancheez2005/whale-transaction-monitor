"""
Dataset Validation Module

This module provides functionality for validating, cleaning, and balancing the labeled dataset.
"""
import logging
import json
import os
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import random

from ..models import LabeledTransaction, LabelType, LabelSource, ChainType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatasetValidator:
    """
    Validate, clean, and balance the labeled dataset
    
    This class handles:
    1. Removing duplicates
    2. Validating data quality
    3. Balancing classes (buy/sell/transfer)
    4. Generating validation sets
    """
    
    def __init__(self, output_dir: str = "output"):
        """
        Initialize the dataset validator
        
        Args:
            output_dir: Directory for output files
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def remove_duplicates(self, transactions: List[LabeledTransaction]) -> List[LabeledTransaction]:
        """
        Remove duplicate transactions based on tx_hash
        
        Args:
            transactions: Labeled transactions to deduplicate
            
        Returns:
            List[LabeledTransaction]: Deduplicated transactions
        """
        logger.info(f"Removing duplicates from {len(transactions)} transactions")
        
        # Keep track of seen tx_hashes and keep the highest confidence label
        tx_hash_to_best_tx = {}
        
        for tx in transactions:
            if tx.tx_hash not in tx_hash_to_best_tx:
                tx_hash_to_best_tx[tx.tx_hash] = tx
            else:
                # If we've seen this tx before, keep the one with higher confidence
                existing_tx = tx_hash_to_best_tx[tx.tx_hash]
                if tx.label_confidence > existing_tx.label_confidence:
                    tx_hash_to_best_tx[tx.tx_hash] = tx
        
        deduplicated = list(tx_hash_to_best_tx.values())
        logger.info(f"Removed {len(transactions) - len(deduplicated)} duplicates, {len(deduplicated)} transactions remaining")
        
        return deduplicated
    
    def validate_data_quality(self, transactions: List[LabeledTransaction]) -> List[LabeledTransaction]:
        """
        Validate data quality and filter out invalid transactions
        
        Args:
            transactions: Labeled transactions to validate
            
        Returns:
            List[LabeledTransaction]: Valid transactions
        """
        logger.info(f"Validating data quality for {len(transactions)} transactions")
        
        valid_transactions = []
        
        for tx in transactions:
            # Check for missing required fields
            if not tx.tx_hash or not tx.from_address or not tx.to_address:
                logger.warning(f"Skipping transaction with missing required fields: {tx.tx_hash}")
                continue
                
            # Check for invalid amount
            if tx.amount <= 0:
                logger.warning(f"Skipping transaction with invalid amount: {tx.tx_hash}")
                continue
                
            # Check for unknown labels with low confidence
            if tx.label == LabelType.UNKNOWN and tx.label_confidence < 0.7:
                logger.warning(f"Skipping transaction with low-confidence UNKNOWN label: {tx.tx_hash}")
                continue
                
            # Add the transaction to the valid list
            valid_transactions.append(tx)
        
        logger.info(f"Removed {len(transactions) - len(valid_transactions)} invalid transactions, {len(valid_transactions)} remaining")
        
        return valid_transactions
    
    def balance_classes(
        self, 
        transactions: List[LabeledTransaction], 
        target_counts: Optional[Dict[LabelType, int]] = None,
        min_confidence: float = 0.6
    ) -> List[LabeledTransaction]:
        """
        Balance classes to ensure even distribution of labels
        
        Args:
            transactions: Labeled transactions to balance
            target_counts: Target count for each class (if None, use the minimum class count)
            min_confidence: Minimum confidence threshold for including transactions
            
        Returns:
            List[LabeledTransaction]: Balanced transactions
        """
        logger.info(f"Balancing classes for {len(transactions)} transactions")
        
        # Filter by confidence first
        high_confidence_txs = [tx for tx in transactions if tx.label_confidence >= min_confidence]
        logger.info(f"Filtered to {len(high_confidence_txs)} transactions with confidence >= {min_confidence}")
        
        # Count transactions by label
        label_counts = {}
        label_to_txs = {}
        
        for tx in high_confidence_txs:
            if tx.label not in label_counts:
                label_counts[tx.label] = 0
                label_to_txs[tx.label] = []
                
            label_counts[tx.label] += 1
            label_to_txs[tx.label].append(tx)
        
        logger.info(f"Label distribution before balancing: {label_counts}")
        
        # Determine target count for each class
        if target_counts is None:
            min_count = min(label_counts.values()) if label_counts else 0
            target_counts = {label: min_count for label in LabelType}
        
        # Balance each class to the target count
        balanced_txs = []
        
        for label, target in target_counts.items():
            if label not in label_to_txs:
                logger.warning(f"No transactions found for label {label}")
                continue
                
            available_txs = label_to_txs[label]
            
            if len(available_txs) <= target:
                # If we have fewer than the target, keep all of them
                balanced_txs.extend(available_txs)
                logger.info(f"Keeping all {len(available_txs)} transactions for label {label}")
            else:
                # Otherwise, sample the target number
                # Sort by confidence (highest first) and prioritize validation set
                sorted_txs = sorted(available_txs, key=lambda tx: (tx.is_validated, tx.label_confidence), reverse=True)
                
                # Always keep validated transactions
                validated_txs = [tx for tx in sorted_txs if tx.is_validated]
                remaining_txs = [tx for tx in sorted_txs if not tx.is_validated]
                
                # If we have more validated than target, take the highest confidence ones
                if len(validated_txs) >= target:
                    balanced_txs.extend(validated_txs[:target])
                    logger.info(f"Keeping top {target} validated transactions for label {label}")
                else:
                    # Keep all validated and sample from remaining
                    balanced_txs.extend(validated_txs)
                    remaining_needed = target - len(validated_txs)
                    
                    # Prioritize higher confidence transactions
                    balanced_txs.extend(remaining_txs[:remaining_needed])
                    logger.info(f"Keeping all {len(validated_txs)} validated and {remaining_needed} non-validated transactions for label {label}")
        
        logger.info(f"Balanced dataset contains {len(balanced_txs)} transactions")
        
        # Count final distribution
        final_counts = {}
        for tx in balanced_txs:
            if tx.label not in final_counts:
                final_counts[tx.label] = 0
            final_counts[tx.label] += 1
            
        logger.info(f"Label distribution after balancing: {final_counts}")
        
        return balanced_txs
    
    def generate_validation_set(
        self, 
        transactions: List[LabeledTransaction], 
        validation_size: int = 500,
        random_seed: int = 42
    ) -> Tuple[List[LabeledTransaction], List[LabeledTransaction]]:
        """
        Generate a validation set by randomly sampling transactions
        
        Args:
            transactions: Labeled transactions to sample from
            validation_size: Size of the validation set
            random_seed: Random seed for reproducibility
            
        Returns:
            Tuple containing (validation_set, remaining_transactions)
        """
        logger.info(f"Generating validation set of {validation_size} transactions from {len(transactions)} total")
        
        # Set random seed for reproducibility
        random.seed(random_seed)
        
        # If we already have validated transactions, prioritize those
        already_validated = [tx for tx in transactions if tx.is_validated]
        not_validated = [tx for tx in transactions if not tx.is_validated]
        
        logger.info(f"Found {len(already_validated)} already validated transactions")
        
        validation_set = []
        
        # If we have more already validated than we need, sample from them
        if len(already_validated) >= validation_size:
            validation_set = random.sample(already_validated, validation_size)
            logger.info(f"Sampled {validation_size} transactions from already validated set")
        else:
            # Otherwise, take all already validated and sample the rest
            validation_set = already_validated.copy()
            remaining_needed = validation_size - len(already_validated)
            
            # Ensure we're not asking for more than available
            remaining_needed = min(remaining_needed, len(not_validated))
            
            validation_set.extend(random.sample(not_validated, remaining_needed))
            logger.info(f"Selected all {len(already_validated)} validated transactions and sampled {remaining_needed} additional transactions")
        
        # Mark all validation set transactions as validated
        for tx in validation_set:
            tx.is_validated = True
        
        # Create the remaining set (excluding validation set)
        validation_tx_hashes = {tx.tx_hash for tx in validation_set}
        remaining = [tx for tx in transactions if tx.tx_hash not in validation_tx_hashes]
        
        logger.info(f"Final validation set: {len(validation_set)} transactions")
        logger.info(f"Remaining transactions: {len(remaining)}")
        
        return validation_set, remaining
    
    def save_to_csv(self, transactions: List[LabeledTransaction], output_file: str) -> None:
        """
        Save labeled transactions to a CSV file
        
        Args:
            transactions: Labeled transactions to save
            output_file: Output file path
        """
        logger.info(f"Saving {len(transactions)} transactions to CSV: {output_file}")
        
        try:
            # Convert to a list of dictionaries
            records = []
            for tx in transactions:
                # Start with the main transaction fields
                record = {
                    "tx_hash": tx.tx_hash,
                    "from_address": tx.from_address,
                    "to_address": tx.to_address,
                    "chain": tx.chain.value,
                    "token": tx.token,
                    "amount": tx.amount,
                    "usd_value": tx.usd_value,
                    "timestamp": tx.timestamp.isoformat() if tx.timestamp else None,
                    "block_number": tx.block_number,
                    "label": tx.label.value,
                    "label_confidence": tx.label_confidence,
                    "label_source": tx.label_source.value,
                    "is_validated": tx.is_validated,
                    "validator_notes": tx.validator_notes
                }
                
                # Add features if available
                if tx.features:
                    for key, value in tx.features.items():
                        # Flatten the features with a prefix
                        record[f"feature_{key}"] = str(value) if isinstance(value, dict) else value
                
                records.append(record)
            
            # Convert to DataFrame
            df = pd.DataFrame(records)
            
            # Ensure the directory exists
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # Save to CSV
            df.to_csv(output_file, index=False)
            
            logger.info(f"Successfully saved to CSV: {output_file}")
            
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")
    
    def generate_dataset_statistics(self, transactions: List[LabeledTransaction]) -> Dict:
        """
        Generate statistics about the dataset
        
        Args:
            transactions: Labeled transactions to analyze
            
        Returns:
            Dict: Statistics about the dataset
        """
        logger.info(f"Generating statistics for {len(transactions)} transactions")
        
        stats = {
            "total_transactions": len(transactions),
            "label_distribution": {},
            "chain_distribution": {},
            "token_distribution": {},
            "source_distribution": {},
            "confidence_stats": {
                "mean": 0.0,
                "median": 0.0,
                "min": 0.0,
                "max": 0.0
            },
            "validation_status": {
                "validated": 0,
                "not_validated": 0
            },
            "timestamp_range": {
                "earliest": None,
                "latest": None
            }
        }
        
        # Count by label
        for tx in transactions:
            # Label distribution
            label = tx.label.value
            if label not in stats["label_distribution"]:
                stats["label_distribution"][label] = 0
            stats["label_distribution"][label] += 1
            
            # Chain distribution
            chain = tx.chain.value
            if chain not in stats["chain_distribution"]:
                stats["chain_distribution"][chain] = 0
            stats["chain_distribution"][chain] += 1
            
            # Token distribution (top 10)
            token = tx.token
            if token not in stats["token_distribution"]:
                stats["token_distribution"][token] = 0
            stats["token_distribution"][token] += 1
            
            # Source distribution
            source = tx.label_source.value
            if source not in stats["source_distribution"]:
                stats["source_distribution"][source] = 0
            stats["source_distribution"][source] += 1
            
            # Validation status
            if tx.is_validated:
                stats["validation_status"]["validated"] += 1
            else:
                stats["validation_status"]["not_validated"] += 1
            
            # Timestamp range
            if tx.timestamp:
                if stats["timestamp_range"]["earliest"] is None or tx.timestamp < stats["timestamp_range"]["earliest"]:
                    stats["timestamp_range"]["earliest"] = tx.timestamp
                    
                if stats["timestamp_range"]["latest"] is None or tx.timestamp > stats["timestamp_range"]["latest"]:
                    stats["timestamp_range"]["latest"] = tx.timestamp
        
        # Confidence statistics
        confidence_values = [tx.label_confidence for tx in transactions]
        if confidence_values:
            stats["confidence_stats"]["mean"] = np.mean(confidence_values)
            stats["confidence_stats"]["median"] = np.median(confidence_values)
            stats["confidence_stats"]["min"] = min(confidence_values)
            stats["confidence_stats"]["max"] = max(confidence_values)
        
        # Limit token distribution to top 10
        stats["token_distribution"] = dict(
            sorted(stats["token_distribution"].items(), key=lambda x: x[1], reverse=True)[:10]
        )
        
        # Convert timestamps to strings for serialization
        if stats["timestamp_range"]["earliest"]:
            stats["timestamp_range"]["earliest"] = stats["timestamp_range"]["earliest"].isoformat()
        if stats["timestamp_range"]["latest"]:
            stats["timestamp_range"]["latest"] = stats["timestamp_range"]["latest"].isoformat()
        
        logger.info(f"Generated statistics: {stats}")
        return stats 
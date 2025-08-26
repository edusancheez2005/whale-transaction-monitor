"""
Training Data Generator Module

This module provides functionality for generating labeled training data
for machine learning models using the rule-based classifier.
"""
import os
import asyncio
import logging
import json
import pandas as pd
from typing import List, Dict, Any, Optional, Union, Set
from datetime import datetime, timedelta
import random
from collections import defaultdict

from address_enrichment import AddressEnrichmentService, ChainType
from rule_engine import RuleEngine, Transaction, AddressMetadata, ClassificationType
from transaction_classifier import TransactionClassifier

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("training_data_generator")

class TrainingDataGenerator:
    """
    Generate labeled training data for transaction classification
    
    This class:
    1. Extracts historical transaction data from various sources
    2. Uses the rule-based classifier to label transactions
    3. Validates and balances the dataset
    4. Exports the data for model training
    """
    
    def __init__(
        self,
        output_dir: str = "output",
        data_dir: str = "data",
        redis_url: Optional[str] = None
    ):
        """
        Initialize the training data generator
        
        Args:
            output_dir: Directory to save training data
            data_dir: Directory containing source data files
            redis_url: Redis connection URL for caching
        """
        self.output_dir = output_dir
        self.data_dir = data_dir
        
        # Create directories if they don't exist
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(data_dir, exist_ok=True)
        
        # Initialize the transaction classifier
        self.classifier = TransactionClassifier(redis_url=redis_url)
        
        # Track processed transaction hashes
        self.processed_tx_hashes: Set[str] = set()
    
    async def extract_from_json(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Extract transactions from a JSON file
        
        Args:
            file_path: Path to the JSON file
            
        Returns:
            List[Dict[str, Any]]: Extracted transactions
        """
        logger.info(f"Extracting transactions from JSON file: {file_path}")
        
        try:
            with open(file_path, "r") as f:
                data = json.load(f)
            
            # Handle different JSON formats
            transactions = []
            
            # If data is a list, process each item
            if isinstance(data, list):
                for item in data:
                    if self._is_valid_transaction(item):
                        transactions.append(item)
            
            # If data has a 'transactions' key, process those
            elif isinstance(data, dict) and "transactions" in data:
                for item in data["transactions"]:
                    if self._is_valid_transaction(item):
                        transactions.append(item)
            
            # Handle Whale Alert format
            elif isinstance(data, dict) and "result" in data and "transactions" in data["result"]:
                for item in data["result"]["transactions"]:
                    tx = self._convert_whale_alert_format(item)
                    if tx:
                        transactions.append(tx)
            
            logger.info(f"Extracted {len(transactions)} valid transactions from {file_path}")
            return transactions
            
        except Exception as e:
            logger.error(f"Error extracting transactions from {file_path}: {e}")
            return []
    
    def _is_valid_transaction(self, item: Dict[str, Any]) -> bool:
        """Check if a transaction dictionary has required fields"""
        required_fields = ["tx_hash", "from_address", "to_address", "chain", "token", "amount"]
        
        if all(field in item for field in required_fields):
            # Skip already processed transactions
            if item["tx_hash"] in self.processed_tx_hashes:
                return False
                
            self.processed_tx_hashes.add(item["tx_hash"])
            return True
            
        return False
    
    def _convert_whale_alert_format(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert Whale Alert format to our transaction format"""
        try:
            # Skip already processed transactions
            if item.get("hash") in self.processed_tx_hashes:
                return None
                
            # Map Whale Alert blockchain to our chain types
            blockchain = item.get("blockchain", "").lower()
            chain_map = {
                "bitcoin": ChainType.BITCOIN.value,
                "ethereum": ChainType.ETHEREUM.value,
                "ripple": ChainType.XRP.value,
                "binance": ChainType.BINANCE.value,
                "bnb": ChainType.BINANCE.value,
                "solana": ChainType.SOLANA.value,
                "polygon": ChainType.POLYGON.value
            }
            
            chain = chain_map.get(blockchain, ChainType.ETHEREUM.value)
            
            # Convert to our format
            tx = {
                "tx_hash": item.get("hash", ""),
                "from_address": item.get("from", {}).get("address", ""),
                "to_address": item.get("to", {}).get("address", ""),
                "chain": chain,
                "token": item.get("symbol", "").upper(),
                "amount": float(item.get("amount", 0)),
                "usd_value": float(item.get("amount_usd", 0)),
                "timestamp": datetime.fromtimestamp(item.get("timestamp", 0)).isoformat()
            }
            
            # Add metadata if available
            from_owner = item.get("from", {}).get("owner", "")
            from_owner_type = item.get("from", {}).get("owner_type", "")
            to_owner = item.get("to", {}).get("owner", "")
            to_owner_type = item.get("to", {}).get("owner_type", "")
            
            if from_owner or from_owner_type or to_owner or to_owner_type:
                tx["metadata"] = {
                    "from_owner": from_owner,
                    "from_owner_type": from_owner_type,
                    "to_owner": to_owner,
                    "to_owner_type": to_owner_type
                }
            
            self.processed_tx_hashes.add(tx["tx_hash"])
            return tx
            
        except Exception as e:
            logger.error(f"Error converting Whale Alert format: {e}")
            return None
    
    async def extract_from_csv(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Extract transactions from a CSV file
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            List[Dict[str, Any]]: Extracted transactions
        """
        logger.info(f"Extracting transactions from CSV file: {file_path}")
        
        try:
            df = pd.read_csv(file_path)
            
            # Process each row
            transactions = []
            for _, row in df.iterrows():
                try:
                    # Convert row to dictionary
                    item = row.to_dict()
                    
                    if self._is_valid_transaction(item):
                        transactions.append(item)
                        
                except Exception as e:
                    logger.error(f"Error processing CSV row: {e}")
            
            logger.info(f"Extracted {len(transactions)} valid transactions from {file_path}")
            return transactions
            
        except Exception as e:
            logger.error(f"Error extracting transactions from {file_path}: {e}")
            return []
    
    async def label_transactions(
        self, 
        transactions: List[Dict[str, Any]],
        min_confidence: float = 0.7
    ) -> List[Dict[str, Any]]:
        """
        Label transactions using the rule-based classifier
        
        Args:
            transactions: Transactions to label
            min_confidence: Minimum confidence threshold
            
        Returns:
            List[Dict[str, Any]]: Labeled transactions
        """
        logger.info(f"Labeling {len(transactions)} transactions")
        
        # Get classifications using the transaction classifier
        classification_results = await self.classifier.classify_transactions(transactions)
        
        # Convert to labeled training data
        labeled_transactions = []
        
        for i, result in enumerate(classification_results):
            # Only include classifications above the confidence threshold
            if result.confidence >= min_confidence:
                # Create labeled transaction record
                labeled_tx = {
                    # Original transaction data
                    "tx_hash": result.transaction.tx_hash,
                    "from_address": result.transaction.from_address,
                    "to_address": result.transaction.to_address,
                    "chain": result.transaction.chain.value,
                    "token": result.transaction.token,
                    "amount": result.transaction.amount,
                    "usd_value": result.transaction.usd_value,
                    "timestamp": result.transaction.timestamp.isoformat() if result.transaction.timestamp else None,
                    "block_number": result.transaction.block_number,
                    
                    # Classification data
                    "label": result.classification.value,
                    "confidence": result.confidence,
                    "rule": result.triggered_rule,
                    "explanation": result.explanation,
                    
                    # Address entity types
                    "from_entity_type": (result.transaction.from_address_metadata.entity_type 
                                        if result.transaction.from_address_metadata else "unknown"),
                    "to_entity_type": (result.transaction.to_address_metadata.entity_type 
                                      if result.transaction.to_address_metadata else "unknown"),
                    
                    # Additional data
                    "processed_at": result.processed_at.isoformat(),
                    "is_validated": False
                }
                
                labeled_transactions.append(labeled_tx)
        
        logger.info(f"Labeled {len(labeled_transactions)} transactions with confidence >= {min_confidence}")
        return labeled_transactions
    
    def balance_dataset(
        self, 
        labeled_transactions: List[Dict[str, Any]],
        target_per_class: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Balance the dataset to have equal representation of classes
        
        Args:
            labeled_transactions: Labeled transactions
            target_per_class: Target number per class (default=use the minimum class count)
            
        Returns:
            List[Dict[str, Any]]: Balanced dataset
        """
        logger.info(f"Balancing dataset of {len(labeled_transactions)} transactions")
        
        # Count transactions by label
        label_counts = defaultdict(int)
        label_to_txs = defaultdict(list)
        
        for tx in labeled_transactions:
            label = tx["label"]
            label_counts[label] += 1
            label_to_txs[label].append(tx)
        
        logger.info(f"Initial class distribution: {dict(label_counts)}")
        
        # Determine target count
        if target_per_class is None:
            # Use the minimum class count
            target_per_class = min(label_counts.values()) if label_counts else 0
        
        # Balance each class
        balanced_dataset = []
        
        for label, transactions in label_to_txs.items():
            # Sort by confidence (highest first)
            sorted_txs = sorted(transactions, key=lambda x: x.get("confidence", 0), reverse=True)
            
            # Take up to target_per_class
            if len(sorted_txs) <= target_per_class:
                # If we have fewer than the target, keep all
                balanced_dataset.extend(sorted_txs)
                logger.info(f"Keeping all {len(sorted_txs)} transactions for class {label}")
            else:
                # Otherwise, take the ones with highest confidence
                balanced_dataset.extend(sorted_txs[:target_per_class])
                logger.info(f"Keeping top {target_per_class} of {len(sorted_txs)} for class {label}")
        
        logger.info(f"Balanced dataset contains {len(balanced_dataset)} transactions")
        return balanced_dataset
    
    def split_dataset(
        self, 
        dataset: List[Dict[str, Any]],
        test_ratio: float = 0.2,
        validation_ratio: float = 0.1,
        random_seed: int = 42
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Split dataset into training, validation, and test sets
        
        Args:
            dataset: The dataset to split
            test_ratio: Ratio of data for testing
            validation_ratio: Ratio of data for validation
            random_seed: Random seed for reproducibility
            
        Returns:
            Dict: Training, validation, and test datasets
        """
        logger.info(f"Splitting dataset of {len(dataset)} transactions")
        
        # Set random seed for reproducibility
        random.seed(random_seed)
        
        # Shuffle the dataset
        shuffled = dataset.copy()
        random.shuffle(shuffled)
        
        # Calculate split indices
        test_size = int(len(shuffled) * test_ratio)
        validation_size = int(len(shuffled) * validation_ratio)
        
        # Split the dataset
        test_set = shuffled[:test_size]
        validation_set = shuffled[test_size:test_size + validation_size]
        training_set = shuffled[test_size + validation_size:]
        
        logger.info(f"Split dataset into training ({len(training_set)}), "
                   f"validation ({len(validation_set)}), and test ({len(test_set)}) sets")
        
        return {
            "training": training_set,
            "validation": validation_set,
            "test": test_set
        }
    
    def save_dataset(self, dataset: List[Dict[str, Any]], file_path: str, format: str = "csv") -> None:
        """
        Save dataset to file
        
        Args:
            dataset: Dataset to save
            file_path: Output file path
            format: Output format (csv or json)
        """
        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        if format.lower() == "csv":
            # Save as CSV
            df = pd.DataFrame(dataset)
            df.to_csv(file_path, index=False)
            logger.info(f"Saved {len(dataset)} transactions to CSV: {file_path}")
            
        elif format.lower() == "json":
            # Save as JSON
            with open(file_path, "w") as f:
                json.dump(dataset, f, indent=2, default=str)
            logger.info(f"Saved {len(dataset)} transactions to JSON: {file_path}")
            
        else:
            logger.error(f"Unsupported format: {format}")
    
    def generate_dataset_stats(self, dataset: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate statistics about the dataset
        
        Args:
            dataset: Dataset to analyze
            
        Returns:
            Dict[str, Any]: Dataset statistics
        """
        # Count transactions by label
        label_counts = defaultdict(int)
        chain_counts = defaultdict(int)
        rule_counts = defaultdict(int)
        
        confidence_values = []
        
        for tx in dataset:
            label_counts[tx["label"]] += 1
            chain_counts[tx["chain"]] += 1
            rule_counts[tx["rule"]] += 1
            confidence_values.append(tx.get("confidence", 0))
        
        # Calculate confidence statistics
        avg_confidence = sum(confidence_values) / len(confidence_values) if confidence_values else 0
        min_confidence = min(confidence_values) if confidence_values else 0
        max_confidence = max(confidence_values) if confidence_values else 0
        
        return {
            "total_transactions": len(dataset),
            "label_distribution": dict(label_counts),
            "chain_distribution": dict(chain_counts),
            "rule_distribution": dict(rule_counts),
            "confidence_stats": {
                "average": avg_confidence,
                "min": min_confidence,
                "max": max_confidence
            }
        }
    
    async def generate_training_data(
        self,
        input_files: List[str],
        min_confidence: float = 0.7,
        target_per_class: Optional[int] = None,
        test_ratio: float = 0.2,
        validation_ratio: float = 0.1
    ) -> Dict[str, Any]:
        """
        Generate training data from input files
        
        Args:
            input_files: List of input files (JSON or CSV)
            min_confidence: Minimum confidence for labels
            target_per_class: Target number of examples per class
            test_ratio: Ratio of data for testing
            validation_ratio: Ratio of data for validation
            
        Returns:
            Dict[str, Any]: Dataset statistics and file paths
        """
        # Extract transactions from all input files
        all_transactions = []
        
        for file_path in input_files:
            if file_path.lower().endswith(".json"):
                transactions = await self.extract_from_json(file_path)
            elif file_path.lower().endswith(".csv"):
                transactions = await self.extract_from_csv(file_path)
            else:
                logger.warning(f"Unsupported file format: {file_path}")
                continue
                
            all_transactions.extend(transactions)
        
        logger.info(f"Extracted total of {len(all_transactions)} transactions from {len(input_files)} files")
        
        # Label transactions
        labeled_transactions = await self.label_transactions(
            all_transactions,
            min_confidence=min_confidence
        )
        
        # Balance dataset
        balanced_dataset = self.balance_dataset(
            labeled_transactions,
            target_per_class=target_per_class
        )
        
        # Split dataset
        dataset_splits = self.split_dataset(
            balanced_dataset,
            test_ratio=test_ratio,
            validation_ratio=validation_ratio
        )
        
        # Save datasets
        output_files = {}
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for split_name, split_data in dataset_splits.items():
            file_path = os.path.join(self.output_dir, f"{split_name}_{timestamp}.csv")
            self.save_dataset(split_data, file_path, format="csv")
            output_files[split_name] = file_path
        
        # Save full dataset as JSON for reference
        full_dataset_path = os.path.join(self.output_dir, f"full_dataset_{timestamp}.json")
        self.save_dataset(balanced_dataset, full_dataset_path, format="json")
        output_files["full"] = full_dataset_path
        
        # Generate and save statistics
        stats = {}
        for split_name, split_data in dataset_splits.items():
            stats[split_name] = self.generate_dataset_stats(split_data)
        
        stats_path = os.path.join(self.output_dir, f"stats_{timestamp}.json")
        with open(stats_path, "w") as f:
            json.dump(stats, f, indent=2)
        
        logger.info(f"Training data generation complete. Files saved to {self.output_dir}")
        
        return {
            "statistics": stats,
            "files": output_files
        }
    
    async def close(self):
        """Close resources"""
        await self.classifier.close()

# Usage example
async def main():
    """Example usage of the training data generator"""
    # Create the generator
    generator = TrainingDataGenerator()
    
    # Generate training data
    input_files = [
        "data/sample_transactions.json",
        "data/whale_alert_transactions.json"
    ]
    
    try:
        # Generate dummy data if real data doesn't exist
        if not os.path.exists(input_files[0]):
            os.makedirs("data", exist_ok=True)
            # Create dummy data
            dummy_transactions = []
            for i in range(100):
                tx = {
                    "tx_hash": f"0x{i:064x}",
                    "from_address": f"0x{i+1:040x}",
                    "to_address": f"0x{i+2:040x}",
                    "chain": "ethereum",
                    "token": "ETH",
                    "amount": 1.0 + (i / 10),
                    "usd_value": 1800.0 + (i * 10),
                    "timestamp": datetime.now().isoformat()
                }
                dummy_transactions.append(tx)
                
            with open(input_files[0], "w") as f:
                json.dump(dummy_transactions, f, indent=2, default=str)
            
            logger.info(f"Created dummy data file: {input_files[0]}")
            
        result = await generator.generate_training_data(
            input_files=input_files,
            min_confidence=0.7,
            target_per_class=50
        )
        
        # Print statistics
        print("\nTraining Data Statistics:")
        for split_name, stats in result["statistics"].items():
            print(f"\n{split_name.upper()} Set:")
            print(f"Total transactions: {stats['total_transactions']}")
            print(f"Label distribution: {stats['label_distribution']}")
            print(f"Average confidence: {stats['confidence_stats']['average']:.2f}")
        
        print("\nOutput files:")
        for split_name, file_path in result["files"].items():
            print(f"{split_name}: {file_path}")
            
    finally:
        # Close resources
        await generator.close()

if __name__ == "__main__":
    asyncio.run(main()) 
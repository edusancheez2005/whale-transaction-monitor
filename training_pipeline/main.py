"""
Training Data Generation Pipeline Main Script

This script orchestrates the entire training data generation pipeline.
"""
import os
import argparse
import logging
import asyncio
import json
import sys

# Add parent directory to path to make imports work when run as a script
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from datetime import datetime, timedelta
from typing import List, Dict, Any

from training_pipeline.models import RawTransaction, LabeledTransaction, ChainType
from training_pipeline.extraction.historical_data import HistoricalDataExtractor
from training_pipeline.labeling.rule_labeler import RuleBasedLabeler, WhaleAlertLabeler
from training_pipeline.validation.dataset_validator import DatasetValidator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("training_pipeline")


async def run_pipeline(args):
    """
    Run the full training data generation pipeline
    
    Args:
        args: Command line arguments
    """
    logger.info("Starting training data generation pipeline")
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Initialize components
    extractor = HistoricalDataExtractor(data_dir=args.data_dir)
    rule_labeler = RuleBasedLabeler(rule_engine_url=args.rule_engine_url)
    whale_labeler = WhaleAlertLabeler()
    validator = DatasetValidator(output_dir=args.output_dir)
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=args.days)
    
    logger.info(f"Extracting data from {start_date} to {end_date}")
    
    # Extract raw transactions from various sources
    raw_transactions = []
    
    # Extract from database
    db_transactions = await extractor.extract_from_database(
        start_date=start_date,
        end_date=end_date,
        chains=[ChainType(chain) for chain in args.chains]
    )
    raw_transactions.extend(db_transactions)
    
    # Extract from Whale Alert API
    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())
    whale_transactions = await extractor.extract_from_whale_alert(
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        min_value=args.min_value
    )
    raw_transactions.extend(whale_transactions)
    
    # Extract from CSV if provided
    if args.csv_files:
        for csv_file in args.csv_files:
            csv_transactions = await extractor.extract_from_csv(csv_file)
            raw_transactions.extend(csv_transactions)
    
    logger.info(f"Extracted total of {len(raw_transactions)} raw transactions")
    
    # Save raw transactions to JSON
    raw_output_file = os.path.join(args.output_dir, "raw_transactions.json")
    extractor.save_to_json(raw_transactions, raw_output_file)
    
    # Label transactions using rule engine
    rule_labeled_transactions = await rule_labeler.label_transactions(raw_transactions)
    
    # Label transactions using Whale Alert metadata
    whale_labeled_transactions = whale_labeler.label_transactions(
        [tx for tx in raw_transactions if tx.source == "whale_alert"]
    )
    
    # Combine labeled transactions
    all_labeled_transactions = rule_labeled_transactions + whale_labeled_transactions
    logger.info(f"Labeled total of {len(all_labeled_transactions)} transactions")
    
    # Remove duplicates
    deduplicated_transactions = validator.remove_duplicates(all_labeled_transactions)
    
    # Validate data quality
    valid_transactions = validator.validate_data_quality(deduplicated_transactions)
    
    # Generate validation set
    validation_set, training_candidates = validator.generate_validation_set(
        valid_transactions, 
        validation_size=args.validation_size
    )
    
    # Balance classes for training set
    balanced_training_set = validator.balance_classes(
        training_candidates,
        min_confidence=args.min_confidence
    )
    
    # Generate dataset statistics
    all_stats = validator.generate_dataset_statistics(valid_transactions)
    validation_stats = validator.generate_dataset_statistics(validation_set)
    training_stats = validator.generate_dataset_statistics(balanced_training_set)
    
    # Save statistics
    with open(os.path.join(args.output_dir, "dataset_stats.json"), "w") as f:
        json.dump({
            "all_data": all_stats,
            "validation_set": validation_stats,
            "training_set": training_stats
        }, f, indent=2)
    
    # Save final datasets
    validator.save_to_csv(validation_set, os.path.join(args.output_dir, "validation_set.csv"))
    validator.save_to_csv(balanced_training_set, os.path.join(args.output_dir, "training_set.csv"))
    
    # Save full dataset (before balancing)
    validator.save_to_csv(valid_transactions, os.path.join(args.output_dir, "full_dataset.csv"))
    
    logger.info("Training data generation pipeline completed successfully")
    logger.info(f"Generated datasets saved to {args.output_dir}")


def main():
    """Main entry point for the training pipeline"""
    parser = argparse.ArgumentParser(description="Training Data Generation Pipeline")
    
    # Data extraction options
    parser.add_argument("--data-dir", type=str, default="data",
                        help="Directory for input data files")
    parser.add_argument("--output-dir", type=str, default="output",
                        help="Directory for output files")
    parser.add_argument("--days", type=int, default=180,
                        help="Number of days of historical data to extract")
    parser.add_argument("--chains", type=str, nargs="+", 
                        default=["ethereum", "solana", "polygon", "xrp"],
                        help="Chains to extract data for")
    parser.add_argument("--min-value", type=int, default=500000,
                        help="Minimum USD value for transactions")
    parser.add_argument("--csv-files", type=str, nargs="*", default=[],
                        help="Additional CSV files to load")
    
    # Labeling options
    parser.add_argument("--rule-engine-url", type=str, default="http://localhost:8001",
                        help="URL of the rule engine API")
    
    # Validation options
    parser.add_argument("--validation-size", type=int, default=1000,
                        help="Size of validation set")
    parser.add_argument("--min-confidence", type=float, default=0.6,
                        help="Minimum confidence threshold for training data")
    
    args = parser.parse_args()
    
    # Run the pipeline
    asyncio.run(run_pipeline(args))


if __name__ == "__main__":
    main() 
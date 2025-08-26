"""
Historical Data Extraction Module

This module provides functionality for extracting historical transaction data
from various sources for the training data generation pipeline.
"""
import logging
import json
import os
import pandas as pd
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timedelta
import aiohttp
import asyncio
import time

from ..models import RawTransaction, ChainType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HistoricalDataExtractor:
    """
    Extract historical transaction data from various sources
    
    This class handles:
    1. Extracting from local database/logs
    2. Fetching from external APIs
    3. Loading from CSV/JSON files
    """
    
    def __init__(self, data_dir: str = "data"):
        """
        Initialize the historical data extractor
        
        Args:
            data_dir: Directory to store/load data files
        """
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        
        # API keys and endpoints
        self.etherscan_api_key = os.getenv("ETHERSCAN_API_KEY", "")
        self.solscan_api_key = os.getenv("SOLSCAN_API_KEY", "")
        self.whale_alert_api_key = os.getenv("WHALE_ALERT_API_KEY", "")
        
        # Cache to avoid re-processing the same transaction
        self.processed_tx_hashes = set()
        
    async def extract_from_database(
        self, 
        start_date: datetime, 
        end_date: datetime, 
        chains: Optional[List[ChainType]] = None
    ) -> List[RawTransaction]:
        """
        Extract transactions from local database
        
        Args:
            start_date: Start date for extraction
            end_date: End date for extraction
            chains: List of chains to extract from (defaults to all)
            
        Returns:
            List[RawTransaction]: Extracted transactions
        """
        # In a real implementation, this would query a database
        # For demonstration, we'll load from a simulated database file
        
        logger.info(f"Extracting transactions from database ({start_date} to {end_date})")
        
        # Simulated database query - load from JSON file
        try:
            with open(os.path.join(self.data_dir, "simulated_db.json"), "r") as f:
                raw_data = json.load(f)
                
            # Filter by date and chain
            filtered_data = []
            for item in raw_data:
                # Parse timestamp
                tx_timestamp = datetime.fromisoformat(item.get("timestamp", "").replace("Z", "+00:00"))
                
                # Apply filters
                if start_date <= tx_timestamp <= end_date:
                    if chains is None or item.get("chain", "") in [c.value for c in chains]:
                        filtered_data.append(item)
            
            # Convert to RawTransaction objects
            transactions = []
            for item in filtered_data:
                # Skip already processed transactions
                if item.get("tx_hash") in self.processed_tx_hashes:
                    continue
                    
                try:
                    tx = RawTransaction(
                        tx_hash=item.get("tx_hash", ""),
                        from_address=item.get("from_address", ""),
                        to_address=item.get("to_address", ""),
                        chain=item.get("chain", ""),
                        token=item.get("token", ""),
                        amount=float(item.get("amount", 0)),
                        usd_value=float(item.get("usd_value", 0)) if item.get("usd_value") else None,
                        timestamp=tx_timestamp,
                        block_number=int(item.get("block_number", 0)) if item.get("block_number") else None,
                        source="database"
                    )
                    transactions.append(tx)
                    self.processed_tx_hashes.add(tx.tx_hash)
                except Exception as e:
                    logger.error(f"Error converting transaction: {e}, Data: {item}")
            
            logger.info(f"Extracted {len(transactions)} transactions from database")
            return transactions
            
        except FileNotFoundError:
            logger.warning(f"Simulated database file not found. Creating dummy data for demonstration.")
            # For demonstration, create some dummy data
            return self._create_dummy_data(start_date, end_date, chains)
            
        except Exception as e:
            logger.error(f"Error extracting from database: {e}")
            return []
    
    async def extract_from_whale_alert(
        self, 
        start_timestamp: int, 
        end_timestamp: int,
        min_value: int = 500000  # $500k minimum
    ) -> List[RawTransaction]:
        """
        Extract transactions from Whale Alert API
        
        Args:
            start_timestamp: Start timestamp (Unix time)
            end_timestamp: End timestamp (Unix time)
            min_value: Minimum transaction value in USD
            
        Returns:
            List[RawTransaction]: Extracted transactions
        """
        if not self.whale_alert_api_key:
            logger.warning("Whale Alert API key not provided, skipping extraction")
            return []
            
        logger.info(f"Extracting transactions from Whale Alert ({start_timestamp} to {end_timestamp})")
        
        # In a real implementation, this would call the Whale Alert API
        # For demonstration, we'll load from a simulated API response file
        
        try:
            # Simulated API call
            with open(os.path.join(self.data_dir, "simulated_whale_alert.json"), "r") as f:
                raw_data = json.load(f)
                
            # Process the raw data
            transactions = []
            for item in raw_data.get("transactions", []):
                # Skip already processed transactions
                if item.get("hash") in self.processed_tx_hashes:
                    continue
                    
                try:
                    # Map Whale Alert data to our model
                    timestamp = datetime.fromtimestamp(item.get("timestamp", 0))
                    
                    # Determine the chain type (whale alert calls them "blockchain")
                    chain = item.get("blockchain", "").lower()
                    if chain == "bitcoin":
                        chain_type = ChainType.BITCOIN
                    elif chain == "ethereum":
                        chain_type = ChainType.ETHEREUM
                    elif chain == "ripple":
                        chain_type = ChainType.XRP
                    elif chain in ["bsc", "binance"]:
                        chain_type = ChainType.BINANCE
                    elif chain == "solana":
                        chain_type = ChainType.SOLANA
                    else:
                        # Skip unsupported chains
                        continue
                    
                    tx = RawTransaction(
                        tx_hash=item.get("hash", ""),
                        from_address=item.get("from", {}).get("address", ""),
                        to_address=item.get("to", {}).get("address", ""),
                        chain=chain_type,
                        token=item.get("symbol", ""),
                        amount=float(item.get("amount", 0)),
                        usd_value=float(item.get("amount_usd", 0)),
                        timestamp=timestamp,
                        source="whale_alert",
                        metadata={
                            "from_owner_type": item.get("from", {}).get("owner_type", "unknown"),
                            "to_owner_type": item.get("to", {}).get("owner_type", "unknown"),
                            "from_owner": item.get("from", {}).get("owner", ""),
                            "to_owner": item.get("to", {}).get("owner", "")
                        }
                    )
                    transactions.append(tx)
                    self.processed_tx_hashes.add(tx.tx_hash)
                except Exception as e:
                    logger.error(f"Error converting Whale Alert transaction: {e}, Data: {item}")
            
            logger.info(f"Extracted {len(transactions)} transactions from Whale Alert")
            return transactions
            
        except FileNotFoundError:
            logger.warning(f"Simulated Whale Alert file not found. Creating dummy data for demonstration.")
            # For demonstration, create some dummy data
            return self._create_dummy_whale_data(start_timestamp, end_timestamp, min_value)
            
        except Exception as e:
            logger.error(f"Error extracting from Whale Alert: {e}")
            return []
    
    async def extract_from_csv(self, file_path: str) -> List[RawTransaction]:
        """
        Extract transactions from CSV file
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            List[RawTransaction]: Extracted transactions
        """
        logger.info(f"Extracting transactions from CSV: {file_path}")
        
        try:
            # Load CSV file
            df = pd.read_csv(file_path)
            
            # Convert to RawTransaction objects
            transactions = []
            for _, row in df.iterrows():
                try:
                    # Skip already processed transactions
                    if row.get("tx_hash") in self.processed_tx_hashes:
                        continue
                        
                    # Parse timestamp if present
                    timestamp = None
                    if "timestamp" in row and pd.notna(row["timestamp"]):
                        timestamp = datetime.fromisoformat(str(row["timestamp"]).replace("Z", "+00:00"))
                    
                    tx = RawTransaction(
                        tx_hash=row.get("tx_hash", ""),
                        from_address=row.get("from_address", ""),
                        to_address=row.get("to_address", ""),
                        chain=row.get("chain", ""),
                        token=row.get("token", ""),
                        amount=float(row.get("amount", 0)),
                        usd_value=float(row.get("usd_value", 0)) if pd.notna(row.get("usd_value")) else None,
                        timestamp=timestamp,
                        block_number=int(row.get("block_number", 0)) if pd.notna(row.get("block_number")) else None,
                        source=f"csv:{os.path.basename(file_path)}"
                    )
                    transactions.append(tx)
                    self.processed_tx_hashes.add(tx.tx_hash)
                except Exception as e:
                    logger.error(f"Error converting CSV row: {e}, Data: {row}")
            
            logger.info(f"Extracted {len(transactions)} transactions from CSV")
            return transactions
            
        except Exception as e:
            logger.error(f"Error extracting from CSV: {e}")
            return []
    
    def save_to_json(self, transactions: List[RawTransaction], output_file: str) -> None:
        """
        Save extracted transactions to JSON file
        
        Args:
            transactions: List of transactions to save
            output_file: Output file path
        """
        logger.info(f"Saving {len(transactions)} transactions to {output_file}")
        
        try:
            # Convert to dictionaries
            data = [tx.dict() for tx in transactions]
            
            # Ensure the directory exists
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            # Save to file
            with open(output_file, "w") as f:
                json.dump(data, f, indent=2, default=str)
                
            logger.info(f"Successfully saved transactions to {output_file}")
            
        except Exception as e:
            logger.error(f"Error saving transactions to JSON: {e}")
    
    def _create_dummy_data(
        self, 
        start_date: datetime, 
        end_date: datetime, 
        chains: Optional[List[ChainType]] = None
    ) -> List[RawTransaction]:
        """
        Create dummy transaction data for demonstration purposes
        
        Args:
            start_date: Start date
            end_date: End date
            chains: List of chains
            
        Returns:
            List[RawTransaction]: Dummy transactions
        """
        if chains is None:
            chains = [ChainType.ETHEREUM, ChainType.SOLANA]
            
        # Generate random transactions
        transactions = []
        
        # Generate 50 dummy transactions per chain
        for chain in chains:
            for i in range(50):
                # Random timestamp between start and end dates
                days_range = (end_date - start_date).days
                random_days = int(time.time()) % max(1, days_range)
                tx_timestamp = start_date + timedelta(days=random_days)
                
                # Generate a unique hash
                tx_hash = f"{chain.value}-dummy-{i}-{int(time.time())}"
                
                # For Ethereum
                if chain == ChainType.ETHEREUM:
                    # Example addresses for Ethereum
                    from_address = f"0x{i:02d}a1e4b0b57991dbb5ee1e9cc167d0824".lower()
                    to_address = f"0x{i:02d}b2d5c1c68a02f3e5f4e7d6a4b3c2d1e0".lower()
                    token = "ETH" if i % 5 == 0 else ["USDT", "USDC", "WBTC", "LINK", "UNI"][i % 5]
                    block_number = 15000000 + i
                
                # For Solana
                elif chain == ChainType.SOLANA:
                    # Example addresses for Solana
                    from_address = f"So{i:02d}ana1111111111111111111111111111111"
                    to_address = f"So{i:02d}ana2222222222222222222222222222222"
                    token = "SOL" if i % 5 == 0 else ["USDC", "RAY", "SRM", "MNGO", "TULIP"][i % 5]
                    block_number = 125000000 + i
                
                # For other chains, use generic values
                else:
                    from_address = f"generic-from-address-{i}"
                    to_address = f"generic-to-address-{i}"
                    token = f"{chain.value.upper()}-TOKEN-{i % 5}"
                    block_number = 1000000 + i
                
                # Create the transaction
                tx = RawTransaction(
                    tx_hash=tx_hash,
                    from_address=from_address,
                    to_address=to_address,
                    chain=chain,
                    token=token,
                    amount=100 + (i * 10),
                    usd_value=1000 + (i * 100),
                    timestamp=tx_timestamp,
                    block_number=block_number,
                    source="dummy_data"
                )
                
                transactions.append(tx)
                self.processed_tx_hashes.add(tx_hash)
        
        logger.info(f"Created {len(transactions)} dummy transactions")
        return transactions
    
    def _create_dummy_whale_data(
        self, 
        start_timestamp: int, 
        end_timestamp: int,
        min_value: int = 500000
    ) -> List[RawTransaction]:
        """
        Create dummy Whale Alert data for demonstration purposes
        
        Args:
            start_timestamp: Start timestamp
            end_timestamp: End timestamp
            min_value: Minimum transaction value
            
        Returns:
            List[RawTransaction]: Dummy transactions
        """
        transactions = []
        
        # Generate 30 dummy whale transactions
        for i in range(30):
            # Random timestamp between start and end
            random_offset = int(time.time()) % max(1, end_timestamp - start_timestamp)
            tx_timestamp = datetime.fromtimestamp(start_timestamp + random_offset)
            
            # Generate a unique hash
            tx_hash = f"whale-dummy-{i}-{int(time.time())}"
            
            # Select chain
            chain_idx = i % 5
            if chain_idx == 0:
                chain = ChainType.ETHEREUM
                token = ["ETH", "USDT", "USDC"][i % 3]
                from_address = f"0x{i:02d}a1e4b0b57991dbb5ee1e9cc167d0824".lower()
                to_address = f"0x{i:02d}b2d5c1c68a02f3e5f4e7d6a4b3c2d1e0".lower()
            elif chain_idx == 1:
                chain = ChainType.BITCOIN
                token = "BTC"
                from_address = f"bc1q{i:02d}a1e4b0b57991dbb5ee1e9cc167d0824".lower()
                to_address = f"bc1q{i:02d}b2d5c1c68a02f3e5f4e7d6a4b3c2d1e0".lower()
            elif chain_idx == 2:
                chain = ChainType.XRP
                token = "XRP"
                from_address = f"r{i:02d}a1e4b0b57991dbb5ee1e9cc167d0824".lower()
                to_address = f"r{i:02d}b2d5c1c68a02f3e5f4e7d6a4b3c2d1e0".lower()
            elif chain_idx == 3:
                chain = ChainType.SOLANA
                token = "SOL"
                from_address = f"So{i:02d}ana1111111111111111111111111111111"
                to_address = f"So{i:02d}ana2222222222222222222222222222222"
            else:
                chain = ChainType.BINANCE
                token = ["BNB", "BUSD"][i % 2]
                from_address = f"bnb1{i:02d}a1e4b0b57991dbb5ee1e9cc167d08".lower()
                to_address = f"bnb1{i:02d}b2d5c1c68a02f3e5f4e7d6a4b3c2d1".lower()
            
            # Decide on owner types to simulate exchanges
            from_owner_type = "unknown"
            to_owner_type = "unknown"
            
            # Every third transaction involves an exchange
            if i % 3 == 0:
                to_owner_type = "exchange"
            elif i % 3 == 1:
                from_owner_type = "exchange"
            
            # Create the transaction
            tx = RawTransaction(
                tx_hash=tx_hash,
                from_address=from_address,
                to_address=to_address,
                chain=chain,
                token=token,
                amount=10 + (i * 5),
                usd_value=min_value + (i * 100000),
                timestamp=tx_timestamp,
                source="whale_alert",
                metadata={
                    "from_owner_type": from_owner_type,
                    "to_owner_type": to_owner_type,
                    "from_owner": f"Entity-{i % 10}" if from_owner_type == "exchange" else "",
                    "to_owner": f"Entity-{(i + 5) % 10}" if to_owner_type == "exchange" else ""
                }
            )
            
            transactions.append(tx)
            self.processed_tx_hashes.add(tx_hash)
        
        logger.info(f"Created {len(transactions)} dummy Whale Alert transactions")
        return transactions 
"""
BigQuery Public Data Extractor - Comprehensive Whale Discovery Enhancement

This module extracts blockchain address data from Google BigQuery's public datasets
with advanced SQL query patterns for identifying whales across multiple chains.

Enhanced with:
- High balance whale detection (>$1M USD)
- High volume transaction analysis (>10 txs of $100K+ USD)
- Recent activity filtering (last 90 days)
- Cross-chain whale analysis
- Confidence scoring based on multiple criteria

Author: Whale Discovery System  
Version: 4.0.0 (Comprehensive Whale Discovery)
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from google.cloud import bigquery
import asyncio

# Import the AddressData class from api_integrations
from .api_integrations import AddressData

# Configure logging
logger = logging.getLogger(__name__)


class ComprehensiveWhaleDetector:
    """Enhanced whale detection with multi-criteria analysis and confidence scoring."""
    
    # Token contract addresses for major tokens (for value calculation)
    MAJOR_TOKENS = {
        'ethereum': {
            'USDT': '0xdAC17F958D2ee523a2206206994597C13D831ec7',
            'USDC': '0xA0b86a33E6441e6C7d3E4081f7567b0b2b2b8b0a',
            'WBTC': '0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599',
            'DAI': '0x6B175474E89094C44Da98b954EedeAC495271d0F',
            'UNI': '0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984'
        },
        'polygon': {
            'USDT': '0xc2132D05D31c914a87C6611C10748AEb04B58e8F',
            'USDC': '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174',
            'WMATIC': '0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270'
        }
    }
    
    # ETH/USD price cache (simplified - in production should use real price feeds)
    APPROXIMATE_PRICES = {
        'ETH': 3000,  # ~$3000 USD
        'BTC': 45000,  # ~$45000 USD
        'MATIC': 0.8,  # ~$0.80 USD
        'SOL': 100,  # ~$100 USD
        'AVAX': 35,  # ~$35 USD
        'ARB': 1.2,  # ~$1.20 USD
        'OP': 2.5   # ~$2.50 USD
    }
    
    @staticmethod
    def calculate_confidence_score(balance_usd: float, tx_count: int, max_tx_usd: float, 
                                 days_since_last_activity: int, cross_chain_presence: bool = False) -> float:
        """
        Calculate confidence score based on multiple whale criteria.
        
        Scoring factors:
        - Balance magnitude (0.0-0.4)
        - Transaction frequency (0.0-0.2) 
        - Transaction size (0.0-0.2)
        - Recent activity (0.0-0.1)
        - Cross-chain presence bonus (0.0-0.1)
        
        Returns: Score between 0.7-1.0 for whale candidates
        """
        score = 0.7  # Base score for meeting minimum criteria
        
        # Balance magnitude score (up to 0.3 points)
        if balance_usd >= 10_000_000:  # $10M+
            score += 0.3
        elif balance_usd >= 5_000_000:  # $5M+
            score += 0.25
        elif balance_usd >= 2_000_000:  # $2M+
            score += 0.15
        elif balance_usd >= 1_000_000:  # $1M+
            score += 0.1
        
        # Transaction frequency score (up to 0.1 points)
        if tx_count >= 1000:
            score += 0.1
        elif tx_count >= 500:
            score += 0.08
        elif tx_count >= 100:
            score += 0.05
        elif tx_count >= 50:
            score += 0.03
        
        # Transaction size score (up to 0.1 points)
        if max_tx_usd >= 10_000_000:  # $10M+ single tx
            score += 0.1
        elif max_tx_usd >= 1_000_000:  # $1M+ single tx
            score += 0.08
        elif max_tx_usd >= 500_000:  # $500K+ single tx
            score += 0.05
        elif max_tx_usd >= 100_000:  # $100K+ single tx
            score += 0.03
        
        # Recent activity score (up to 0.05 points)
        if days_since_last_activity <= 7:
            score += 0.05
        elif days_since_last_activity <= 30:
            score += 0.03
        elif days_since_last_activity <= 90:
            score += 0.02
        
        # Cross-chain presence bonus (up to 0.05 points)
        if cross_chain_presence:
            score += 0.05
        
        return min(1.0, score)


class BigQueryPublicDatasetExtractorBase(ABC):
    """Base class for extracting addresses from BigQuery public datasets."""
    
    def __init__(self, bigquery_client: bigquery.Client, project_id: str):
        self.bigquery_client = bigquery_client
        self.project_id = project_id
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.whale_detector = ComprehensiveWhaleDetector()
    
    def _execute_query(self, query: str, job_config: Optional[bigquery.QueryJobConfig] = None) -> List[Dict]:
        """Execute a BigQuery SQL query and return results as a list of dictionaries."""
        try:
            self.logger.info(f"Executing BigQuery query: {query[:200]}...")
            
            if job_config is None:
                job_config = bigquery.QueryJobConfig()
            
            query_job = self.bigquery_client.query(query, job_config=job_config)
            results = query_job.result()
            
            # Convert results to list of dictionaries
            rows = [dict(row) for row in results]
            
            self.logger.info(f"Query executed successfully, returned {len(rows)} rows")
            return rows
            
        except Exception as e:
            self.logger.error(f"Failed to execute BigQuery query: {e}")
            self.logger.error(f"Query: {query}")
            raise
    
    @abstractmethod
    def extract_addresses(self, limit_per_query: int = 5000) -> List[AddressData]:
        """Extract addresses from the public dataset. Must be implemented by subclasses."""
        pass
    
    def extract_whale_addresses(self, limit_per_query: int = 5000) -> List[AddressData]:
        """Extract whale addresses using comprehensive criteria."""
        whale_addresses = []
        
        # Get high balance whales
        high_balance_whales = self._get_high_balance_whales(limit_per_query)
        whale_addresses.extend(high_balance_whales)
        
        # Get high volume transaction whales
        high_volume_whales = self._get_high_volume_whales(limit_per_query)
        whale_addresses.extend(high_volume_whales)
        
        # Get recently active whales
        recent_active_whales = self._get_recent_active_whales(limit_per_query)
        whale_addresses.extend(recent_active_whales)
        
        return whale_addresses


class EthereumPublicDataExtractor(BigQueryPublicDatasetExtractorBase):
    """Enhanced Ethereum whale extractor with comprehensive detection criteria."""
    
    def extract_addresses(self, limit_per_query: int = 5000) -> List[AddressData]:
        """Extract whale addresses from Ethereum public dataset using enhanced criteria."""
        return self.extract_whale_addresses(limit_per_query)
    
    def _get_high_balance_whales(self, limit: int) -> List[AddressData]:
        """Get Ethereum addresses with current balance > $1M USD."""
        addresses = []
        
        try:
            # Calculate current ETH price for USD conversion
            eth_price_usd = self.whale_detector.APPROXIMATE_PRICES['ETH']
            min_eth_balance = 1_000_000 / eth_price_usd  # ~333 ETH for $1M
            
            query = f"""
            WITH current_balances AS (
                SELECT 
                    address,
                    SUM(CAST(value AS NUMERIC) / 1e18) as total_eth_balance,
                    COUNT(*) as tx_count,
                    MAX(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), block_timestamp, DAY)) as days_since_last_activity
                FROM (
                    -- Incoming transactions
                    SELECT 
                        to_address as address, 
                        value, 
                        block_timestamp
                    FROM `bigquery-public-data.crypto_ethereum.transactions`
                    WHERE to_address IS NOT NULL 
                      AND to_address != ''
                      AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 730 DAY)
                    
                    UNION ALL
                    
                    -- Outgoing transactions (negative value)
                    SELECT 
                        from_address as address, 
                        -CAST(value AS NUMERIC) as value,
                        block_timestamp
                    FROM `bigquery-public-data.crypto_ethereum.transactions`
                    WHERE from_address IS NOT NULL 
                      AND from_address != ''
                      AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 730 DAY)
                )
                GROUP BY address
                HAVING total_eth_balance >= {min_eth_balance}
                  AND days_since_last_activity <= 90  -- Active in last 90 days
            ),
            whale_candidates AS (
                SELECT 
                    cb.*,
                    cb.total_eth_balance * {eth_price_usd} as balance_usd,
                    -- Get max transaction size
                    (SELECT MAX(CAST(value AS NUMERIC) / 1e18 * {eth_price_usd})
                     FROM `bigquery-public-data.crypto_ethereum.transactions` t
                     WHERE (t.from_address = cb.address OR t.to_address = cb.address)
                       AND t.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
                    ) as max_tx_usd
                FROM current_balances cb
            )
            SELECT 
                address,
                total_eth_balance,
                balance_usd,
                tx_count,
                days_since_last_activity,
                max_tx_usd
            FROM whale_candidates
            WHERE balance_usd >= 1000000  -- Minimum $1M USD
            ORDER BY balance_usd DESC
            LIMIT {limit}
            """
            
            results = self._execute_query(query)
            
            for row in results:
                confidence_score = self.whale_detector.calculate_confidence_score(
                    balance_usd=float(row['balance_usd'] or 0),
                    tx_count=int(row['tx_count'] or 0),
                    max_tx_usd=float(row['max_tx_usd'] or 0),
                    days_since_last_activity=int(row['days_since_last_activity'] or 999)
                )
                
                addresses.append(AddressData(
                    address=row['address'],
                    blockchain='ethereum',
                    source_system='BigQuery-Ethereum',
                    initial_label='whale',
                    metadata={
                        'balance_eth': float(row['total_eth_balance'] or 0),
                        'balance_usd': float(row['balance_usd'] or 0),
                        'tx_count': int(row['tx_count'] or 0),
                        'max_tx_usd': float(row['max_tx_usd'] or 0),
                        'days_since_last_activity': int(row['days_since_last_activity'] or 999),
                        'detection_method': 'high_balance',
                        'last_active': (datetime.utcnow() - timedelta(days=int(row['days_since_last_activity'] or 999))).isoformat()
                    },
                    confidence_score=confidence_score
                ))
            
            self.logger.info(f"Extracted {len(addresses)} high-balance Ethereum whales")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to get high-balance Ethereum whales: {e}")
            return []
    
    def _get_high_volume_whales(self, limit: int) -> List[AddressData]:
        """Get addresses involved in >10 transactions ≥ $100K USD."""
        addresses = []
        
        try:
            eth_price_usd = self.whale_detector.APPROXIMATE_PRICES['ETH']
            min_tx_eth = 100_000 / eth_price_usd  # ~33 ETH for $100K
            
            query = f"""
            WITH high_value_tx_addresses AS (
                SELECT 
                    address,
                    COUNT(*) as high_value_tx_count,
                    SUM(tx_value_usd) as total_volume_usd,
                    MAX(tx_value_usd) as max_tx_usd,
                    MAX(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), block_timestamp, DAY)) as days_since_last_activity
                FROM (
                    -- From addresses (senders)
                    SELECT 
                        from_address as address,
                        CAST(value AS NUMERIC) / 1e18 * {eth_price_usd} as tx_value_usd,
                        block_timestamp
                    FROM `bigquery-public-data.crypto_ethereum.transactions`
                    WHERE from_address IS NOT NULL 
                      AND from_address != ''
                      AND CAST(value AS NUMERIC) / 1e18 >= {min_tx_eth}
                      AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
                    
                    UNION ALL
                    
                    -- To addresses (receivers)
                    SELECT 
                        to_address as address,
                        CAST(value AS NUMERIC) / 1e18 * {eth_price_usd} as tx_value_usd,
                        block_timestamp
                    FROM `bigquery-public-data.crypto_ethereum.transactions`
                    WHERE to_address IS NOT NULL 
                      AND to_address != ''
                      AND CAST(value AS NUMERIC) / 1e18 >= {min_tx_eth}
                      AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
                )
                GROUP BY address
                HAVING high_value_tx_count >= 10  -- Minimum 10 high-value transactions
                  AND total_volume_usd >= 1000000  -- Minimum $1M total volume
                  AND days_since_last_activity <= 90  -- Active in last 90 days
            )
            SELECT 
                address,
                high_value_tx_count as tx_count,
                total_volume_usd,
                max_tx_usd,
                days_since_last_activity
            FROM high_value_tx_addresses
            ORDER BY total_volume_usd DESC
            LIMIT {limit}
            """
            
            results = self._execute_query(query)
            
            for row in results:
                confidence_score = self.whale_detector.calculate_confidence_score(
                    balance_usd=float(row['total_volume_usd'] or 0) * 0.1,  # Estimate balance as 10% of volume
                    tx_count=int(row['tx_count'] or 0),
                    max_tx_usd=float(row['max_tx_usd'] or 0),
                    days_since_last_activity=int(row['days_since_last_activity'] or 999)
                )
                
                addresses.append(AddressData(
                    address=row['address'],
                    blockchain='ethereum',
                    source_system='BigQuery-Ethereum',
                    initial_label='whale',
                    metadata={
                        'tx_volume_usd': float(row['total_volume_usd'] or 0),
                        'tx_count': int(row['tx_count'] or 0),
                        'max_tx_usd': float(row['max_tx_usd'] or 0),
                        'days_since_last_activity': int(row['days_since_last_activity'] or 999),
                        'detection_method': 'high_volume',
                        'last_active': (datetime.utcnow() - timedelta(days=int(row['days_since_last_activity'] or 999))).isoformat()
                    },
                    confidence_score=confidence_score
                ))
            
            self.logger.info(f"Extracted {len(addresses)} high-volume Ethereum whales")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to get high-volume Ethereum whales: {e}")
            return []
    
    def _get_recent_active_whales(self, limit: int) -> List[AddressData]:
        """Get recently active addresses with significant value movement."""
        addresses = []
        
        try:
            eth_price_usd = self.whale_detector.APPROXIMATE_PRICES['ETH']
            
            query = f"""
            WITH recent_whale_activity AS (
                SELECT 
                    address,
                    COUNT(*) as recent_tx_count,
                    SUM(tx_value_usd) as recent_volume_usd,
                    MAX(tx_value_usd) as max_recent_tx_usd,
                    MIN(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), block_timestamp, DAY)) as days_since_last_activity
                FROM (
                    SELECT 
                        from_address as address,
                        CAST(value AS NUMERIC) / 1e18 * {eth_price_usd} as tx_value_usd,
                        block_timestamp
                    FROM `bigquery-public-data.crypto_ethereum.transactions`
                    WHERE from_address IS NOT NULL 
                      AND from_address != ''
                      AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
                      AND CAST(value AS NUMERIC) / 1e18 > 1  -- > 1 ETH transactions
                    
                    UNION ALL
                    
                    SELECT 
                        to_address as address,
                        CAST(value AS NUMERIC) / 1e18 * {eth_price_usd} as tx_value_usd,
                        block_timestamp
                    FROM `bigquery-public-data.crypto_ethereum.transactions`
                    WHERE to_address IS NOT NULL 
                      AND to_address != ''
                      AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
                      AND CAST(value AS NUMERIC) / 1e18 > 1  -- > 1 ETH transactions
                )
                GROUP BY address
                HAVING recent_volume_usd >= 500000  -- Minimum $500K recent volume
                  AND recent_tx_count >= 20  -- Minimum 20 recent transactions
                  AND days_since_last_activity <= 7  -- Active in last week
            )
            SELECT 
                address,
                recent_tx_count as tx_count,
                recent_volume_usd,
                max_recent_tx_usd,
                days_since_last_activity
            FROM recent_whale_activity
            ORDER BY recent_volume_usd DESC
            LIMIT {limit}
            """
            
            results = self._execute_query(query)
            
            for row in results:
                confidence_score = self.whale_detector.calculate_confidence_score(
                    balance_usd=float(row['recent_volume_usd'] or 0) * 0.2,  # Estimate balance as 20% of recent volume
                    tx_count=int(row['tx_count'] or 0),
                    max_tx_usd=float(row['max_recent_tx_usd'] or 0),
                    days_since_last_activity=int(row['days_since_last_activity'] or 999)
                )
                
                addresses.append(AddressData(
                    address=row['address'],
                    blockchain='ethereum',
                    source_system='BigQuery-Ethereum',
                    initial_label='whale',
                    metadata={
                        'recent_volume_usd': float(row['recent_volume_usd'] or 0),
                        'tx_count': int(row['tx_count'] or 0),
                        'max_recent_tx_usd': float(row['max_recent_tx_usd'] or 0),
                        'days_since_last_activity': int(row['days_since_last_activity'] or 999),
                        'detection_method': 'recent_activity',
                        'last_active': (datetime.utcnow() - timedelta(days=int(row['days_since_last_activity'] or 999))).isoformat()
                    },
                    confidence_score=confidence_score
                ))
            
            self.logger.info(f"Extracted {len(addresses)} recently active Ethereum whales")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to get recently active Ethereum whales: {e}")
            return []


class SolanaPublicDataExtractor(BigQueryPublicDatasetExtractorBase):
    """Solana whale extractor using Flipside BigQuery data."""
    
    def extract_addresses(self, limit_per_query: int = 5000) -> List[AddressData]:
        """Extract whale addresses from Solana using Flipside dataset."""
        addresses = []
        
        try:
            sol_price_usd = self.whale_detector.APPROXIMATE_PRICES['SOL']
            min_sol_balance = 1_000_000 / sol_price_usd  # ~10,000 SOL for $1M
            
            query = f"""
            WITH solana_balances AS (
                SELECT 
                    account,
                    SUM(amount) / 1e9 as total_sol_balance,
                    COUNT(*) as tx_count,
                    MAX(block_timestamp) as last_activity
                FROM `flipside.crypto_solana.transactions`
                WHERE block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
                  AND account IS NOT NULL
                GROUP BY account
                HAVING total_sol_balance >= {min_sol_balance}
                  AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_activity, DAY) <= 90
            )
            SELECT 
                account as address,
                total_sol_balance,
                total_sol_balance * {sol_price_usd} as balance_usd,
                tx_count,
                TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_activity, DAY) as days_since_last_activity
            FROM solana_balances
            WHERE total_sol_balance * {sol_price_usd} >= 1000000
            ORDER BY balance_usd DESC
            LIMIT {limit_per_query}
            """
            
            results = self._execute_query(query)
            
            for row in results:
                confidence_score = self.whale_detector.calculate_confidence_score(
                    balance_usd=row['balance_usd'],
                    tx_count=row['tx_count'],
                    max_tx_usd=row['balance_usd'] * 0.1,  # Estimate max tx as 10% of balance
                    days_since_last_activity=row['days_since_last_activity']
                )
                
                addresses.append(AddressData(
                    address=row['address'],
                    blockchain='solana',
                    source_system='BigQuery-Solana-Flipside',
                    initial_label='whale',
                    confidence_score=confidence_score,
                    metadata={
                        'balance_sol': row['total_sol_balance'],
                        'balance_usd': row['balance_usd'],
                        'tx_count': row['tx_count'],
                        'days_since_last_activity': row['days_since_last_activity'],
                        'detection_method': 'high_balance_solana'
                    }
                ))
            
            self.logger.info(f"Extracted {len(addresses)} Solana whale addresses")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to extract Solana whale addresses: {e}")
            return []


class PolygonPublicDataExtractor(BigQueryPublicDatasetExtractorBase):
    """Polygon whale extractor using public BigQuery data."""
    
    def extract_addresses(self, limit_per_query: int = 5000) -> List[AddressData]:
        """Extract whale addresses from Polygon dataset."""
        addresses = []
        
        try:
            matic_price_usd = self.whale_detector.APPROXIMATE_PRICES['MATIC']
            min_matic_balance = 1_000_000 / matic_price_usd  # ~1.25M MATIC for $1M
            
            query = f"""
            WITH polygon_balances AS (
                SELECT 
                    address,
                    SUM(CAST(value AS NUMERIC) / 1e18) as total_matic_balance,
                    COUNT(*) as tx_count,
                    MAX(block_timestamp) as last_activity
                FROM (
                    SELECT 
                        to_address as address, 
                        value, 
                        block_timestamp
                    FROM `bigquery-public-data.crypto_polygon.transactions`
                    WHERE to_address IS NOT NULL 
                      AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
                    
                    UNION ALL
                    
                    SELECT 
                        from_address as address, 
                        -CAST(value AS NUMERIC) as value,
                        block_timestamp
                    FROM `bigquery-public-data.crypto_polygon.transactions`
                    WHERE from_address IS NOT NULL 
                      AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
                )
                GROUP BY address
                HAVING total_matic_balance >= {min_matic_balance}
                  AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_activity, DAY) <= 90
            )
            SELECT 
                address,
                total_matic_balance,
                total_matic_balance * {matic_price_usd} as balance_usd,
                tx_count,
                TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_activity, DAY) as days_since_last_activity
            FROM polygon_balances
            WHERE total_matic_balance * {matic_price_usd} >= 1000000
            ORDER BY balance_usd DESC
            LIMIT {limit_per_query}
            """
            
            results = self._execute_query(query)
            
            for row in results:
                confidence_score = self.whale_detector.calculate_confidence_score(
                    balance_usd=row['balance_usd'],
                    tx_count=row['tx_count'],
                    max_tx_usd=row['balance_usd'] * 0.1,
                    days_since_last_activity=row['days_since_last_activity']
                )
                
                addresses.append(AddressData(
                    address=row['address'],
                    blockchain='polygon',
                    source_system='BigQuery-Polygon',
                    initial_label='whale',
                    confidence_score=confidence_score,
                    metadata={
                        'balance_matic': row['total_matic_balance'],
                        'balance_usd': row['balance_usd'],
                        'tx_count': row['tx_count'],
                        'days_since_last_activity': row['days_since_last_activity'],
                        'detection_method': 'high_balance_polygon'
                    }
                ))
            
            self.logger.info(f"Extracted {len(addresses)} Polygon whale addresses")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to extract Polygon whale addresses: {e}")
            return []


class AvalanchePublicDataExtractor(BigQueryPublicDatasetExtractorBase):
    """Avalanche whale extractor using public BigQuery data."""
    
    def extract_addresses(self, limit_per_query: int = 5000) -> List[AddressData]:
        """Extract whale addresses from Avalanche dataset."""
        addresses = []
        
        try:
            avax_price_usd = self.whale_detector.APPROXIMATE_PRICES['AVAX']
            min_avax_balance = 1_000_000 / avax_price_usd  # ~28,571 AVAX for $1M
            
            # Try different dataset name variations
            dataset_names = [
                'bigquery-public-data.crypto_avalanche_c',
                'bigquery-public-data.crypto_avalanche',
                'public-datasets.avalanche'
            ]
            
            for dataset_name in dataset_names:
                try:
                    query = f"""
                    WITH avalanche_balances AS (
                        SELECT 
                            address,
                            SUM(CAST(value AS NUMERIC) / 1e18) as total_avax_balance,
                            COUNT(*) as tx_count,
                            MAX(block_timestamp) as last_activity
                        FROM (
                            SELECT 
                                to_address as address, 
                                value, 
                                block_timestamp
                            FROM `{dataset_name}.transactions`
                            WHERE to_address IS NOT NULL 
                              AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
                            
                            UNION ALL
                            
                            SELECT 
                                from_address as address, 
                                -CAST(value AS NUMERIC) as value,
                                block_timestamp
                            FROM `{dataset_name}.transactions`
                            WHERE from_address IS NOT NULL 
                              AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
                        )
                        GROUP BY address
                        HAVING total_avax_balance >= {min_avax_balance}
                          AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_activity, DAY) <= 90
                    )
                    SELECT 
                        address,
                        total_avax_balance,
                        total_avax_balance * {avax_price_usd} as balance_usd,
                        tx_count,
                        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_activity, DAY) as days_since_last_activity
                    FROM avalanche_balances
                    WHERE total_avax_balance * {avax_price_usd} >= 1000000
                    ORDER BY balance_usd DESC
                    LIMIT {limit_per_query}
                    """
                    
                    results = self._execute_query(query)
                    
                    for row in results:
                        confidence_score = self.whale_detector.calculate_confidence_score(
                            balance_usd=row['balance_usd'],
                            tx_count=row['tx_count'],
                            max_tx_usd=row['balance_usd'] * 0.1,
                            days_since_last_activity=row['days_since_last_activity']
                        )
                        
                        addresses.append(AddressData(
                            address=row['address'],
                            blockchain='avalanche',
                            source_system=f'BigQuery-Avalanche-{dataset_name.split(".")[-1]}',
                            initial_label='whale',
                            confidence_score=confidence_score,
                            metadata={
                                'balance_avax': row['total_avax_balance'],
                                'balance_usd': row['balance_usd'],
                                'tx_count': row['tx_count'],
                                'days_since_last_activity': row['days_since_last_activity'],
                                'detection_method': 'high_balance_avalanche',
                                'dataset_used': dataset_name
                            }
                        ))
                    
                    self.logger.info(f"Extracted {len(addresses)} Avalanche whale addresses from {dataset_name}")
                    break  # Successfully found working dataset
                    
                except Exception as dataset_error:
                    self.logger.debug(f"Dataset {dataset_name} not available: {dataset_error}")
                    continue
            
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to extract Avalanche whale addresses: {e}")
            return []


class ArbitrumPublicDataExtractor(BigQueryPublicDatasetExtractorBase):
    """Arbitrum whale extractor using public BigQuery data."""
    
    def extract_addresses(self, limit_per_query: int = 5000) -> List[AddressData]:
        """Extract whale addresses from Arbitrum dataset."""
        addresses = []
        
        try:
            # Arbitrum uses ETH as native token
            eth_price_usd = self.whale_detector.APPROXIMATE_PRICES['ETH']
            min_eth_balance = 1_000_000 / eth_price_usd  # ~333 ETH for $1M
            
            query = f"""
            WITH arbitrum_balances AS (
                SELECT 
                    address,
                    SUM(CAST(value AS NUMERIC) / 1e18) as total_eth_balance,
                    COUNT(*) as tx_count,
                    MAX(block_timestamp) as last_activity
                FROM (
                    SELECT 
                        to_address as address, 
                        value, 
                        block_timestamp
                    FROM `bigquery-public-data.crypto_arbitrum.transactions`
                    WHERE to_address IS NOT NULL 
                      AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
                    
                    UNION ALL
                    
                    SELECT 
                        from_address as address, 
                        -CAST(value AS NUMERIC) as value,
                        block_timestamp
                    FROM `bigquery-public-data.crypto_arbitrum.transactions`
                    WHERE from_address IS NOT NULL 
                      AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
                )
                GROUP BY address
                HAVING total_eth_balance >= {min_eth_balance}
                  AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_activity, DAY) <= 90
            )
            SELECT 
                address,
                total_eth_balance,
                total_eth_balance * {eth_price_usd} as balance_usd,
                tx_count,
                TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_activity, DAY) as days_since_last_activity
            FROM arbitrum_balances
            WHERE total_eth_balance * {eth_price_usd} >= 1000000
            ORDER BY balance_usd DESC
            LIMIT {limit_per_query}
            """
            
            results = self._execute_query(query)
            
            for row in results:
                confidence_score = self.whale_detector.calculate_confidence_score(
                    balance_usd=row['balance_usd'],
                    tx_count=row['tx_count'],
                    max_tx_usd=row['balance_usd'] * 0.1,
                    days_since_last_activity=row['days_since_last_activity']
                )
                
                addresses.append(AddressData(
                    address=row['address'],
                    blockchain='arbitrum',
                    source_system='BigQuery-Arbitrum',
                    initial_label='whale',
                    confidence_score=confidence_score,
                    metadata={
                        'balance_eth': row['total_eth_balance'],
                        'balance_usd': row['balance_usd'],
                        'tx_count': row['tx_count'],
                        'days_since_last_activity': row['days_since_last_activity'],
                        'detection_method': 'high_balance_arbitrum'
                    }
                ))
            
            self.logger.info(f"Extracted {len(addresses)} Arbitrum whale addresses")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to extract Arbitrum whale addresses: {e}")
            return []


class OptimismPublicDataExtractor(BigQueryPublicDatasetExtractorBase):
    """Optimism whale extractor using public BigQuery data."""
    
    def extract_addresses(self, limit_per_query: int = 5000) -> List[AddressData]:
        """Extract whale addresses from Optimism dataset."""
        addresses = []
        
        try:
            # Optimism uses ETH as native token
            eth_price_usd = self.whale_detector.APPROXIMATE_PRICES['ETH']
            min_eth_balance = 1_000_000 / eth_price_usd  # ~333 ETH for $1M
            
            query = f"""
            WITH optimism_balances AS (
                SELECT 
                    address,
                    SUM(CAST(value AS NUMERIC) / 1e18) as total_eth_balance,
                    COUNT(*) as tx_count,
                    MAX(block_timestamp) as last_activity
                FROM (
                    SELECT 
                        to_address as address, 
                        value, 
                        block_timestamp
                    FROM `bigquery-public-data.crypto_optimism.transactions`
                    WHERE to_address IS NOT NULL 
                      AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
                    
                    UNION ALL
                    
                    SELECT 
                        from_address as address, 
                        -CAST(value AS NUMERIC) as value,
                        block_timestamp
                    FROM `bigquery-public-data.crypto_optimism.transactions`
                    WHERE from_address IS NOT NULL 
                      AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
                )
                GROUP BY address
                HAVING total_eth_balance >= {min_eth_balance}
                  AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_activity, DAY) <= 90
            )
            SELECT 
                address,
                total_eth_balance,
                total_eth_balance * {eth_price_usd} as balance_usd,
                tx_count,
                TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_activity, DAY) as days_since_last_activity
            FROM optimism_balances
            WHERE total_eth_balance * {eth_price_usd} >= 1000000
            ORDER BY balance_usd DESC
            LIMIT {limit_per_query}
            """
            
            results = self._execute_query(query)
            
            for row in results:
                confidence_score = self.whale_detector.calculate_confidence_score(
                    balance_usd=row['balance_usd'],
                    tx_count=row['tx_count'],
                    max_tx_usd=row['balance_usd'] * 0.1,
                    days_since_last_activity=row['days_since_last_activity']
                )
                
                addresses.append(AddressData(
                    address=row['address'],
                    blockchain='optimism',
                    source_system='BigQuery-Optimism',
                    initial_label='whale',
                    confidence_score=confidence_score,
                    metadata={
                        'balance_eth': row['total_eth_balance'],
                        'balance_usd': row['balance_usd'],
                        'tx_count': row['tx_count'],
                        'days_since_last_activity': row['days_since_last_activity'],
                        'detection_method': 'high_balance_optimism'
                    }
                ))
            
            self.logger.info(f"Extracted {len(addresses)} Optimism whale addresses")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to extract Optimism whale addresses: {e}")
            return []


class BitcoinPublicDataExtractor(BigQueryPublicDatasetExtractorBase):
    """Extracts addresses from bigquery-public-data.crypto_bitcoin dataset."""
    
    def extract_addresses(self, limit_per_query: int = 5000) -> List[AddressData]:
        """Extract high-activity Bitcoin addresses from public dataset."""
        addresses = []
        
        try:
            # Get Bitcoin addresses with high recent output activity
            btc_addresses = self._get_high_activity_bitcoin_addresses(limit_per_query)
            addresses.extend(btc_addresses)
            
            self.logger.info(f"Extracted {len(addresses)} total addresses from Bitcoin public dataset")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to extract Bitcoin addresses: {e}")
            return []
    
    def _get_high_activity_bitcoin_addresses(self, limit: int) -> List[AddressData]:
        """Get Bitcoin addresses with high recent output activity (last 30-90 days)."""
        addresses = []
        
        try:
            # Calculate date range for last 60 days
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=60)
            
            query = f"""
            WITH bitcoin_activity AS (
                SELECT 
                    output.addresses[SAFE_OFFSET(0)] as address,
                    COUNT(*) as output_count,
                    SUM(output.value) as total_satoshis_received
                FROM `bigquery-public-data.crypto_bitcoin.transactions` t,
                UNNEST(outputs) as output
                WHERE t.block_timestamp >= TIMESTAMP('{start_date.isoformat()}')
                  AND t.block_timestamp <= TIMESTAMP('{end_date.isoformat()}')
                  AND ARRAY_LENGTH(output.addresses) > 0
                  AND output.addresses[SAFE_OFFSET(0)] IS NOT NULL
                  AND output.addresses[SAFE_OFFSET(0)] != ''
                GROUP BY address
                HAVING output_count >= 20  -- Minimum 20 outputs
                  AND total_satoshis_received >= 100000  -- Minimum 0.001 BTC
            )
            SELECT 
                address,
                output_count,
                total_satoshis_received
            FROM bitcoin_activity
            ORDER BY output_count DESC, total_satoshis_received DESC
            LIMIT {limit}
            """
            
            results = self._execute_query(query)
            
            for row in results:
                addresses.append(AddressData(
                    address=row['address'],
                    blockchain='bitcoin',
                    source_system='bq_public_bitcoin_activity',
                    initial_label=f"High Activity Address (BTC Public Data: {row['output_count']} outputs)",
                    metadata={
                        'output_count': row['output_count'],
                        'total_satoshis_received': row['total_satoshis_received'],
                        'total_btc_received': row['total_satoshis_received'] / 100000000,  # Convert to BTC
                        'query_type': 'bitcoin_activity',
                        'date_range_days': 60
                    },
                    confidence_score=0.65
                ))
            
            self.logger.info(f"Extracted {len(addresses)} high-activity Bitcoin addresses")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to get high-activity Bitcoin addresses: {e}")
            return []


class BigQueryPublicDataIntegrationManager:
    """Manager class to coordinate all public dataset extractors with Phase 3 advanced heuristics."""
    
    def __init__(self, bigquery_client: bigquery.Client, project_id: str):
        self.bigquery_client = bigquery_client
        self.project_id = project_id
        self.logger = logging.getLogger(f"{__name__}.BigQueryPublicDataIntegrationManager")
        
        # Initialize whale detector for price constants
        self.whale_detector = ComprehensiveWhaleDetector()
        
        # Initialize extractors
        self.extractors = {
            'ethereum': EthereumPublicDataExtractor(bigquery_client, project_id),
            'bitcoin': BitcoinPublicDataExtractor(bigquery_client, project_id),
            'solana': SolanaPublicDataExtractor(bigquery_client, project_id),
            'polygon': PolygonPublicDataExtractor(bigquery_client, project_id),
            'avalanche': AvalanchePublicDataExtractor(bigquery_client, project_id),
            'arbitrum': ArbitrumPublicDataExtractor(bigquery_client, project_id),
            'optimism': OptimismPublicDataExtractor(bigquery_client, project_id)
        }
        
        self.logger.info(f"Initialized {len(self.extractors)} public dataset extractors")
    
    def collect_addresses(self, limit_per_query: int = 5000) -> List[AddressData]:
        """
        Collect addresses from all public dataset extractors.
        
        This method serves as the interface expected by BlockchainDataProcessor.
        
        Args:
            limit_per_query: Maximum number of addresses to collect per query
            
        Returns:
            List[AddressData]: Collected addresses from all public datasets
        """
        return self.collect_all_public_data_addresses(limit_per_query=limit_per_query)
    
    def collect_all_public_data_addresses(self, limit_per_query: int = 5000) -> List[AddressData]:
        """Collect addresses from all public dataset extractors."""
        all_addresses = []
        
        for name, extractor in self.extractors.items():
            try:
                self.logger.info(f"Collecting addresses from {name} public dataset...")
                addresses = extractor.extract_addresses(limit_per_query=limit_per_query)
                all_addresses.extend(addresses)
                self.logger.info(f"Collected {len(addresses)} addresses from {name} public dataset")
                
            except Exception as e:
                self.logger.error(f"Failed to collect from {name} public dataset: {e}")
                continue
        
        self.logger.info(f"Total addresses collected from all public datasets: {len(all_addresses)}")
        return all_addresses
    
    # ============================================================================
    # PHASE 3: ADVANCED SQL QUERY PATTERNS
    # ============================================================================
    
    def generate_exchange_identification_query(self, 
                                             chain: str = 'ethereum',
                                             lookback_days: int = 30,
                                             min_transactions: int = 1000,
                                             clustering_confidence_threshold: float = 0.7,
                                             limit: int = 5000) -> str:
        """
        Generate SQL query to identify potential exchange addresses based on transaction patterns.
        
        Args:
            chain: Blockchain to analyze ('ethereum', 'bitcoin')
            lookback_days: Number of days to look back for analysis
            min_transactions: Minimum number of transactions for consideration
            clustering_confidence_threshold: Confidence threshold for exchange classification
        
        Returns:
            SQL query string for exchange identification
        """
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=lookback_days)
        
        if chain.lower() == 'ethereum':
            return f"""
            WITH address_patterns AS (
                SELECT 
                    address,
                    COUNT(*) as total_transactions,
                    COUNT(DISTINCT CASE WHEN direction = 'incoming' THEN counterparty END) as unique_senders,
                    COUNT(DISTINCT CASE WHEN direction = 'outgoing' THEN counterparty END) as unique_receivers,
                    AVG(CASE WHEN direction = 'incoming' THEN value_eth END) as avg_incoming_value,
                    AVG(CASE WHEN direction = 'outgoing' THEN value_eth END) as avg_outgoing_value,
                    STDDEV(CASE WHEN direction = 'incoming' THEN value_eth END) as stddev_incoming_value,
                    COUNT(CASE WHEN direction = 'incoming' AND value_eth > 10 THEN 1 END) as large_incoming_count,
                    COUNT(CASE WHEN direction = 'outgoing' AND value_eth > 10 THEN 1 END) as large_outgoing_count
                FROM (
                    SELECT 
                        to_address as address,
                        from_address as counterparty,
                        CAST(value AS NUMERIC) / 1e18 as value_eth,
                        'incoming' as direction,
                        block_timestamp
                    FROM `bigquery-public-data.crypto_ethereum.transactions`
                    WHERE block_timestamp >= TIMESTAMP('{start_date.isoformat()}')
                      AND block_timestamp <= TIMESTAMP('{end_date.isoformat()}')
                      AND to_address IS NOT NULL
                      AND from_address IS NOT NULL
                      AND value > 0
                    
                    UNION ALL
                    
                    SELECT 
                        from_address as address,
                        to_address as counterparty,
                        CAST(value AS NUMERIC) / 1e18 as value_eth,
                        'outgoing' as direction,
                        block_timestamp
                    FROM `bigquery-public-data.crypto_ethereum.transactions`
                    WHERE block_timestamp >= TIMESTAMP('{start_date.isoformat()}')
                      AND block_timestamp <= TIMESTAMP('{end_date.isoformat()}')
                      AND to_address IS NOT NULL
                      AND from_address IS NOT NULL
                      AND value > 0
                )
                GROUP BY address
                HAVING total_transactions >= {min_transactions}
            ),
            exchange_indicators AS (
                SELECT 
                    address,
                    total_transactions,
                    unique_senders,
                    unique_receivers,
                    avg_incoming_value,
                    avg_outgoing_value,
                    -- Exchange pattern scoring
                    CASE 
                        WHEN unique_senders > 100 AND unique_receivers > 50 THEN 0.3
                        WHEN unique_senders > 50 AND unique_receivers > 25 THEN 0.2
                        ELSE 0.0
                    END +
                    CASE 
                        WHEN large_incoming_count > large_outgoing_count * 2 THEN 0.2
                        WHEN large_outgoing_count > large_incoming_count * 2 THEN 0.2
                        ELSE 0.0
                    END +
                    CASE 
                        WHEN stddev_incoming_value > avg_incoming_value * 2 THEN 0.2
                        ELSE 0.0
                    END +
                    CASE 
                        WHEN total_transactions > 10000 THEN 0.3
                        WHEN total_transactions > 5000 THEN 0.2
                        WHEN total_transactions > 2000 THEN 0.1
                        ELSE 0.0
                    END as exchange_confidence_score,
                    large_incoming_count,
                    large_outgoing_count
                FROM address_patterns
            )
            SELECT 
                address,
                'Potential Exchange' as suspected_exchange_type,
                exchange_confidence_score,
                total_transactions,
                unique_senders,
                unique_receivers,
                ROUND(avg_incoming_value, 4) as avg_incoming_value_eth,
                ROUND(avg_outgoing_value, 4) as avg_outgoing_value_eth,
                large_incoming_count,
                large_outgoing_count,
                CASE 
                    WHEN unique_senders > unique_receivers * 2 THEN 'deposit_heavy'
                    WHEN unique_receivers > unique_senders * 2 THEN 'withdrawal_heavy'
                    ELSE 'balanced'
                END as pattern_type
            FROM exchange_indicators
            WHERE exchange_confidence_score >= {clustering_confidence_threshold}
            ORDER BY exchange_confidence_score DESC, total_transactions DESC
            LIMIT {limit}
            """
        
        elif chain.lower() == 'bitcoin':
            return f"""
            WITH bitcoin_patterns AS (
                SELECT 
                    address,
                    COUNT(*) as total_outputs,
                    COUNT(DISTINCT tx_hash) as unique_transactions,
                    SUM(value) as total_satoshis,
                    AVG(value) as avg_value_satoshis,
                    STDDEV(value) as stddev_value_satoshis,
                    COUNT(CASE WHEN value > 100000000 THEN 1 END) as large_output_count  -- > 1 BTC
                FROM (
                    SELECT 
                        output.addresses[SAFE_OFFSET(0)] as address,
                        output.value,
                        t.hash as tx_hash,
                        t.block_timestamp
                    FROM `bigquery-public-data.crypto_bitcoin.transactions` t,
                    UNNEST(outputs) as output
                    WHERE t.block_timestamp >= TIMESTAMP('{start_date.isoformat()}')
                      AND t.block_timestamp <= TIMESTAMP('{end_date.isoformat()}')
                      AND ARRAY_LENGTH(output.addresses) > 0
                      AND output.addresses[SAFE_OFFSET(0)] IS NOT NULL
                )
                GROUP BY address
                HAVING total_outputs >= {min_transactions // 10}  -- Bitcoin has fewer transactions
            )
            SELECT 
                address,
                'Potential Bitcoin Exchange' as suspected_exchange_type,
                CASE 
                    WHEN total_outputs > 1000 AND large_output_count > 10 THEN 0.8
                    WHEN total_outputs > 500 AND large_output_count > 5 THEN 0.6
                    WHEN total_outputs > 200 THEN 0.4
                    ELSE 0.2
                END as exchange_confidence_score,
                total_outputs,
                unique_transactions,
                ROUND(total_satoshis / 100000000, 4) as total_btc,
                ROUND(avg_value_satoshis / 100000000, 6) as avg_value_btc,
                large_output_count
            FROM bitcoin_patterns
            WHERE total_outputs >= {min_transactions // 10}
            ORDER BY total_outputs DESC, total_satoshis DESC
            LIMIT {limit}
            """
        
        else:
            raise ValueError(f"Unsupported chain: {chain}")
    
    def generate_whale_identification_query(self,
                                          min_tx_volume_usd: float,
                                          min_tx_count: int,
                                          lookback_days: int = 30,
                                          limit: int = 5000,
                                          chain: str = 'ethereum') -> str:
        """
        Generate sophisticated SQL query to identify high-volume trader whales with multi-layered filtering.
        
        This method constructs a powerful SQL query that leverages joins with label datasets to pre-filter
        non-trader entities at the database level, focusing on genuine market-moving traders.
        
        Args:
            min_tx_volume_usd: Minimum total transaction volume in USD for trader consideration
            min_tx_count: Minimum number of transactions for trader consideration
            lookback_days: Number of days to look back for analysis (default: 30)
            limit: Maximum number of results to return (default: 5000)
            chain: Blockchain to analyze ('ethereum', 'bitcoin', 'polygon', etc.)
        
        Returns:
            SQL query string for high-fidelity trader whale identification
        """
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=lookback_days)
        
        if chain.lower() == 'ethereum':
            # Ethereum approximate prices for USD conversion
            eth_price_usd = self.whale_detector.APPROXIMATE_PRICES['ETH']
            min_volume_eth = min_tx_volume_usd / eth_price_usd
            
            return f"""
            -- Multi-layered Ethereum trader whale identification with sophisticated filtering
            WITH known_labels AS (
                SELECT DISTINCT
                    address,
                    label,
                    name
                FROM `bigquery-public-data.crypto_ethereum.labels`
                WHERE address IS NOT NULL
            ),
            address_activity AS (
                SELECT 
                    from_address as address,
                    SUM(CAST(value AS NUMERIC) / 1e18) as total_volume_native,
                    SUM(CAST(value AS NUMERIC) / 1e18 * {eth_price_usd}) as total_volume_usd,
                    COUNT(*) as total_transactions,
                    COUNT(DISTINCT to_address) as unique_counterparties,
                    AVG(CAST(value AS NUMERIC) / 1e18 * {eth_price_usd}) as avg_tx_value_usd,
                    MAX(CAST(value AS NUMERIC) / 1e18 * {eth_price_usd}) as max_tx_value_usd,
                    COUNT(CASE WHEN CAST(value AS NUMERIC) / 1e18 * {eth_price_usd} > 100000 THEN 1 END) as large_tx_count,
                    MAX(block_timestamp) as last_activity
                FROM `bigquery-public-data.crypto_ethereum.transactions`
                WHERE block_timestamp >= TIMESTAMP('{start_date.isoformat()}')
                  AND block_timestamp <= TIMESTAMP('{end_date.isoformat()}')
                  AND from_address IS NOT NULL
                  AND to_address IS NOT NULL
                  AND value > 0
                  AND from_address != to_address  -- Exclude self-transfers
                GROUP BY from_address
                HAVING total_volume_usd >= {min_tx_volume_usd}
                   AND total_transactions >= {min_tx_count}
                   AND unique_counterparties < 5000  -- Exclude service wallets with too many interactions
                   AND unique_counterparties >= 3    -- Ensure some trading activity
            ),
            contract_creations AS (
                SELECT DISTINCT from_address as address
                FROM `bigquery-public-data.crypto_ethereum.transactions`
                WHERE to_address IS NULL  -- Contract creation transactions
                  AND block_timestamp >= TIMESTAMP('{start_date.isoformat()}')
                  AND block_timestamp <= TIMESTAMP('{end_date.isoformat()}')
            )
            SELECT 
                t.address,
                t.total_volume_usd,
                t.total_transactions,
                t.unique_counterparties,
                t.avg_tx_value_usd,
                t.max_tx_value_usd,
                t.large_tx_count,
                t.last_activity,
                -- Trader confidence score based on activity patterns
                CASE 
                    WHEN t.total_volume_usd > 50000000 THEN 0.95    -- > $50M volume
                    WHEN t.total_volume_usd > 10000000 THEN 0.85    -- > $10M volume
                    WHEN t.total_volume_usd > 5000000 THEN 0.75     -- > $5M volume
                    WHEN t.total_volume_usd > 1000000 THEN 0.65     -- > $1M volume
                    ELSE 0.55
                END +
                CASE 
                    WHEN t.avg_tx_value_usd > 500000 THEN 0.1       -- High avg transaction size
                    WHEN t.avg_tx_value_usd > 100000 THEN 0.05
                    ELSE 0.0
                END +
                CASE 
                    WHEN t.large_tx_count > 50 THEN 0.1             -- Frequent large transactions
                    WHEN t.large_tx_count > 20 THEN 0.05
                    ELSE 0.0
                END +
                CASE 
                    WHEN t.unique_counterparties BETWEEN 10 AND 500 THEN 0.05  -- Reasonable counterparty diversity
                    ELSE 0.0
                END as trader_confidence_score,
                'high_volume_trader' as whale_type
            FROM address_activity t
            LEFT JOIN known_labels l ON t.address = l.address
            LEFT JOIN contract_creations cc ON t.address = cc.address
            WHERE 
                -- Exclude known non-trader entities via labels
                (l.address IS NULL OR (
                    LOWER(COALESCE(l.label, '')) NOT IN ('exchange', 'contract', 'bridge', 'mev_bot', 'staking_pool', 'burn_address', 'genesis', 'coinbase')
                    AND LOWER(COALESCE(l.name, '')) NOT LIKE '%exchange%'
                    AND LOWER(COALESCE(l.name, '')) NOT LIKE '%bridge%'
                    AND LOWER(COALESCE(l.name, '')) NOT LIKE '%pool%'
                    AND LOWER(COALESCE(l.name, '')) NOT LIKE '%staking%'
                    AND LOWER(COALESCE(l.name, '')) NOT LIKE '%validator%'
                    AND LOWER(COALESCE(l.name, '')) NOT LIKE '%binance%'
                    AND LOWER(COALESCE(l.name, '')) NOT LIKE '%coinbase%'
                    AND LOWER(COALESCE(l.name, '')) NOT LIKE '%kraken%'
                    AND LOWER(COALESCE(l.name, '')) NOT LIKE '%okex%'
                    AND LOWER(COALESCE(l.name, '')) NOT LIKE '%huobi%'
                ))
                -- Exclude contract creators (likely not individual traders)
                AND cc.address IS NULL
            ORDER BY t.total_volume_usd DESC
            LIMIT {limit}
            """
        
        elif chain.lower() == 'bitcoin':
            btc_price_usd = self.whale_detector.APPROXIMATE_PRICES['BTC']
            min_volume_satoshis = int(min_tx_volume_usd * 100000000 / btc_price_usd)
            
            return f"""
            -- Multi-layered Bitcoin trader whale identification
            WITH address_activity AS (
                SELECT 
                    address,
                    SUM(value) as total_volume_satoshis,
                    SUM(value * {btc_price_usd} / 100000000) as total_volume_usd,
                    COUNT(*) as total_transactions,
                    COUNT(DISTINCT transaction_id) as unique_transactions,
                    AVG(value * {btc_price_usd} / 100000000) as avg_tx_value_usd,
                    MAX(value * {btc_price_usd} / 100000000) as max_tx_value_usd,
                    COUNT(CASE WHEN value * {btc_price_usd} / 100000000 > 100000 THEN 1 END) as large_tx_count,
                    MAX(block_timestamp) as last_activity
                FROM (
                    SELECT 
                        output.addresses[SAFE_OFFSET(0)] as address,
                        output.value,
                        t.hash as transaction_id,
                        t.block_timestamp
                    FROM `bigquery-public-data.crypto_bitcoin.transactions` t,
                    UNNEST(outputs) as output
                    WHERE t.block_timestamp >= TIMESTAMP('{start_date.isoformat()}')
                      AND t.block_timestamp <= TIMESTAMP('{end_date.isoformat()}')
                      AND ARRAY_LENGTH(output.addresses) > 0
                      AND output.addresses[SAFE_OFFSET(0)] IS NOT NULL
                      AND output.value > 0
                )
                GROUP BY address
                HAVING total_volume_usd >= {min_tx_volume_usd}
                   AND total_transactions >= {min_tx_count}
            )
            SELECT 
                address,
                total_volume_usd,
                total_transactions,
                unique_transactions,
                avg_tx_value_usd,
                max_tx_value_usd,
                large_tx_count,
                last_activity,
                CASE 
                    WHEN total_volume_usd > 50000000 THEN 0.9       -- > $50M volume
                    WHEN total_volume_usd > 10000000 THEN 0.8       -- > $10M volume
                    WHEN total_volume_usd > 5000000 THEN 0.7        -- > $5M volume
                    ELSE 0.6
                END as trader_confidence_score,
                'high_volume_trader' as whale_type
            FROM address_activity
            ORDER BY total_volume_usd DESC
            LIMIT {limit}
            """
        
        elif chain.lower() == 'polygon':
            matic_price_usd = self.whale_detector.APPROXIMATE_PRICES['MATIC']
            min_volume_matic = min_tx_volume_usd / matic_price_usd
            
            return f"""
            -- Multi-layered Polygon trader whale identification
            WITH known_labels AS (
                SELECT DISTINCT
                    address,
                    label,
                    name
                FROM `bigquery-public-data.crypto_polygon.labels`
                WHERE address IS NOT NULL
            ),
            address_activity AS (
                SELECT 
                    from_address as address,
                    SUM(CAST(value AS NUMERIC) / 1e18) as total_volume_native,
                    SUM(CAST(value AS NUMERIC) / 1e18 * {matic_price_usd}) as total_volume_usd,
                    COUNT(*) as total_transactions,
                    COUNT(DISTINCT to_address) as unique_counterparties,
                    AVG(CAST(value AS NUMERIC) / 1e18 * {matic_price_usd}) as avg_tx_value_usd,
                    MAX(CAST(value AS NUMERIC) / 1e18 * {matic_price_usd}) as max_tx_value_usd,
                    COUNT(CASE WHEN CAST(value AS NUMERIC) / 1e18 * {matic_price_usd} > 10000 THEN 1 END) as large_tx_count,
                    MAX(block_timestamp) as last_activity
                FROM `bigquery-public-data.crypto_polygon.transactions`
                WHERE block_timestamp >= TIMESTAMP('{start_date.isoformat()}')
                  AND block_timestamp <= TIMESTAMP('{end_date.isoformat()}')
                  AND from_address IS NOT NULL
                  AND to_address IS NOT NULL
                  AND value > 0
                  AND from_address != to_address
                GROUP BY from_address
                HAVING total_volume_usd >= {min_tx_volume_usd}
                   AND total_transactions >= {min_tx_count}
                   AND unique_counterparties < 2000
                   AND unique_counterparties >= 3
            )
            SELECT 
                t.address,
                t.total_volume_usd,
                t.total_transactions,
                t.unique_counterparties,
                t.avg_tx_value_usd,
                t.max_tx_value_usd,
                t.large_tx_count,
                t.last_activity,
                CASE 
                    WHEN t.total_volume_usd > 10000000 THEN 0.85
                    WHEN t.total_volume_usd > 5000000 THEN 0.75
                    WHEN t.total_volume_usd > 1000000 THEN 0.65
                    ELSE 0.55
                END as trader_confidence_score,
                'high_volume_trader' as whale_type
            FROM address_activity t
            LEFT JOIN known_labels l ON t.address = l.address
            WHERE 
                (l.address IS NULL OR (
                    LOWER(COALESCE(l.label, '')) NOT IN ('exchange', 'contract', 'bridge', 'mev_bot', 'staking_pool')
                    AND LOWER(COALESCE(l.name, '')) NOT LIKE '%exchange%'
                    AND LOWER(COALESCE(l.name, '')) NOT LIKE '%bridge%'
                    AND LOWER(COALESCE(l.name, '')) NOT LIKE '%pool%'
                ))
            ORDER BY t.total_volume_usd DESC
            LIMIT {limit}
            """
        
        else:
            # Generic query for other chains
            return f"""
            -- Generic high-volume trader identification for {chain}
            WITH address_activity AS (
                SELECT 
                    from_address as address,
                    COUNT(*) as total_transactions,
                    COUNT(DISTINCT to_address) as unique_counterparties,
                    MAX(block_timestamp) as last_activity
                FROM `bigquery-public-data.crypto_{chain}.transactions`
                WHERE block_timestamp >= TIMESTAMP('{start_date.isoformat()}')
                  AND block_timestamp <= TIMESTAMP('{end_date.isoformat()}')
                  AND from_address IS NOT NULL
                  AND to_address IS NOT NULL
                  AND value > 0
                GROUP BY from_address
                HAVING total_transactions >= {min_tx_count}
                   AND unique_counterparties < 1000
                   AND unique_counterparties >= 3
            )
            SELECT 
                address,
                total_transactions,
                unique_counterparties,
                last_activity,
                0.6 as trader_confidence_score,
                'active_trader' as whale_type
            FROM address_activity
            ORDER BY total_transactions DESC
            LIMIT {limit}
            """
    
    def generate_defi_interaction_query(self,
                                      chain: str = 'ethereum',
                                      protocol_addresses_or_types: List[str] = None,
                                      lookback_days: int = 30,
                                      min_interaction_count: int = 10) -> str:
        """
        Generate SQL query to identify addresses with significant DeFi protocol interactions.
        
        Args:
            chain: Blockchain to analyze ('ethereum')
            protocol_addresses_or_types: List of protocol contract addresses or types
            lookback_days: Number of days to look back for analysis
            min_interaction_count: Minimum number of interactions for consideration
        
        Returns:
            SQL query string for DeFi interaction analysis
        """
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=lookback_days)
        
        if protocol_addresses_or_types is None:
            # Default to major DeFi protocols (example addresses)
            protocol_addresses_or_types = [
                '0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D',  # Uniswap V2 Router
                '0xE592427A0AEce92De3Edee1F18E0157C05861564',  # Uniswap V3 Router
                '0xd9e1cE17f2641f24aE83637ab66a2cca9C378B9F',  # SushiSwap Router
                '0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9',  # Aave LendingPool
                '0x3d9819210A31b4961b30EF54bE2aeD79B9c9Cd3B',  # Compound Comptroller
                '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',  # WETH
                '0xA0b86a33E6441e6C7d3E4081f7567b0b2b2b8b0a',  # USDC
            ]
        
        protocol_list = "', '".join(protocol_addresses_or_types)
        
        if chain.lower() == 'ethereum':
            return f"""
            WITH defi_interactions AS (
                SELECT 
                    from_address as user_address,
                    to_address as protocol_address,
                    COUNT(*) as interaction_count,
                    SUM(CAST(value AS NUMERIC) / 1e18) as total_value_eth,
                    AVG(CAST(value AS NUMERIC) / 1e18) as avg_value_eth,
                    MIN(block_timestamp) as first_interaction,
                    MAX(block_timestamp) as last_interaction,
                    COUNT(DISTINCT DATE(block_timestamp)) as active_days
                FROM `bigquery-public-data.crypto_ethereum.transactions`
                WHERE block_timestamp >= TIMESTAMP('{start_date.isoformat()}')
                  AND block_timestamp <= TIMESTAMP('{end_date.isoformat()}')
                  AND to_address IN ('{protocol_list}')
                  AND from_address IS NOT NULL
                  AND from_address != to_address
                GROUP BY from_address, to_address
                HAVING interaction_count >= {min_interaction_count}
            ),
            user_defi_summary AS (
                SELECT 
                    user_address,
                    COUNT(DISTINCT protocol_address) as unique_protocols,
                    SUM(interaction_count) as total_interactions,
                    SUM(total_value_eth) as total_defi_volume_eth,
                    AVG(avg_value_eth) as overall_avg_value_eth,
                    MIN(first_interaction) as first_defi_interaction,
                    MAX(last_interaction) as last_defi_interaction,
                    SUM(active_days) as total_active_days,
                    ARRAY_AGG(
                        STRUCT(
                            protocol_address,
                            interaction_count,
                            ROUND(total_value_eth, 4) as volume_eth
                        ) 
                        ORDER BY interaction_count DESC
                    ) as protocol_interactions
                FROM defi_interactions
                GROUP BY user_address
            ),
            defi_power_users AS (
                SELECT 
                    user_address,
                    unique_protocols,
                    total_interactions,
                    total_defi_volume_eth,
                    overall_avg_value_eth,
                    first_defi_interaction,
                    last_defi_interaction,
                    total_active_days,
                    protocol_interactions,
                    -- DeFi activity scoring
                    CASE 
                        WHEN unique_protocols >= 5 AND total_interactions >= 100 THEN 0.9
                        WHEN unique_protocols >= 3 AND total_interactions >= 50 THEN 0.7
                        WHEN unique_protocols >= 2 AND total_interactions >= 25 THEN 0.5
                        ELSE 0.3
                    END +
                    CASE 
                        WHEN total_defi_volume_eth > 1000 THEN 0.1  -- > $2M volume
                        WHEN total_defi_volume_eth > 100 THEN 0.05  -- > $200k volume
                        ELSE 0.0
                    END as defi_activity_score
                FROM user_defi_summary
            )
            SELECT 
                user_address as address,
                unique_protocols,
                total_interactions,
                ROUND(total_defi_volume_eth, 4) as total_defi_volume_eth,
                ROUND(overall_avg_value_eth, 6) as avg_transaction_value_eth,
                first_defi_interaction,
                last_defi_interaction,
                total_active_days,
                defi_activity_score,
                CASE 
                    WHEN defi_activity_score >= 0.8 THEN 'defi_power_user'
                    WHEN defi_activity_score >= 0.6 THEN 'defi_active_user'
                    WHEN defi_activity_score >= 0.4 THEN 'defi_regular_user'
                    ELSE 'defi_casual_user'
                END as defi_user_tier,
                protocol_interactions
            FROM defi_power_users
            WHERE defi_activity_score >= 0.3
            ORDER BY defi_activity_score DESC, total_interactions DESC
            LIMIT {limit}
            """
        
        else:
            raise ValueError(f"DeFi interaction analysis currently only supports Ethereum")
    
    def execute_advanced_query(self, query_type: str, **kwargs) -> List[Dict[str, Any]]:
        """
        Execute one of the advanced query patterns and return results.
        
        Args:
            query_type: Type of query ('exchange', 'whale', 'defi')
            **kwargs: Parameters for the specific query type
        
        Returns:
            List of query results as dictionaries
        """
        try:
            if query_type == 'exchange':
                query = self.generate_exchange_identification_query(**kwargs)
            elif query_type == 'whale':
                query = self.generate_whale_identification_query(**kwargs)
            elif query_type == 'defi':
                query = self.generate_defi_interaction_query(**kwargs)
            else:
                raise ValueError(f"Unsupported query type: {query_type}")
            
            # Execute query with cost optimization
            job_config = bigquery.QueryJobConfig()
            job_config.use_query_cache = True
            job_config.use_legacy_sql = False
            
            results = self._execute_query(query, job_config)
            self.logger.info(f"Advanced {query_type} query returned {len(results)} results")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Failed to execute advanced {query_type} query: {e}")
            return []
    
    def _execute_query(self, query: str, job_config: Optional[bigquery.QueryJobConfig] = None) -> List[Dict]:
        """Execute a BigQuery SQL query and return results as a list of dictionaries."""
        try:
            self.logger.info(f"Executing BigQuery query: {query[:200]}...")
            
            if job_config is None:
                job_config = bigquery.QueryJobConfig()
            
            query_job = self.bigquery_client.query(query, job_config=job_config)
            results = query_job.result()
            
            # Convert results to list of dictionaries
            rows = [dict(row) for row in results]
            
            self.logger.info(f"Query executed successfully, returned {len(rows)} rows")
            return rows
            
        except Exception as e:
            self.logger.error(f"Failed to execute BigQuery query: {e}")
            self.logger.error(f"Query: {query}")
            raise 
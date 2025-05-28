"""
BigQuery Public Data Extractor - Phase 3: Advanced Heuristics & Post-Processing

This module extracts blockchain address data from Google BigQuery's public datasets
with advanced SQL query patterns for identifying exchanges, whales, and DeFi interactions.

Author: Address Collector System
Version: 3.0.0 (Phase 3 - Advanced BigQuery Heuristics)
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from google.cloud import bigquery

# Import the AddressData class from api_integrations
from .api_integrations import AddressData

# Configure logging
logger = logging.getLogger(__name__)


class BigQueryPublicDatasetExtractorBase(ABC):
    """Base class for extracting addresses from BigQuery public datasets."""
    
    def __init__(self, bigquery_client: bigquery.Client, project_id: str):
        self.bigquery_client = bigquery_client
        self.project_id = project_id
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def _execute_query(self, query: str, job_config: Optional[bigquery.QueryJobConfig] = None) -> List[Dict]:
        """Execute a BigQuery SQL query and return results as a list of dictionaries."""
        try:
            self.logger.info(f"Executing BigQuery query: {query[:100]}...")
            
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


class EthereumPublicDataExtractor(BigQueryPublicDatasetExtractorBase):
    """Extracts addresses from bigquery-public-data.crypto_ethereum dataset."""
    
    def extract_addresses(self, limit_per_query: int = 5000) -> List[AddressData]:
        """Extract high-activity Ethereum addresses from public dataset."""
        addresses = []
        
        try:
            # Get EOAs with high recent activity
            eoa_addresses = self._get_high_activity_eoas(limit_per_query)
            addresses.extend(eoa_addresses)
            
            # Get smart contracts with high recent interaction
            contract_addresses = self._get_active_contracts(limit_per_query)
            addresses.extend(contract_addresses)
            
            self.logger.info(f"Extracted {len(addresses)} total addresses from Ethereum public dataset")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to extract Ethereum addresses: {e}")
            return []
    
    def _get_high_activity_eoas(self, limit: int) -> List[AddressData]:
        """Get Externally Owned Accounts with high recent activity (last 30-90 days)."""
        addresses = []
        
        try:
            # Calculate date range for last 60 days (compromise between 30-90)
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=60)
            
            query = f"""
            WITH address_activity AS (
                SELECT 
                    address,
                    COUNT(*) as tx_count,
                    SUM(CAST(value AS NUMERIC) / 1e18) as total_value_eth
                FROM (
                    SELECT from_address as address, value, block_timestamp
                    FROM `bigquery-public-data.crypto_ethereum.transactions`
                    WHERE block_timestamp >= TIMESTAMP('{start_date.isoformat()}')
                      AND block_timestamp <= TIMESTAMP('{end_date.isoformat()}')
                      AND from_address IS NOT NULL
                      AND from_address != ''
                    
                    UNION ALL
                    
                    SELECT to_address as address, value, block_timestamp
                    FROM `bigquery-public-data.crypto_ethereum.transactions`
                    WHERE block_timestamp >= TIMESTAMP('{start_date.isoformat()}')
                      AND block_timestamp <= TIMESTAMP('{end_date.isoformat()}')
                      AND to_address IS NOT NULL
                      AND to_address != ''
                      AND to_address NOT IN (
                          SELECT address FROM `bigquery-public-data.crypto_ethereum.contracts`
                          WHERE address IS NOT NULL
                      )
                )
                GROUP BY address
                HAVING tx_count >= 50  -- Minimum 50 transactions
            )
            SELECT 
                address,
                tx_count,
                total_value_eth
            FROM address_activity
            ORDER BY tx_count DESC, total_value_eth DESC
            LIMIT {limit}
            """
            
            results = self._execute_query(query)
            
            for row in results:
                addresses.append(AddressData(
                    address=row['address'],
                    blockchain='ethereum',
                    source_system='bq_public_ethereum_eoa',
                    initial_label=f"High Activity EOA (ETH Public Data: {row['tx_count']} txs)",
                    metadata={
                        'tx_count': row['tx_count'],
                        'total_value_eth': float(row['total_value_eth']) if row['total_value_eth'] else 0.0,
                        'query_type': 'eoa_activity',
                        'date_range_days': 60
                    },
                    confidence_score=0.65
                ))
            
            self.logger.info(f"Extracted {len(addresses)} high-activity EOAs")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to get high-activity EOAs: {e}")
            return []
    
    def _get_active_contracts(self, limit: int) -> List[AddressData]:
        """Get smart contracts with high recent interaction (last 30-90 days)."""
        addresses = []
        
        try:
            # Calculate date range for last 60 days
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=60)
            
            query = f"""
            WITH contract_activity AS (
                SELECT 
                    c.address,
                    COUNT(t.hash) as interaction_count,
                    SUM(CAST(t.value AS NUMERIC) / 1e18) as total_value_received_eth
                FROM `bigquery-public-data.crypto_ethereum.contracts` c
                JOIN `bigquery-public-data.crypto_ethereum.transactions` t
                    ON c.address = t.to_address
                WHERE t.block_timestamp >= TIMESTAMP('{start_date.isoformat()}')
                  AND t.block_timestamp <= TIMESTAMP('{end_date.isoformat()}')
                  AND c.address IS NOT NULL
                  AND c.address != ''
                GROUP BY c.address
                HAVING interaction_count >= 100  -- Minimum 100 interactions
            )
            SELECT 
                address,
                interaction_count,
                total_value_received_eth
            FROM contract_activity
            ORDER BY interaction_count DESC
            LIMIT {limit}
            """
            
            results = self._execute_query(query)
            
            for row in results:
                addresses.append(AddressData(
                    address=row['address'],
                    blockchain='ethereum',
                    source_system='bq_public_ethereum_contract',
                    initial_label=f"Active Contract (ETH Public Data: {row['interaction_count']} interactions)",
                    metadata={
                        'interaction_count': row['interaction_count'],
                        'total_value_received_eth': float(row['total_value_received_eth']) if row['total_value_received_eth'] else 0.0,
                        'query_type': 'contract_activity',
                        'date_range_days': 60
                    },
                    confidence_score=0.70
                ))
            
            self.logger.info(f"Extracted {len(addresses)} active contracts")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to get active contracts: {e}")
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
        
        # Initialize extractors
        self.extractors = {
            'ethereum': EthereumPublicDataExtractor(bigquery_client, project_id),
            'bitcoin': BitcoinPublicDataExtractor(bigquery_client, project_id)
        }
        
        self.logger.info(f"Initialized {len(self.extractors)} public dataset extractors")
    
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
                                             clustering_confidence_threshold: float = 0.7) -> str:
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
            LIMIT 1000
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
            LIMIT 1000
            """
        
        else:
            raise ValueError(f"Unsupported chain: {chain}")
    
    def generate_whale_identification_query(self,
                                          chain: str = 'ethereum',
                                          lookback_days: int = 30,
                                          min_balance_usd: float = 1000000,
                                          min_avg_tx_value_usd: float = 50000,
                                          min_unique_protocols_interacted: int = 3) -> str:
        """
        Generate SQL query to identify potential whale addresses based on balance and activity patterns.
        
        Args:
            chain: Blockchain to analyze ('ethereum', 'bitcoin')
            lookback_days: Number of days to look back for analysis
            min_balance_usd: Minimum balance in USD for whale consideration
            min_avg_tx_value_usd: Minimum average transaction value in USD
            min_unique_protocols_interacted: Minimum unique protocols interacted with
        
        Returns:
            SQL query string for whale identification
        """
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=lookback_days)
        
        if chain.lower() == 'ethereum':
            return f"""
            WITH address_activity AS (
                SELECT 
                    address,
                    COUNT(*) as total_transactions,
                    SUM(value_eth) as total_volume_eth,
                    AVG(value_eth) as avg_transaction_value_eth,
                    MAX(value_eth) as max_transaction_value_eth,
                    COUNT(DISTINCT to_address) as unique_counterparties,
                    COUNT(CASE WHEN value_eth > 50 THEN 1 END) as large_tx_count,
                    COUNT(DISTINCT DATE(block_timestamp)) as active_days
                FROM (
                    SELECT 
                        from_address as address,
                        to_address,
                        CAST(value AS NUMERIC) / 1e18 as value_eth,
                        block_timestamp
                    FROM `bigquery-public-data.crypto_ethereum.transactions`
                    WHERE block_timestamp >= TIMESTAMP('{start_date.isoformat()}')
                      AND block_timestamp <= TIMESTAMP('{end_date.isoformat()}')
                      AND from_address IS NOT NULL
                      AND to_address IS NOT NULL
                      AND value > 0
                )
                GROUP BY address
                HAVING total_volume_eth >= {min_balance_usd / 2000}  -- Assuming ~$2000 per ETH
                   AND avg_transaction_value_eth >= {min_avg_tx_value_usd / 2000}
            ),
            contract_interactions AS (
                SELECT 
                    from_address as address,
                    COUNT(DISTINCT to_address) as unique_contracts_interacted
                FROM `bigquery-public-data.crypto_ethereum.transactions` t
                WHERE t.block_timestamp >= TIMESTAMP('{start_date.isoformat()}')
                  AND t.block_timestamp <= TIMESTAMP('{end_date.isoformat()}')
                  AND t.to_address IN (
                      SELECT address FROM `bigquery-public-data.crypto_ethereum.contracts`
                      WHERE address IS NOT NULL
                  )
                GROUP BY from_address
                HAVING unique_contracts_interacted >= {min_unique_protocols_interacted}
            ),
            whale_candidates AS (
                SELECT 
                    a.address,
                    a.total_transactions,
                    a.total_volume_eth,
                    a.avg_transaction_value_eth,
                    a.max_transaction_value_eth,
                    a.unique_counterparties,
                    a.large_tx_count,
                    a.active_days,
                    COALESCE(c.unique_contracts_interacted, 0) as unique_contracts_interacted,
                    -- Whale scoring algorithm
                    CASE 
                        WHEN a.total_volume_eth > 10000 THEN 0.4  -- > $20M volume
                        WHEN a.total_volume_eth > 5000 THEN 0.3   -- > $10M volume
                        WHEN a.total_volume_eth > 1000 THEN 0.2   -- > $2M volume
                        ELSE 0.1
                    END +
                    CASE 
                        WHEN a.avg_transaction_value_eth > 100 THEN 0.3  -- > $200k avg
                        WHEN a.avg_transaction_value_eth > 50 THEN 0.2   -- > $100k avg
                        WHEN a.avg_transaction_value_eth > 25 THEN 0.1   -- > $50k avg
                        ELSE 0.0
                    END +
                    CASE 
                        WHEN a.large_tx_count > 50 THEN 0.2
                        WHEN a.large_tx_count > 20 THEN 0.1
                        ELSE 0.0
                    END +
                    CASE 
                        WHEN COALESCE(c.unique_contracts_interacted, 0) > 10 THEN 0.1
                        WHEN COALESCE(c.unique_contracts_interacted, 0) > 5 THEN 0.05
                        ELSE 0.0
                    END as whale_score
                FROM address_activity a
                LEFT JOIN contract_interactions c ON a.address = c.address
            )
            SELECT 
                address,
                whale_score,
                total_transactions,
                ROUND(total_volume_eth, 2) as total_volume_eth,
                ROUND(avg_transaction_value_eth, 4) as avg_transaction_value_eth,
                ROUND(max_transaction_value_eth, 2) as max_transaction_value_eth,
                unique_counterparties,
                large_tx_count,
                active_days,
                unique_contracts_interacted,
                CASE 
                    WHEN whale_score >= 0.8 THEN 'ultra_whale'
                    WHEN whale_score >= 0.6 THEN 'major_whale'
                    WHEN whale_score >= 0.4 THEN 'whale'
                    ELSE 'large_trader'
                END as whale_tier,
                ARRAY(
                    SELECT signal FROM UNNEST([
                        CASE WHEN total_volume_eth > 5000 THEN 'high_volume' ELSE NULL END,
                        CASE WHEN avg_transaction_value_eth > 50 THEN 'high_avg_value' ELSE NULL END,
                        CASE WHEN large_tx_count > 20 THEN 'frequent_large_txs' ELSE NULL END,
                        CASE WHEN unique_contracts_interacted > 5 THEN 'defi_active' ELSE NULL END
                    ]) AS signal WHERE signal IS NOT NULL
                ) as whale_signals
            FROM whale_candidates
            WHERE whale_score >= 0.3
            ORDER BY whale_score DESC, total_volume_eth DESC
            LIMIT 1000
            """
        
        elif chain.lower() == 'bitcoin':
            return f"""
            WITH bitcoin_whale_activity AS (
                SELECT 
                    address,
                    COUNT(*) as total_outputs,
                    SUM(value) as total_satoshis,
                    AVG(value) as avg_output_value_satoshis,
                    MAX(value) as max_output_value_satoshis,
                    COUNT(CASE WHEN value > 100000000 THEN 1 END) as large_output_count,  -- > 1 BTC
                    COUNT(DISTINCT DATE(block_timestamp)) as active_days
                FROM (
                    SELECT 
                        output.addresses[SAFE_OFFSET(0)] as address,
                        output.value,
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
                HAVING total_satoshis >= {min_balance_usd * 100000000 // 50000}  -- Assuming ~$50k per BTC
            )
            SELECT 
                address,
                CASE 
                    WHEN total_satoshis > 10000000000 THEN 0.9  -- > 100 BTC
                    WHEN total_satoshis > 5000000000 THEN 0.7   -- > 50 BTC
                    WHEN total_satoshis > 1000000000 THEN 0.5   -- > 10 BTC
                    ELSE 0.3
                END as whale_score,
                total_outputs,
                ROUND(total_satoshis / 100000000, 4) as total_btc,
                ROUND(avg_output_value_satoshis / 100000000, 6) as avg_output_value_btc,
                ROUND(max_output_value_satoshis / 100000000, 4) as max_output_value_btc,
                large_output_count,
                active_days,
                CASE 
                    WHEN total_satoshis > 10000000000 THEN 'ultra_whale'
                    WHEN total_satoshis > 5000000000 THEN 'major_whale'
                    WHEN total_satoshis > 1000000000 THEN 'whale'
                    ELSE 'large_holder'
                END as whale_tier
            FROM bitcoin_whale_activity
            ORDER BY total_satoshis DESC
            LIMIT 1000
            """
        
        else:
            raise ValueError(f"Unsupported chain: {chain}")
    
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
            LIMIT 1000
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
            self.logger.info(f"Executing BigQuery query: {query[:100]}...")
            
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
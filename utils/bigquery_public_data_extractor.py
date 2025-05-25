"""
BigQuery Public Data Extractor - Phase 2: Public Dataset Integration

This module extracts blockchain address data from Google BigQuery's public datasets
for Ethereum, Bitcoin, and other blockchains based on on-chain activity patterns.

Author: Address Collector System
Version: 2.0.0 (Phase 2 - Public Dataset Integration)
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
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
    """Manager class to coordinate all public dataset extractors."""
    
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
import os
from google.cloud import bigquery
from config.api_keys import GCP_PROJECT_ID
import logging
from typing import Dict, Any, Optional, List, Tuple

logger = logging.getLogger(__name__)

class BigQueryAnalyzer:
    """
    Handles historical analysis of Ethereum addresses using Google BigQuery.
    """
    def __init__(self):
        self.client = self._initialize_client()

    def _initialize_client(self) -> Optional[bigquery.Client]:
        """Initializes the BigQuery client using service account credentials."""
        try:
            from google.oauth2 import service_account
            from config.api_keys import GOOGLE_APPLICATION_CREDENTIALS
            import os
            
            # Check if credentials file exists
            if not GOOGLE_APPLICATION_CREDENTIALS or not os.path.exists(GOOGLE_APPLICATION_CREDENTIALS):
                logger.warning(f"BigQuery credentials file not found: {GOOGLE_APPLICATION_CREDENTIALS}")
                logger.info("BigQuery features will be disabled. To enable:")
                logger.info("1. Ensure bigquery_credentials.json exists in config/ directory")
                logger.info("2. Verify service account has 'BigQuery Job User' role")
                return None
            
            # Load service account credentials
            credentials = service_account.Credentials.from_service_account_file(
                GOOGLE_APPLICATION_CREDENTIALS,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            
            # Initialize client with explicit credentials and project
            project_id = credentials.project_id or GCP_PROJECT_ID
            client = bigquery.Client(credentials=credentials, project=project_id)
            
            logger.info(f"BigQuery client initialized with service account for project: {project_id}")
            
            # Test connection with a simple query
            test_query = "SELECT 1 as test_value"
            results = client.query(test_query).result()
            list(results)  # Consume the results to test the connection
            logger.info("✅ BigQuery client connection verified successfully")
            return client
            
        except Exception as e:
            error_msg = str(e)
            if "403" in error_msg or "Access Denied" in error_msg:
                logger.warning("⚠️ BigQuery 403 Access Denied")
                logger.warning("⚠️ Service account needs 'BigQuery Job User' role")
                logger.warning("⚠️ Run: gcloud projects add-iam-policy-binding peak-seat-465413-u9 \\")
                logger.warning("    --member='serviceAccount:bigqueryserviceaccount@peak-seat-465413-u9.iam.gserviceaccount.com' \\")
                logger.warning("    --role='roles/bigquery.jobUser'")
            else:
                logger.warning(f"BigQuery initialization failed: {e}")
            
            logger.info("BigQuery features will be disabled.")
            return None

    def analyze_address_whale_patterns(self, address: str) -> Optional[Dict[str, Any]]:
        """
        Analyze whale patterns for an address using BigQuery historical data.
        
        This method provides comprehensive whale intelligence including:
        - Transaction volume analysis
        - Behavioral patterns
        - Whale classification tiers
        - Historical activity insights
        
        Args:
            address: Ethereum address to analyze
            
        Returns:
            Dictionary with whale pattern analysis or None if analysis fails
        """
        if not self.client:
            logger.debug(f"BigQuery client not available for whale analysis of {address}")
            return None

        try:
            address = address.lower()
            
            # Get historical stats first
            historical_stats = self.get_address_historical_stats(address)
            if not historical_stats:
                return None
            
            # Extract key metrics with defensive coding for None values
            total_eth_volume = float(historical_stats.get('total_eth_volume') or 0)
            max_eth_in_tx = float(historical_stats.get('max_eth_in_tx') or 0)
            total_transactions = int(historical_stats.get('total_transactions') or 0)
            active_days = int(historical_stats.get('active_days') or 0)
            unique_counterparties = int(historical_stats.get('unique_counterparties') or 0)
            
            # Whale classification logic
            whale_tier = "UNKNOWN"
            whale_confidence = 0.0
            whale_signals = []
            
            # Volume-based classification
            if total_eth_volume >= 10000:  # 10,000+ ETH
                whale_tier = "MEGA_WHALE"
                whale_confidence = 0.95
                whale_signals.append("MEGA_VOLUME_WHALE")
            elif total_eth_volume >= 1000:  # 1,000+ ETH
                whale_tier = "ULTRA_WHALE"
                whale_confidence = 0.85
                whale_signals.append("ULTRA_VOLUME_WHALE")
            elif total_eth_volume >= 100:   # 100+ ETH
                whale_tier = "WHALE"
                whale_confidence = 0.70
                whale_signals.append("HIGH_VOLUME_WHALE")
            elif total_eth_volume >= 10:    # 10+ ETH
                whale_tier = "MINI_WHALE"
                whale_confidence = 0.50
                whale_signals.append("MODERATE_VOLUME")
            
            # Single transaction size analysis
            if max_eth_in_tx >= 1000:
                whale_signals.append("MEGA_SINGLE_TX")
                whale_confidence = min(0.95, whale_confidence + 0.15)
            elif max_eth_in_tx >= 100:
                whale_signals.append("LARGE_SINGLE_TX")
                whale_confidence = min(0.90, whale_confidence + 0.10)
            
            # Activity pattern analysis
            if total_transactions >= 1000:
                whale_signals.append("HIGH_FREQUENCY_TRADER")
                whale_confidence = min(0.90, whale_confidence + 0.05)
            
            if active_days >= 100:
                whale_signals.append("PERSISTENT_ACTOR")
                whale_confidence = min(0.90, whale_confidence + 0.05)
            
            if unique_counterparties >= 100:
                whale_signals.append("PROTOCOL_INTERACTOR")
                whale_confidence = min(0.90, whale_confidence + 0.05)
            
            # Prepare analysis result
            analysis_result = {
                'whale_tier': whale_tier,
                'whale_confidence': whale_confidence,
                'whale_signals': whale_signals,
                'historical_stats': historical_stats,
                'analysis_summary': {
                    'total_volume_eth': total_eth_volume,
                    'max_single_tx_eth': max_eth_in_tx,
                    'activity_score': min(100, (active_days * total_transactions) / 10),
                    'network_reach': unique_counterparties
                },
                'is_whale': whale_confidence >= 0.50,
                'classification_method': 'bigquery_historical_analysis'
            }
            
            logger.debug(f"BigQuery whale analysis for {address}: {whale_tier} (confidence: {whale_confidence:.2f})")
            
            return analysis_result
            
        except Exception as e:
            logger.warning(f"BigQuery whale pattern analysis failed for {address}: {e}")
            return None

    def get_address_historical_stats(self, address: str) -> Optional[Dict[str, Any]]:
        """
        Queries BigQuery for historical transaction stats for a given address.
        
        Analyzes:
        - total_transactions: High activity indicates a bot or very active trader
        - active_days: Consistent activity over many days suggests persistent entity
        - total_eth_volume: Primary indicator of financial weight
        - max_eth_in_tx: Large single transactions are strong whale signals
        - unique_counterparties: High number suggests interaction with many protocols
        """
        if not self.client:
            return None

        address = address.lower()
        query = """
            SELECT
                COUNT(*) AS total_transactions,
                COUNT(DISTINCT DATE(block_timestamp)) as active_days,
                SUM(value / POW(10, 18)) AS total_eth_volume,
                AVG(value / POW(10, 18)) AS avg_eth_per_tx,
                MAX(value / POW(10, 18)) AS max_eth_in_tx,
                COUNT(DISTINCT to_address) as unique_counterparties
            FROM
                `bigquery-public-data.crypto_ethereum.transactions`
            WHERE
                (from_address = @address OR to_address = @address)
                AND block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("address", "STRING", address),
            ]
        )

        try:
            logger.debug(f"Running BigQuery historical analysis for address: {address}")
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()  # Waits for the job to complete.
            
            for row in results:
                # Convert row to a serializable dictionary
                result_dict = {key: value for key, value in row.items()}
                logger.debug(f"BigQuery stats for {address}: {result_dict}")
                return result_dict
            return None  # Should not be reached if there's a result
        
        except Exception as e:
            logger.error(f"BigQuery query failed for address {address}: {e}")
            return None

    def get_whale_addresses_by_volume(self, min_volume_eth: float = 1000) -> Optional[Dict[str, Any]]:
        """
        Queries BigQuery to find addresses with high transaction volumes.
        This can be used to populate our whale database.
        """
        if not self.client:
            return None

        query = """
            WITH address_volumes AS (
                SELECT
                    from_address as address,
                    SUM(value / POW(10, 18)) AS total_volume_eth
                FROM
                    `bigquery-public-data.crypto_ethereum.transactions`
                WHERE
                    block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
                GROUP BY from_address
                HAVING total_volume_eth >= @min_volume
                
                UNION ALL
                
                SELECT
                    to_address as address,
                    SUM(value / POW(10, 18)) AS total_volume_eth
                FROM
                    `bigquery-public-data.crypto_ethereum.transactions`
                WHERE
                    block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
                GROUP BY to_address
                HAVING total_volume_eth >= @min_volume
            )
            SELECT 
                address,
                SUM(total_volume_eth) as combined_volume
            FROM address_volumes
            GROUP BY address
            ORDER BY combined_volume DESC
            LIMIT 1000
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("min_volume", "FLOAT", min_volume_eth),
            ]
        )

        try:
            logger.info(f"Running BigQuery whale discovery query (min volume: {min_volume_eth} ETH)")
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()
            
            whale_addresses = []
            for row in results:
                whale_addresses.append({
                    'address': row['address'],
                    'volume_eth': row['combined_volume']
                })
            
            logger.info(f"Found {len(whale_addresses)} potential whale addresses")
            return whale_addresses
        
        except Exception as e:
            logger.error(f"BigQuery whale discovery query failed: {e}")
            return None

# Global instance
bigquery_analyzer = BigQueryAnalyzer() 
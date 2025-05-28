"""
Blockchain Data Processor - Phase 2: Data Acquisition

This module provides comprehensive blockchain address collection from APIs and repositories.
Extended from Phase 1 with data acquisition capabilities.

Author: Address Collector System
Version: 2.0.0 (Phase 2)
"""

import os
import logging
import json
import asyncio
import time
from typing import Optional, Dict, Any, List, Union, Set, Tuple
from dotenv import load_dotenv
from datetime import datetime

# Third-party imports
try:
    from supabase import create_client, Client
    from google.cloud import bigquery
    from google.oauth2 import service_account
except ImportError as e:
    logging.error(f"Missing required dependencies: {e}")
    logging.error("Please install: pip install supabase-py google-cloud-bigquery python-dotenv")
    raise

# Local imports
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.api_keys import (
    SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, GOOGLE_APPLICATION_CREDENTIALS,
    ETHERSCAN_API_KEY, COVALENT_API_KEY, MORALIS_API_KEY, HELIUS_API_KEY
)

# Phase 2 imports
from utils.api_integrations import APIIntegrationManager, AddressData
from utils.github_data_extractor import GitHubDataManager, GitHubAddressData
from utils.bigquery_public_data_extractor import BigQueryPublicDataIntegrationManager


class BlockchainDataProcessor:
    """
    Main class for processing blockchain address data across multiple chains.
    
    Extended for Phase 2 with comprehensive data acquisition capabilities.
    """
    
    def __init__(self, log_level: str = "INFO"):
        """
        Initialize the BlockchainDataProcessor.
        
        Args:
            log_level (str): Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.setup_logging(log_level)
        self.logger = logging.getLogger(__name__)
        
        # Load environment variables
        self.load_environment()
        
        # Initialize clients
        self.supabase_client: Optional[Client] = None
        self.bigquery_client: Optional[bigquery.Client] = None
        
        # Phase 2: Initialize data acquisition managers
        self.api_manager: Optional[APIIntegrationManager] = None
        self.github_manager: Optional[GitHubDataManager] = None
        self.bigquery_public_data_manager: Optional[BigQueryPublicDataIntegrationManager] = None
        
        self.logger.info("BlockchainDataProcessor initialized")
    
    def setup_logging(self, log_level: str = "INFO") -> None:
        """
        Configure logging for the application.
        
        Args:
            log_level (str): The logging level to set
        """
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('blockchain_processor.log'),
                logging.StreamHandler()
            ]
        )
    
    def load_environment(self) -> None:
        """
        Load environment variables from .env file and validate configuration.
        """
        # Load .env file if it exists
        load_dotenv()
        
        # Override with environment variables if they exist
        self.supabase_url = os.getenv('SUPABASE_URL', SUPABASE_URL)
        self.supabase_service_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY', SUPABASE_SERVICE_ROLE_KEY)
        self.gcp_credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', GOOGLE_APPLICATION_CREDENTIALS)
        
        # API Keys (can be overridden by environment variables)
        self.api_keys = {
            'ETHERSCAN_API_KEY': os.getenv('ETHERSCAN_API_KEY', ETHERSCAN_API_KEY),
            'POLYGONSCAN_API_KEY': os.getenv('POLYGONSCAN_API_KEY', ''),
            'SOLSCAN_API_KEY': os.getenv('SOLSCAN_API_KEY', ''),
            'HELIUS_API_KEY': os.getenv('HELIUS_API_KEY', HELIUS_API_KEY),
            'COVALENT_API_KEY': os.getenv('COVALENT_API_KEY', COVALENT_API_KEY),
            'MORALIS_API_KEY': os.getenv('MORALIS_API_KEY', MORALIS_API_KEY),
            'WHALE_ALERT_API_KEY': os.getenv('WHALE_ALERT_API_KEY', ''),
            'BITQUERY_API_KEY': os.getenv('BITQUERY_API_KEY', ''),
            'DUNE_API_KEY': os.getenv('DUNE_API_KEY', ''),
            'QUICKNODE_API_KEY': os.getenv('QUICKNODE_API_KEY', ''),
        }
        
        self.logger.info("Environment variables loaded successfully")
    
    def create_supabase_client(self) -> Client:
        """
        Create and return a Supabase client instance.
        
        Returns:
            Client: Configured Supabase client
            
        Raises:
            Exception: If client creation fails
        """
        try:
            if self.supabase_service_key == "YOUR_SUPABASE_SERVICE_ROLE_KEY_HERE":
                raise ValueError("Please set your actual Supabase Service Role Key")
            
            self.supabase_client = create_client(self.supabase_url, self.supabase_service_key)
            self.logger.info("Supabase client created successfully")
            return self.supabase_client
            
        except Exception as e:
            self.logger.error(f"Failed to create Supabase client: {e}")
            raise
    
    def test_supabase_connection(self) -> bool:
        """
        Test the Supabase connection by performing a basic health check.
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            if not self.supabase_client:
                self.create_supabase_client()
            
            # Perform a simple query to test connection
            # This will list all tables in the public schema
            result = self.supabase_client.table('addresses').select('*').limit(1).execute()
            
            self.logger.info("Supabase connection test successful")
            self.logger.info(f"Found addresses table with {len(result.data)} sample records")
            return True
            
        except Exception as e:
            self.logger.error(f"Supabase connection test failed: {e}")
            return False
    
    def create_bigquery_client(self) -> bigquery.Client:
        """
        Create and return a BigQuery client instance.
        
        Returns:
            bigquery.Client: Configured BigQuery client
            
        Raises:
            Exception: If client creation fails
        """
        try:
            if self.gcp_credentials_path == "path/to/your/gcp-service-account-key.json":
                raise ValueError("Please set the correct path to your GCP service account key")
            
            if not os.path.exists(self.gcp_credentials_path):
                raise FileNotFoundError(f"GCP credentials file not found: {self.gcp_credentials_path}")
            
            # Load credentials from JSON file
            credentials = service_account.Credentials.from_service_account_file(
                self.gcp_credentials_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            
            self.bigquery_client = bigquery.Client(
                credentials=credentials,
                project=credentials.project_id
            )
            
            self.logger.info(f"BigQuery client created successfully for project: {credentials.project_id}")
            return self.bigquery_client
            
        except Exception as e:
            self.logger.error(f"Failed to create BigQuery client: {e}")
            raise
    
    def test_bigquery_connection(self) -> bool:
        """
        Test the BigQuery connection by executing a simple query.
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            if not self.bigquery_client:
                self.create_bigquery_client()
            
            # Execute a simple test query
            query = "SELECT 1 as test_value"
            query_job = self.bigquery_client.query(query)
            results = query_job.result()
            
            for row in results:
                test_value = row.test_value
                if test_value == 1:
                    self.logger.info("BigQuery connection test successful")
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"BigQuery connection test failed: {e}")
            return False
    
    def list_bigquery_datasets(self) -> list:
        """
        List available BigQuery datasets to verify access.
        
        Returns:
            list: List of dataset names
        """
        try:
            if not self.bigquery_client:
                self.create_bigquery_client()
            
            datasets = list(self.bigquery_client.list_datasets())
            dataset_names = [dataset.dataset_id for dataset in datasets]
            
            self.logger.info(f"Found {len(dataset_names)} datasets: {dataset_names}")
            return dataset_names
            
        except Exception as e:
            self.logger.error(f"Failed to list BigQuery datasets: {e}")
            return []
    
    # ========== Phase 2: Data Acquisition Methods ==========
    
    def initialize_data_acquisition(self) -> bool:
        """
        Initialize Phase 2 data acquisition managers.
        
        Returns:
            bool: True if initialization successful
        """
        try:
            # Initialize API integration manager
            self.api_manager = APIIntegrationManager(self.api_keys)
            
            # Initialize GitHub data manager
            self.github_manager = GitHubDataManager()
            
            # Initialize BigQuery public data manager
            # Ensure BigQuery client is created
            if not self.bigquery_client:
                self.create_bigquery_client()
            
            if self.bigquery_client:
                self.bigquery_public_data_manager = BigQueryPublicDataIntegrationManager(
                    self.bigquery_client, 
                    self.bigquery_client.project
                )
                self.logger.info("BigQuery public data manager initialized successfully")
            else:
                self.logger.warning("BigQuery client not available, skipping public data manager initialization")
            
            self.logger.info("Phase 2 data acquisition managers initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize data acquisition managers: {e}")
            return False
    
    def collect_api_data(self) -> List[AddressData]:
        """
        Collect address data from all configured APIs.
        
        Returns:
            List[AddressData]: Collected address data
        """
        if not self.api_manager:
            if not self.initialize_data_acquisition():
                return []
        
        try:
            self.logger.info("Starting API data collection...")
            addresses = self.api_manager.collect_all_addresses()
            self.logger.info(f"Collected {len(addresses)} addresses from APIs")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to collect API data: {e}")
            return []
    
    async def collect_realtime_api_data(self, duration_minutes: int = 5) -> List[AddressData]:
        """
        Collect real-time address data from WebSocket APIs.
        
        Args:
            duration_minutes: How long to collect real-time data
            
        Returns:
            List[AddressData]: Collected real-time address data
        """
        if not self.api_manager:
            if not self.initialize_data_acquisition():
                return []
        
        try:
            self.logger.info(f"Starting real-time API data collection for {duration_minutes} minutes...")
            addresses = await self.api_manager.collect_realtime_data(duration_minutes)
            self.logger.info(f"Collected {len(addresses)} addresses from real-time APIs")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to collect real-time API data: {e}")
            return []
    
    def collect_github_data(self, repo_names: Optional[List[str]] = None) -> List[GitHubAddressData]:
        """
        Collect address data from GitHub repositories.
        
        Args:
            repo_names: Specific repositories to extract from. If None, extracts from all.
            
        Returns:
            List[GitHubAddressData]: Collected GitHub address data
        """
        if not self.github_manager:
            if not self.initialize_data_acquisition():
                return []
        
        try:
            self.logger.info("Starting GitHub data collection...")
            
            if repo_names:
                addresses = self.github_manager.extract_specific_repositories(repo_names)
            else:
                addresses = self.github_manager.extract_all_repositories()
            
            self.logger.info(f"Collected {len(addresses)} addresses from GitHub repositories")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to collect GitHub data: {e}")
            return []
        finally:
            if self.github_manager:
                self.github_manager.cleanup()
    
    def collect_bigquery_public_data_addresses(self, limit_per_query: int = 5000) -> List[AddressData]:
        """
        Collect address data from BigQuery public datasets.
        
        Args:
            limit_per_query: Maximum number of addresses to collect per query
            
        Returns:
            List[AddressData]: Collected address data from public datasets
        """
        if not self.bigquery_public_data_manager:
            if not self.initialize_data_acquisition():
                return []
        
        try:
            if not self.bigquery_public_data_manager:
                self.logger.warning("BigQuery public data manager not available")
                return []
            
            self.logger.info("Starting BigQuery public dataset collection...")
            addresses = self.bigquery_public_data_manager.collect_all_public_data_addresses(
                limit_per_query=limit_per_query
            )
            self.logger.info(f"Collected {len(addresses)} addresses from BigQuery public datasets")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to collect BigQuery public data: {e}")
            return []
    
    def collect_all_data(self, include_realtime: bool = False, realtime_duration: int = 5) -> Dict[str, List]:
        """
        Collect address data from all sources (APIs and GitHub repositories).
        
        Args:
            include_realtime: Whether to include real-time WebSocket data
            realtime_duration: Duration in minutes for real-time collection
            
        Returns:
            Dict containing collected data from different sources
        """
        collected_data = {
            'api_data': [],
            'github_data': [],
            'realtime_data': []
        }
        
        try:
            # Collect API data
            self.logger.info("=== Starting comprehensive data collection ===")
            
            api_data = self.collect_api_data()
            collected_data['api_data'] = api_data
            
            # Collect GitHub data
            github_data = self.collect_github_data()
            collected_data['github_data'] = github_data
            
            # Collect real-time data if requested
            if include_realtime:
                realtime_data = asyncio.run(self.collect_realtime_api_data(realtime_duration))
                collected_data['realtime_data'] = realtime_data
            
            total_addresses = len(api_data) + len(github_data) + len(collected_data['realtime_data'])
            self.logger.info(f"=== Data collection complete: {total_addresses} total addresses ===")
            
            return collected_data
            
        except Exception as e:
            self.logger.error(f"Failed to collect all data: {e}")
            return collected_data
    
    def store_collected_data(self, collected_data: Dict[str, List]) -> bool:
        """
        Store collected address data to Supabase.
        
        Args:
            collected_data: Dictionary containing collected data from different sources
            
        Returns:
            bool: True if storage successful
        """
        try:
            if not self.supabase_client:
                self.create_supabase_client()
            
            total_stored = 0
            
            # Store API data
            for address_data in collected_data.get('api_data', []):
                try:
                    record = {
                        'address': address_data.address,
                        'blockchain': address_data.blockchain,
                        'label': address_data.initial_label,
                        'source': address_data.source_system,
                        'confidence': address_data.confidence_score,
                        'address_type': 'collected'
                    }
                    
                    result = self.supabase_client.table('addresses').insert(record).execute()
                    total_stored += 1
                    
                except Exception as e:
                    self.logger.error(f"Failed to store API address {address_data.address}: {e}")
                    continue
            
            # Store GitHub data
            for github_data in collected_data.get('github_data', []):
                try:
                    record = {
                        'address': github_data.address,
                        'blockchain': github_data.blockchain,
                        'label': github_data.initial_label,
                        'source': github_data.source_system,
                        'confidence': github_data.confidence_score,
                        'address_type': 'collected'
                    }
                    
                    result = self.supabase_client.table('addresses').insert(record).execute()
                    total_stored += 1
                    
                except Exception as e:
                    self.logger.error(f"Failed to store GitHub address {github_data.address}: {e}")
                    continue
            
            # Store real-time data
            for realtime_data in collected_data.get('realtime_data', []):
                try:
                    record = {
                        'address': realtime_data.address,
                        'blockchain': realtime_data.blockchain,
                        'label': realtime_data.initial_label,
                        'source': realtime_data.source_system,
                        'confidence': realtime_data.confidence_score,
                        'address_type': 'collected'
                    }
                    
                    result = self.supabase_client.table('addresses').insert(record).execute()
                    total_stored += 1
                    
                except Exception as e:
                    self.logger.error(f"Failed to store realtime address {realtime_data.address}: {e}")
                    continue
            
            self.logger.info(f"Successfully stored {total_stored} addresses to Supabase")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to store collected data: {e}")
            return False
    
    def run_phase2_data_acquisition(self, include_realtime: bool = False, store_data: bool = True) -> Dict[str, Any]:
        """
        Run complete Phase 2 data acquisition workflow.
        
        Args:
            include_realtime: Whether to include real-time data collection
            store_data: Whether to store collected data to Supabase
            
        Returns:
            Dict containing acquisition results and statistics
        """
        results = {
            'success': False,
            'collected_data': {},
            'statistics': {},
            'errors': []
        }
        
        try:
            self.logger.info("=== Starting Phase 2 Data Acquisition ===")
            
            # Initialize data acquisition
            if not self.initialize_data_acquisition():
                results['errors'].append("Failed to initialize data acquisition managers")
                return results
            
            # Collect all data
            collected_data = self.collect_all_data(
                include_realtime=include_realtime,
                realtime_duration=5
            )
            
            results['collected_data'] = collected_data
            
            # Calculate statistics
            api_count = len(collected_data.get('api_data', []))
            github_count = len(collected_data.get('github_data', []))
            realtime_count = len(collected_data.get('realtime_data', []))
            total_count = api_count + github_count + realtime_count
            
            results['statistics'] = {
                'api_addresses': api_count,
                'github_addresses': github_count,
                'realtime_addresses': realtime_count,
                'total_addresses': total_count,
                'unique_blockchains': len(set(
                    [addr.blockchain for addr in collected_data.get('api_data', [])] +
                    [addr.blockchain for addr in collected_data.get('github_data', [])] +
                    [addr.blockchain for addr in collected_data.get('realtime_data', [])]
                )),
                'collection_timestamp': datetime.utcnow().isoformat()
            }
            
            # Store data if requested
            if store_data and total_count > 0:
                storage_success = self.store_collected_data(collected_data)
                results['data_stored'] = storage_success
                
                if not storage_success:
                    results['errors'].append("Failed to store some or all collected data")
            
            results['success'] = True
            self.logger.info("=== Phase 2 Data Acquisition Complete ===")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Phase 2 data acquisition failed: {e}")
            results['errors'].append(str(e))
            return results
    
    # ========== Existing Phase 1 Methods ==========
    
    def get_configuration_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the current configuration.
        
        Returns:
            Dict[str, Any]: Configuration summary
        """
        # Count configured API keys
        configured_apis = sum(1 for key, value in self.api_keys.items() if value and value.strip())
        
        return {
            "supabase_url": self.supabase_url,
            "supabase_configured": self.supabase_service_key != "YOUR_SUPABASE_SERVICE_ROLE_KEY_HERE",
            "bigquery_configured": self.gcp_credentials_path != "path/to/your/gcp-service-account-key.json",
            "api_keys_configured": {
                "total_configured": configured_apis,
                "etherscan": bool(self.api_keys.get('ETHERSCAN_API_KEY')),
                "covalent": bool(self.api_keys.get('COVALENT_API_KEY')),
                "moralis": bool(self.api_keys.get('MORALIS_API_KEY')),
                "helius": bool(self.api_keys.get('HELIUS_API_KEY')),
                "whale_alert": bool(self.api_keys.get('WHALE_ALERT_API_KEY')),
                "bitquery": bool(self.api_keys.get('BITQUERY_API_KEY')),
                "dune": bool(self.api_keys.get('DUNE_API_KEY'))
            },
            "phase2_ready": APIIntegrationManager is not None and GitHubDataManager is not None
        }
    
    def run_initial_tests(self) -> Dict[str, bool]:
        """
        Run all initial connection tests.
        
        Returns:
            Dict[str, bool]: Test results
        """
        self.logger.info("Starting initial connection tests...")
        
        results = {
            "supabase_connection": False,
            "bigquery_connection": False,
            "phase2_initialization": False
        }
        
        # Test Supabase connection
        try:
            results["supabase_connection"] = self.test_supabase_connection()
        except Exception as e:
            self.logger.error(f"Supabase test failed: {e}")
        
        # Test BigQuery connection
        try:
            results["bigquery_connection"] = self.test_bigquery_connection()
        except Exception as e:
            self.logger.error(f"BigQuery test failed: {e}")
        
        # Test Phase 2 initialization
        try:
            results["phase2_initialization"] = self.initialize_data_acquisition()
        except Exception as e:
            self.logger.error(f"Phase 2 initialization test failed: {e}")
        
        # Log summary
        passed_tests = sum(results.values())
        total_tests = len(results)
        self.logger.info(f"Initial tests completed: {passed_tests}/{total_tests} passed")
        
        return results

    def bulk_check_existing_address_blockchain_pairs(self, address_data_list: List[Dict]) -> Set[Tuple[str, str]]:
        """
        Check which (address, blockchain) pairs already exist in Supabase.
        Enhanced version with more thorough duplicate detection.
        
        Args:
            address_data_list: List of address data dictionaries
            
        Returns:
            Set of (address, blockchain) tuples that already exist
        """
        if not address_data_list:
            return set()
        
        try:
            if not self.supabase_client:
                self.create_supabase_client()
            
            existing_pairs = set()
            batch_size = 50  # Reduced batch size for more reliable checking
            
            # Extract unique (address, blockchain) pairs from input
            unique_pairs = set()
            for addr_data in address_data_list:
                address = addr_data.get('address', '').lower().strip()
                blockchain = addr_data.get('blockchain', '').lower().strip()
                if address and blockchain:
                    unique_pairs.add((address, blockchain))
            
            unique_pairs_list = list(unique_pairs)
            self.logger.info(f"Checking {len(unique_pairs_list)} unique (address, blockchain) pairs for existence...")
            
            for i in range(0, len(unique_pairs_list), batch_size):
                batch = unique_pairs_list[i:i + batch_size]
                
                try:
                    # Build OR conditions for each (address, blockchain) pair
                    if not batch:
                        continue
                    
                    conditions = []
                    for address, blockchain in batch:
                        # Properly quote the values to handle any special characters
                        # Use double quotes for string values in PostgREST filters
                        conditions.append(f'and(address.eq."{address}",blockchain.eq."{blockchain}")')
                    
                    # Use the .or_() method instead of .filter('or', ...)
                    # This is the correct supabase-py syntax for OR conditions
                    or_filter_string = ",".join(conditions)
                    
                    response = self.supabase_client.table('addresses')\
                        .select('address, blockchain')\
                        .or_(or_filter_string)\
                        .execute()
                    
                    if response.data:
                        for record in response.data:
                            addr = record.get('address', '').lower().strip()
                            blockchain = record.get('blockchain', '').lower().strip()
                            if addr and blockchain:
                                existing_pairs.add((addr, blockchain))
                    
                    self.logger.debug(f"Batch {i//batch_size + 1}: Found {len(response.data) if response.data else 0} existing pairs")
                    
                except Exception as batch_error:
                    self.logger.warning(f"Error checking batch {i//batch_size + 1}: {batch_error}")
                    # Fall back to individual checks for this batch
                    for address, blockchain in batch:
                        try:
                            individual_response = self.supabase_client.table('addresses')\
                                .select('address, blockchain')\
                                .eq('address', address)\
                                .eq('blockchain', blockchain)\
                                .limit(1)\
                                .execute()
                            
                            if individual_response.data:
                                existing_pairs.add((address, blockchain))
                        except Exception as individual_error:
                            self.logger.warning(f"Error checking individual pair ({address}, {blockchain}): {individual_error}")
                            continue
                
                # Small delay to avoid rate limiting
                time.sleep(0.1)
            
            self.logger.info(f"Bulk check complete: {len(existing_pairs)} existing pairs found out of {len(unique_pairs_list)} checked")
            return existing_pairs
            
        except Exception as e:
            self.logger.error(f"Error in bulk_check_existing_address_blockchain_pairs: {e}")
            return set()

    def store_collected_data_smart(self, collected_data: Dict[str, List]) -> Dict[str, int]:
        """
        Store collected address data to Supabase with smart duplicate checking.
        
        Args:
            collected_data: Dictionary containing collected data from different sources
            
        Returns:
            Dict with storage statistics: {'stored': int, 'duplicates_skipped': int, 'errors': int, 'constraint_errors': int}
        """
        stats = {'stored': 0, 'duplicates_skipped': 0, 'errors': 0, 'constraint_errors': 0}
        
        try:
            if not self.supabase_client:
                self.create_supabase_client()
            
            # Collect all address data objects
            all_address_data = []
            for source_type in ['api_data', 'github_data', 'bigquery_data', 'realtime_data']:
                all_address_data.extend(collected_data.get(source_type, []))
            
            if not all_address_data:
                self.logger.info("No address data to store")
                return stats
            
            self.logger.info(f"Starting smart storage for {len(all_address_data)} addresses")
            
            # Enhanced internal deduplication BEFORE database check
            # This prevents Type B errors by ensuring no internal duplicates reach the database
            seen_pairs = set()
            internal_duplicates_removed = 0
            deduplicated_input = []
            
            for addr_data in all_address_data:
                addr_key = (addr_data.address.lower(), addr_data.blockchain.lower())
                if addr_key not in seen_pairs:
                    seen_pairs.add(addr_key)
                    deduplicated_input.append(addr_data)
                else:
                    internal_duplicates_removed += 1
            
            if internal_duplicates_removed > 0:
                self.logger.info(f"Removed {internal_duplicates_removed} internal duplicates before database check")
            
            # Bulk check for existing addresses using deduplicated input
            existing_pairs = self.bulk_check_existing_address_blockchain_pairs(deduplicated_input)
            
            # Filter out database duplicates
            new_addresses = []
            for addr_data in deduplicated_input:
                addr_key = (addr_data.address.lower(), addr_data.blockchain.lower())
                if addr_key not in existing_pairs:
                    new_addresses.append(addr_data)
                else:
                    stats['duplicates_skipped'] += 1
            
            # Add internal duplicates to the skipped count
            stats['duplicates_skipped'] += internal_duplicates_removed
            
            self.logger.info(f"After all deduplication: {len(new_addresses)} new addresses to store, {stats['duplicates_skipped']} total duplicates skipped")
            
            # Insert new addresses with enhanced error handling
            for addr_data in new_addresses:
                try:
                    record = {
                        'address': addr_data.address,
                        'blockchain': addr_data.blockchain,
                        'source': addr_data.source_system,
                        'label': addr_data.initial_label,
                        'confidence': addr_data.confidence_score,
                        'address_type': 'collected'
                    }
                    
                    # Add entity_name if available
                    if hasattr(addr_data, 'entity_name') and addr_data.entity_name:
                        record['entity_name'] = addr_data.entity_name
                    
                    result = self.supabase_client.table('addresses').insert(record).execute()
                    stats['stored'] += 1
                    
                except Exception as e:
                    error_msg = str(e)
                    
                    # Categorize errors for better debugging
                    if '23505' in error_msg:  # Unique constraint violation
                        if 'unique_address' in error_msg:
                            self.logger.warning(f"Type A error (unique_address constraint): {addr_data.address}")
                            stats['constraint_errors'] += 1
                        elif 'addresses_address_blockchain_key' in error_msg:
                            self.logger.warning(f"Type B error (composite key constraint): {addr_data.address} on {addr_data.blockchain}")
                            stats['constraint_errors'] += 1
                        else:
                            self.logger.warning(f"Unknown constraint error for {addr_data.address}: {error_msg}")
                            stats['errors'] += 1
                    else:
                        self.logger.warning(f"Non-constraint error for {addr_data.address}: {error_msg}")
                        stats['errors'] += 1
                    continue
            
            # Enhanced logging with error breakdown
            total_errors = stats['errors'] + stats['constraint_errors']
            self.logger.info(f"Smart storage complete: {stats['stored']} stored, {stats['duplicates_skipped']} duplicates skipped")
            if total_errors > 0:
                self.logger.info(f"Errors: {total_errors} total ({stats['constraint_errors']} constraint violations, {stats['errors']} other errors)")
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Smart storage failed: {e}")
            total_input = sum(len(collected_data.get(source_type, [])) for source_type in ['api_data', 'github_data', 'bigquery_data', 'realtime_data'])
            stats['errors'] += total_input
            return stats

    def get_known_addresses_dict(self) -> Dict[str, str]:
        """
        Get all known addresses from Supabase as a dictionary with pagination support.
        
        Returns:
            Dict mapping address -> label
        """
        try:
            if not self.supabase_client:
                self.create_supabase_client()
            
            self.logger.info("Loading known addresses from Supabase with pagination...")
            
            known_addresses = {}
            page_size = 1000
            offset = 0
            total_loaded = 0
            
            while True:
                # Get addresses in batches to handle large datasets
                response = self.supabase_client.table('addresses')\
                    .select('address, label')\
                    .range(offset, offset + page_size - 1)\
                    .execute()
                
                if not response.data:
                    break
                
                batch_count = 0
                for record in response.data:
                    address = record.get('address', '').lower()
                    label = record.get('label', 'unknown')
                    if address:
                        known_addresses[address] = label
                        batch_count += 1
                
                total_loaded += batch_count
                self.logger.info(f"Loaded batch: {batch_count} addresses (total: {total_loaded})")
                
                # If we got fewer records than page_size, we've reached the end
                if len(response.data) < page_size:
                    break
                
                offset += page_size
            
            self.logger.info(f"Completed loading {total_loaded} known addresses from Supabase")
            return known_addresses
            
        except Exception as e:
            self.logger.error(f"Failed to load known addresses: {e}")
            return {}

    def run_advanced_bigquery_analysis(self, query_types: List[str] = None, chain: str = 'ethereum', lookback_days: int = 30) -> Dict:
        """
        Run advanced BigQuery analysis for whale detection and address classification.
        
        Args:
            query_types: List of analysis types to run ['exchange', 'whale', 'defi']
            chain: Blockchain to analyze
            lookback_days: Number of days to look back
            
        Returns:
            Dict containing analysis results
        """
        try:
            if not self.bigquery_client:
                self.create_bigquery_client()
            
            if not query_types:
                query_types = ['exchange', 'whale', 'defi']
            
            self.logger.info(f"Running advanced BigQuery analysis: {query_types} on {chain}")
            
            results = {
                'success': True,
                'raw_results': {},
                'summary': {},
                'query_types': query_types,
                'chain': chain,
                'lookback_days': lookback_days
            }
            
            # Exchange detection query
            if 'exchange' in query_types:
                exchange_query = f"""
                SELECT 
                    to_address as address,
                    COUNT(*) as transaction_count,
                    COUNT(DISTINCT from_address) as unique_senders,
                    SUM(CAST(value AS FLOAT64)) / 1e18 as total_eth_received,
                    'potential_exchange' as label_type
                FROM `bigquery-public-data.crypto_ethereum.transactions`
                WHERE DATE(block_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
                    AND to_address IS NOT NULL
                    AND CAST(value AS FLOAT64) > 0
                GROUP BY to_address
                HAVING transaction_count > 100 AND unique_senders > 50
                ORDER BY transaction_count DESC
                LIMIT 1000
                """
                
                try:
                    exchange_results = list(self.bigquery_client.query(exchange_query))
                    results['raw_results']['exchange'] = [dict(row) for row in exchange_results]
                    results['summary']['exchange_addresses'] = len(exchange_results)
                    self.logger.info(f"Found {len(exchange_results)} potential exchange addresses")
                except Exception as e:
                    self.logger.warning(f"Exchange query failed: {e}")
                    results['raw_results']['exchange'] = []
                    results['summary']['exchange_addresses'] = 0
            
            # Whale detection query
            if 'whale' in query_types:
                whale_query = f"""
                SELECT 
                    from_address as address,
                    COUNT(*) as transaction_count,
                    SUM(CAST(value AS FLOAT64)) / 1e18 as total_eth_sent,
                    AVG(CAST(value AS FLOAT64)) / 1e18 as avg_eth_per_tx,
                    'whale_address' as label_type
                FROM `bigquery-public-data.crypto_ethereum.transactions`
                WHERE DATE(block_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
                    AND from_address IS NOT NULL
                    AND CAST(value AS FLOAT64) > 10e18  -- More than 10 ETH per transaction
                GROUP BY from_address
                HAVING total_eth_sent > 100  -- More than 100 ETH total
                ORDER BY total_eth_sent DESC
                LIMIT 1000
                """
                
                try:
                    whale_results = list(self.bigquery_client.query(whale_query))
                    results['raw_results']['whale'] = [dict(row) for row in whale_results]
                    results['summary']['whale_addresses'] = len(whale_results)
                    self.logger.info(f"Found {len(whale_results)} potential whale addresses")
                except Exception as e:
                    self.logger.warning(f"Whale query failed: {e}")
                    results['raw_results']['whale'] = []
                    results['summary']['whale_addresses'] = 0
            
            # DeFi detection query
            if 'defi' in query_types:
                defi_query = f"""
                SELECT 
                    to_address as address,
                    COUNT(*) as transaction_count,
                    COUNT(DISTINCT from_address) as unique_users,
                    'defi_protocol' as label_type
                FROM `bigquery-public-data.crypto_ethereum.transactions`
                WHERE DATE(block_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_days} DAY)
                    AND to_address IS NOT NULL
                    AND input != '0x'  -- Has contract interaction data
                GROUP BY to_address
                HAVING transaction_count > 50 AND unique_users > 20
                ORDER BY transaction_count DESC
                LIMIT 1000
                """
                
                try:
                    defi_results = list(self.bigquery_client.query(defi_query))
                    results['raw_results']['defi'] = [dict(row) for row in defi_results]
                    results['summary']['defi_addresses'] = len(defi_results)
                    self.logger.info(f"Found {len(defi_results)} potential DeFi addresses")
                except Exception as e:
                    self.logger.warning(f"DeFi query failed: {e}")
                    results['raw_results']['defi'] = []
                    results['summary']['defi_addresses'] = 0
            
            total_found = sum(results['summary'].values())
            self.logger.info(f"BigQuery analysis complete: {total_found} total addresses found")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Advanced BigQuery analysis failed: {e}")
            return {'success': False, 'error': str(e), 'raw_results': {}}

    def refine_bq_address_labels(self, bq_results: List[Dict], existing_known_addresses: Dict[str, str] = None) -> List[Dict]:
        """
        Refine BigQuery results by cross-referencing with known addresses.
        
        Args:
            bq_results: Raw BigQuery results
            existing_known_addresses: Dict of known addresses for cross-referencing
            
        Returns:
            List of refined address records
        """
        try:
            if not existing_known_addresses:
                existing_known_addresses = self.get_known_addresses_dict()
            
            refined_results = []
            
            for result in bq_results:
                address = result.get('address', '').lower()
                label_type = result.get('label_type', 'unknown')
                
                # Check if we already know this address
                existing_label = existing_known_addresses.get(address)
                
                refined_record = {
                    'address': address,
                    'blockchain': 'ethereum',
                    'source': 'bigquery_analysis',
                    'label': label_type,
                    'confidence': 0.7,  # Default confidence for BigQuery analysis
                    'address_type': 'analyzed',
                    'analysis_metadata': {
                        'transaction_count': result.get('transaction_count', 0),
                        'existing_label': existing_label,
                        'cross_referenced': existing_label is not None
                    }
                }
                
                # Adjust confidence based on cross-referencing
                if existing_label:
                    if existing_label.lower() in label_type.lower() or label_type.lower() in existing_label.lower():
                        refined_record['confidence'] = 0.9  # High confidence if labels match
                        refined_record['label'] = f"{label_type}_confirmed"
                    else:
                        refined_record['confidence'] = 0.5  # Lower confidence if labels conflict
                        refined_record['label'] = f"{label_type}_needs_review"
                
                refined_results.append(refined_record)
            
            self.logger.info(f"Refined {len(refined_results)} BigQuery results with cross-referencing")
            return refined_results
            
        except Exception as e:
            self.logger.error(f"Failed to refine BigQuery results: {e}")
            return []

    def collect_whale_data_from_analytics_platforms(self) -> Dict[str, Dict]:
        """
        Collect whale address data from analytics platforms like Dune Analytics.
        
        Returns:
            Dict containing addresses data from analytics platforms
        """
        try:
            self.logger.info("Collecting whale data from analytics platforms...")
            
            # Initialize analytics data collection
            analytics_addresses = {}
            
            # Try to collect from various analytics sources
            # This could include Dune Analytics, Nansen, Chainalysis, etc.
            
            # For now, we'll implement a basic structure that can be extended
            # You can add specific analytics platform integrations here
            
            # Example: Load from local analytics files if they exist
            analytics_files = [
                'data/analytics/dune_whale_addresses.json',
                'data/analytics/nansen_whale_addresses.json',
                'data/analytics/manual_whale_addresses.json'
            ]
            
            for file_path in analytics_files:
                if os.path.exists(file_path):
                    try:
                        with open(file_path, 'r') as f:
                            file_data = json.load(f)
                            
                        # Process the file data
                        if isinstance(file_data, dict):
                            for address, data in file_data.items():
                                if address not in analytics_addresses:
                                    analytics_addresses[address] = {
                                        'label': data.get('label', 'Analytics Platform'),
                                        'source': data.get('source', 'analytics_platform'),
                                        'blockchain': data.get('blockchain', 'ethereum'),
                                        'confidence': data.get('confidence', 0.8),
                                        'metadata': data.get('metadata', {})
                                    }
                        elif isinstance(file_data, list):
                            for item in file_data:
                                if isinstance(item, dict) and 'address' in item:
                                    address = item['address']
                                    analytics_addresses[address] = {
                                        'label': item.get('label', 'Analytics Platform'),
                                        'source': item.get('source', 'analytics_platform'),
                                        'blockchain': item.get('blockchain', 'ethereum'),
                                        'confidence': item.get('confidence', 0.8),
                                        'metadata': item.get('metadata', {})
                                    }
                        
                        self.logger.info(f"Loaded {len(file_data)} addresses from {file_path}")
                        
                    except Exception as e:
                        self.logger.warning(f"Failed to load analytics file {file_path}: {e}")
                        continue
            
            # TODO: Add actual API integrations for analytics platforms
            # Example integrations that could be added:
            # - Dune Analytics API
            # - Nansen API  
            # - Chainalysis API
            # - Elliptic API
            
            self.logger.info(f"Analytics platforms data collection complete: {len(analytics_addresses)} addresses")
            
            return {
                'addresses': analytics_addresses,
                'source_count': len([f for f in analytics_files if os.path.exists(f)]),
                'total_addresses': len(analytics_addresses)
            }
            
        except Exception as e:
            self.logger.error(f"Analytics platform data collection failed: {e}")
            return {
                'addresses': {},
                'source_count': 0,
                'total_addresses': 0
            }


def main():
    """
    Main function to demonstrate the BlockchainDataProcessor Phase 2 capabilities.
    """
    print("=== Blockchain Data Processor - Phase 2: Data Acquisition ===\n")
    
    # Initialize the processor
    processor = BlockchainDataProcessor(log_level="INFO")
    
    # Show configuration summary
    config = processor.get_configuration_summary()
    print("Configuration Summary:")
    print(f"  Supabase URL: {config['supabase_url']}")
    print(f"  Supabase Configured: {config['supabase_configured']}")
    print(f"  BigQuery Configured: {config['bigquery_configured']}")
    print(f"  API Keys Configured: {config['api_keys_configured']['total_configured']}")
    print(f"  Phase 2 Ready: {config['phase2_ready']}")
    print()
    
    # Run initial tests
    print("Running connection tests...")
    test_results = processor.run_initial_tests()
    
    print("\nTest Results:")
    for test_name, result in test_results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {test_name}: {status}")
    
    # Additional BigQuery info if connection successful
    if test_results.get("bigquery_connection"):
        print("\nBigQuery Datasets:")
        datasets = processor.list_bigquery_datasets()
        for dataset in datasets[:5]:  # Show first 5 datasets
            print(f"  - {dataset}")
        if len(datasets) > 5:
            print(f"  ... and {len(datasets) - 5} more")
    
    # Phase 2 demonstration
    if test_results.get("phase2_initialization"):
        print("\n=== Phase 2 Data Acquisition Demo ===")
        print("Note: This is a demonstration. Actual data collection may take several minutes.")
        print("For full data collection, run: processor.run_phase2_data_acquisition()")
        
        # Show available APIs
        if processor.api_manager:
            print(f"\nAvailable API integrations: {len(processor.api_manager.apis)}")
            for api_name in processor.api_manager.apis.keys():
                print(f"  - {api_name}")
        
        # Show available GitHub extractors
        if processor.github_manager:
            print(f"\nAvailable GitHub extractors: {len(processor.github_manager.extractors)}")
            for extractor_name in processor.github_manager.extractors.keys():
                print(f"  - {extractor_name}")
    
    print("\n=== Phase 2 Setup Complete ===")
    print("\nTo run data acquisition:")
    print("  results = processor.run_phase2_data_acquisition()")
    print("  print(f\"Collected {results['statistics']['total_addresses']} addresses\")")


if __name__ == "__main__":
    main() 
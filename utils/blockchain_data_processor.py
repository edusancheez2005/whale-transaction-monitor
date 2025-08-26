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
from typing import Optional, Dict, Any, List, Union
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
    
    def collect_whale_data_from_analytics_platforms(self) -> List[AddressData]:
        """
        Collect whale address data from analytics platforms and files.
        
        Searches for analytics files in data/analytics/ directory including:
        - dune_whale_addresses.json
        - nansen_whale_addresses.json  
        - manual_whale_addresses.json
        
        Returns:
            List[AddressData]: List of collected whale address data
        """
        analytics_data = []
        analytics_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "analytics")
        
        # Define analytics files to search for
        analytics_files = [
            "dune_whale_addresses.json",
            "nansen_whale_addresses.json", 
            "manual_whale_addresses.json"
        ]
        
        self.logger.info(f"Searching for analytics files in: {analytics_dir}")
        
        for filename in analytics_files:
            filepath = os.path.join(analytics_dir, filename)
            
            if os.path.exists(filepath):
                try:
                    with open(filepath, 'r') as f:
                        data = json.load(f)
                    
                    # Handle both dict and list formats
                    addresses_list = []
                    if isinstance(data, dict):
                        if 'addresses' in data:
                            addresses_list = data['addresses']
                        elif 'whale_addresses' in data:
                            addresses_list = data['whale_addresses']
                        else:
                            # Assume the dict values contain address info
                            addresses_list = list(data.values())
                    elif isinstance(data, list):
                        addresses_list = data
                    
                    # Convert to AddressData objects with correct parameter names
                    for addr_info in addresses_list:
                        if isinstance(addr_info, str):
                            # Simple address string
                            addr_data = AddressData(
                                address=addr_info,
                                blockchain="ethereum",  # Default
                                initial_label=f"Whale Address ({filename})",
                                source_system=f"analytics_{filename}",
                                confidence_score=0.7,
                                collected_at=datetime.utcnow()
                            )
                            analytics_data.append(addr_data)
                        elif isinstance(addr_info, dict):
                            # Structured address data
                            addr_data = AddressData(
                                address=addr_info.get('address', ''),
                                blockchain=addr_info.get('blockchain', 'ethereum'),
                                initial_label=addr_info.get('label', f"Whale Address ({filename})"),
                                source_system=f"analytics_{filename}",
                                confidence_score=addr_info.get('confidence', 0.7),
                                metadata=addr_info,
                                collected_at=datetime.utcnow()
                            )
                            analytics_data.append(addr_data)
                    
                    self.logger.info(f"Loaded {len(addresses_list)} addresses from {filename}")
                    
                except Exception as e:
                    self.logger.error(f"Error reading {filename}: {e}")
            else:
                self.logger.debug(f"Analytics file not found: {filename}")
        
        self.logger.info(f"Total analytics addresses collected: {len(analytics_data)}")
        return analytics_data

    def _load_master_entity_lists(self) -> Dict[str, Any]:
        """
        Load DeFiLlama master entity lists for enhanced address classification.
        
        Returns:
            Dict containing loaded DeFi and CEX entity data with address mappings
        """
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        exports_dir = os.path.join(base_dir, "data", "defillama_exports")
        
        master_lists = {
            'defi_entities': [],
            'cex_entities': [],
            'address_to_defi': {},      # address -> entity info
            'address_to_cex': {},       # address -> entity info  
            'entity_name_to_defi': {},  # entity_name -> entity info
            'entity_name_to_cex': {},   # entity_name -> entity info
            'load_success': False,
            'stats': {
                'total_defi_entities': 0,
                'defi_with_addresses': 0,
                'total_cex_entities': 0, 
                'cex_with_addresses': 0
            }
        }
        
        try:
            # Load DeFi entities
            defi_path = os.path.join(exports_dir, "defi_entities.json")
            if os.path.exists(defi_path):
                with open(defi_path, 'r') as f:
                    defi_entities = json.load(f)
                
                master_lists['defi_entities'] = defi_entities
                master_lists['stats']['total_defi_entities'] = len(defi_entities)
                
                # Build address and entity name mappings
                for entity in defi_entities:
                    if entity.get('address'):
                        # Normalize address (remove chain prefixes, lowercase)
                        address = entity['address'].lower()
                        if ':' in address:
                            address = address.split(':')[-1]
                        
                        master_lists['address_to_defi'][address] = entity
                        master_lists['stats']['defi_with_addresses'] += 1
                    
                    # Entity name mapping
                    entity_name = entity.get('entity_name', '').lower()
                    if entity_name:
                        master_lists['entity_name_to_defi'][entity_name] = entity
                
                self.logger.info(f"Loaded {len(defi_entities)} DeFi entities, {master_lists['stats']['defi_with_addresses']} with addresses")
            
            # Load CEX entities  
            cex_path = os.path.join(exports_dir, "cex_entities.json")
            if os.path.exists(cex_path):
                with open(cex_path, 'r') as f:
                    cex_entities = json.load(f)
                
                master_lists['cex_entities'] = cex_entities
                master_lists['stats']['total_cex_entities'] = len(cex_entities)
                
                # Build address and entity name mappings
                for entity in cex_entities:
                    if entity.get('address'):
                        # Normalize address 
                        address = entity['address'].lower()
                        if ':' in address:
                            address = address.split(':')[-1]
                            
                        master_lists['address_to_cex'][address] = entity
                        master_lists['stats']['cex_with_addresses'] += 1
                    
                    # Entity name mapping
                    entity_name = entity.get('entity_name', '').lower()
                    if entity_name:
                        master_lists['entity_name_to_cex'][entity_name] = entity
                
                self.logger.info(f"Loaded {len(cex_entities)} CEX entities, {master_lists['stats']['cex_with_addresses']} with addresses")
            
            master_lists['load_success'] = True
            
        except Exception as e:
            self.logger.error(f"Error loading master entity lists: {e}")
            
        return master_lists

    def _enhance_address_with_master_lists(self, address: str, blockchain: str, 
                                         current_label: str, master_lists: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enhance address classification using DeFiLlama master lists.
        
        Args:
            address: The address to enhance
            blockchain: The blockchain (ethereum, polygon, etc.)
            current_label: Current label for the address
            master_lists: Loaded master entity lists from _load_master_entity_lists()
            
        Returns:
            Dict containing enhanced classification info
        """
        enhancement = {
            'enhanced': False,
            'entity_name': None,
            'address_type': None,
            'label': current_label,
            'confidence': 0.5,
            'source': 'original',
            'analysis_tags': {}
        }
        
        if not master_lists.get('load_success'):
            return enhancement
        
        # Normalize address for lookup
        normalized_address = address.lower()
        if ':' in normalized_address:
            normalized_address = normalized_address.split(':')[-1]
        
        # Check for direct DeFi address match
        if normalized_address in master_lists['address_to_defi']:
            entity = master_lists['address_to_defi'][normalized_address]
            enhancement.update({
                'enhanced': True,
                'entity_name': entity['entity_name'],
                'address_type': entity['address_type'],
                'label': entity['label'],
                'confidence': 0.9,  # High confidence for direct address match
                'source': 'defillama_direct_match',
                'analysis_tags': {
                    'defillama_slug': entity.get('defillama_slug'),
                    'defillama_category': entity.get('defillama_category'),
                    'official_url': entity.get('official_url'),
                    'all_chains': entity.get('all_chains_from_defillama', []),
                    'parent_protocol': entity.get('parent_protocol'),
                    'match_type': 'direct_address'
                }
            })
            self.logger.debug(f"Direct DeFi match for {address}: {entity['entity_name']}")
            return enhancement
        
        # Check for direct CEX address match
        if normalized_address in master_lists['address_to_cex']:
            entity = master_lists['address_to_cex'][normalized_address]
            enhancement.update({
                'enhanced': True,
                'entity_name': entity['entity_name'],
                'address_type': entity['address_type'],
                'label': entity['label'],
                'confidence': 0.9,  # High confidence for direct address match
                'source': 'defillama_cex_direct_match',
                'analysis_tags': {
                    'defillama_slug': entity.get('defillama_slug'),
                    'defillama_category': entity.get('defillama_category'),
                    'official_url': entity.get('official_url'),
                    'all_chains': entity.get('all_chains_from_defillama', []),
                    'match_type': 'direct_address'
                }
            })
            self.logger.debug(f"Direct CEX match for {address}: {entity['entity_name']}")
            return enhancement
            
        # Fuzzy matching on current label for entity names
        current_label_lower = current_label.lower()
        
        # Check DeFi entity names
        for entity_name, entity in master_lists['entity_name_to_defi'].items():
            if entity_name in current_label_lower or any(word in current_label_lower for word in entity_name.split()):
                enhancement.update({
                    'enhanced': True,
                    'entity_name': entity['entity_name'],
                    'address_type': entity['address_type'],
                    'label': f"{entity['entity_name']} - {current_label}",
                    'confidence': 0.7,  # Medium confidence for name match
                    'source': 'defillama_name_match',
                    'analysis_tags': {
                        'defillama_slug': entity.get('defillama_slug'),
                        'defillama_category': entity.get('defillama_category'),
                        'official_url': entity.get('official_url'),
                        'all_chains': entity.get('all_chains_from_defillama', []),
                        'parent_protocol': entity.get('parent_protocol'),
                        'match_type': 'entity_name_fuzzy'
                    }
                })
                self.logger.debug(f"DeFi name match for {address}: {entity['entity_name']}")
                return enhancement
        
        # Check CEX entity names
        for entity_name, entity in master_lists['entity_name_to_cex'].items():
            if entity_name in current_label_lower or any(word in current_label_lower for word in entity_name.split()):
                enhancement.update({
                    'enhanced': True,
                    'entity_name': entity['entity_name'],
                    'address_type': entity['address_type'],
                    'label': f"{entity['entity_name']} - {current_label}",
                    'confidence': 0.7,  # Medium confidence for name match
                    'source': 'defillama_cex_name_match',
                    'analysis_tags': {
                        'defillama_slug': entity.get('defillama_slug'),
                        'defillama_category': entity.get('defillama_category'),
                        'official_url': entity.get('official_url'),
                        'all_chains': entity.get('all_chains_from_defillama', []),
                        'match_type': 'entity_name_fuzzy'
                    }
                })
                self.logger.debug(f"CEX name match for {address}: {entity['entity_name']}")
                return enhancement
        
        return enhancement
    
    def collect_all_data(self, include_realtime: bool = False, realtime_duration: int = 5) -> Dict[str, List]:
        """
        Collect address data from all sources (APIs, GitHub repositories, Analytics, and BigQuery).
        
        Args:
            include_realtime: Whether to include real-time WebSocket data
            realtime_duration: Duration in minutes for real-time collection
            
        Returns:
            Dict containing collected data from different sources
        """
        collected_data = {
            'api_data': [],
            'github_data': [],
            'realtime_data': [],
            'analytics_data': [],
            'bigquery_data': []
        }
        
        try:
            # Collect API data
            self.logger.info("=== Starting comprehensive data collection ===")
            
            api_data = self.collect_api_data()
            collected_data['api_data'] = api_data
            
            # Collect GitHub data
            github_data = self.collect_github_data()
            collected_data['github_data'] = github_data
            
            # Collect Analytics data (whale addresses from analytics platforms)
            analytics_data = self.collect_whale_data_from_analytics_platforms()
            collected_data['analytics_data'] = analytics_data
            
            # Collect BigQuery public data
            bigquery_data = self.collect_bigquery_public_data_addresses()
            collected_data['bigquery_data'] = bigquery_data
            
            # Collect real-time data if requested
            if include_realtime:
                realtime_data = asyncio.run(self.collect_realtime_api_data(realtime_duration))
                collected_data['realtime_data'] = realtime_data
            
            total_addresses = (len(api_data) + len(github_data) + len(analytics_data) + 
                             len(bigquery_data) + len(collected_data['realtime_data']))
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
            
            # Store analytics data
            for analytics_addr in collected_data.get('analytics_data', []):
                try:
                    record = {
                        'address': analytics_addr.address,
                        'blockchain': analytics_addr.blockchain,
                        'label': analytics_addr.initial_label,
                        'source': analytics_addr.source_system,
                        'confidence': analytics_addr.confidence_score,
                        'address_type': 'collected'
                    }
                    
                    result = self.supabase_client.table('addresses').insert(record).execute()
                    total_stored += 1
                    
                except Exception as e:
                    self.logger.error(f"Failed to store analytics address {analytics_addr.address}: {e}")
                    continue
            
            # Store BigQuery data
            for bigquery_addr in collected_data.get('bigquery_data', []):
                try:
                    record = {
                        'address': bigquery_addr.address,
                        'blockchain': bigquery_addr.blockchain,
                        'label': bigquery_addr.initial_label,
                        'source': bigquery_addr.source_system,
                        'confidence': bigquery_addr.confidence_score,
                        'address_type': 'collected'
                    }
                    
                    result = self.supabase_client.table('addresses').insert(record).execute()
                    total_stored += 1
                    
                except Exception as e:
                    self.logger.error(f"Failed to store BigQuery address {bigquery_addr.address}: {e}")
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
            analytics_count = len(collected_data.get('analytics_data', []))
            bigquery_count = len(collected_data.get('bigquery_data', []))
            total_count = api_count + github_count + realtime_count + analytics_count + bigquery_count
            
            results['statistics'] = {
                'api_addresses': api_count,
                'github_addresses': github_count,
                'realtime_addresses': realtime_count,
                'analytics_addresses': analytics_count,
                'bigquery_addresses': bigquery_count,
                'total_addresses': total_count,
                'unique_blockchains': len(set(
                    [addr.blockchain for addr in collected_data.get('api_data', [])] +
                    [addr.blockchain for addr in collected_data.get('github_data', [])] +
                    [addr.blockchain for addr in collected_data.get('realtime_data', [])] +
                    [addr.blockchain for addr in collected_data.get('analytics_data', [])] +
                    [addr.blockchain for addr in collected_data.get('bigquery_data', [])]
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
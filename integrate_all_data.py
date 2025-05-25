#!/usr/bin/env python3
"""
Comprehensive Data Integration Script - Phase 2

This script integrates all address data sources:
1. API data collection (9 sources)
2. GitHub repository extraction (6 sources)
3. Existing address files (addresses.py, market_makers.py)
4. Deduplication algorithm
5. Supabase storage
6. BigQuery integration

Usage:
    python integrate_all_data.py
"""

import sys
import os
import hashlib
from typing import Dict, List, Set, Tuple
from datetime import datetime
import logging

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.blockchain_data_processor import BlockchainDataProcessor
from utils.api_integrations import AddressData
from utils.github_data_extractor import GitHubAddressData

# Import existing address data
from data.addresses import DEX_ADDRESSES, SOLANA_DEX_ADDRESSES, MARKET_MAKER_ADDRESSES, known_exchange_addresses
from data.market_makers import MARKET_MAKER_ADDRESSES as MM_ADDRESSES

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_integration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class AddressDeduplicator:
    """Handles address deduplication and conflict resolution."""
    
    def __init__(self):
        self.seen_addresses: Set[str] = set()
        self.address_registry: Dict[str, Dict] = {}
        self.conflicts: List[Dict] = []
        
    def normalize_address(self, address: str) -> str:
        """Normalize address format for comparison."""
        address = address.strip().lower()
        
        # Ethereum addresses
        if address.startswith('0x') and len(address) == 42:
            return address.lower()
        
        # Solana addresses (base58)
        elif len(address) >= 32 and len(address) <= 44 and not address.startswith('0x'):
            return address  # Keep original case for Solana
        
        # Bitcoin addresses
        elif address.startswith(('1', '3', 'bc1')):
            return address  # Keep original case for Bitcoin
        
        # Other blockchain addresses
        else:
            return address.lower()
    
    def calculate_confidence_score(self, source_system: str, initial_confidence: float = 0.5) -> float:
        """Calculate confidence score based on source reliability."""
        source_weights = {
            # High confidence sources
            'ofac_addresses_repo': 0.95,
            'etherscan_labels_repo': 0.85,
            'sybil_list_repo': 0.85,
            'eth_labels_repo': 0.80,
            
            # Medium confidence sources
            'whale_alert_rest': 0.75,
            'whale_alert_ws': 0.75,
            'moralis_api': 0.70,
            'etherscan_api': 0.70,
            'polygonscan_api': 0.70,
            
            # Lower confidence sources
            'solscan_api': 0.60,
            'helius_api': 0.60,
            'covalent_api': 0.60,
            'bitquery_api': 0.65,
            'dune_api': 0.65,
            
            # Existing data (high confidence)
            'existing_dex_addresses': 0.90,
            'existing_exchange_addresses': 0.90,
            'existing_market_makers': 0.85,
            
            # BigQuery public datasets (medium-high confidence)
            'bq_public_ethereum_eoa': 0.65,
            'bq_public_ethereum_contract': 0.70,
            'bq_public_bitcoin_activity': 0.65,
        }
        
        return source_weights.get(source_system, initial_confidence)
    
    def resolve_conflict(self, existing: Dict, new: Dict) -> Dict:
        """Resolve conflicts when the same address has different labels."""
        # Prefer higher confidence sources
        if new['confidence_score'] > existing['confidence_score']:
            # Keep new data but merge metadata
            merged_metadata = existing.get('metadata', {})
            merged_metadata.update(new.get('metadata', {}))
            merged_metadata['previous_labels'] = existing.get('labels', [])
            
            new['metadata'] = merged_metadata
            return new
        
        # Keep existing but add new label as alternative
        existing_metadata = existing.get('metadata', {})
        existing_metadata['alternative_labels'] = existing_metadata.get('alternative_labels', [])
        existing_metadata['alternative_labels'].append({
            'label': new['label'],
            'source': new['source_system'],
            'confidence': new['confidence_score']
        })
        existing['metadata'] = existing_metadata
        
        return existing
    
    def add_address(self, address: str, label: str, source_system: str, 
                   blockchain: str, metadata: Dict = None, confidence_score: float = None) -> bool:
        """Add an address to the registry with deduplication."""
        normalized_addr = self.normalize_address(address)
        
        if not normalized_addr:
            return False
        
        # Calculate confidence score
        if confidence_score is None:
            confidence_score = self.calculate_confidence_score(source_system)
        
        new_entry = {
            'address': normalized_addr,
            'original_address': address,  # Keep original format
            'label': label,
            'source_system': source_system,
            'blockchain': blockchain,
            'confidence_score': confidence_score,
            'metadata': metadata or {},
            'collected_at': datetime.utcnow().isoformat()
        }
        
        # Check for duplicates
        if normalized_addr in self.address_registry:
            existing = self.address_registry[normalized_addr]
            
            # Log conflict
            self.conflicts.append({
                'address': normalized_addr,
                'existing_label': existing['label'],
                'existing_source': existing['source_system'],
                'new_label': label,
                'new_source': source_system,
                'resolution': 'merged'
            })
            
            # Resolve conflict
            resolved = self.resolve_conflict(existing, new_entry)
            self.address_registry[normalized_addr] = resolved
            
            logger.info(f"Conflict resolved for {normalized_addr}: {existing['label']} vs {label}")
            
        else:
            # New address
            self.address_registry[normalized_addr] = new_entry
            self.seen_addresses.add(normalized_addr)
        
        return True
    
    def get_all_addresses(self) -> List[Dict]:
        """Get all deduplicated addresses."""
        return list(self.address_registry.values())
    
    def get_statistics(self) -> Dict:
        """Get deduplication statistics."""
        blockchains = {}
        sources = {}
        
        for addr_data in self.address_registry.values():
            blockchain = addr_data['blockchain']
            source = addr_data['source_system']
            
            blockchains[blockchain] = blockchains.get(blockchain, 0) + 1
            sources[source] = sources.get(source, 0) + 1
        
        return {
            'total_unique_addresses': len(self.address_registry),
            'total_conflicts_resolved': len(self.conflicts),
            'blockchains': blockchains,
            'sources': sources,
            'conflicts': self.conflicts
        }


class ComprehensiveDataIntegrator:
    """Main class for comprehensive data integration."""
    
    def __init__(self):
        self.processor = BlockchainDataProcessor()
        self.deduplicator = AddressDeduplicator()
        self.logger = logging.getLogger(__name__)
        
    def load_existing_addresses(self):
        """Load addresses from existing Python files."""
        self.logger.info("Loading existing address data...")
        
        # Load DEX addresses
        for address, label in DEX_ADDRESSES.items():
            self.deduplicator.add_address(
                address=address,
                label=f"DEX: {label}",
                source_system="existing_dex_addresses",
                blockchain="ethereum",
                metadata={"category": "dex", "original_label": label}
            )
        
        # Load Solana DEX addresses
        for address, label in SOLANA_DEX_ADDRESSES.items():
            self.deduplicator.add_address(
                address=address,
                label=f"Solana DEX: {label}",
                source_system="existing_dex_addresses",
                blockchain="solana",
                metadata={"category": "dex", "original_label": label}
            )
        
        # Load known exchange addresses
        for address, label in known_exchange_addresses.items():
            self.deduplicator.add_address(
                address=address,
                label=f"Exchange: {label}",
                source_system="existing_exchange_addresses",
                blockchain="ethereum",
                metadata={"category": "exchange", "original_label": label}
            )
        
        # Load market maker addresses (from MARKET_MAKER_ADDRESSES)
        for address, label in MARKET_MAKER_ADDRESSES.items():
            blockchain = "solana" if len(address) > 35 else "ethereum"
            self.deduplicator.add_address(
                address=address,
                label=f"Market Maker: {label}",
                source_system="existing_market_makers",
                blockchain=blockchain,
                metadata={"category": "market_maker", "original_label": label}
            )
        
        # Load market maker addresses (from MM_ADDRESSES)
        for address, label in MM_ADDRESSES.items():
            if address.startswith('r'):  # XRP address
                blockchain = "xrp"
            elif len(address) > 35:  # Solana address
                blockchain = "solana"
            else:  # Ethereum address
                blockchain = "ethereum"
                
            self.deduplicator.add_address(
                address=address,
                label=f"Market Maker: {label}",
                source_system="existing_market_makers",
                blockchain=blockchain,
                metadata={"category": "market_maker", "original_label": label}
            )
        
        self.logger.info("Existing address data loaded successfully")
    
    def collect_api_data(self):
        """Collect data from all APIs with enhanced volume and 30-day filtering."""
        self.logger.info("Collecting data from APIs (enhanced: up to 5000 per source, last 30 days)...")
        
        if not self.processor.initialize_data_acquisition():
            self.logger.error("Failed to initialize data acquisition")
            return
        
        # Collect API data with enhanced parameters
        api_addresses = self.processor.collect_api_data()
        
        # Log collection statistics by source
        source_stats = {}
        blockchain_stats = {}
        for addr_data in api_addresses:
            source = addr_data.source_system
            blockchain = addr_data.blockchain
            source_stats[source] = source_stats.get(source, 0) + 1
            blockchain_stats[blockchain] = blockchain_stats.get(blockchain, 0) + 1
        
        self.logger.info("Enhanced API Collection Statistics:")
        self.logger.info(f"Total addresses collected: {len(api_addresses)}")
        self.logger.info("By source:")
        for source, count in sorted(source_stats.items()):
            self.logger.info(f"  {source}: {count} addresses")
        self.logger.info("By blockchain:")
        for blockchain, count in sorted(blockchain_stats.items()):
            self.logger.info(f"  {blockchain}: {count} addresses")
        
        for addr_data in api_addresses:
            self.deduplicator.add_address(
                address=addr_data.address,
                label=addr_data.initial_label,
                source_system=addr_data.source_system,
                blockchain=addr_data.blockchain,
                metadata=addr_data.metadata,
                confidence_score=addr_data.confidence_score
            )
        
        self.logger.info(f"Processed {len(api_addresses)} addresses from APIs (enhanced collection)")
    
    def collect_github_data(self):
        """Collect data from GitHub repositories."""
        self.logger.info("Collecting data from GitHub repositories...")
        
        if not self.processor.initialize_data_acquisition():
            self.logger.error("Failed to initialize data acquisition")
            return
        
        # Collect GitHub data
        github_addresses = self.processor.collect_github_data()
        
        for addr_data in github_addresses:
            self.deduplicator.add_address(
                address=addr_data.address,
                label=addr_data.initial_label,
                source_system=addr_data.source_system,
                blockchain=addr_data.blockchain,
                metadata=addr_data.metadata,
                confidence_score=addr_data.confidence_score
            )
        
        self.logger.info(f"Processed {len(github_addresses)} addresses from GitHub")
    
    def collect_bigquery_public_dataset_data(self):
        """Collect data from BigQuery public datasets (Ethereum, Bitcoin)."""
        self.logger.info("Collecting data from BigQuery public datasets...")
        
        if not self.processor.initialize_data_acquisition():
            self.logger.error("Failed to initialize data acquisition")
            return
        
        # Collect BigQuery public dataset data
        public_dataset_addresses = self.processor.collect_bigquery_public_data_addresses(limit_per_query=5000)
        
        # Log collection statistics by source
        source_stats = {}
        blockchain_stats = {}
        for addr_data in public_dataset_addresses:
            source = addr_data.source_system
            blockchain = addr_data.blockchain
            source_stats[source] = source_stats.get(source, 0) + 1
            blockchain_stats[blockchain] = blockchain_stats.get(blockchain, 0) + 1
        
        self.logger.info("BigQuery Public Dataset Collection Statistics:")
        self.logger.info(f"Total addresses collected: {len(public_dataset_addresses)}")
        self.logger.info("By source:")
        for source, count in sorted(source_stats.items()):
            self.logger.info(f"  {source}: {count} addresses")
        self.logger.info("By blockchain:")
        for blockchain, count in sorted(blockchain_stats.items()):
            self.logger.info(f"  {blockchain}: {count} addresses")
        
        for addr_data in public_dataset_addresses:
            self.deduplicator.add_address(
                address=addr_data.address,
                label=addr_data.initial_label,
                source_system=addr_data.source_system,
                blockchain=addr_data.blockchain,
                metadata=addr_data.metadata,
                confidence_score=addr_data.confidence_score
            )
        
        self.logger.info(f"Processed {len(public_dataset_addresses)} addresses from BigQuery public datasets")
    
    def store_to_supabase(self) -> bool:
        """Store all deduplicated addresses to Supabase."""
        self.logger.info("Storing addresses to Supabase...")
        
        try:
            if not self.processor.supabase_client:
                self.processor.create_supabase_client()
            
            all_addresses = self.deduplicator.get_all_addresses()
            stored_count = 0
            error_count = 0
            
            # Store in batches
            batch_size = 100
            for i in range(0, len(all_addresses), batch_size):
                batch = all_addresses[i:i + batch_size]
                
                try:
                    # Prepare batch data for Supabase using the correct schema
                    supabase_batch = []
                    for addr_data in batch:
                        record = {
                            'address': addr_data['original_address'],
                            'blockchain': addr_data['blockchain'],
                            'label': addr_data.get('label', ''),
                            'source': addr_data.get('source_system', ''),
                            'confidence': addr_data.get('confidence_score', 0.5),
                            'created_at': addr_data.get('collected_at'),
                            'address_type': 'collected',
                            'entity_name': addr_data.get('label', '').split(':')[-1].strip() if addr_data.get('label') else ''
                        }
                        supabase_batch.append(record)
                    
                    # Insert batch with upsert to handle duplicates using the unique constraint
                    result = self.processor.supabase_client.table('addresses').upsert(
                        supabase_batch, 
                        on_conflict='address'
                    ).execute()
                    
                    stored_count += len(batch)
                    self.logger.info(f"Stored batch {i//batch_size + 1}: {len(batch)} addresses")
                    
                except Exception as e:
                    self.logger.error(f"Failed to store batch {i//batch_size + 1}: {e}")
                    error_count += len(batch)
                    continue
            
            self.logger.info(f"Storage complete: {stored_count} stored, {error_count} errors")
            return error_count == 0
            
        except Exception as e:
            self.logger.error(f"Failed to store to Supabase: {e}")
            return False
    
    def setup_bigquery_integration(self):
        """Set up BigQuery integration for querying."""
        self.logger.info("Setting up BigQuery integration...")
        
        try:
            if not self.processor.bigquery_client:
                self.processor.create_bigquery_client()
            
            # Create dataset if it doesn't exist
            dataset_id = "blockchain_addresses"
            dataset_ref = self.processor.bigquery_client.dataset(dataset_id)
            
            try:
                dataset = self.processor.bigquery_client.get_dataset(dataset_ref)
                self.logger.info(f"Dataset {dataset_id} already exists")
            except:
                # Create dataset
                from google.cloud import bigquery
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "US"
                dataset = self.processor.bigquery_client.create_dataset(dataset)
                self.logger.info(f"Created dataset {dataset_id}")
            
            # Import BigQuery here to avoid import issues
            from google.cloud import bigquery
            
            # Create table schema
            table_id = f"{dataset_id}.addresses"
            schema = [
                bigquery.SchemaField("address", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("blockchain", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("label", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("source_system", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("confidence_score", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("metadata", "JSON", mode="NULLABLE"),
                bigquery.SchemaField("collected_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("data_type", "STRING", mode="NULLABLE"),
            ]
            
            table_ref = self.processor.bigquery_client.dataset(dataset_id).table("addresses")
            
            try:
                table = self.processor.bigquery_client.get_table(table_ref)
                self.logger.info("BigQuery table already exists")
            except:
                # Create table
                table = bigquery.Table(table_ref, schema=schema)
                table = self.processor.bigquery_client.create_table(table)
                self.logger.info("Created BigQuery table")
            
            # Load data from Supabase to BigQuery
            self.sync_supabase_to_bigquery()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to setup BigQuery integration: {e}")
            return False
    
    def sync_supabase_to_bigquery(self):
        """Sync data from Supabase to BigQuery."""
        self.logger.info("Syncing data from Supabase to BigQuery...")
        
        try:
            # Get all addresses from Supabase
            result = self.processor.supabase_client.table('addresses').select('*').execute()
            
            if not result.data:
                self.logger.warning("No data found in Supabase")
                return
            
            # Prepare data for BigQuery
            bq_data = []
            for record in result.data:
                bq_record = {
                    'address': record['address'],
                    'blockchain': record['blockchain'],
                    'label': record.get('label'),
                    'source_system': record['source'],
                    'confidence_score': record.get('confidence'),
                    'metadata': record.get('metadata'),
                    'collected_at': record['collected_at'],
                    'data_type': record.get('data_type', 'integrated')
                }
                bq_data.append(bq_record)
            
            # Import BigQuery here too
            from google.cloud import bigquery
            
            # Load to BigQuery
            table_id = "blockchain_addresses.addresses"
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",  # Replace existing data
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            
            job = self.processor.bigquery_client.load_table_from_json(
                bq_data, table_id, job_config=job_config
            )
            job.result()  # Wait for job to complete
            
            self.logger.info(f"Synced {len(bq_data)} records to BigQuery")
            
        except Exception as e:
            self.logger.error(f"Failed to sync to BigQuery: {e}")
    
    def run_bigquery_analysis(self):
        """Run predefined BigQuery queries for analysis and return classification results."""
        self.logger.info("Running BigQuery analysis...")
        if not self.processor.bigquery_client:
            self.logger.error("BigQuery client not initialized. Skipping analysis.")
            return {}

        # Classification queries that return specific addresses for tagging
        classification_queries = {
            "potential_whale_addresses_bq": '''
                SELECT DISTINCT address
                FROM `sodium-pager-460916-j2.blockchain_addresses.addresses`
                WHERE 
                    (
                        LOWER(label) LIKE '%whale%' OR
                        LOWER(source_system) LIKE '%whale_alert%' OR
                        (LOWER(label) LIKE '%high activity%' AND confidence_score > 0.7)
                    )
                    AND confidence_score > 0.6
                ORDER BY address
                LIMIT 5000
            ''',
            "potential_mev_bot_addresses_bq": '''
                SELECT DISTINCT address
                FROM `sodium-pager-460916-j2.blockchain_addresses.addresses`
                WHERE 
                    (
                        LOWER(label) LIKE '%mev bot%' OR
                        LOWER(label) LIKE '%arbitrage%' OR
                        LOWER(label) LIKE '%sandwich%' OR
                        LOWER(label) LIKE '%frontrun%'
                    )
                    AND confidence_score > 0.6
                ORDER BY address
                LIMIT 5000
            ''',
            "potential_arbitrage_addresses_bq": '''
                SELECT DISTINCT address
                FROM `sodium-pager-460916-j2.blockchain_addresses.addresses`
                WHERE 
                    (
                        LOWER(label) LIKE '%arbitrage%' OR
                        LOWER(label) LIKE '%arb%' OR
                        LOWER(source_system) LIKE '%bitquery%'
                    )
                    AND confidence_score > 0.6
                ORDER BY address
                LIMIT 5000
            ''',
            "potential_large_trader_addresses_bq": '''
                SELECT DISTINCT address
                FROM `sodium-pager-460916-j2.blockchain_addresses.addresses`
                WHERE 
                    (
                        LOWER(label) LIKE '%large trader%' OR
                        LOWER(label) LIKE '%high activity%' OR
                        LOWER(label) LIKE '%active%'
                    )
                    AND confidence_score > 0.6
                ORDER BY address
                LIMIT 5000
            ''',
            "potential_exchange_addresses_bq": '''
                SELECT DISTINCT address
                FROM `sodium-pager-460916-j2.blockchain_addresses.addresses`
                WHERE 
                    (
                        LOWER(label) LIKE '%exchange%' OR
                        LOWER(source_system) LIKE '%exchange%' OR
                        LOWER(label) LIKE '%binance%' OR
                        LOWER(label) LIKE '%coinbase%' OR
                        LOWER(label) LIKE '%kraken%' OR
                        LOWER(label) LIKE '%okx%'
                    )
                    AND confidence_score > 0.7
                ORDER BY address
                LIMIT 5000
            ''',
            "potential_defi_protocol_addresses_bq": '''
                SELECT DISTINCT address
                FROM `sodium-pager-460916-j2.blockchain_addresses.addresses`
                WHERE 
                    (
                        LOWER(label) LIKE '%dex%' OR
                        LOWER(label) LIKE '%uniswap%' OR
                        LOWER(label) LIKE '%sushiswap%' OR
                        LOWER(label) LIKE '%pancakeswap%' OR
                        LOWER(label) LIKE '%contract%' OR
                        LOWER(source_system) LIKE '%contract%'
                    )
                    AND confidence_score > 0.6
                ORDER BY address
                LIMIT 5000
            '''
        }

        # Statistical queries for reporting (don't return addresses for tagging)
        statistical_queries = {
            "addresses_per_blockchain": '''
                SELECT blockchain, COUNT(DISTINCT address) as unique_address_count
                FROM `sodium-pager-460916-j2.blockchain_addresses.addresses`
                GROUP BY blockchain
                ORDER BY unique_address_count DESC
            ''',
            "top_labeled_entity_types_bq": '''
                SELECT 
                    CASE 
                        WHEN STRPOS(label, ':') > 0 THEN SUBSTR(label, 0, STRPOS(label, ':')-1)
                        ELSE label 
                    END as entity_type,
                    COUNT(DISTINCT address) as unique_address_count,
                    AVG(confidence_score) as avg_confidence
                FROM `sodium-pager-460916-j2.blockchain_addresses.addresses`
                WHERE label IS NOT NULL AND label != ''
                GROUP BY entity_type
                ORDER BY unique_address_count DESC
                LIMIT 5000
            '''
        }

        # Execute classification queries and collect results
        analysis_results = {}
        
        self.logger.info("=== Executing Classification Queries ===")
        for tag_name, query_sql in classification_queries.items():
            try:
                self.logger.info(f"Executing classification query: {tag_name}")
                query_job = self.processor.bigquery_client.query(query_sql)
                results = query_job.result()
                
                # Extract addresses from results
                addresses = [row.address for row in results if row.address]
                analysis_results[tag_name.replace('_bq', '')] = addresses
                
                self.logger.info(f"Found {len(addresses)} addresses for {tag_name}")
                
            except Exception as e:
                self.logger.error(f"Failed to execute classification query {tag_name}: {e}")
                analysis_results[tag_name.replace('_bq', '')] = []

        # Execute statistical queries for reporting
        self.logger.info("=== Executing Statistical Queries ===")
        for query_name, query_sql in statistical_queries.items():
            try:
                print(f"\n--- Running Statistical Query: {query_name} ---")
                self.logger.info(f"Executing statistical query: {query_name}")
                query_job = self.processor.bigquery_client.query(query_sql)
                results = query_job.result()

                if results.total_rows > 0:
                    print(f"Results for {query_name}:")
                    rows = [dict(row) for row in results]
                    for row_dict in rows[:20]:  # Limit display to first 20 rows
                        print(row_dict)
                    if results.total_rows > 20:
                        print(f"... and {results.total_rows - 20} more rows")
                else:
                    print(f"No results found for {query_name}.")
                
            except Exception as e:
                self.logger.error(f"Failed to execute statistical query {query_name}: {e}")
                print(f"Error running query {query_name}: {e}")

        # Log summary of classification results
        total_classified_addresses = sum(len(addresses) for addresses in analysis_results.values())
        self.logger.info(f"=== Classification Summary ===")
        self.logger.info(f"Total addresses classified: {total_classified_addresses}")
        for tag, addresses in analysis_results.items():
            self.logger.info(f"  {tag}: {len(addresses)} addresses")

        return analysis_results
    
    def update_supabase_with_analysis_tags(self, analysis_results: Dict[str, List[str]]):
        """Update Supabase addresses with analysis tags from BigQuery classification."""
        self.logger.info("=== Starting Supabase Analysis Tags Update ===")
        
        if not analysis_results:
            self.logger.warning("No analysis results to process")
            return
        
        if not self.processor.supabase_client:
            self.processor.create_supabase_client()
        
        total_updates = 0
        total_errors = 0
        
        for classification_tag, address_list in analysis_results.items():
            if not address_list:
                self.logger.info(f"No addresses found for tag: {classification_tag}")
                continue
            
            self.logger.info(f"Processing {len(address_list)} addresses for tag: {classification_tag}")
            
            # Process addresses in batches for efficiency
            batch_size = 50
            for i in range(0, len(address_list), batch_size):
                batch = address_list[i:i + batch_size]
                batch_updates = 0
                batch_errors = 0
                
                for address in batch:
                    try:
                        # Fetch current analysis_tags for this address
                        result = self.processor.supabase_client.table('addresses').select('analysis_tags').eq('address', address).execute()
                        
                        if not result.data:
                            self.logger.warning(f"Address {address} not found in Supabase")
                            batch_errors += 1
                            continue
                        
                        # Get current tags (handle NULL case)
                        current_tags = result.data[0].get('analysis_tags') or []
                        
                        # Ensure current_tags is a list
                        if not isinstance(current_tags, list):
                            current_tags = []
                        
                        # Add new tag if not already present (ensure uniqueness)
                        if classification_tag not in current_tags:
                            updated_tags = current_tags + [classification_tag]
                            
                            # Update the record in Supabase
                            update_result = self.processor.supabase_client.table('addresses').update({
                                'analysis_tags': updated_tags
                            }).eq('address', address).execute()
                            
                            if update_result.data:
                                batch_updates += 1
                            else:
                                self.logger.error(f"Failed to update address {address} with tag {classification_tag}")
                                batch_errors += 1
                        else:
                            # Tag already exists, no update needed
                            self.logger.debug(f"Tag {classification_tag} already exists for address {address}")
                            batch_updates += 1  # Count as successful (idempotent)
                        
                    except Exception as e:
                        self.logger.error(f"Error updating address {address} with tag {classification_tag}: {e}")
                        batch_errors += 1
                        continue
                
                total_updates += batch_updates
                total_errors += batch_errors
                
                self.logger.info(f"Batch {i//batch_size + 1} for {classification_tag}: {batch_updates} updates, {batch_errors} errors")
            
            self.logger.info(f"Completed tag {classification_tag}: {len([a for a in address_list if a])} addresses processed")
        
        self.logger.info("=== Supabase Analysis Tags Update Complete ===")
        self.logger.info(f"Total successful updates: {total_updates}")
        self.logger.info(f"Total errors: {total_errors}")
        
        return {
            'total_updates': total_updates,
            'total_errors': total_errors,
            'success_rate': (total_updates / (total_updates + total_errors)) * 100 if (total_updates + total_errors) > 0 else 0
        }
    
    def run_comprehensive_integration(self):
        """Run the complete enhanced data integration workflow."""
        self.logger.info("=== Starting Enhanced Comprehensive Data Integration ===")
        self.logger.info("🚀 Enhancement Features:")
        self.logger.info("   • Up to 5,000 addresses per API endpoint/category")
        self.logger.info("   • 30-day filtering for recent activity")
        self.logger.info("   • High-value transaction filtering (>$1000)")
        self.logger.info("   • Significant holder filtering (>$100 value)")
        self.logger.info("   • Enhanced pagination and rate limiting")
        self.logger.info("   • BigQuery public datasets integration (Ethereum, Bitcoin)")
        self.logger.info("   • High-activity EOAs and smart contracts from public data")
        self.logger.info("   • Automated address classification and tagging")
        self.logger.info("   • Enriched Supabase data with analysis tags for whale algorithms")
        
        start_time = datetime.utcnow()
        
        try:
            # Step 1: Load existing addresses
            self.load_existing_addresses()
            
            # Step 2: Collect enhanced API data
            self.collect_api_data()
            
            # Step 3: Collect GitHub data
            self.collect_github_data()
            
            # Step 4: Collect BigQuery public dataset data
            self.collect_bigquery_public_dataset_data()
            
            # Step 5: Get statistics
            stats = self.deduplicator.get_statistics()
            
            self.logger.info("=== Enhanced Integration Statistics ===")
            self.logger.info(f"Total unique addresses: {stats['total_unique_addresses']:,}")
            self.logger.info(f"Conflicts resolved: {stats['total_conflicts_resolved']:,}")
            self.logger.info("Blockchain distribution:")
            for blockchain, count in sorted(stats['blockchains'].items()):
                self.logger.info(f"  {blockchain}: {count:,} addresses")
            self.logger.info("Source distribution:")
            for source, count in sorted(stats['sources'].items()):
                self.logger.info(f"  {source}: {count:,} addresses")
            
            # Step 6: Store to Supabase
            storage_success = self.store_to_supabase()
            
            # Step 7: Setup BigQuery integration
            bigquery_success = self.setup_bigquery_integration()
            
            # Step 8: Run BigQuery analysis and get classification results
            self.logger.info("=== Step 8: Running BigQuery Analysis ===")
            analysis_data = self.run_bigquery_analysis()
            
            # Step 9: Update Supabase with analysis tags
            self.logger.info("=== Step 9: Updating Supabase with Analysis Tags ===")
            tagging_results = self.update_supabase_with_analysis_tags(analysis_data)
            
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            self.logger.info("=== Enhanced Integration Complete ===")
            self.logger.info(f"Duration: {duration:.2f} seconds")
            self.logger.info(f"Final dataset size: {stats['total_unique_addresses']:,} unique addresses")
            self.logger.info(f"Supabase storage: {'✅ SUCCESS' if storage_success else '❌ FAILED'}")
            self.logger.info(f"BigQuery integration: {'✅ SUCCESS' if bigquery_success else '❌ FAILED'}")
            
            if tagging_results:
                self.logger.info(f"Analysis tagging: {'✅ SUCCESS' if tagging_results['total_errors'] == 0 else '⚠️ PARTIAL'}")
                self.logger.info(f"  • Tagged addresses: {tagging_results['total_updates']:,}")
                self.logger.info(f"  • Success rate: {tagging_results['success_rate']:.1f}%")
            
            return {
                'success': True,
                'statistics': stats,
                'duration_seconds': duration,
                'supabase_success': storage_success,
                'bigquery_success': bigquery_success,
                'analysis_tagging': tagging_results,
                'classification_summary': {tag: len(addresses) for tag, addresses in analysis_data.items()} if analysis_data else {}
            }
            
        except Exception as e:
            self.logger.error(f"Enhanced integration failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'duration_seconds': (datetime.utcnow() - start_time).total_seconds()
            }


def main():
    """Main function to run enhanced comprehensive data integration."""
    print("🚀 Enhanced Comprehensive Data Integration - Phase 2")
    print("=" * 70)
    print("🔥 ENHANCED FEATURES:")
    print("   • Up to 5,000 addresses per API endpoint/category")
    print("   • 30-day filtering for recent activity")
    print("   • High-value transaction filtering (>$1000)")
    print("   • Significant holder filtering (>$100 value)")
    print("   • Enhanced pagination and rate limiting")
    print("   • BigQuery public datasets integration (Ethereum, Bitcoin)")
    print("   • High-activity EOAs and smart contracts from public data")
    print("   • Automated address classification and tagging")
    print("   • Enriched Supabase data with analysis tags for whale algorithms")
    print("=" * 70)
    print()
    
    integrator = ComprehensiveDataIntegrator()
    results = integrator.run_comprehensive_integration()
    
    print("\n" + "=" * 70)
    if results['success']:
        print("🎉 Enhanced Data Integration Complete!")
        print(f"📊 Total Addresses: {results['statistics']['total_unique_addresses']:,}")
        print(f"🔄 Conflicts Resolved: {results['statistics']['total_conflicts_resolved']:,}")
        print(f"⏱️  Duration: {results['duration_seconds']:.2f} seconds")
        
        print("\n📈 Blockchain Distribution:")
        for blockchain, count in sorted(results['statistics']['blockchains'].items()):
            print(f"  {blockchain}: {count:,} addresses")
        
        print("\n🔗 Data Sources:")
        for source, count in sorted(results['statistics']['sources'].items()):
            print(f"  {source}: {count:,} addresses")
        
        print(f"\n💾 Supabase: {'✅ Stored' if results['supabase_success'] else '❌ Failed'}")
        print(f"📊 BigQuery: {'✅ Ready' if results['bigquery_success'] else '❌ Failed'}")
        
        # Display analysis tagging results
        if results.get('analysis_tagging'):
            tagging = results['analysis_tagging']
            status = '✅ Complete' if tagging['total_errors'] == 0 else '⚠️ Partial'
            print(f"🏷️  Analysis Tagging: {status}")
            print(f"   • Tagged addresses: {tagging['total_updates']:,}")
            print(f"   • Success rate: {tagging['success_rate']:.1f}%")
        
        # Display classification summary
        if results.get('classification_summary'):
            print(f"\n🎯 Address Classifications:")
            for tag, count in results['classification_summary'].items():
                print(f"   • {tag.replace('_', ' ').title()}: {count:,} addresses")
    
    else:
        print("❌ Enhanced Data Integration Failed!")
        print(f"Error: {results.get('error', 'Unknown error')}")
        print(f"Duration: {results['duration_seconds']:.2f} seconds")


if __name__ == "__main__":
    main() 
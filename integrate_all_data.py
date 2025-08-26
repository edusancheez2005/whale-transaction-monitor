#!/usr/bin/env python3
"""
Comprehensive Data Integration Script - All Phases Integrated

This script integrates all address data sources across all phases:
Phase 1: Static address files and helper utilities
Phase 2: API data collection, GitHub extraction, basic BigQuery integration
Phase 3: Advanced BigQuery heuristics, intelligent post-processing, clustering

Features:
1. Static address collection from known sources
2. Analytics platform integration (Dune, manual files)
3. Live API data collection (9+ sources)
4. GitHub repository extraction (6+ sources)
5. BigQuery public dataset integration
6. Advanced BigQuery analysis with sophisticated heuristics
7. Intelligent post-processing with cross-referencing
8. Address clustering and confidence scoring
9. Automated classification and tagging
10. Comprehensive reporting and analytics

Usage:
    python integrate_all_data.py
"""

import sys
import os
import hashlib
from typing import Dict, List, Set, Tuple, Optional
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
    """Handles address deduplication and conflict resolution with enhanced capabilities."""
    
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
        """Calculate confidence score based on source reliability with Phase 3 enhancements."""
        source_weights = {
            # Highest confidence sources (Phase 1 static + analytics platforms)
            'ofac_addresses_repo': 0.95,
            'etherscan_labels_repo': 0.90,
            'sybil_list_repo': 0.90,
            'eth_labels_repo': 0.85,
            'existing_dex_addresses': 0.95,
            'existing_exchange_addresses': 0.95,
            'existing_market_makers': 0.90,
            'dune_analytics_whale_data': 0.85,
            'manual_analytics_platform_data': 0.80,
            
            # High confidence sources (Phase 2 APIs)
            'whale_alert_rest': 0.80,
            'whale_alert_ws': 0.80,
            'moralis_api': 0.75,
            'etherscan_api': 0.75,
            'polygonscan_api': 0.75,
            
            # Medium-high confidence sources
            'solscan_api': 0.65,
            'helius_api': 0.65,
            'covalent_api': 0.65,
            'bitquery_api': 0.70,
            'dune_api': 0.70,
            
            # BigQuery sources (Phase 2 basic + Phase 3 advanced)
            'bq_public_ethereum_eoa': 0.70,
            'bq_public_ethereum_contract': 0.75,
            'bq_public_bitcoin_activity': 0.70,
            'bq_advanced_exchange_detection': 0.85,
            'bq_advanced_whale_identification': 0.80,
            'bq_advanced_defi_analysis': 0.75,
            
            # Phase 3 refined sources
            'phase3_exchange_refined': 0.90,
            'phase3_whale_refined': 0.85,
            'phase3_defi_refined': 0.80,
        }
        
        return source_weights.get(source_system, initial_confidence)
    
    def resolve_conflict(self, existing: Dict, new: Dict) -> Dict:
        """Enhanced conflict resolution with Phase 3 intelligence."""
        # Prefer higher confidence sources
        if new['confidence_score'] > existing['confidence_score']:
            # Keep new data but merge metadata
            merged_metadata = existing.get('metadata', {})
            merged_metadata.update(new.get('metadata', {}))
            merged_metadata['previous_labels'] = existing.get('labels', [])
            merged_metadata['conflict_resolution'] = 'higher_confidence_preferred'
            
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
        existing_metadata['conflict_resolution'] = 'existing_preferred_with_alternatives'
        existing['metadata'] = existing_metadata
        
        return existing
    
    def add_address(self, address: str, label: str, source_system: str, 
                   blockchain: str, metadata: Dict = None, confidence_score: float = None) -> bool:
        """Add an address to the registry with enhanced deduplication."""
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
            
            logger.debug(f"Conflict resolved for {normalized_addr}: {existing['label']} vs {label}")
            
        else:
            # New address
            self.address_registry[normalized_addr] = new_entry
            self.seen_addresses.add(normalized_addr)
        
        return True
    
    def get_all_addresses(self) -> List[Dict]:
        """Get all deduplicated addresses."""
        return list(self.address_registry.values())
    
    def get_known_addresses_dict(self) -> Dict[str, str]:
        """Get a dictionary of known addresses for Phase 3 cross-referencing."""
        known_dict = {}
        for addr_data in self.address_registry.values():
            # Only include high-confidence addresses for cross-referencing
            if addr_data.get('confidence_score', 0) >= 0.7:
                known_dict[addr_data['address']] = addr_data['label']
        return known_dict
    
    def get_statistics(self) -> Dict:
        """Get comprehensive deduplication statistics."""
        blockchains = {}
        sources = {}
        confidence_distribution = {'high': 0, 'medium': 0, 'low': 0}
        
        for addr_data in self.address_registry.values():
            blockchain = addr_data['blockchain']
            source = addr_data['source_system']
            confidence = addr_data.get('confidence_score', 0.5)
            
            blockchains[blockchain] = blockchains.get(blockchain, 0) + 1
            sources[source] = sources.get(source, 0) + 1
            
            if confidence >= 0.8:
                confidence_distribution['high'] += 1
            elif confidence >= 0.6:
                confidence_distribution['medium'] += 1
            else:
                confidence_distribution['low'] += 1
        
        return {
            'total_unique_addresses': len(self.address_registry),
            'total_conflicts_resolved': len(self.conflicts),
            'blockchains': blockchains,
            'sources': sources,
            'confidence_distribution': confidence_distribution,
            'conflicts': self.conflicts
        }


class ComprehensiveDataIntegrator:
    """
    Main class for comprehensive data integration across all phases.
    
    This class orchestrates the complete pipeline:
    - Phase 1: Static address collection and helper utilities
    - Phase 2: Live API collection, GitHub extraction, basic BigQuery
    - Phase 3: Advanced BigQuery analysis, intelligent post-processing, clustering
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize the comprehensive data integrator.
        
        Args:
            config: Optional configuration dictionary for analytics platforms
        """
        self.processor = BlockchainDataProcessor()
        self.deduplicator = AddressDeduplicator()
        self.logger = logging.getLogger(__name__)
        
        # Configuration for analytics platforms (Phase 2 whale identification)
        self.config = config or {}
        self.dune_query_ids = self.config.get('dune_query_ids', [])
        self.manual_analytics_files = self.config.get('manual_analytics_files', {})
        
        # Statistics tracking
        self.pipeline_stats = {
            'phase1_addresses': 0,
            'phase2_api_addresses': 0,
            'phase2_github_addresses': 0,
            'phase2_bigquery_addresses': 0,
            'phase2_analytics_addresses': 0,
            'phase3_refined_addresses': 0,
            'total_processing_time': 0
        }
        
    def _load_all_known_addresses_and_labels(self) -> None:
        """
        Phase 1 + Phase 2 Analytics: Load all known addresses from static files and analytics platforms.
        """
        self.logger.info("=== Phase 1 + Analytics: Loading Known Addresses ===")
        
        # Phase 1: Load static address files
        self.logger.info("Loading static address files...")
        
        # Load DEX addresses
        for address, label in DEX_ADDRESSES.items():
            self.deduplicator.add_address(
                address=address,
                label=f"DEX: {label}",
                source_system="existing_dex_addresses",
                blockchain="ethereum",
                metadata={"category": "dex", "original_label": label, "phase": "1"}
            )
        
        # Load Solana DEX addresses
        for address, label in SOLANA_DEX_ADDRESSES.items():
            self.deduplicator.add_address(
                address=address,
                label=f"Solana DEX: {label}",
                source_system="existing_dex_addresses",
                blockchain="solana",
                metadata={"category": "dex", "original_label": label, "phase": "1"}
            )
        
        # Load known exchange addresses
        for address, label in known_exchange_addresses.items():
            self.deduplicator.add_address(
                address=address,
                label=f"Exchange: {label}",
                source_system="existing_exchange_addresses",
                blockchain="ethereum",
                metadata={"category": "exchange", "original_label": label, "phase": "1"}
            )
        
        # Load market maker addresses (from MARKET_MAKER_ADDRESSES)
        for address, label in MARKET_MAKER_ADDRESSES.items():
            blockchain = "solana" if len(address) > 35 else "ethereum"
            self.deduplicator.add_address(
                address=address,
                label=f"Market Maker: {label}",
                source_system="existing_market_makers",
                blockchain=blockchain,
                metadata={"category": "market_maker", "original_label": label, "phase": "1"}
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
                metadata={"category": "market_maker", "original_label": label, "phase": "1"}
            )
        
        phase1_count = len(self.deduplicator.get_all_addresses())
        self.pipeline_stats['phase1_addresses'] = phase1_count
        self.logger.info(f"Phase 1 static addresses loaded: {phase1_count}")
        
        # Phase 2: Load analytics platform data (whale identification)
        self.logger.info("Loading analytics platform data...")
        
        try:
            if not self.processor.initialize_whale_identification_system():
                self.logger.warning("Failed to initialize whale identification system")
                return
            
            # Collect whale data from analytics platforms
            whale_data = self.processor.collect_whale_data_from_analytics_platforms(
                dune_query_ids=self.dune_query_ids,
                manual_file_paths=self.manual_analytics_files
            )
            
            # Process Dune whale data
            for dune_whale in whale_data.get('dune_whales', []):
                address = dune_whale.get('address', '')
                if address:
                    self.deduplicator.add_address(
                        address=address,
                        label=f"Dune Whale: {dune_whale.get('label', 'Unknown')}",
                        source_system="dune_analytics_whale_data",
                        blockchain=dune_whale.get('blockchain', 'ethereum'),
                        metadata={
                            "category": "whale",
                            "dune_data": dune_whale,
                            "phase": "2_analytics"
                        }
                    )
            
            # Process manual analytics data
            for address, analytics_data in whale_data.get('manual_analytics_whales', {}).items():
                if address:
                    self.deduplicator.add_address(
                        address=address,
                        label=f"Analytics Whale: {analytics_data.get('label', 'Unknown')}",
                        source_system="manual_analytics_platform_data",
                        blockchain=analytics_data.get('blockchain', 'ethereum'),
                        metadata={
                            "category": "whale",
                            "analytics_data": analytics_data,
                            "phase": "2_analytics"
                        }
                    )
            
            analytics_count = whale_data.get('total_unique_whales', 0)
            self.pipeline_stats['phase2_analytics_addresses'] = analytics_count
            self.logger.info(f"Phase 2 analytics addresses loaded: {analytics_count}")
            
        except Exception as e:
            self.logger.error(f"Error loading analytics platform data: {e}")
        
        total_known = len(self.deduplicator.get_all_addresses())
        self.logger.info(f"Total known addresses loaded: {total_known}")
    
    def _collect_dynamic_data_from_live_sources(self) -> None:
        """
        Phase 2: Collect data from live sources (APIs, GitHub, BigQuery public datasets).
        """
        self.logger.info("=== Phase 2: Collecting Dynamic Data from Live Sources ===")
        
        if not self.processor.initialize_data_acquisition():
            self.logger.error("Failed to initialize data acquisition")
            return
        
        # Collect API data
        self.logger.info("Collecting enhanced API data (up to 5000 per source, last 30 days)...")
        api_addresses = self.processor.collect_api_data()
        
        for addr_data in api_addresses:
            metadata = addr_data.metadata or {}
            metadata['phase'] = '2_api'
            self.deduplicator.add_address(
                address=addr_data.address,
                label=addr_data.initial_label,
                source_system=addr_data.source_system,
                blockchain=addr_data.blockchain,
                metadata=metadata,
                confidence_score=addr_data.confidence_score
            )
        
        self.pipeline_stats['phase2_api_addresses'] = len(api_addresses)
        self.logger.info(f"Phase 2 API addresses collected: {len(api_addresses)}")
        
        # Collect GitHub data
        self.logger.info("Collecting GitHub repository data...")
        github_addresses = self.processor.collect_github_data()
        
        for addr_data in github_addresses:
            metadata = addr_data.metadata or {}
            metadata['phase'] = '2_github'
            self.deduplicator.add_address(
                address=addr_data.address,
                label=addr_data.initial_label,
                source_system=addr_data.source_system,
                blockchain=addr_data.blockchain,
                metadata=metadata,
                confidence_score=addr_data.confidence_score
            )
        
        self.pipeline_stats['phase2_github_addresses'] = len(github_addresses)
        self.logger.info(f"Phase 2 GitHub addresses collected: {len(github_addresses)}")
        
        # Collect BigQuery public dataset data
        self.logger.info("Collecting BigQuery public dataset data...")
        public_dataset_addresses = self.processor.collect_bigquery_public_data_addresses(limit_per_query=15000)
        
        for addr_data in public_dataset_addresses:
            metadata = addr_data.metadata or {}
            metadata['phase'] = '2_bigquery_public'
            self.deduplicator.add_address(
                address=addr_data.address,
                label=addr_data.initial_label,
                source_system=addr_data.source_system,
                blockchain=addr_data.blockchain,
                metadata=metadata,
                confidence_score=addr_data.confidence_score
            )
        
        self.pipeline_stats['phase2_bigquery_addresses'] = len(public_dataset_addresses)
        self.logger.info(f"Phase 2 BigQuery public addresses collected: {len(public_dataset_addresses)}")
        
        total_dynamic = len(api_addresses) + len(github_addresses) + len(public_dataset_addresses)
        self.logger.info(f"Total dynamic addresses collected: {total_dynamic}")
    
    def _store_raw_data_and_prepare_bq(self) -> bool:
        """
        Store all collected data to Supabase and prepare BigQuery for advanced analysis.
        """
        self.logger.info("=== Storing Raw Data and Preparing BigQuery ===")
        
        # Store to Supabase
        self.logger.info("Storing addresses to Supabase...")
        storage_success = self._store_to_supabase()
        
        if not storage_success:
            self.logger.error("Failed to store data to Supabase")
            return False
        
        # Setup BigQuery integration
        self.logger.info("Setting up BigQuery integration...")
        bigquery_success = self._setup_bigquery_integration()
        
        if not bigquery_success:
            self.logger.error("Failed to setup BigQuery integration")
            return False
        
        # Sync to BigQuery
        self.logger.info("Syncing data to BigQuery...")
        self._sync_supabase_to_bigquery()
        
        return True
    
    def _perform_advanced_analysis_and_refinement(self) -> Dict[str, List[str]]:
        """
        Phase 3: Perform advanced BigQuery analysis with intelligent post-processing.
        """
        self.logger.info("=== Phase 3: Advanced Analysis and Intelligent Refinement ===")
        
        if not self.processor.bigquery_public_data_manager:
            self.logger.warning("BigQuery public data manager not available, skipping advanced analysis")
            return {}
        
        try:
            # Get comprehensive known addresses for cross-referencing
            known_addresses = self.deduplicator.get_known_addresses_dict()
            self.logger.info(f"Using {len(known_addresses)} known addresses for cross-referencing")
            
            # Run comprehensive advanced BigQuery analysis with enhanced cross-referencing
            analysis_results = self.processor.run_advanced_bigquery_analysis(
                query_types=['exchange', 'whale', 'defi'],
                chain='ethereum',
                lookback_days=30
            )
            
            self.logger.info("=== Phase 3 Advanced Analysis Results ===")
            self.logger.info(f"Analysis timestamp: {analysis_results.get('analysis_timestamp')}")
            
            # Process refined results for address tagging
            phase3_classification_results = {}
            
            refined_results = analysis_results.get('refined_results', {})
            total_refined = 0
            
            for query_type, results in refined_results.items():
                if results:
                    # Extract high-confidence addresses for tagging
                    high_confidence_addresses = []
                    for result in results:
                        confidence = result.get('confidence_score', 0.5)
                        if confidence >= 0.7:  # High confidence threshold
                            high_confidence_addresses.append(result['address'])
                            total_refined += 1
                    
                    # Map to classification tags with Phase 3 designation
                    if query_type == 'exchange':
                        phase3_classification_results['potential_exchange_addresses_phase3'] = high_confidence_addresses
                    elif query_type == 'whale':
                        phase3_classification_results['potential_whale_addresses_phase3'] = high_confidence_addresses
                    elif query_type == 'defi':
                        phase3_classification_results['potential_defi_power_users_phase3'] = high_confidence_addresses
                    
                    self.logger.info(f"Phase 3 {query_type}: {len(high_confidence_addresses)} high-confidence addresses")
            
            # Log summary statistics
            summary = analysis_results.get('summary_statistics', {})
            self.logger.info(f"Total addresses analyzed: {summary.get('total_addresses_analyzed', 0)}")
            self.logger.info(f"Confidence distribution: {summary.get('confidence_distribution', {})}")
            self.logger.info(f"Label distribution: {summary.get('label_distribution', {})}")
            
            self.pipeline_stats['phase3_refined_addresses'] = total_refined
            
            return phase3_classification_results
            
        except Exception as e:
            self.logger.error(f"Phase 3 advanced analysis failed: {e}")
            return {}
    
    def _update_final_tags_in_supabase(self, analysis_results: Dict[str, List[str]]) -> Dict:
        """
        Update Supabase with final analysis tags from Phase 3.
        """
        self.logger.info("=== Updating Final Tags in Supabase ===")
        
        if not analysis_results:
            self.logger.warning("No analysis results to process")
            return {'total_updates': 0, 'total_errors': 0, 'success_rate': 0}
        
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
                            batch_updates += 1  # Count as successful (idempotent)
                        
                    except Exception as e:
                        self.logger.error(f"Error updating address {address} with tag {classification_tag}: {e}")
                        batch_errors += 1
                        continue
                
                total_updates += batch_updates
                total_errors += batch_errors
                
                self.logger.info(f"Batch {i//batch_size + 1} for {classification_tag}: {batch_updates} updates, {batch_errors} errors")
            
            self.logger.info(f"Completed tag {classification_tag}: {len([a for a in address_list if a])} addresses processed")
        
        self.logger.info("=== Final Tags Update Complete ===")
        self.logger.info(f"Total successful updates: {total_updates}")
        self.logger.info(f"Total errors: {total_errors}")
        
        return {
            'total_updates': total_updates,
            'total_errors': total_errors,
            'success_rate': (total_updates / (total_updates + total_errors)) * 100 if (total_updates + total_errors) > 0 else 0
        }
    
    def _generate_and_log_reports(self) -> None:
        """
        Generate comprehensive reports and statistics from all phases.
        """
        self.logger.info("=== Generating Comprehensive Reports ===")
        
        # Execute statistical queries for reporting
        if self.processor.bigquery_client:
            statistical_queries = {
                "addresses_per_blockchain": '''
                    SELECT blockchain, COUNT(DISTINCT address) as unique_address_count
                    FROM `sodium-pager-460916-j2.blockchain_addresses.addresses`
                    GROUP BY blockchain
                    ORDER BY unique_address_count DESC
                ''',
                "top_labeled_entity_types": '''
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
                    LIMIT 20
                ''',
                "confidence_score_distribution": '''
                    SELECT 
                        CASE 
                            WHEN confidence_score >= 0.8 THEN 'High (0.8+)'
                            WHEN confidence_score >= 0.6 THEN 'Medium (0.6-0.8)'
                            WHEN confidence_score >= 0.4 THEN 'Low (0.4-0.6)'
                            ELSE 'Very Low (<0.4)'
                        END as confidence_tier,
                        COUNT(*) as address_count,
                        ROUND(AVG(confidence_score), 3) as avg_confidence
                    FROM `sodium-pager-460916-j2.blockchain_addresses.addresses`
                    GROUP BY confidence_tier
                    ORDER BY avg_confidence DESC
                '''
            }
            
            self.logger.info("=== Statistical Analysis Reports ===")
            for query_name, query_sql in statistical_queries.items():
                try:
                    print(f"\n--- {query_name.replace('_', ' ').title()} ---")
                    self.logger.info(f"Executing statistical query: {query_name}")
                    query_job = self.processor.bigquery_client.query(query_sql)
                    results = query_job.result()

                    if results.total_rows > 0:
                        rows = [dict(row) for row in results]
                        for row_dict in rows:
                            print(row_dict)
                    else:
                        print(f"No results found for {query_name}.")
                    
                except Exception as e:
                    self.logger.error(f"Failed to execute statistical query {query_name}: {e}")
                    print(f"Error running query {query_name}: {e}")
        
        # Log pipeline statistics
        print(f"\n--- Pipeline Processing Statistics ---")
        for stat_name, count in self.pipeline_stats.items():
            print(f"{stat_name.replace('_', ' ').title()}: {count:,}")
    
    def _store_to_supabase(self) -> bool:
        """Store all deduplicated addresses to Supabase."""
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
                    
                    # Insert batch with upsert to handle duplicates
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
    
    def _setup_bigquery_integration(self) -> bool:
        """Set up BigQuery integration for advanced analysis."""
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
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to setup BigQuery integration: {e}")
            return False
    
    def _sync_supabase_to_bigquery(self) -> None:
        """Sync data from Supabase to BigQuery."""
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
    
    def run_comprehensive_integration(self) -> Dict:
        """
        Run the complete integrated pipeline across all phases.
        
        Returns:
            Dict containing comprehensive results and statistics
        """
        self.logger.info("=== Starting Comprehensive Multi-Phase Integration ===")
        self.logger.info("🚀 All Phases Integrated:")
        self.logger.info("   • Phase 1: Static address files and helper utilities")
        self.logger.info("   • Phase 2: Live API collection, GitHub extraction, analytics platforms")
        self.logger.info("   • Phase 3: Advanced BigQuery heuristics and intelligent post-processing")
        self.logger.info("   • Enhanced deduplication with conflict resolution")
        self.logger.info("   • Cross-referencing with comprehensive known address database")
        self.logger.info("   • Address clustering and confidence scoring")
        self.logger.info("   • Automated classification and tagging")
        self.logger.info("   • Comprehensive reporting and analytics")
        
        start_time = datetime.utcnow()
        
        try:
            # Phase 1 + Analytics: Load all known addresses
            self._load_all_known_addresses_and_labels()
            
            # Phase 2: Collect dynamic data from live sources
            self._collect_dynamic_data_from_live_sources()
            
            # Store raw data and prepare BigQuery
            storage_success = self._store_raw_data_and_prepare_bq()
            
            # Phase 3: Advanced analysis and refinement
            analysis_results = self._perform_advanced_analysis_and_refinement()
            
            # Update final tags in Supabase
            tagging_results = self._update_final_tags_in_supabase(analysis_results)
            
            # Generate comprehensive reports
            self._generate_and_log_reports()
            
            # Get final statistics
            stats = self.deduplicator.get_statistics()
            
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            self.pipeline_stats['total_processing_time'] = duration
            
            self.logger.info("=== Comprehensive Integration Complete ===")
            self.logger.info(f"Duration: {duration:.2f} seconds")
            self.logger.info(f"Final dataset size: {stats['total_unique_addresses']:,} unique addresses")
            self.logger.info(f"Supabase storage: {'✅ SUCCESS' if storage_success else '❌ FAILED'}")
            
            if tagging_results:
                self.logger.info(f"Analysis tagging: {'✅ SUCCESS' if tagging_results['total_errors'] == 0 else '⚠️ PARTIAL'}")
                self.logger.info(f"  • Tagged addresses: {tagging_results['total_updates']:,}")
                self.logger.info(f"  • Success rate: {tagging_results['success_rate']:.1f}%")
            
            return {
                'success': True,
                'statistics': stats,
                'pipeline_stats': self.pipeline_stats,
                'duration_seconds': duration,
                'supabase_success': storage_success,
                'analysis_tagging': tagging_results,
                'classification_summary': {tag: len(addresses) for tag, addresses in analysis_results.items()} if analysis_results else {}
            }
            
        except Exception as e:
            self.logger.error(f"Comprehensive integration failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'duration_seconds': (datetime.utcnow() - start_time).total_seconds()
            }


def main():
    """Main function to run the comprehensive multi-phase data integration."""
    print("🚀 Comprehensive Multi-Phase Data Integration")
    print("=" * 80)
    print("🔥 ALL PHASES INTEGRATED:")
    print("   • Phase 1: Static address files and helper utilities")
    print("   • Phase 2: Live API collection (9+ sources), GitHub extraction (6+ sources)")
    print("   • Phase 2: Analytics platform integration (Dune, manual files)")
    print("   • Phase 2: BigQuery public dataset integration (Ethereum, Bitcoin)")
    print("   • Phase 3: Advanced BigQuery heuristics (exchange/whale/DeFi detection)")
    print("   • Phase 3: Intelligent post-processing with cross-referencing")
    print("   • Phase 3: Address clustering from transaction flow analysis")
    print("   • Phase 3: Enhanced confidence scoring with conflict resolution")
    print("   • Automated classification and comprehensive tagging")
    print("   • Advanced reporting and analytics across all data sources")
    print("=" * 80)
    print()
    
    # Optional configuration for analytics platforms
    config = {
        'dune_query_ids': [],  # Add Dune query IDs if available
        'manual_analytics_files': {}  # Add file paths if available
    }
    
    integrator = ComprehensiveDataIntegrator(config=config)
    results = integrator.run_comprehensive_integration()
    
    print("\n" + "=" * 80)
    if results['success']:
        print("🎉 Comprehensive Multi-Phase Integration Complete!")
        print(f"📊 Total Addresses: {results['statistics']['total_unique_addresses']:,}")
        print(f"🔄 Conflicts Resolved: {results['statistics']['total_conflicts_resolved']:,}")
        print(f"⏱️  Duration: {results['duration_seconds']:.2f} seconds")
        
        print("\n📈 Blockchain Distribution:")
        for blockchain, count in sorted(results['statistics']['blockchains'].items()):
            print(f"  {blockchain}: {count:,} addresses")
        
        print("\n🔗 Data Sources:")
        for source, count in sorted(results['statistics']['sources'].items()):
            print(f"  {source}: {count:,} addresses")
        
        print("\n📊 Pipeline Statistics:")
        for stat_name, count in results['pipeline_stats'].items():
            if stat_name != 'total_processing_time':
                print(f"  {stat_name.replace('_', ' ').title()}: {count:,}")
        
        print("\n🎯 Confidence Distribution:")
        conf_dist = results['statistics']['confidence_distribution']
        print(f"  High (≥0.8): {conf_dist['high']:,} addresses")
        print(f"  Medium (0.6-0.8): {conf_dist['medium']:,} addresses")
        print(f"  Low (<0.6): {conf_dist['low']:,} addresses")
        
        print(f"\n💾 Supabase: {'✅ Stored' if results['supabase_success'] else '❌ Failed'}")
        
        # Display analysis tagging results
        if results.get('analysis_tagging'):
            tagging = results['analysis_tagging']
            status = '✅ Complete' if tagging['total_errors'] == 0 else '⚠️ Partial'
            print(f"🏷️  Analysis Tagging: {status}")
            print(f"   • Tagged addresses: {tagging['total_updates']:,}")
            print(f"   • Success rate: {tagging['success_rate']:.1f}%")
        
        # Display classification summary
        if results.get('classification_summary'):
            print(f"\n🎯 Phase 3 Classifications:")
            for tag, count in results['classification_summary'].items():
                print(f"   • {tag.replace('_', ' ').title()}: {count:,} addresses")
    
    else:
        print("❌ Comprehensive Integration Failed!")
        print(f"Error: {results.get('error', 'Unknown error')}")
        print(f"Duration: {results['duration_seconds']:.2f} seconds")


if __name__ == "__main__":
    main() 
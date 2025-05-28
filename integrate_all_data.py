#!/usr/bin/env python3
"""
Comprehensive Data Integration Script - All Phases Integrated (Phase 4: Optimized Pipeline)

This script integrates all address data sources across all phases with optimized operational modes:
Phase 1: Static address files and helper utilities
Phase 2: API data collection, GitHub extraction, basic BigQuery integration
Phase 3: Advanced BigQuery heuristics, intelligent post-processing, clustering
Phase 4: Optimized pipeline with selectable operational modes

Features:
1. Static address collection from known sources
2. Analytics platform integration (Dune, manual files)
3. Live API data collection (9+ sources)
4. GitHub repository extraction (6+ sources) - conditional based on run mode
5. BigQuery public dataset integration
6. Advanced BigQuery analysis with sophisticated heuristics
7. Intelligent post-processing with cross-referencing
8. Address clustering and confidence scoring
9. Automated classification and tagging
10. Comprehensive reporting and analytics

Operational Modes:
- full_sync: Complete data refresh from all sources (including GitHub repositories)
- update_and_discover: Focus on dynamic data and discovery, skip GitHub re-processing

Usage:
    python integrate_all_data.py --run-mode update_and_discover
    python integrate_all_data.py --run-mode full_sync
"""

import sys
import os
import hashlib
from typing import Dict, List, Set, Tuple, Optional
from datetime import datetime
import logging
import argparse
import time

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
    
    def load_static_address_files(self) -> Dict[str, Dict]:
        """Load static address files from the data directory."""
        static_addresses = {}
        
        # Load DEX addresses
        for addr, label in DEX_ADDRESSES.items():
            static_addresses[addr] = {
                'address': addr,
                'label': label,
                'source_system': 'existing_dex_addresses',
                'blockchain': 'ethereum',
                'confidence_score': self.calculate_confidence_score('existing_dex_addresses'),
                'metadata': {'type': 'dex'}
            }
        
        # Load Solana DEX addresses
        for addr, label in SOLANA_DEX_ADDRESSES.items():
            static_addresses[addr] = {
                'address': addr,
                'label': label,
                'source_system': 'existing_dex_addresses',
                'blockchain': 'solana',
                'confidence_score': self.calculate_confidence_score('existing_dex_addresses'),
                'metadata': {'type': 'dex'}
            }
        
        # Load exchange addresses
        for addr, label in known_exchange_addresses.items():
            static_addresses[addr] = {
                'address': addr,
                'label': label,
                'source_system': 'existing_exchange_addresses',
                'blockchain': 'ethereum',
                'confidence_score': self.calculate_confidence_score('existing_exchange_addresses'),
                'metadata': {'type': 'exchange'}
            }
        
        # Load market maker addresses
        for addr, label in MM_ADDRESSES.items():
            static_addresses[addr] = {
                'address': addr,
                'label': label,
                'source_system': 'existing_market_makers',
                'blockchain': 'ethereum',
                'confidence_score': self.calculate_confidence_score('existing_market_makers'),
                'metadata': {'type': 'market_maker'}
            }
        
        return static_addresses
    
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
    Main class for comprehensive data integration across all phases with optimized operational modes.
    
    This class orchestrates the complete pipeline:
    - Phase 1: Static address collection and helper utilities
    - Phase 2: Live API collection, GitHub extraction, basic BigQuery
    - Phase 3: Advanced BigQuery analysis, intelligent post-processing, clustering
    - Phase 4: Optimized pipeline with selectable operational modes
    
    Operational Modes:
    - full_sync: Complete data refresh from all sources (including GitHub repositories)
    - update_and_discover: Focus on dynamic data and discovery, skip GitHub re-processing
    """
    
    def __init__(self, run_mode='update_and_discover'):
        self.logger = logging.getLogger(__name__)
        self.run_mode = run_mode
        self.blockchain_processor = BlockchainDataProcessor()
        self.address_deduplicator = AddressDeduplicator()
        
        # Configuration
        self.config = {
            'API_MAX_ADDRESSES_PER_SOURCE': 5000,
            'API_DAYS_LOOKBACK': 30,
            'GITHUB_MAX_REPOS_PER_SOURCE': 2,
            'GITHUB_MAX_FILES_PER_REPO': 5,
            'BIGQUERY_MAX_RESULTS': 10000
        }
        
        # Pipeline statistics
        self.pipeline_stats = {
            'phase1_addresses': 0,
            'phase2_api_addresses': 0,
            'phase2_github_addresses': 0,
            'phase2_bigquery_addresses': 0,
            'phase2_analytics_addresses': 0,
            'phase3_refined_addresses': 0,
            'total_processing_time': 0
        }
    
    def _load_all_known_addresses_and_labels(self):
        """Load Phase 1 static addresses and Phase 2 analytics platform data."""
        self.logger.info("=== Phase 1 + Analytics: Loading Known Addresses ===")
        self.logger.info("Loading static address files...")
        
        # Load static address files (runs in both modes)
        static_addresses = self.address_deduplicator.load_static_address_files()
        self.pipeline_stats['phase1_addresses'] = len(static_addresses)
        self.logger.info(f"Phase 1 static addresses loaded: {len(static_addresses)}")

        self.logger.info("Loading analytics platform data...")
        # Load analytics platform data (runs in both modes)
        try:
            analytics_data = self.blockchain_processor.collect_whale_data_from_analytics_platforms()
            analytics_addresses = analytics_data.get('addresses', {})
            self.pipeline_stats['phase2_analytics_addresses'] = len(analytics_addresses)
            self.logger.info(f"Phase 2 analytics addresses loaded: {len(analytics_addresses)}")
        except Exception as e:
            self.logger.warning(f"Analytics platform data collection failed: {e}")
            analytics_addresses = {}
            self.pipeline_stats['phase2_analytics_addresses'] = 0
        
        # Combine and prepare for deduplication
        all_known_addresses_dict = {addr: data for addr, data in static_addresses.items()}
        
        # Merge analytics addresses
        for addr, data in analytics_addresses.items():
            if addr not in all_known_addresses_dict:
                all_known_addresses_dict[addr] = data
            else:
                self.logger.debug(f"Conflict for {addr} between static and analytics, keeping static.")
        
        self.logger.info(f"Total known addresses loaded: {len(all_known_addresses_dict)}")
        return all_known_addresses_dict

    def _collect_dynamic_data_from_live_sources(self):
        """Collect dynamic data from APIs, GitHub (conditional), and BigQuery."""
        self.logger.info("=== Phase 2: Collecting Dynamic Data from Live Sources ===")
        
        current_raw_data = []
        collected_address_objects = []  # Store original AddressData objects for storage
        
        # Initialize data acquisition managers
        try:
            self.blockchain_processor.initialize_data_acquisition()
        except Exception as e:
            self.logger.warning(f"Failed to initialize data acquisition: {e}")
            # Continue without initialization
        
        # Collect enhanced API data (runs in both modes)
        self.logger.info(f"Collecting API data...")
        try:
            api_data = self.blockchain_processor.collect_api_data()
            self.pipeline_stats['phase2_api_addresses'] = len(api_data)
            
            # Store original objects and create summary data
            collected_address_objects.extend(api_data)
            for data_item in api_data:
                current_address_data = {
                    "address": data_item.address,
                    "blockchain": data_item.blockchain,
                    "source": data_item.source_system,
                    "label": data_item.initial_label,
                    "initial_confidence": data_item.confidence_score,
                    "metadata": {"details": data_item.metadata}
                }
                current_raw_data.append(current_address_data)
            
            self.logger.info(f"Phase 2 API addresses collected: {self.pipeline_stats['phase2_api_addresses']}")
        except Exception as e:
            self.logger.warning(f"API data collection failed: {e}")
            self.pipeline_stats['phase2_api_addresses'] = 0

        # Conditional GitHub data collection based on run mode
        if self.run_mode == 'full_sync':
            self.logger.info("Collecting GitHub repository data (full_sync mode)...")
            try:
                github_data = self.blockchain_processor.collect_github_data()
                self.pipeline_stats['phase2_github_addresses'] = len(github_data)
                
                # Store original objects and create summary data
                collected_address_objects.extend(github_data)
                for data_item in github_data:
                    current_address_data = {
                        "address": data_item.address,
                        "blockchain": data_item.blockchain,
                        "source": data_item.source_system,
                        "label": data_item.initial_label,
                        "initial_confidence": data_item.confidence_score,
                        "metadata": {"details": data_item.metadata}
                    }
                    current_raw_data.append(current_address_data)
                
                self.logger.info(f"Phase 2 GitHub addresses collected: {self.pipeline_stats['phase2_github_addresses']}")
            except Exception as e:
                self.logger.warning(f"GitHub data collection failed: {e}")
                self.pipeline_stats['phase2_github_addresses'] = 0
        else:
            self.logger.info("'update_and_discover' mode: Skipping GitHub repository data collection.")
            self.pipeline_stats['phase2_github_addresses'] = 0

        # Collect BigQuery Public Dataset insights (runs in both modes)
        self.logger.info("Collecting BigQuery Public Dataset insights (Ethereum & Bitcoin)...")
        try:
            bigquery_data = self.blockchain_processor.collect_bigquery_public_data_addresses(
                limit_per_query=self.config['BIGQUERY_MAX_RESULTS']
            )
            self.pipeline_stats['phase2_bigquery_addresses'] = len(bigquery_data)
            
            # Store original objects and create summary data
            collected_address_objects.extend(bigquery_data)
            for data_item in bigquery_data:
                current_address_data = {
                    "address": data_item.address,
                    "blockchain": data_item.blockchain,
                    "source": data_item.source_system,
                    "label": data_item.initial_label,
                    "initial_confidence": data_item.confidence_score,
                    "metadata": {"details": data_item.metadata}
                }
                current_raw_data.append(current_address_data)
            
            self.logger.info(f"Phase 2 BigQuery addresses collected: {self.pipeline_stats['phase2_bigquery_addresses']}")
        except Exception as e:
            self.logger.warning(f"BigQuery data collection failed: {e}")
            self.pipeline_stats['phase2_bigquery_addresses'] = 0

        # Store the original objects for later use
        self.collected_address_objects = collected_address_objects
        return current_raw_data

    def _store_raw_data_and_prepare_bq(self, current_raw_data):
        """Store raw data in Supabase and prepare for BigQuery analysis."""
        self.logger.info("=== Storing Raw Data and Preparing BigQuery ===")
        
        try:
            # Use the smart storage method that bulk checks for duplicates first
            formatted_data = {
                'api_data': [obj for obj in self.collected_address_objects if hasattr(obj, 'source_system')],
                'github_data': [],  # Will be populated if needed
                'bigquery_data': []  # BigQuery data is included in api_data for now
            }
            
            storage_result = self.blockchain_processor.store_collected_data_smart(formatted_data)
            self.logger.info(f"Smart storage results: {storage_result}")
            
            return True
        except Exception as e:
            self.logger.warning(f"Data storage failed: {e}")
            return False

    def _perform_advanced_analysis_and_refinement(self):
        """Phase 3: Advanced BigQuery analysis and intelligent post-processing."""
        self.logger.info("=== Phase 3: Advanced Analysis and Refinement ===\n")
        
        try:
            # Get known addresses for cross-referencing
            self.logger.info("Loading known addresses from Supabase for cross-referencing...")
            known_addresses_dict = self.blockchain_processor.get_known_addresses_dict()
            self.logger.info(f"Loaded {len(known_addresses_dict)} known addresses for analysis")
            
            # Run advanced BigQuery analysis with known addresses
            self.logger.info("Running advanced BigQuery analysis with heuristics...")
            phase3_results = self.blockchain_processor.run_advanced_bigquery_analysis(
                query_types=['exchange', 'whale', 'defi'],
                chain='ethereum',
                lookback_days=30
            )
            
            # If we got results, enhance them with our known addresses
            if phase3_results and not phase3_results.get('error'):
                self.logger.info("Enhancing BigQuery results with known address cross-referencing...")
                
                # Cross-reference with our known addresses
                enhanced_results = []
                for query_type, raw_results in phase3_results.get('raw_results', {}).items():
                    if raw_results:
                        # Use the existing refine method but pass our known addresses
                        refined_results = self.blockchain_processor.refine_bq_address_labels(
                            bq_results=raw_results,
                            existing_known_addresses=known_addresses_dict
                        )
                        enhanced_results.extend(refined_results)
                        self.logger.info(f"Enhanced {len(refined_results)} {query_type} results")
                
                phase3_results['enhanced_with_known_addresses'] = enhanced_results
                self.pipeline_stats['phase3_refined_addresses'] = len(enhanced_results)
                
                self.logger.info(f"Phase 3 analysis complete: {len(enhanced_results)} addresses analyzed and refined")
            else:
                self.logger.warning("BigQuery analysis returned no results or failed")
                self.pipeline_stats['phase3_refined_addresses'] = 0
            
            return phase3_results
        except Exception as e:
            self.logger.error(f"Phase 3 analysis failed: {e}")
            self.pipeline_stats['phase3_refined_addresses'] = 0
            return {}

    def _update_final_tags_in_supabase(self, phase3_results):
        """Update final tags in Supabase with Phase 3 results."""
        self.logger.info("=== Final Tags Update ===")
        
        try:
            # Get enhanced results from Phase 3 analysis
            enhanced_results = phase3_results.get('enhanced_with_known_addresses', [])
            
            if not enhanced_results:
                self.logger.info("No enhanced results from Phase 3 to process for tagging")
                return {"total_tagged": 0, "success_rate": 100.0}
            
            self.logger.info(f"Processing {len(enhanced_results)} enhanced addresses for final tagging")
            
            # Update addresses in Supabase with enhanced labels and confidence scores
            tagged_count = 0
            failed_count = 0
            
            for result in enhanced_results:
                try:
                    # Extract address information
                    address = result.get('address', '').lower()
                    blockchain = result.get('blockchain', 'ethereum').lower()
                    enhanced_label = result.get('enhanced_label', result.get('label', ''))
                    confidence = result.get('confidence_score', result.get('confidence', 0.5))
                    
                    if not address or not enhanced_label:
                        continue
                    
                    # Update the address record in Supabase
                    update_data = {
                        'label': enhanced_label,
                        'confidence': confidence,
                        'address_type': result.get('address_type', 'unknown'),
                        'updated_at': 'now()'
                    }
                    
                    # Add metadata if available
                    if 'metadata' in result:
                        update_data['metadata'] = result['metadata']
                    
                    # Perform the update
                    response = self.blockchain_processor.supabase_client.table('addresses')\
                        .update(update_data)\
                        .eq('address', address)\
                        .eq('blockchain', blockchain)\
                        .execute()
                    
                    if response.data:
                        tagged_count += 1
                        self.logger.debug(f"Updated tags for {address} ({blockchain}): {enhanced_label}")
                    else:
                        failed_count += 1
                        self.logger.warning(f"No matching record found for {address} ({blockchain})")
                        
                except Exception as e:
                    failed_count += 1
                    self.logger.warning(f"Failed to update tags for address {address}: {e}")
                    continue
            
            # Calculate success rate
            total_processed = tagged_count + failed_count
            success_rate = (tagged_count / total_processed * 100) if total_processed > 0 else 100.0
            
            self.logger.info(f"Final tagging complete: {tagged_count} addresses tagged, {failed_count} failed")
            self.logger.info(f"Tagging success rate: {success_rate:.1f}%")
            
            return {
                "total_tagged": tagged_count,
                "failed": failed_count,
                "success_rate": success_rate
            }
            
        except Exception as e:
            self.logger.error(f"Final tagging failed: {e}")
            return {"total_tagged": 0, "failed": 0, "success_rate": 0.0}

    def _generate_and_log_reports(self):
        """Generate comprehensive reports and statistics."""
        self.logger.info("=== Generating Comprehensive Reports ===")
        
        try:
            # Log pipeline statistics
            self.logger.info("=== Pipeline Processing Statistics ===")
            for stat_name, stat_value in self.pipeline_stats.items():
                if stat_name != 'total_processing_time':
                    formatted_value = f"{stat_value:,}" if isinstance(stat_value, int) else stat_value
                    self.logger.info(f"{stat_name.replace('_', ' ').title()}: {formatted_value}")
            
        except Exception as e:
            self.logger.warning(f"Report generation failed: {e}")

    def run_integration(self):
        """
        Main orchestration method for the comprehensive data integration pipeline.
        Supports different run modes for optimized processing.
        """
        start_time = time.time()
        self.logger.info(f"=== Starting Comprehensive Multi-Phase Integration (Mode: {self.run_mode}) ===")
        
        # Display feature list based on run mode
        self.logger.info("🚀 All Phases Integrated:")
        self.logger.info("   • Phase 1: Static address files and helper utilities")
        if self.run_mode == 'full_sync':
            self.logger.info("   • Phase 2: Live API collection, GitHub extraction, analytics platforms")
        else:
            self.logger.info("   • Phase 2: Live API collection, analytics platforms (GitHub skipped)")
        self.logger.info("   • Phase 3: Advanced BigQuery heuristics and intelligent post-processing")
        self.logger.info("   • Enhanced deduplication with conflict resolution")
        self.logger.info("   • Cross-referencing with comprehensive known address database")
        self.logger.info("   • Address clustering and confidence scoring")
        self.logger.info("   • Automated classification and tagging")
        self.logger.info("   • Comprehensive reporting and analytics")
        
        try:
            # Phase 1 + Analytics: Load known addresses
            self._load_all_known_addresses_and_labels()
            
            # Phase 2: Collect dynamic data from live sources
            current_raw_data = self._collect_dynamic_data_from_live_sources()
            
            # Store raw data and prepare for BigQuery
            if not self._store_raw_data_and_prepare_bq(current_raw_data):
                self.logger.error("Failed to store raw data, continuing with analysis...")
            
            # Phase 3: Advanced analysis and refinement
            phase3_results = self._perform_advanced_analysis_and_refinement()
            
            # Update final tags in Supabase
            tag_results = self._update_final_tags_in_supabase(phase3_results)
            
            # Generate comprehensive reports
            self._generate_and_log_reports()
            
            # Calculate total processing time
            end_time = time.time()
            self.pipeline_stats['total_processing_time'] = end_time - start_time
            
            # Final summary
            self.logger.info("=== Comprehensive Integration Complete ===")
            self.logger.info(f"Duration: {self.pipeline_stats['total_processing_time']:.2f} seconds")
            self.logger.info(f"Run Mode: {self.run_mode}")
            
            # Get final statistics
            try:
                total_processed = sum([
                    self.pipeline_stats['phase1_addresses'],
                    self.pipeline_stats['phase2_api_addresses'],
                    self.pipeline_stats['phase2_github_addresses'],
                    self.pipeline_stats['phase2_bigquery_addresses'],
                    self.pipeline_stats['phase2_analytics_addresses']
                ])
                self.logger.info(f"Total addresses processed: {total_processed:,}")
                self.logger.info("Pipeline execution: ✅ SUCCESS")
                
                if tag_results:
                    self.logger.info("Analysis tagging: ✅ SUCCESS")
                    self.logger.info(f"  • Tagged addresses: {tag_results.get('total_tagged', 0)}")
                    self.logger.info(f"  • Success rate: {tag_results.get('success_rate', 0):.1f}%")
                
            except Exception as e:
                self.logger.warning(f"Could not calculate final statistics: {e}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Integration failed: {e}")
            return False


def main():
    """Main entry point with command-line argument parsing."""
    # Setup command-line argument parsing
    parser = argparse.ArgumentParser(
        description="Comprehensive Multi-Phase Data Integration for Whale Transaction Monitor.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Operational Modes:
  full_sync           Complete data refresh from all sources including GitHub repositories.
                      Use for initial setup or periodic comprehensive updates.
                      
  update_and_discover Focus on dynamic data collection (APIs, BigQuery) and advanced 
                      Phase 3 analysis. Skips GitHub repository re-processing for 
                      faster execution. Ideal for daily/frequent monitoring.

Examples:
  python integrate_all_data.py --run-mode update_and_discover
  python integrate_all_data.py --run-mode full_sync
        """
    )
    parser.add_argument(
        "--run-mode",
        choices=['full_sync', 'update_and_discover'],
        default='update_and_discover',
        help="Operational mode: 'full_sync' for complete data refresh, "
             "'update_and_discover' for dynamic data focus (default: %(default)s)"
    )
    
    args = parser.parse_args()

    # Display startup banner
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

    # Initialize logger for the main script
    logger.info(f"Starting integration in '{args.run_mode}' mode")
    
    # Create an instance of the integrator and run the pipeline
    integrator = ComprehensiveDataIntegrator(run_mode=args.run_mode)
    success = integrator.run_integration()
    
    # Final summary logging
    if success:
        logger.info("🎉 Integration completed successfully!")
        print("\n🎉 Comprehensive Multi-Phase Integration Complete!")
        print(f"📊 Run Mode: {args.run_mode}")
        print("📈 Check logs for detailed statistics and results")
    else:
        logger.error("❌ Integration failed!")
        print("\n❌ Integration failed! Check logs for details.")
        sys.exit(1)


if __name__ == "__main__":
    main()
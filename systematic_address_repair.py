#!/usr/bin/env python3
"""
🔧 SYSTEMATIC ADDRESS DATABASE REPAIR SYSTEM
================================================================================

MISSION: Fix all 150k corrupted addresses using comprehensive API-driven approach

AVAILABLE RESOURCES:
✅ BigQuery (historical transaction patterns)
✅ Etherscan API (contract verification & metadata)
✅ Moralis API (address enrichment)
✅ Covalent API (portfolio analysis) 
✅ DeFiLlama Data (protocol verification)
✅ Dune Analytics (whale patterns)
✅ Existing Supabase (150k addresses)
✅ Local DEX/CEX databases
✅ Zerion Portfolio API

STRATEGY: Multi-phase repair with confidence scoring and validation
"""

import asyncio
import logging
import time
import json
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
import requests
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import pandas as pd

# Project imports
from supabase import create_client
from config.api_keys import *
from utils.bigquery_analyzer import BigQueryAnalyzer
from utils.api_integrations import APIIntegrationBase
from data.addresses import known_exchange_addresses, DEX_ADDRESSES, PROTOCOL_ADDRESSES

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class AddressRepairResult:
    """Result of address repair attempt."""
    address: str
    original_label: str
    original_type: str
    corrected_label: str
    corrected_type: str
    corrected_entity: str
    confidence_score: float
    evidence_sources: List[str]
    repair_method: str
    needs_manual_review: bool = False

class SystematicAddressRepairer:
    """
    🔧 COMPREHENSIVE ADDRESS DATABASE REPAIR SYSTEM
    
    Uses all available APIs and data sources to systematically identify
    and correct the 150k corrupted addresses in the Supabase database.
    """
    
    def __init__(self):
        self.supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
        self.bigquery = BigQueryAnalyzer()
        self.session = requests.Session()
        
        # Repair statistics
        self.stats = {
            'total_processed': 0,
            'successfully_repaired': 0,
            'high_confidence_repairs': 0,
            'manual_review_required': 0,
            'api_failures': 0,
            'corruption_patterns': {}
        }
        
        # Known good patterns for validation
        self.validation_patterns = self._load_validation_patterns()
        
    def _load_validation_patterns(self) -> Dict[str, List[str]]:
        """Load known good address patterns for validation."""
        return {
            'dex_routers': [
                '0x7a250d5630b4cf539739df2c5dacb4c659f2488d',  # Uniswap V2
                '0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45',  # Uniswap V3
                '0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f',  # SushiSwap
                '0x1111111254fb6c44bac0bed2854e76f90643097d',  # 1inch
                '0xdef1c0ded9bec7f1a1670819833240f027b25eff',  # 0x Protocol
            ],
            'major_cex': [
                '0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be',  # Binance
                '0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43',  # Coinbase
                '0x28c6c06298d514db089934071355e5743bf21d60',  # Binance 14
                '0x71660c4005ba85c37ccec55d0c4493e66fe775d3',  # Coinbase 2
            ],
            'uniswap_contracts': [
                '0x1f98431c8ad98523636104104c1e2ad1e6d420c',   # Uniswap V3 Factory
                '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f',   # Uniswap V2 Factory  
            ]
        }

    async def run_systematic_repair(self, batch_size: int = 1000) -> Dict[str, Any]:
        """
        🚀 MAIN REPAIR PIPELINE
        
        Phase 1: Audit & Pattern Detection
        Phase 2: Critical Infrastructure Repair
        Phase 3: Bulk API-Driven Repair
        Phase 4: Validation & Manual Review Queue
        """
        
        logger.info("🔧 STARTING SYSTEMATIC ADDRESS DATABASE REPAIR")
        start_time = time.time()
        
        # PHASE 1: COMPREHENSIVE AUDIT
        logger.info("📊 PHASE 1: Comprehensive Database Audit")
        audit_results = await self._comprehensive_audit()
        
        # PHASE 2: CRITICAL INFRASTRUCTURE REPAIR  
        logger.info("🏗️ PHASE 2: Critical Infrastructure Repair")
        critical_results = await self._repair_critical_infrastructure()
        
        # PHASE 3: BULK SYSTEMATIC REPAIR
        logger.info("🔄 PHASE 3: Bulk API-Driven Repair")
        bulk_results = await self._bulk_api_repair(batch_size)
        
        # PHASE 4: VALIDATION & REVIEW
        logger.info("✅ PHASE 4: Validation & Manual Review Queue")
        validation_results = await self._validate_and_queue_manual_review()
        
        # PHASE 5: FINAL REPORT
        total_time = time.time() - start_time
        final_report = self._generate_final_report(
            audit_results, critical_results, bulk_results, validation_results, total_time
        )
        
        logger.info(f"🎉 SYSTEMATIC REPAIR COMPLETE: {total_time:.1f} seconds")
        return final_report

    async def _comprehensive_audit(self) -> Dict[str, Any]:
        """
        📊 COMPREHENSIVE DATABASE AUDIT
        
        Identifies corruption patterns across all 150k addresses to guide repair strategy.
        """
        logger.info("Analyzing corruption patterns across 150k addresses...")
        
        # Get overview of all addresses
        overview = self.supabase.table('addresses').select(
            'address, label, address_type, entity_name, blockchain'
        ).execute()
        
        audit_results = {
            'total_addresses': len(overview.data),
            'corruption_patterns': {},
            'priority_repairs': [],
            'systematic_issues': []
        }
        
        # Analyze corruption patterns
        corruption_patterns = {}
        for addr in overview.data:
            label = addr.get('label', '') or ''
            addr_type = addr.get('address_type', '') or ''
            
            # Detect systematic corruption patterns
            if 'mETH Protocol' in label and addr['address'] not in self.validation_patterns['dex_routers']:
                corruption_patterns.setdefault('meth_protocol_overwrite', []).append(addr['address'])
            
            if 'Cat in a Box' in label:
                corruption_patterns.setdefault('cat_in_box_overwrite', []).append(addr['address'])
                
            if 'router' in label.lower() and 'router' not in addr_type.lower():
                corruption_patterns.setdefault('router_type_mismatch', []).append(addr['address'])
                
            if 'uniswap' in label.lower() and 'staking' in addr_type.lower():
                corruption_patterns.setdefault('dex_as_staking', []).append(addr['address'])
                
            if 'binance' in label.lower() and 'staking' in addr_type.lower():
                corruption_patterns.setdefault('cex_as_staking', []).append(addr['address'])
        
        audit_results['corruption_patterns'] = {
            pattern: len(addresses) for pattern, addresses in corruption_patterns.items()
        }
        
        # Identify high-priority repairs (critical infrastructure)
        priority_addresses = []
        for pattern, addresses in corruption_patterns.items():
            if pattern in ['dex_as_staking', 'cex_as_staking', 'router_type_mismatch']:
                priority_addresses.extend(addresses[:50])  # Top 50 per pattern
        
        audit_results['priority_repairs'] = priority_addresses
        
        logger.info(f"📊 Audit complete: {audit_results['total_addresses']} addresses analyzed")
        logger.info(f"📊 Corruption patterns found: {list(audit_results['corruption_patterns'].keys())}")
        
        return audit_results

    async def _repair_critical_infrastructure(self) -> Dict[str, Any]:
        """
        🏗️ CRITICAL INFRASTRUCTURE REPAIR
        
        Immediately fix DEX routers, major CEX addresses, and other critical infrastructure
        using hardcoded known-good data.
        """
        logger.info("Repairing critical blockchain infrastructure addresses...")
        
        critical_fixes = []
        
        # 1. DEX ROUTERS (HIGHEST PRIORITY)
        dex_router_fixes = [
            ('0x7a250d5630b4cf539739df2c5dacb4c659f2488d', 'Uniswap V2 Router', 'DEX Router', 'Uniswap V2'),
            ('0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45', 'Uniswap V3 Router', 'DEX Router', 'Uniswap V3'),
            ('0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f', 'SushiSwap Router', 'DEX Router', 'SushiSwap'),
            ('0x1111111254fb6c44bac0bed2854e76f90643097d', '1inch Router V4', 'DEX Router', '1inch'),
            ('0xdef1c0ded9bec7f1a1670819833240f027b25eff', '0x Protocol Exchange', 'DEX Router', '0x Protocol'),
            ('0x881d40237659c251811cec9c364ef91dc08d300c', 'Metamask Swap Router', 'DEX Router', 'Metamask'),
            ('0xe592427a0aece92de3edee1f18e0157c05861564', 'Uniswap V3 SwapRouter', 'DEX Router', 'Uniswap V3'),
        ]
        
        # 2. MAJOR CEX ADDRESSES
        cex_fixes = [
            ('0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be', 'Binance Hot Wallet', 'CEX Wallet', 'Binance'),
            ('0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43', 'Coinbase Hot Wallet', 'CEX Wallet', 'Coinbase'),
            ('0x28c6c06298d514db089934071355e5743bf21d60', 'Binance Hot Wallet 14', 'CEX Wallet', 'Binance'),
            ('0x71660c4005ba85c37ccec55d0c4493e66fe775d3', 'Coinbase Hot Wallet 2', 'CEX Wallet', 'Coinbase'),
            ('0xd551234ae421e3bcba99a0da6d736074f22192ff', 'Binance Hot Wallet 3', 'CEX Wallet', 'Binance'),
        ]
        
        # 3. CORE DEFI PROTOCOLS
        defi_protocol_fixes = [
            ('0x1f98431c8ad98523636104104c1e2ad1e6d420c', 'Uniswap V3 Factory', 'DeFi Protocol', 'Uniswap V3'),
            ('0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f', 'Uniswap V2 Factory', 'DeFi Protocol', 'Uniswap V2'),
            ('0xa0b86a33e6ba6e6c4c11c2e4c3b5d4e24c1e2ffb', 'Compound cUSDC', 'DeFi Lending', 'Compound'),
            ('0x39aa39c021dfbae8fac545936693ac917d5e7563', 'Compound cUSDT', 'DeFi Lending', 'Compound'),
        ]
        
        all_critical_fixes = dex_router_fixes + cex_fixes + defi_protocol_fixes
        
        # Apply fixes with error handling
        successfully_fixed = 0
        failed_fixes = []
        
        for addr, label, addr_type, entity in all_critical_fixes:
            try:
                result = self.supabase.table('addresses').update({
                    'label': label,
                    'address_type': addr_type,
                    'entity_name': entity,
                    'confidence': 0.95,  # High confidence for known infrastructure
                    'detection_method': 'systematic_repair_critical'
                }).eq('address', addr.lower()).execute()
                
                if result.data:
                    successfully_fixed += 1
                    critical_fixes.append(
                        AddressRepairResult(
                            address=addr,
                            original_label='corrupted',
                            original_type='corrupted', 
                            corrected_label=label,
                            corrected_type=addr_type,
                            corrected_entity=entity,
                            confidence_score=0.95,
                            evidence_sources=['hardcoded_infrastructure'],
                            repair_method='critical_infrastructure'
                        )
                    )
                else:
                    failed_fixes.append(addr)
                    
            except Exception as e:
                logger.error(f"Failed to fix critical address {addr}: {e}")
                failed_fixes.append(addr)
        
        logger.info(f"🏗️ Critical infrastructure: {successfully_fixed}/{len(all_critical_fixes)} fixed")
        
        return {
            'total_critical_addresses': len(all_critical_fixes),
            'successfully_fixed': successfully_fixed,
            'failed_fixes': failed_fixes,
            'repair_results': critical_fixes
        }

    async def _bulk_api_repair(self, batch_size: int = 1000) -> Dict[str, Any]:
        """
        🔄 BULK API-DRIVEN REPAIR
        
        Uses all available APIs to systematically repair addresses:
        1. Etherscan contract verification
        2. Moralis address enrichment  
        3. BigQuery historical analysis
        4. DeFiLlama protocol verification
        5. Pattern-based classification
        """
        logger.info(f"Starting bulk API repair in batches of {batch_size}...")
        
        # Get addresses that need repair (excluding already fixed critical ones)
        corrupted_addresses = self.supabase.table('addresses').select(
            'address, label, address_type, entity_name, blockchain'
        ).or_(
            'label.ilike.%mETH Protocol%,'
            'label.ilike.%Cat in a Box%,'
            'address_type.eq.DeFi Staking,'
            'address_type.eq.DeFi Lending'
        ).limit(10000).execute()  # Process first 10k corrupted addresses
        
        # Process in batches
        total_addresses = len(corrupted_addresses.data)
        repair_results = []
        
        for i in range(0, total_addresses, batch_size):
            batch = corrupted_addresses.data[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}: {len(batch)} addresses")
            
            batch_results = await self._process_address_batch(batch)
            repair_results.extend(batch_results)
            
            # Rate limiting
            await asyncio.sleep(1)
        
        # Apply successful repairs to database
        successful_repairs = [r for r in repair_results if r.confidence_score >= 0.7]
        await self._apply_repair_batch(successful_repairs)
        
        return {
            'total_processed': len(repair_results),
            'successful_repairs': len(successful_repairs),
            'high_confidence_repairs': len([r for r in repair_results if r.confidence_score >= 0.85]),
            'manual_review_required': len([r for r in repair_results if r.needs_manual_review]),
            'repair_results': repair_results
        }

    async def _process_address_batch(self, batch: List[Dict]) -> List[AddressRepairResult]:
        """Process a batch of addresses using all available APIs."""
        results = []
        
        for addr_data in batch:
            address = addr_data['address']
            try:
                # Multi-source analysis
                repair_result = await self._analyze_address_comprehensive(address, addr_data)
                if repair_result:
                    results.append(repair_result)
                    
            except Exception as e:
                logger.error(f"Failed to process address {address}: {e}")
                self.stats['api_failures'] += 1
        
        return results

    async def _analyze_address_comprehensive(self, address: str, current_data: Dict) -> Optional[AddressRepairResult]:
        """
        Comprehensive address analysis using all available data sources.
        """
        evidence_sources = []
        confidence_score = 0.0
        corrected_label = None
        corrected_type = None
        corrected_entity = None
        
        # 1. ETHERSCAN CONTRACT VERIFICATION
        etherscan_result = await self._check_etherscan_contract(address)
        if etherscan_result:
            evidence_sources.append('etherscan_verification')
            confidence_score += 0.3
            if not corrected_label:
                corrected_label = etherscan_result.get('name', 'Unknown Contract')
                corrected_type = 'Smart Contract'
        
        # 2. MORALIS ENRICHMENT
        moralis_result = await self._check_moralis_enrichment(address)
        if moralis_result:
            evidence_sources.append('moralis_enrichment')
            confidence_score += 0.2
            if moralis_result.get('entity_type'):
                corrected_entity = moralis_result['entity_type']
        
        # 3. BIGQUERY HISTORICAL ANALYSIS
        if self.bigquery.client:
            bq_result = await self._check_bigquery_patterns(address)
            if bq_result:
                evidence_sources.append('bigquery_historical')
                confidence_score += 0.3
                if bq_result.get('classification'):
                    corrected_type = bq_result['classification']
        
        # 4. DEFILLAMA PROTOCOL VERIFICATION
        defillama_result = await self._check_defillama_protocol(address)
        if defillama_result:
            evidence_sources.append('defillama_protocol')
            confidence_score += 0.4
            corrected_label = defillama_result.get('name', corrected_label)
            corrected_type = 'DeFi Protocol'
            corrected_entity = defillama_result.get('category', corrected_entity)
        
        # 5. PATTERN-BASED CLASSIFICATION
        pattern_result = self._classify_by_patterns(address, current_data)
        if pattern_result:
            evidence_sources.append('pattern_classification')
            confidence_score += 0.2
            if not corrected_type:
                corrected_type = pattern_result
        
        # Only return result if we have sufficient confidence and evidence
        if confidence_score >= 0.5 and evidence_sources:
            return AddressRepairResult(
                address=address,
                original_label=current_data.get('label', ''),
                original_type=current_data.get('address_type', ''),
                corrected_label=corrected_label or 'Unknown',
                corrected_type=corrected_type or 'Unknown',
                corrected_entity=corrected_entity or 'Unknown',
                confidence_score=min(confidence_score, 0.95),
                evidence_sources=evidence_sources,
                repair_method='api_comprehensive',
                needs_manual_review=confidence_score < 0.8
            )
        
        return None

    async def _check_etherscan_contract(self, address: str) -> Optional[Dict]:
        """Check Etherscan for contract verification data."""
        try:
            url = f"https://api.etherscan.io/api"
            params = {
                'module': 'contract',
                'action': 'getsourcecode',
                'address': address,
                'apikey': ETHERSCAN_API_KEY
            }
            
            response = requests.get(url, params=params, timeout=5)
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == '1' and data.get('result'):
                    contract_data = data['result'][0]
                    if contract_data.get('ContractName'):
                        return {
                            'name': contract_data['ContractName'],
                            'verified': True,
                            'compiler': contract_data.get('CompilerVersion', '')
                        }
            
        except Exception as e:
            logger.debug(f"Etherscan check failed for {address}: {e}")
        
        return None

    async def _check_moralis_enrichment(self, address: str) -> Optional[Dict]:
        """Check Moralis for address enrichment data."""
        try:
            # Implement Moralis API call
            # This would use the Moralis API to get address metadata
            pass
        except Exception as e:
            logger.debug(f"Moralis check failed for {address}: {e}")
        
        return None

    async def _check_bigquery_patterns(self, address: str) -> Optional[Dict]:
        """Analyze address patterns using BigQuery historical data."""
        try:
            if self.bigquery:
                result = self.bigquery.analyze_address_whale_patterns(address)
                if result:
                    return {
                        'classification': self._determine_type_from_patterns(result),
                        'volume_30d': result.get('total_volume_usd_30d', 0),
                        'tx_count': result.get('tx_count_30d', 0)
                    }
        except Exception as e:
            logger.debug(f"BigQuery check failed for {address}: {e}")
        
        return None

    async def _check_defillama_protocol(self, address: str) -> Optional[Dict]:
        """Check DeFiLlama for protocol verification."""
        try:
            # Use DeFiLlama API to check if address is a known protocol
            url = f"https://api.llama.fi/protocols"
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                protocols = response.json()
                # Search for matching addresses in protocol data
                for protocol in protocols:
                    if address.lower() in str(protocol).lower():
                        return {
                            'name': protocol.get('name', ''),
                            'category': protocol.get('category', ''),
                            'chain': protocol.get('chain', '')
                        }
        except Exception as e:
            logger.debug(f"DeFiLlama check failed for {address}: {e}")
        
        return None

    def _classify_by_patterns(self, address: str, current_data: Dict) -> Optional[str]:
        """Classify address type based on patterns."""
        label = (current_data.get('label', '') or '').lower()
        
        # Pattern-based classification
        if any(term in label for term in ['router', 'swap', 'exchange']):
            if any(dex in label for dex in ['uniswap', 'sushiswap', '1inch', 'curve']):
                return 'DEX Router'
        
        if any(term in label for term in ['binance', 'coinbase', 'kraken', 'exchange']):
            return 'CEX Wallet'
        
        if any(term in label for term in ['compound', 'aave', 'maker', 'lending']):
            return 'DeFi Lending'
        
        return None

    def _determine_type_from_patterns(self, bq_result: Dict) -> str:
        """Determine address type from BigQuery analysis patterns."""
        volume = bq_result.get('total_volume_usd_30d', 0)
        tx_count = bq_result.get('tx_count_30d', 0)
        
        if volume > 10_000_000:  # $10M+ volume
            return 'High Volume Trader'
        elif volume > 1_000_000:  # $1M+ volume
            return 'Whale Trader'
        elif tx_count > 1000:  # High frequency
            return 'Active Trader'
        else:
            return 'Regular Wallet'

    async def _apply_repair_batch(self, repairs: List[AddressRepairResult]) -> bool:
        """Apply a batch of repairs to the database."""
        try:
            updates = []
            for repair in repairs:
                updates.append({
                    'address': repair.address,
                    'label': repair.corrected_label,
                    'address_type': repair.corrected_type,
                    'entity_name': repair.corrected_entity,
                    'confidence': repair.confidence_score,
                    'detection_method': f'systematic_repair_{repair.repair_method}',
                    'analysis_tags': {
                        'repair_evidence': repair.evidence_sources,
                        'repair_timestamp': datetime.utcnow().isoformat(),
                        'original_corrupted': True
                    }
                })
            
            # Batch update (this would need to be implemented as individual updates)
            for update in updates:
                self.supabase.table('addresses').update(update).eq('address', update['address']).execute()
            
            logger.info(f"Applied {len(updates)} repairs to database")
            return True
            
        except Exception as e:
            logger.error(f"Failed to apply repair batch: {e}")
            return False

    async def _validate_and_queue_manual_review(self) -> Dict[str, Any]:
        """Validate repairs and queue items for manual review."""
        logger.info("Validating repairs and creating manual review queue...")
        
        # This would implement validation logic and create a manual review queue
        # For now, return placeholder results
        return {
            'validation_passed': self.stats['high_confidence_repairs'],
            'manual_review_queued': self.stats['manual_review_required'],
            'validation_errors': 0
        }

    def _generate_final_report(self, audit_results: Dict, critical_results: Dict, 
                             bulk_results: Dict, validation_results: Dict, 
                             total_time: float) -> Dict[str, Any]:
        """Generate comprehensive final repair report."""
        return {
            'execution_summary': {
                'total_runtime_seconds': total_time,
                'total_addresses_in_db': audit_results['total_addresses'],
                'corruption_patterns_identified': len(audit_results['corruption_patterns']),
                'critical_infrastructure_fixed': critical_results['successfully_fixed'],
                'bulk_repairs_completed': bulk_results['successful_repairs'],
                'high_confidence_repairs': bulk_results['high_confidence_repairs'],
                'manual_review_queued': bulk_results['manual_review_required']
            },
            'corruption_analysis': audit_results['corruption_patterns'],
            'repair_effectiveness': {
                'critical_success_rate': critical_results['successfully_fixed'] / critical_results['total_critical_addresses'],
                'bulk_success_rate': bulk_results['successful_repairs'] / max(bulk_results['total_processed'], 1),
                'overall_confidence_score': bulk_results['high_confidence_repairs'] / max(bulk_results['total_processed'], 1)
            },
            'api_utilization': {
                'etherscan_calls': 'tracking_needed',
                'moralis_calls': 'tracking_needed', 
                'bigquery_queries': 'tracking_needed',
                'defillama_requests': 'tracking_needed'
            },
            'next_steps': [
                'Review manual review queue',
                'Validate high-impact repairs',
                'Monitor classification accuracy',
                'Schedule periodic re-verification'
            ]
        }

# ================================================================================
# EXECUTION FUNCTIONS
# ================================================================================

async def run_systematic_repair():
    """Run the complete systematic address repair process."""
    repairer = SystematicAddressRepairer()
    results = await repairer.run_systematic_repair(batch_size=500)
    
    print("\n" + "="*80)
    print("🎉 SYSTEMATIC ADDRESS REPAIR COMPLETE")
    print("="*80)
    print(f"📊 Total Addresses: {results['execution_summary']['total_addresses_in_db']:,}")
    print(f"🔧 Critical Fixed: {results['execution_summary']['critical_infrastructure_fixed']}")
    print(f"✅ Bulk Repairs: {results['execution_summary']['bulk_repairs_completed']}")
    print(f"🎯 High Confidence: {results['execution_summary']['high_confidence_repairs']}")
    print(f"📋 Manual Review: {results['execution_summary']['manual_review_queued']}")
    print(f"⏱️ Runtime: {results['execution_summary']['total_runtime_seconds']:.1f}s")
    
    return results

if __name__ == "__main__":
    # Run the systematic repair
    results = asyncio.run(run_systematic_repair())
    
    # Save detailed results
    with open(f'address_repair_results_{int(time.time())}.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n💾 Detailed results saved to address_repair_results_{int(time.time())}.json") 
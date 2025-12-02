# super_simple_monitor.py

#!/usr/bin/env python3
"""
Production-Ready Crypto Transaction Monitor with Structured Logging

Features:
- Production-grade structured JSON logging
- Real-time whale intelligence analysis
- Color-coded transaction display
- Comprehensive transaction storage
- Clean summary reporting
"""

import os
import sys
import time
import signal
import threading
from collections import defaultdict
import traceback
import asyncio
import json
import logging
import argparse
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from colorama import Fore, Style

# Production logging imports
from config.logging_config import production_logger, get_transaction_logger
from utils.classification_final import WhaleIntelligenceEngine, ClassificationType

# Use the production logger throughout this module (including simulation path)
logger = production_logger
# Local imports
from config.settings import (
    shutdown_flag,
    GLOBAL_USD_THRESHOLD,
    etherscan_buy_counts,
    etherscan_sell_counts,
    whale_buy_counts,
    whale_sell_counts,
    solana_buy_counts,
    solana_sell_counts,
    xrp_buy_counts,
    xrp_sell_counts
)
from chains.ethereum import print_new_erc20_transfers, test_etherscan_connection
from chains.whale_alert import start_whale_thread
from chains.xrp import start_xrp_thread 
from chains.solana import start_solana_thread
from chains.polygon import print_new_polygon_transfers, test_polygonscan_connection
from chains.solana_api import print_new_solana_transfers, test_helius_connection
from models.classes import initialize_prices
from utils.dedup import get_stats, deduped_transactions
from utils.base_helpers import log_error, print_error_summary
from data.tokens import TOP_100_ERC20_TOKENS, TOKEN_PRICES
from data.addresses import DEX_ADDRESSES
from web3 import Web3

# 🔧 PROFESSIONAL PIPELINE DEDUPLICATION SYSTEM
pipeline_processed_txs = set()
pipeline_lock = threading.Lock()
pipeline_stats = defaultdict(int)

# ═══════════════════════════════════════════════════════════════════════════════
# 🛡️ NEAR-DUPLICATE SUPPRESSION CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════
# Detects and prevents mirror trades, transfer shadows, and wash-trade patterns

# Time window for considering transactions as potential duplicates (seconds)
# Increased to 900 seconds (15 minutes) to catch cross-exchange arbitrage/transfers
# Whale moving funds between Coinbase→Binance can take 10-15 minutes
NEAR_DUPE_TIME_WINDOW = 900

# USD value tolerance for matching transactions (absolute threshold)
NEAR_DUPE_USD_THRESHOLD = 5.0

# Percentage tolerance for matching USD values (0.15 = 0.15%)
NEAR_DUPE_PERCENTAGE_THRESHOLD = 0.0015

# Maximum in-memory cache size for recent transactions per whale
NEAR_DUPE_CACHE_SIZE = 50

# Number of recent transactions to check from database
NEAR_DUPE_DB_LOOKBACK = 200

# Minimum USD value to apply safeguards (never dedupe above this)
NEAR_DUPE_SAFEGUARD_USD = 5_000_000

# Token-level cache size for cross-entity duplicate detection
TOKEN_DUPE_CACHE_SIZE = 200

# Patterns to detect
MIRROR_PATTERNS = [
    ('BUY', 'SELL'),
    ('SELL', 'BUY'),
]

TRANSFER_SHADOW_PATTERNS = [
    ('BUY', 'TRANSFER'),
    ('TRANSFER', 'BUY'),
    ('SELL', 'TRANSFER'),
    ('TRANSFER', 'SELL'),
]

# In-memory cache for near-duplicate detection
# Structure: {(whale_addr, token): [{'timestamp': ..., 'usd_value': ..., 'classification': ..., 'tx_hash': ...}]}
near_dupe_cache = defaultdict(list)
near_dupe_cache_lock = threading.Lock()
near_dupe_stats = defaultdict(int)

# Token-level cache to detect cross-whale duplicates (e.g., BUY/SELL pairs of same size)
token_dupe_cache = defaultdict(list)
token_dupe_cache_lock = threading.Lock()

HIGH_RISK_COUNTERPARTY_TYPES = {'CEX', 'DEX'}
TRADE_COUNTERPARTY_TYPES = {'CEX', 'DEX'}

def is_transaction_already_processed(tx_hash: str) -> bool:
    """
    🔧 PROFESSIONAL PIPELINE DEDUPLICATION
    
    Thread-safe function to check and add transaction hashes to prevent duplicates.
    
    Args:
        tx_hash: Transaction hash to check
        
    Returns:
        bool: True if already processed, False if new
    """
    with pipeline_lock:
        if tx_hash in pipeline_processed_txs:
            pipeline_stats['duplicates_prevented'] += 1
            return True
        else:
            pipeline_processed_txs.add(tx_hash)
            pipeline_stats['unique_processed'] += 1
            
            # Keep set size manageable
            if len(pipeline_processed_txs) > 10000:
                # Remove oldest 2000 entries
                old_txs = list(pipeline_processed_txs)[:2000]
                for old_tx in old_txs:
                    pipeline_processed_txs.remove(old_tx)
                    
            return False

def get_pipeline_stats() -> dict:
    """Get pipeline processing statistics"""
    with pipeline_lock:
        return dict(pipeline_stats)


# ═══════════════════════════════════════════════════════════════════════════════
# 🛡️ NEAR-DUPLICATE DETECTION SYSTEM
# ═══════════════════════════════════════════════════════════════════════════════

def is_usd_value_match(usd1: float, usd2: float) -> bool:
    """
    Check if two USD values match within configured tolerances.
    
    Uses both absolute threshold ($5) and percentage threshold (0.15%).
    
    Args:
        usd1: First USD value
        usd2: Second USD value
        
    Returns:
        bool: True if values match within tolerance
    """
    if usd1 == 0 and usd2 == 0:
        return True
    
    diff = abs(usd1 - usd2)
    
    # Check absolute threshold
    if diff <= NEAR_DUPE_USD_THRESHOLD:
        return True
    
    # Check percentage threshold
    avg = (usd1 + usd2) / 2
    if avg > 0:
        percentage_diff = diff / avg
        if percentage_diff <= NEAR_DUPE_PERCENTAGE_THRESHOLD:
            return True
    
    return False


def detect_duplicate_pattern(existing_class: str, incoming_class: str, 
                            existing_counterparty: str, incoming_counterparty: str,
                            existing_is_cex: bool, incoming_is_cex: bool) -> Optional[str]:
    """
    Detect if two transactions match a duplicate pattern.
    
    Args:
        existing_class: Classification of existing transaction
        incoming_class: Classification of incoming transaction
        existing_counterparty: Counterparty type of existing
        incoming_counterparty: Counterparty type of incoming
        existing_is_cex: Is existing a CEX transaction
        incoming_is_cex: Is incoming a CEX transaction
        
    Returns:
        str: Reason code if duplicate pattern detected, None otherwise
    """
    # Pattern 1: Mirror direction (BUY/SELL pair)
    if (existing_class, incoming_class) in MIRROR_PATTERNS:
        return "mirror_direction"
    
    # Pattern 2: Transfer shadow (BUY/TRANSFER or SELL/TRANSFER pair)
    if (existing_class, incoming_class) in TRANSFER_SHADOW_PATTERNS:
        return "transfer_shadow"
    
    # Pattern 3: Same classification but opposite flow indicators
    if existing_class == incoming_class:
        # Check if counterparty types suggest opposite flows
        if existing_counterparty != incoming_counterparty:
            # CEX vs EOA with same classification might be duplicate reporting
            if {existing_counterparty, incoming_counterparty} == {'CEX', 'EOA'}:
                return "counterparty_mismatch"
        
        # Check if is_cex_transaction differs (one source says CEX, another doesn't)
        if existing_is_cex != incoming_is_cex:
            return "cex_flag_mismatch"
    
    # Pattern 4: DEFI classification - never dedupe (auditing requirement)
    if existing_class == 'DEFI' or incoming_class == 'DEFI':
        return None
    
    return None


def _normalize_address(addr: Optional[str]) -> str:
    """Normalize an address for comparisons."""
    return addr.lower() if addr else ''


def _has_shared_entity(existing_entry: Dict[str, Any], incoming_entry: Dict[str, Any]) -> bool:
    """Check if two transactions share any participant (whale or counterparty)."""
    existing_parties = {
        _normalize_address(existing_entry.get('whale_address')),
        _normalize_address(existing_entry.get('counterparty_address'))
    }
    incoming_parties = {
        _normalize_address(incoming_entry.get('whale_address')),
        _normalize_address(incoming_entry.get('counterparty_address'))
    }
    existing_parties.discard('')
    incoming_parties.discard('')
    return bool(existing_parties & incoming_parties)


def _is_high_risk_flow(entry: Dict[str, Any]) -> bool:
    """Return True if the transaction involves a high-risk counterparty (CEX/DEX)."""
    counterparty = (entry.get('counterparty_type') or '').upper()
    return entry.get('is_cex_transaction', False) or counterparty in HIGH_RISK_COUNTERPARTY_TYPES


def should_merge_cross_entity(existing_entry: Dict[str, Any], incoming_entry: Dict[str, Any], reason: str) -> bool:
    """
    Decide if two transactions that belong to different whales should be merged.
    
    Rules:
    - Always merge if they share any participant (either whale or counterparty)
    - Otherwise, only merge mirror/transfer-shadow patterns when at least one side involves CEX/DEX flow
    """
    if _has_shared_entity(existing_entry, incoming_entry):
        return True
    
    if reason in ('mirror_direction', 'transfer_shadow'):
        if _is_high_risk_flow(existing_entry) or _is_high_risk_flow(incoming_entry):
            return True
    
    return False


def is_trade_counterparty(counterparty_type: Optional[str]) -> bool:
    """Return True if counterparty represents a trade venue (CEX/DEX)."""
    if not counterparty_type:
        return False
    return counterparty_type.upper() in TRADE_COUNTERPARTY_TYPES


def classify_from_whale_perspective(
    whale_address: Optional[str],
    from_address: Optional[str],
    to_address: Optional[str],
    counterparty_type: Optional[str],
    original_classification: str
) -> str:
    """
    Determine BUY/SELL/TRANSFER/DEFI from the whale's perspective by following token flow.

    Args:
        whale_address: Determined whale address (post perspective logic)
        from_address: Transaction sender
        to_address: Transaction recipient
        counterparty_type: Counterparty classification (CEX/DEX/EOA/etc)
        original_classification: Classification suggested by upstream analysis

    Returns:
        str: Adjusted classification
    """
    whale_addr = (whale_address or '').lower()
    from_addr = (from_address or '').lower()
    to_addr = (to_address or '').lower()
    counterparty = (counterparty_type or '').upper()
    trade = counterparty in TRADE_COUNTERPARTY_TYPES

    # If we cannot determine whale address, fall back to original classification
    if not whale_addr:
        return original_classification

    if whale_addr == to_addr:
        # Whale receives tokens
        return 'BUY' if trade else 'TRANSFER'

    if whale_addr == from_addr:
        # Whale sends tokens
        return 'SELL' if trade else 'TRANSFER'

    # If whale is neither sender nor receiver, treat as DEFI/UNKNOWN interaction
    if original_classification == 'DEFI':
        return 'DEFI'

    if trade:
        # Unknown mapping but involves trade venue: prefer original classification
        return original_classification if original_classification in {'BUY', 'SELL'} else 'DEFI'

    # Non-trade interaction defaults to TRANSFER unless explicitly DEFI
    if original_classification == 'TRANSFER':
        return 'TRANSFER'

    return 'DEFI'


def check_near_duplicate(whale_addr: str, token_symbol: str, usd_value: float,
                        classification: str, timestamp: datetime, tx_hash: str,
                        counterparty_type: str, is_cex_transaction: bool,
                        supabase_client=None, counterparty_address: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Check if incoming transaction is a near-duplicate of recent transactions.
    
    This function checks both in-memory cache and recent database records to detect:
    - Mirror trades (BUY/SELL pairs within seconds)
    - Transfer shadows (BUY/TRANSFER or SELL/TRANSFER pairs)
    - Counterparty mismatches (same trade reported differently)
    
    Args:
        whale_addr: Whale address (or from_address if whale is None)
        token_symbol: Token symbol
        usd_value: Transaction USD value
        classification: Transaction classification (BUY/SELL/TRANSFER/DEFI)
        timestamp: Transaction timestamp
        tx_hash: Transaction hash
        counterparty_type: Type of counterparty (CEX/DEX/EOA)
        is_cex_transaction: Whether this is a CEX transaction
        supabase_client: Supabase client for database lookback
        
    Returns:
        dict: Duplicate info with 'existing_tx', 'reason', 'action' if duplicate found
        None: If not a duplicate
    """
    # Safeguard: Never dedupe large transactions
    if usd_value > NEAR_DUPE_SAFEGUARD_USD:
        return None
    
    # Safeguard: Never dedupe DEFI transactions (audit trail)
    if classification == 'DEFI':
        return None
    
    incoming_entry = {
        'tx_hash': tx_hash,
        'usd_value': usd_value,
        'classification': classification,
        'timestamp': timestamp,
        'counterparty_type': counterparty_type,
        'is_cex_transaction': is_cex_transaction,
        'whale_address': whale_addr,
        'counterparty_address': counterparty_address
    }
    
    cache_key = (whale_addr.lower() if whale_addr else '', token_symbol.upper())
    
    with near_dupe_cache_lock:
        # Check in-memory cache
        recent_txs = near_dupe_cache.get(cache_key, [])
        
        for existing in recent_txs:
            # Skip if same transaction hash
            if existing['tx_hash'] == tx_hash:
                continue
            
            # Check timestamp window
            time_diff = abs((timestamp - existing['timestamp']).total_seconds())
            if time_diff > NEAR_DUPE_TIME_WINDOW:
                continue
            
            # Check USD value match
            if not is_usd_value_match(usd_value, existing['usd_value']):
                continue
            
            # Check for duplicate pattern
            reason = detect_duplicate_pattern(
                existing['classification'], classification,
                existing.get('counterparty_type', 'EOA'), counterparty_type,
                existing.get('is_cex_transaction', False), is_cex_transaction
            )
            
            if reason:
                near_dupe_stats['cache_hits'] += 1
                near_dupe_stats[f'reason_{reason}'] += 1
                
                production_logger.info("Near-duplicate detected in cache", extra={'extra_fields': {
                    'incoming_tx': tx_hash,
                    'existing_tx': existing['tx_hash'],
                    'reason': reason,
                    'time_diff_seconds': time_diff,
                    'usd_value': usd_value,
                    'token': token_symbol,
                    'pattern': f"{existing['classification']} + {classification}"
                }})
                
                return {
                    'existing_tx': existing,
                    'reason': reason,
                    'action': 'merge'  # Always merge, keep earliest
                }
    
    # Token-level cache for cross-entity duplicates
    cross_cache_result = _check_token_cache_for_duplicate(
        token_symbol=token_symbol,
        incoming_entry=incoming_entry
    )
    if cross_cache_result:
        return cross_cache_result
    
    # Check database for recent transactions
    if supabase_client and whale_addr:
        try:
            cutoff_time = timestamp - timedelta(seconds=NEAR_DUPE_TIME_WINDOW)
            
            result = supabase_client.table('whale_transactions')\
                .select('transaction_hash, classification, usd_value, timestamp, counterparty_type, is_cex_transaction, confidence, whale_address, counterparty_address')\
                .eq('whale_address', whale_addr)\
                .eq('token_symbol', token_symbol)\
                .gte('timestamp', cutoff_time.isoformat())\
                .order('timestamp', desc=True)\
                .limit(NEAR_DUPE_DB_LOOKBACK)\
                .execute()
            
            if result.data:
                for existing in result.data:
                    # Skip if same transaction hash
                    if existing['transaction_hash'] == tx_hash:
                        continue
                    
                    existing_timestamp = datetime.fromisoformat(existing['timestamp'].replace('Z', '+00:00'))
                    time_diff = abs((timestamp - existing_timestamp).total_seconds())
                    
                    if time_diff > NEAR_DUPE_TIME_WINDOW:
                        continue
                    
                    existing_usd = float(existing.get('usd_value', 0))
                    if not is_usd_value_match(usd_value, existing_usd):
                        continue
                    
                    reason = detect_duplicate_pattern(
                        existing['classification'], classification,
                        existing.get('counterparty_type', 'EOA'), counterparty_type,
                        existing.get('is_cex_transaction', False), is_cex_transaction
                    )
                    
                    if reason:
                        near_dupe_stats['db_hits'] += 1
                        near_dupe_stats[f'reason_{reason}'] += 1
                        
                        production_logger.info("Near-duplicate detected in database", extra={'extra_fields': {
                            'incoming_tx': tx_hash,
                            'existing_tx': existing['transaction_hash'],
                            'reason': reason,
                            'time_diff_seconds': time_diff,
                            'usd_value': usd_value,
                            'token': token_symbol,
                            'pattern': f"{existing['classification']} + {classification}"
                        }})
                        
                        return {
                            'existing_tx': existing,
                            'reason': reason,
                            'action': 'merge'
                        }
                        
        except Exception as e:
            production_logger.warning("Error checking database for near-duplicates", 
                                    extra={'extra_fields': {'error': str(e)}})
    
    # Cross-entity database lookback (same token, any whale)
    if supabase_client:
        try:
            cutoff_time = timestamp - timedelta(seconds=NEAR_DUPE_TIME_WINDOW)
            
            result = supabase_client.table('whale_transactions')\
                .select('transaction_hash, classification, usd_value, timestamp, counterparty_type, is_cex_transaction, whale_address, counterparty_address')\
                .eq('token_symbol', token_symbol)\
                .gte('timestamp', cutoff_time.isoformat())\
                .order('timestamp', desc=True)\
                .limit(NEAR_DUPE_DB_LOOKBACK)\
                .execute()
            
            if result.data:
                for existing in result.data:
                    if existing.get('transaction_hash') == tx_hash:
                        continue
                    
                    existing_entry = {
                        'tx_hash': existing.get('transaction_hash'),
                        'usd_value': float(existing.get('usd_value', 0) or 0),
                        'classification': existing.get('classification', ''),
                        'timestamp': datetime.fromisoformat(existing['timestamp'].replace('Z', '+00:00')),
                        'counterparty_type': existing.get('counterparty_type', 'EOA'),
                        'is_cex_transaction': existing.get('is_cex_transaction', False),
                        'whale_address': existing.get('whale_address'),
                        'counterparty_address': existing.get('counterparty_address')
                    }
                    
                    time_diff = abs((timestamp - existing_entry['timestamp']).total_seconds())
                    if time_diff > NEAR_DUPE_TIME_WINDOW:
                        continue
                    
                    if not is_usd_value_match(usd_value, existing_entry['usd_value']):
                        continue
                    
                    reason = detect_duplicate_pattern(
                        existing_entry['classification'], classification,
                        existing_entry.get('counterparty_type', 'EOA'), counterparty_type,
                        existing_entry.get('is_cex_transaction', False), is_cex_transaction
                    )
                    
                    if reason and should_merge_cross_entity(existing_entry, incoming_entry, reason):
                        near_dupe_stats['db_cross_hits'] += 1
                        near_dupe_stats[f'reason_{reason}'] += 1
                        
                        production_logger.info("Near-duplicate detected across whales (database)", extra={'extra_fields': {
                            'incoming_tx': tx_hash,
                            'existing_tx': existing_entry['tx_hash'],
                            'reason': reason,
                            'time_diff_seconds': time_diff,
                            'usd_value': usd_value,
                            'token': token_symbol,
                            'pattern': f"{existing_entry['classification']} + {classification}"
                        }})
                        
                        return {
                            'existing_tx': existing,
                            'reason': reason,
                            'action': 'merge'
                        }
                        
        except Exception as e:
            production_logger.warning("Error checking cross-entity duplicates in database", 
                                      extra={'extra_fields': {'error': str(e)}})
    
    return None


def _check_token_cache_for_duplicate(token_symbol: str, incoming_entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Check token-level cache for cross-entity duplicates."""
    token_key = token_symbol.upper()
    
    with token_dupe_cache_lock:
        recent_txs = token_dupe_cache.get(token_key, [])
        if not recent_txs:
            return None
        
        # Remove stale entries while iterating
        fresh_entries = []
        for existing in recent_txs:
            time_diff = abs((incoming_entry['timestamp'] - existing['timestamp']).total_seconds())
            
            if time_diff > NEAR_DUPE_TIME_WINDOW * 2:
                # Drop stale entry
                continue
            
            fresh_entries.append(existing)
            
            if time_diff > NEAR_DUPE_TIME_WINDOW:
                continue
            
            if not is_usd_value_match(incoming_entry['usd_value'], existing['usd_value']):
                continue
            
            reason = detect_duplicate_pattern(
                existing['classification'], incoming_entry['classification'],
                existing.get('counterparty_type', 'EOA'), incoming_entry.get('counterparty_type', 'EOA'),
                existing.get('is_cex_transaction', False), incoming_entry.get('is_cex_transaction', False)
            )
            
            if not reason:
                continue
            
            if not should_merge_cross_entity(existing, incoming_entry, reason):
                continue
            
            time_diff = abs((incoming_entry['timestamp'] - existing['timestamp']).total_seconds())
            near_dupe_stats['token_cache_hits'] += 1
            near_dupe_stats[f'reason_{reason}'] += 1
            
            production_logger.info("Near-duplicate detected across whales (cache)", extra={'extra_fields': {
                'incoming_tx': incoming_entry['tx_hash'],
                'existing_tx': existing['tx_hash'],
                'reason': reason,
                'time_diff_seconds': time_diff,
                'usd_value': incoming_entry['usd_value'],
                'token': token_symbol,
                'pattern': f"{existing['classification']} + {incoming_entry['classification']}"
            }})
            
            return {
                'existing_tx': existing,
                'reason': reason,
                'action': 'merge'
            }
        
        # Update cache with trimmed fresh entries
        fresh_entries.sort(key=lambda x: x['timestamp'], reverse=True)
        token_dupe_cache[token_key] = fresh_entries[:TOKEN_DUPE_CACHE_SIZE]
    
    return None


def add_to_near_dupe_cache(whale_addr: str, token_symbol: str, usd_value: float,
                          classification: str, timestamp: datetime, tx_hash: str,
                          counterparty_type: str, is_cex_transaction: bool,
                          counterparty_address: Optional[str] = None):
    """
    Add a transaction to the near-duplicate detection cache.
    
    Args:
        whale_addr: Whale address
        token_symbol: Token symbol
        usd_value: USD value
        classification: Transaction classification
        timestamp: Transaction timestamp
        tx_hash: Transaction hash
        counterparty_type: Counterparty type
        is_cex_transaction: Is CEX transaction
    """
    if not whale_addr:
        return
    
    cache_key = (whale_addr.lower(), token_symbol.upper())
    
    with near_dupe_cache_lock:
        tx_info = {
            'tx_hash': tx_hash,
            'usd_value': usd_value,
            'classification': classification,
            'timestamp': timestamp,
            'counterparty_type': counterparty_type,
            'is_cex_transaction': is_cex_transaction,
            'whale_address': whale_addr,
            'counterparty_address': counterparty_address
        }
        
        recent_txs = near_dupe_cache[cache_key]
        recent_txs.append(tx_info)
        
        # Keep cache size manageable - remove oldest entries
        if len(recent_txs) > NEAR_DUPE_CACHE_SIZE:
            # Sort by timestamp and keep most recent
            recent_txs.sort(key=lambda x: x['timestamp'], reverse=True)
            near_dupe_cache[cache_key] = recent_txs[:NEAR_DUPE_CACHE_SIZE]
    
    add_to_token_dupe_cache(
        token_symbol=token_symbol,
        tx_info={
            'tx_hash': tx_hash,
            'usd_value': usd_value,
            'classification': classification,
            'timestamp': timestamp,
            'counterparty_type': counterparty_type,
            'is_cex_transaction': is_cex_transaction,
            'whale_address': whale_addr,
            'counterparty_address': counterparty_address
        }
    )


def add_to_token_dupe_cache(token_symbol: str, tx_info: Dict[str, Any]) -> None:
    """Track recent transactions per token for cross-entity duplicate detection."""
    token_key = token_symbol.upper()
    with token_dupe_cache_lock:
        token_entries = token_dupe_cache[token_key]
        token_entries.append(tx_info)
        token_entries.sort(key=lambda x: x['timestamp'], reverse=True)
        token_dupe_cache[token_key] = token_entries[:TOKEN_DUPE_CACHE_SIZE]


def get_near_dupe_stats() -> dict:
    """Get near-duplicate detection statistics."""
    with near_dupe_cache_lock:
        stats = dict(near_dupe_stats)
        stats['cache_size'] = sum(len(txs) for txs in near_dupe_cache.values())
        stats['cache_keys'] = len(near_dupe_cache)
    
    with token_dupe_cache_lock:
        stats['token_cache_size'] = sum(len(txs) for txs in token_dupe_cache.values())
        stats['token_cache_keys'] = len(token_dupe_cache)
    
    return stats

# New imports for real-time market flow engine and Whale Intelligence
try:
    from utils.real_time_classification import classify_swap_transaction, ClassifiedSwap
    from utils.classification_final import whale_intelligence_engine
    from utils.etherscan_labels import label_provider
    from utils.token_intelligence import token_intelligence
    from utils.whale_registry import whale_registry
    from supabase import create_client, Client
    import config.api_keys as api_keys
    import asyncio
    import json
    from datetime import datetime, timezone
    from whale_sentiment_aggregator import whale_sentiment_aggregator
    REAL_TIME_ENABLED = True
    WHALE_INTELLIGENCE_ENABLED = True
    SENTIMENT_AGGREGATION_ENABLED = True
    ENHANCED_INTELLIGENCE_ENABLED = True
except ImportError as e:
    production_logger.warning("Real-time classification not available", error=str(e))
    REAL_TIME_ENABLED = False
    WHALE_INTELLIGENCE_ENABLED = False
    SENTIMENT_AGGREGATION_ENABLED = False
    ENHANCED_INTELLIGENCE_ENABLED = False

# Initialize Production Whale Intelligence Engine
whale_engine = WhaleIntelligenceEngine()

# Import the new classification system
from address_enrichment import AddressEnrichmentService, EnrichedAddress, ChainType
from rule_engine import RuleEngine, Transaction, AddressMetadata, ClassificationType
from transaction_classifier import TransactionClassifier

# Basic colors
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
PURPLE = '\033[95m'
BOLD = '\033[1m'
END = '\033[0m'

# Global settings
min_transaction_value = GLOBAL_USD_THRESHOLD
active_threads = []
monitoring_enabled = True  # Flag to control transaction display

# Web3 Transfer topic signature
ERC20_TRANSFER_TOPIC = Web3.keccak(text='Transfer(address,address,uint256)').hex()

# Rolling cursors for Etherscan fallback and Web3 monitors
LAST_BLOCK_BY_CONTRACT: dict[str, int] = {}
LAST_BLOCK_WEB3_BY_CHAIN: dict[str, int] = {}
LAST_BLOCK_WEB3_SWAP_BY_CHAIN: dict[str, int] = {}

def run_web3_transfer_monitor(chain: str) -> None:
    """Web3 log monitor for ERC-20 Transfer events with rolling block cursors."""
    try:
        from data.tokens import TOP_100_ERC20_TOKENS, TOKEN_PRICES
        import config.api_keys as api_keys
        from utils.enhanced_classification import process_with_enhanced_intelligence
    except Exception as e:
        print(RED + f"Web3 monitor init failed: {e}" + END)
        return

    # Build Web3 client per chain
    if chain == 'ethereum':
        rpc_url = api_keys.ETHEREUM_RPC_URL
    elif chain == 'polygon':
        rpc_url = api_keys.POLYGON_RPC_URL
    else:
        print(YELLOW + f"Unsupported chain for Web3 monitor: {chain}" + END)
        return

    # Detailed Web3 connection with error logging
    try:
        print(f"🔗 Attempting Web3 connection to {chain}: {rpc_url[:50]}...")
        w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': 10}))
        
        # Test connection with detailed error reporting
        if not w3.is_connected():
            print(RED + f"❌ Web3 connection test failed for {chain}" + END)
            try:
                # Try to get more details about why it failed
                block = w3.eth.get_block('latest')
                print(f"✅ Actually connected! Latest block: {block['number']}")
            except Exception as test_error:
                error_msg = str(test_error)
                print(RED + f"❌ Web3 connection error for {chain}:" + END)
                print(RED + f"   Error type: {type(test_error).__name__}" + END)
                print(RED + f"   Error message: {error_msg}" + END)
                
                # Check for specific error types
                if "429" in error_msg or "rate limit" in error_msg.lower():
                    print(YELLOW + "   🚨 RATE LIMIT DETECTED - Too many API requests" + END)
                elif "timeout" in error_msg.lower():
                    print(YELLOW + "   ⏱️  TIMEOUT - RPC endpoint not responding" + END)
                elif "unauthorized" in error_msg.lower() or "403" in error_msg:
                    print(YELLOW + "   🔒 UNAUTHORIZED - Check your API key" + END)
                elif "connection" in error_msg.lower():
                    print(YELLOW + "   🌐 CONNECTION ERROR - Network issue" + END)
                
                return
        else:
            print(GREEN + f"✅ Web3 connected successfully to {chain}" + END)
    except Exception as init_error:
        print(RED + f"❌ Failed to initialize Web3 for {chain}:" + END)
        print(RED + f"   Error: {type(init_error).__name__}: {init_error}" + END)
        return

    # Precompute token metadata maps
    token_meta_by_addr = {t['address'].lower(): t for t in TOP_100_ERC20_TOKENS}
    token_price_by_symbol = TOKEN_PRICES

    # Rolling cursor
    last_block = LAST_BLOCK_WEB3_BY_CHAIN.get(chain)

    while not shutdown_flag.is_set() and monitoring_enabled:
        try:
            latest = w3.eth.block_number
            if last_block is None:
                # Start a few blocks behind to catch up
                last_block = max(0, latest - 12)

            from_block = last_block + 1
            to_block = min(latest, from_block + 12)
            if to_block < from_block:
                time.sleep(0.5)
                continue

            # Batch query: gather logs for tracked token contracts in this window
            addresses = [t['address'] for t in TOP_100_ERC20_TOKENS]
            logs: list = []
            # Some providers require splitting by address; iterate small subsets
            batch_size = 15
            for i in range(0, len(addresses), batch_size):
                addr_batch = [Web3.to_checksum_address(a) for a in addresses[i:i + batch_size]]
                try:
                    filter_params = {
                        'fromBlock': hex(from_block),
                        'toBlock': hex(to_block),
                        'topics': [ERC20_TRANSFER_TOPIC],
                    }
                    # web3.py doesn't support multiple addresses in a single call for some providers; loop
                    for addr in addr_batch:
                        params = dict(filter_params)
                        params['address'] = addr
                        try:
                            part = w3.eth.get_logs(params)
                            if part:
                                logs.extend(part)
                        except Exception:
                            continue
                except Exception:
                    continue

            # Process logs
            for lg in logs:
                try:
                    contract_addr = lg['address'].lower()
                    meta = token_meta_by_addr.get(contract_addr)
                    if not meta:
                        continue
                    symbol = meta.get('symbol')
                    decimals = meta.get('decimals', 18)
                    price = token_price_by_symbol.get(symbol, 0)
                    if price == 0:
                        continue
                    data_hex = lg['data'] if isinstance(lg['data'], str) else lg['data'].hex()
                    raw_value = int(data_hex, 16)
                    token_amount = raw_value / (10 ** decimals)
                    estimated_usd = token_amount * price
                    if estimated_usd < GLOBAL_USD_THRESHOLD:
                        continue
                    from_addr = '0x' + lg['topics'][1].hex()[-40:] if isinstance(lg['topics'][1], (bytes, bytearray)) else lg['topics'][1][-40:]
                    to_addr = '0x' + lg['topics'][2].hex()[-40:] if isinstance(lg['topics'][2], (bytes, bytearray)) else lg['topics'][2][-40:]
                    tx_hash = lg['transactionHash'].hex()

                    event = {
                        'blockchain': chain,
                        'tx_hash': tx_hash,
                        'from': Web3.to_checksum_address(from_addr),
                        'to': Web3.to_checksum_address(to_addr),
                        'symbol': symbol,
                        'amount': token_amount,
                        'estimated_usd': estimated_usd,
                        'block_number': int(lg['blockNumber'])
                    }
                    # Use enhanced classification with Etherscan labels, token intelligence, and whale registry
                    from utils.enhanced_classification import process_with_enhanced_intelligence
                    enriched = process_with_enhanced_intelligence(event)
                    if enriched:
                        # Display and trigger storage via display path
                        display_transaction({
                            'chain': chain,
                            'hash': tx_hash,
                            'from_address': event['from'],
                            'to_address': event['to'],
                            'usd_value': estimated_usd,
                            'token_symbol': symbol
                        })
                except Exception:
                    continue

            # Advance cursor
            last_block = to_block
            LAST_BLOCK_WEB3_BY_CHAIN[chain] = last_block
            time.sleep(0.4)
        except Exception:
            time.sleep(1.0)
            continue

# Real-time market flow engine components
class TransactionStorage:
    """Handles storage of classified transactions to Supabase."""
    
    def __init__(self):
        """Initialize Supabase client if real-time features are enabled."""
        self.supabase = None
        self.storage_enabled = False
        
        if REAL_TIME_ENABLED:
            try:
                self.supabase: Client = create_client(
                    api_keys.SUPABASE_URL,
                    api_keys.SUPABASE_SERVICE_ROLE_KEY
                )
                self.storage_enabled = True
                production_logger.info("Database storage initialized", 
                                     extra={'extra_fields': {'supabase_url': api_keys.SUPABASE_URL}})
            except Exception as e:
                production_logger.error("Database storage failed to initialize", 
                                      extra={'extra_fields': {'error': str(e), 'stack_trace': traceback.format_exc()}})
    
    def store_classified_swap(self, swap: ClassifiedSwap) -> bool:
        """Store a classified swap in the database."""
        if not self.storage_enabled or not self.supabase:
            return False
        
        try:
            # Convert swap to database format
            # Normalize DEX to satisfy Supabase enum/check constraint while preserving original in metadata
            allowed_dex_values = {"uniswap_v2", "uniswap_v3", "sushiswap", "quickswap"}
            dex_value = swap.dex if swap.dex in allowed_dex_values else "defi"
            raw_meta = swap.raw_log_data or {}
            if dex_value == "defi" and swap.dex:
                # Preserve original dex label for auditability
                try:
                    if isinstance(raw_meta, dict):
                        raw_meta.setdefault('original_dex', swap.dex)
                    else:
                        raw_meta = {'original_dex': swap.dex}
                except Exception:
                    raw_meta = {'original_dex': swap.dex}
            
            swap_data = {
                'transaction_hash': swap.transaction_hash,
                'block_number': swap.block_number,
                'block_timestamp': swap.block_timestamp.isoformat(),
                'chain': swap.chain,
                'dex': dex_value,
                'token_in_address': swap.token_in_address,
                'token_out_address': swap.token_out_address,
                'token_in_symbol': swap.token_in_symbol,
                'token_out_symbol': swap.token_out_symbol,
                'token_in_decimals': swap.token_in_decimals,
                'token_out_decimals': swap.token_out_decimals,
                'amount_in': str(swap.amount_in),
                'amount_out': str(swap.amount_out),
                'amount_in_usd': str(swap.amount_in_usd) if swap.amount_in_usd else None,
                'amount_out_usd': str(swap.amount_out_usd) if swap.amount_out_usd else None,
                'classification': swap.classification,
                'confidence_score': swap.confidence_score,
                'sender_address': swap.sender_address,
                'recipient_address': swap.recipient_address,
                'is_whale_transaction': swap.is_whale_transaction,
                'whale_classification': swap.whale_classification,
                'token_price_usd': str(swap.token_price_usd) if swap.token_price_usd else None,
                'gas_used': swap.gas_used,
                'gas_price': str(swap.gas_price) if swap.gas_price else None,
                'transaction_fee_usd': str(swap.transaction_fee_usd) if swap.transaction_fee_usd else None,
                'raw_log_data': raw_meta,
                'classification_method': swap.classification_method
            }
            
            # Upsert into database for idempotency
            result = self.supabase.table('transaction_monitoring').upsert(
                swap_data,
                on_conflict='transaction_hash'
            ).execute()
            
            if result.data:
                return True
            else:
                production_logger.error("Failed to store swap", 
                                      extra={'extra_fields': {'transaction_hash': swap.transaction_hash}})
                return False
                
        except Exception as e:
            production_logger.error("Database storage error", 
                                  extra={'extra_fields': {
                                      'transaction_hash': swap.transaction_hash,
                                      'error': str(e), 
                                      'stack_trace': traceback.format_exc()
                                  }})
            return False
    
    def _determine_whale_perspective(self, from_addr: str, to_addr: str, blockchain: str) -> dict:
        """
        🐋 WHALE PERSPECTIVE LOGIC - Identify the actual whale and counterparty
        
        Rules:
        - CEX/DEX addresses are NEVER whales, they're counterparties
        - Classification is from the NON-CEX party's viewpoint
        - CEX → User = User is BUYING
        - User → CEX = User is SELLING
        
        Returns:
            dict with whale_address, counterparty_address, counterparty_type, is_cex_transaction
        """
        try:
            # Query address types from Supabase
            from_data = None
            to_data = None
            
            if from_addr and self.supabase:
                result = self.supabase.table('addresses')\
                    .select('address, address_type, label, entity_name')\
                    .eq('address', from_addr.lower())\
                    .eq('blockchain', blockchain)\
                    .execute()
                from_data = result.data[0] if result.data else None
            
            if to_addr and self.supabase:
                result = self.supabase.table('addresses')\
                    .select('address, address_type, label, entity_name')\
                    .eq('address', to_addr.lower())\
                    .eq('blockchain', blockchain)\
                    .execute()
                to_data = result.data[0] if result.data else None
            
            # Determine address types
            from_type = from_data.get('address_type', '') if from_data else ''
            to_type = to_data.get('address_type', '') if to_data else ''
            from_label = from_data.get('label', '') if from_data else ''
            to_label = to_data.get('label', '') if to_data else ''
            
            # Check if addresses are CEX or DEX
            from_is_cex = from_type in ['CEX Wallet', 'exchange', 'Exchange Wallet'] or 'binance' in from_label.lower() or 'coinbase' in from_label.lower()
            to_is_cex = to_type in ['CEX Wallet', 'exchange', 'Exchange Wallet'] or 'binance' in to_label.lower() or 'coinbase' in to_label.lower()
            from_is_dex = from_type in ['DEX', 'dex_router', 'DEX Router'] or from_addr.lower() in DEX_ADDRESSES
            to_is_dex = to_type in ['DEX', 'dex_router', 'DEX Router'] or to_addr.lower() in DEX_ADDRESSES
            
            # Determine whale and counterparty
            whale_address = None
            counterparty_address = None
            counterparty_type = 'EOA'
            is_cex_transaction = False
            
            if from_is_cex and not to_is_cex:
                # CEX → User: User is the whale (receiving/buying)
                whale_address = to_addr
                counterparty_address = from_addr
                counterparty_type = 'CEX'
                is_cex_transaction = True
                
            elif to_is_cex and not from_is_cex:
                # User → CEX: User is the whale (sending/selling)
                whale_address = from_addr
                counterparty_address = to_addr
                counterparty_type = 'CEX'
                is_cex_transaction = True
                
            elif from_is_dex and not to_is_dex:
                # DEX → User: User is the whale (receiving/buying from DEX)
                whale_address = to_addr
                counterparty_address = from_addr
                counterparty_type = 'DEX'
                
            elif to_is_dex and not from_is_dex:
                # User → DEX: User is the whale (sending/selling to DEX)
                whale_address = from_addr
                counterparty_address = to_addr
                counterparty_type = 'DEX'
                
            elif from_is_cex and to_is_cex:
                # CEX → CEX: Internal transfer, no whale
                whale_address = None
                counterparty_address = None
                counterparty_type = 'CEX_INTERNAL'
                is_cex_transaction = True
                
            else:
                # Wallet → Wallet: Could be both whales, track both
                # Default: from_address is the initiator/whale
                whale_address = from_addr
                counterparty_address = to_addr
                counterparty_type = 'EOA'
            
            return {
                'whale_address': whale_address,
                'counterparty_address': counterparty_address,
                'counterparty_type': counterparty_type,
                'is_cex_transaction': is_cex_transaction,
                'from_label': from_label,
                'to_label': to_label
            }
            
        except Exception as e:
            production_logger.error("Error determining whale perspective", 
                                  extra={'extra_fields': {'error': str(e)}})
            # Fallback: treat from_address as whale
            return {
                'whale_address': from_addr,
                'counterparty_address': to_addr,
                'counterparty_type': 'EOA',
                'is_cex_transaction': False,
                'from_label': '',
                'to_label': ''
            }
    
    def store_whale_transaction(self, tx_data: dict, intelligence_result: dict) -> bool:
        """Store a whale transaction classification in the whale_transactions table."""
        if not self.storage_enabled or not self.supabase:
            return False
        
        try:
            # Extract key information
            classification = getattr(intelligence_result, 'classification', ClassificationType.TRANSFER)
            # Handle ClassificationType enum - get string value
            if hasattr(classification, 'value'):
                classification_str = classification.value
            else:
                classification_str = str(classification)
            
            confidence = getattr(intelligence_result, 'confidence', 0.0)
            whale_score = getattr(intelligence_result, 'final_whale_score', 0.0)
            
            # Store BUY/SELL by default; also store TRANSFERs above threshold
            if classification_str not in ['BUY', 'SELL']:
                usd_value = float(tx_data.get('estimated_usd', 0) or tx_data.get('value_usd', 0) or tx_data.get('usd_value', 0) or 0)
                if usd_value < GLOBAL_USD_THRESHOLD:
                    return False
                # infer flow direction if available
                flow_direction = tx_data.get('cex_flow_direction')
            
            # Extract token symbol using multiple fallback methods
            token_symbol = self._extract_token_symbol(tx_data)
            if not token_symbol:
                production_logger.warning("No token symbol found, skipping storage", 
                                        extra={'extra_fields': {'tx_hash': tx_data.get('tx_hash', '')}})
                return False
            
            # 🐋 NEW: Determine whale perspective
            from_address = tx_data.get('from_address', tx_data.get('from', ''))
            to_address = tx_data.get('to_address', tx_data.get('to', ''))
            blockchain = tx_data.get('blockchain', tx_data.get('chain', 'ethereum'))
            
            whale_perspective = self._determine_whale_perspective(from_address, to_address, blockchain)
            
            # Skip CEX-to-CEX internal transfers
            if whale_perspective['counterparty_type'] == 'CEX_INTERNAL':
                production_logger.info("Skipping CEX internal transfer", 
                                     extra={'extra_fields': {'tx_hash': tx_data.get('tx_hash', '')}})
                return False

            adjusted_classification = classify_from_whale_perspective(
                whale_address=whale_perspective['whale_address'],
                from_address=from_address,
                to_address=to_address,
                counterparty_type=whale_perspective['counterparty_type'],
                original_classification=classification_str
            )
            if adjusted_classification != classification_str:
                production_logger.info(
                    "Classification adjusted based on whale perspective",
                    extra={'extra_fields': {
                        'tx_hash': tx_data.get('tx_hash', ''),
                        'original_classification': classification_str,
                        'adjusted_classification': adjusted_classification,
                        'whale_address': whale_perspective['whale_address'],
                        'counterparty_type': whale_perspective['counterparty_type']
                    }}
                )
                classification_str = adjusted_classification
            
            # Prepare whale transaction data with new perspective columns
            whale_data = {
                'transaction_hash': tx_data.get('tx_hash', tx_data.get('hash', '')),
                'token_symbol': token_symbol,
                'token_address': tx_data.get('token_address', ''),
                'classification': classification_str,
                'confidence': confidence,
                'usd_value': float(tx_data.get('estimated_usd', 0) or tx_data.get('value_usd', 0) or tx_data.get('usd_value', 0) or 0),
                'whale_score': whale_score,
                'blockchain': blockchain,
                'from_address': from_address,
                'to_address': to_address,
                'analysis_phases': len(getattr(intelligence_result, 'phase_results', {})),
                'reasoning': getattr(intelligence_result, 'master_classifier_reasoning', ''),
                # 🐋 NEW COLUMNS - Whale Perspective
                'whale_address': whale_perspective['whale_address'],
                'counterparty_address': whale_perspective['counterparty_address'],
                'counterparty_type': whale_perspective['counterparty_type'],
                'is_cex_transaction': whale_perspective['is_cex_transaction'],
                'from_label': whale_perspective['from_label'],
                'to_label': whale_perspective['to_label']
            }
            # Note: Do not add non-existent columns (e.g., flow_direction) to upsert payload
            
            # 🛡️ NEAR-DUPLICATE DETECTION - Check before storing
            # Use whale_address if available, otherwise fall back to from_address for matching
            check_whale_addr = whale_perspective['whale_address'] or from_address
            tx_timestamp = datetime.utcnow()  # Use current time as default
            
            # Try to parse transaction timestamp if available
            if 'timestamp' in tx_data:
                try:
                    if isinstance(tx_data['timestamp'], (int, float)):
                        tx_timestamp = datetime.fromtimestamp(tx_data['timestamp'])
                    elif isinstance(tx_data['timestamp'], str):
                        tx_timestamp = datetime.fromisoformat(tx_data['timestamp'].replace('Z', '+00:00'))
                except Exception:
                    pass  # Use current time as fallback
            
            # Check for near-duplicates
            dupe_check = check_near_duplicate(
                whale_addr=check_whale_addr,
                token_symbol=token_symbol,
                usd_value=whale_data['usd_value'],
                classification=classification_str,
                timestamp=tx_timestamp,
                tx_hash=whale_data['transaction_hash'],
                counterparty_type=whale_perspective['counterparty_type'],
                is_cex_transaction=whale_perspective['is_cex_transaction'],
                supabase_client=self.supabase,
                counterparty_address=whale_perspective['counterparty_address']
            )
            
            if dupe_check:
                # Near-duplicate detected - skip storage but log it
                production_logger.warning("Near-duplicate suppressed - not storing", extra={'extra_fields': {
                    'incoming_tx': whale_data['transaction_hash'],
                    'existing_tx': dupe_check['existing_tx'].get('tx_hash') or dupe_check['existing_tx'].get('transaction_hash'),
                    'reason': dupe_check['reason'],
                    'token': token_symbol,
                    'usd_value': whale_data['usd_value'],
                    'pattern': f"{dupe_check['existing_tx'].get('classification', 'N/A')} + {classification_str}"
                }})
                
                near_dupe_stats['suppressed'] += 1
                return False  # Don't store duplicate
            
            # Add to cache for future duplicate detection
            add_to_near_dupe_cache(
                whale_addr=check_whale_addr,
                token_symbol=token_symbol,
                usd_value=whale_data['usd_value'],
                classification=classification_str,
                timestamp=tx_timestamp,
                tx_hash=whale_data['transaction_hash'],
                counterparty_type=whale_perspective['counterparty_type'],
                is_cex_transaction=whale_perspective['is_cex_transaction'],
                counterparty_address=whale_perspective['counterparty_address']
            )
            
            # Upsert into whale_transactions table to avoid duplicate key errors
            result = self.supabase.table('whale_transactions').upsert(
                whale_data,
                on_conflict='transaction_hash'
            ).execute()
            
            if result.data:
                production_logger.info("Whale transaction stored successfully", 
                                     extra={'extra_fields': {
                                         'tx_hash': whale_data['transaction_hash'],
                                         'classification': classification_str,
                                         'token_symbol': token_symbol,
                                         'usd_value': whale_data['usd_value']
                                     }})
                return True
            else:
                production_logger.error("Failed to store whale transaction", 
                                      extra={'extra_fields': {'transaction_hash': whale_data['transaction_hash']}})
                return False
                
        except Exception as e:
            production_logger.error("Whale transaction storage error", 
                                  extra={'extra_fields': {
                                      'transaction_hash': tx_data.get('tx_hash', ''),
                                      'error': str(e), 
                                      'stack_trace': traceback.format_exc()
                                  }})
            return False
    
    def _extract_token_symbol(self, tx_data: dict) -> str:
        """Extract token symbol from transaction data using multiple methods."""
        # Try direct symbol fields first
        symbol = (tx_data.get('symbol') or 
                 tx_data.get('token_symbol') or 
                 tx_data.get('token_in_symbol') or 
                 tx_data.get('token_out_symbol'))
        
        if symbol and symbol != 'Unknown':
            return symbol.upper()
        
        # Try to extract from token addresses or contract calls
        token_address = (tx_data.get('token_address') or 
                        tx_data.get('to_address') or 
                        tx_data.get('to'))
        
        if token_address:
            # Common token addresses mapping (can be expanded)
            common_tokens = {
                '0xa0b86a33e6c57': 'USDC',
                '0x6b175474e89094c': 'DAI', 
                '0xdac17f958d2ee523': 'USDT',
                '0x2260fac5e5542a773': 'WBTC',
                '0xc02aaa39b223fe8d0': 'WETH',
                '0x7d1afa7b718fb893': 'MATIC',
                '0x514910771af9ca656': 'LINK'
            }
            
            # Check if address starts with any known token prefix
            for prefix, token in common_tokens.items():
                if token_address.lower().startswith(prefix.lower()):
                    return token
        
        # Default fallback based on blockchain
        blockchain = tx_data.get('blockchain', tx_data.get('chain', 'ethereum')).lower()
        if blockchain == 'ethereum':
            return 'ETH'
        elif blockchain == 'polygon':
            return 'MATIC'
        elif blockchain == 'solana':
            return 'SOL'
        elif blockchain in ['bitcoin', 'btc']:
            return 'BTC'
        else:
            return 'UNKNOWN'
    
    def get_recent_swaps(self, limit: int = 10) -> list:
        """Get recent swaps from the database."""
        if not self.storage_enabled or not self.supabase:
            return []
        
        try:
            result = self.supabase.table('transaction_monitoring')\
                .select('*')\
                .order('block_timestamp', desc=True)\
                .limit(limit)\
                .execute()
            
            return result.data if result.data else []
            
        except Exception as e:
            production_logger.error("Failed to fetch recent swaps", 
                                  extra={'extra_fields': {'error': str(e), 'stack_trace': traceback.format_exc()}})
            return []

# Global storage instance
transaction_storage = TransactionStorage()

async def process_real_time_swap(log_data: dict, chain: str, dex: str) -> bool:
    """
    Process a real-time swap transaction with classification and storage.
    Task 7: All transactions now go through the unified MasterClassifier pipeline.
    """
    if not REAL_TIME_ENABLED:
        return False
    
    try:
        # Task 7: Use unified whale intelligence engine for ALL transactions
        transaction_data = {
            'blockchain': chain,
            'dex': dex,
            'tx_hash': log_data.get('transactionHash', ''),
            'from_address': log_data.get('from', ''),
            'to_address': log_data.get('to', ''),
            'value_usd': log_data.get('value_usd', 0),
            'gas_price': log_data.get('gasPrice', 0),
            'timestamp': log_data.get('timestamp', time.time())
        }
        
        # Pass through full 7-phase analysis
        intelligence_result = whale_engine.analyze_transaction_comprehensive(transaction_data)
        
        # Create classified swap from intelligence result
        classified_swap = _create_classified_swap_from_intelligence(transaction_data, intelligence_result)
        
        # Store in database
        stored = transaction_storage.store_classified_swap(classified_swap)
        
        # Print to console (integrate with existing display)
        if classified_swap.amount_in_usd and float(classified_swap.amount_in_usd) >= min_transaction_value:
            print_classified_swap(classified_swap)
        
        return stored
        
    except Exception as e:
        production_logger.error("Failed to process real-time swap", 
                              extra={'extra_fields': {'error': str(e), 'log_data': log_data}})
        return False

def _create_classified_swap_from_intelligence(transaction_data: dict, intelligence_result: dict):
    """
    Task 7: Convert whale intelligence result to ClassifiedSwap format.
    
    Args:
        transaction_data (dict): Original transaction data
        intelligence_result (dict): Result from 7-phase analysis
        
    Returns:
        ClassifiedSwap: Standardized swap object
    """
    try:
        from utils.real_time_classification import ClassifiedSwap
        from datetime import datetime, timezone
        
        # Extract classification results
        classification = intelligence_result.get('classification', 'TRANSFER')
        confidence = intelligence_result.get('confidence', 0.5)
        evidence = intelligence_result.get('evidence', [])
        whale_classification = intelligence_result.get('whale_classification', {})
        
        # Create ClassifiedSwap object
        classified_swap = ClassifiedSwap(
            transaction_hash=transaction_data.get('tx_hash', ''),
            block_number=0,  # Will be populated by actual block data
            block_timestamp=datetime.fromtimestamp(transaction_data.get('timestamp', time.time()), tz=timezone.utc),
            chain=transaction_data.get('blockchain', 'ethereum'),
            dex=transaction_data.get('dex', 'unknown'),
            token_in_address='',  # Will be populated from log data
            token_out_address='',  # Will be populated from log data
            token_in_symbol='',
            token_out_symbol='',
            token_in_decimals=18,
            token_out_decimals=18,
            amount_in=0,
            amount_out=0,
            amount_in_usd=transaction_data.get('value_usd', 0),
            amount_out_usd=transaction_data.get('value_usd', 0),
            classification=classification,
            confidence_score=confidence,
            sender_address=transaction_data.get('from_address', ''),
            recipient_address=transaction_data.get('to_address', ''),
            is_whale_transaction=whale_classification.get('is_whale', False),
            whale_classification=classification,
            token_price_usd=0,
            gas_used=0,
            gas_price=transaction_data.get('gas_price', 0),
            transaction_fee_usd=0,
            raw_log_data=transaction_data,
            classification_method='whale_intelligence_7_phase'
        )
        
        return classified_swap
        
    except Exception as e:
        production_logger.error("Failed to create classified swap", 
                              extra={'extra_fields': {'error': str(e)}})
        return None

def _generate_investment_signal(whale_data: dict, transaction_data: dict, intelligence_result) -> None:
    """
    🚀 INVESTMENT OPPORTUNITY ENGINE 🚀
    Generate actionable investment signals based on whale movements.
    """
    try:
        # Extract key data
        from_address = whale_data.get('from', {})
        to_address = whale_data.get('to', {})
        amount_usd = whale_data.get('amount_usd', 0)
        symbol = whale_data.get('symbol', '')
        amount = whale_data.get('amount', 0)
        
        # Get owner types (CEX, wallet, etc.)
        from_owner_type = from_address.get('owner_type', '')
        to_owner_type = to_address.get('owner_type', '')
        from_owner = from_address.get('owner', '')
        to_owner = to_address.get('owner', '')
        
        # Investment signal logic
        signal_generated = False
        
        # BEARISH SIGNAL: Large move TO exchange (potential sell-off)
        if (from_owner_type == 'wallet' and to_owner_type == 'exchange' and 
            amount_usd >= 1000000):  # $1M+ threshold for major signals
            
            print(f"\n{Fore.RED}🚀 INVESTMENT SIGNAL: POTENTIAL SELL-OFF 🚀{Style.RESET_ALL}")
            print(f"{Fore.RED}   Whale Alert: ${amount_usd:,.0f} of {symbol} moved to {to_owner}{Style.RESET_ALL}")
            print(f"{Fore.RED}   Amount: {amount:,.2f} {symbol}{Style.RESET_ALL}")
            print(f"{Fore.RED}   Signal: HIGH SELL PRESSURE detected. Potential price drop imminent.{Style.RESET_ALL}")
            print(f"{Fore.RED}   Action: Monitor market for SHORT entry or exit long positions.{Style.RESET_ALL}")
            
            production_logger.info("🚀 BEARISH INVESTMENT SIGNAL", extra={
                'extra_fields': {
                    'signal_type': 'BEARISH',
                    'amount_usd': amount_usd,
                    'symbol': symbol,
                    'from_type': from_owner_type,
                    'to_exchange': to_owner,
                    'action': 'POTENTIAL_SELL_OFF'
                }
            })
            signal_generated = True
            
        # BULLISH SIGNAL: Large move FROM exchange (potential accumulation)
        elif (from_owner_type == 'exchange' and to_owner_type == 'wallet' and 
              amount_usd >= 1000000):  # $1M+ threshold for major signals
            
            print(f"\n{Fore.GREEN}🚀 INVESTMENT SIGNAL: POTENTIAL ACCUMULATION 🚀{Style.RESET_ALL}")
            print(f"{Fore.GREEN}   Whale Alert: ${amount_usd:,.0f} of {symbol} withdrawn from {from_owner}{Style.RESET_ALL}")
            print(f"{Fore.GREEN}   Amount: {amount:,.2f} {symbol}{Style.RESET_ALL}")
            print(f"{Fore.GREEN}   Signal: WHALE ACCUMULATION detected. Potential price increase likely.{Style.RESET_ALL}")
            print(f"{Fore.GREEN}   Action: Monitor market for LONG entry or hold existing positions.{Style.RESET_ALL}")
            
            production_logger.info("🚀 BULLISH INVESTMENT SIGNAL", extra={
                'extra_fields': {
                    'signal_type': 'BULLISH',
                    'amount_usd': amount_usd,
                    'symbol': symbol,
                    'from_exchange': from_owner,
                    'to_type': to_owner_type,
                    'action': 'POTENTIAL_ACCUMULATION'
                }
            })
            signal_generated = True
        
        # Medium-sized signals (100K - 1M)
        elif amount_usd >= 100000:
            if from_owner_type == 'wallet' and to_owner_type == 'exchange':
                print(f"\n{Fore.YELLOW}📈 Medium Signal: ${amount_usd:,.0f} {symbol} → {to_owner} (Bearish){Style.RESET_ALL}")
            elif from_owner_type == 'exchange' and to_owner_type == 'wallet':
                print(f"\n{Fore.CYAN}📈 Medium Signal: ${amount_usd:,.0f} {symbol} ← {from_owner} (Bullish){Style.RESET_ALL}")
        
        if signal_generated:
            print(f"{Fore.MAGENTA}💡 Tip: Use this signal alongside technical analysis for best results.{Style.RESET_ALL}\n")
            
    except Exception as e:
        production_logger.error("Failed to generate investment signal", 
                              extra={'extra_fields': {'error': str(e)}})

def process_whale_alert_transaction(whale_data: dict) -> bool:
    """
    Task 7: Process Whale Alert transaction through unified 7-phase pipeline.
    
    Args:
        whale_data (dict): Whale Alert transaction data
        
    Returns:
        bool: Success status
    """
    try:
        # Extract transaction details from Whale Alert format
        transaction_data = {
            'tx_hash': whale_data.get('hash', ''),
            'blockchain': whale_data.get('blockchain', 'ethereum'),
            'from_address': whale_data.get('from', {}).get('address', ''),
            'to_address': whale_data.get('to', {}).get('address', ''),
            'value_usd': whale_data.get('amount_usd', 0),
            'symbol': whale_data.get('symbol', ''),
            'amount': whale_data.get('amount', 0),
            'timestamp': whale_data.get('timestamp', time.time()),
            'gas_price': 0,  # Whale Alert doesn't provide gas price
            'source': 'whale_alert'
        }
        
        # Task 7: Pass through full MasterClassifier.analyze_transaction pipeline
        intelligence_result = whale_engine.analyze_transaction_comprehensive(transaction_data)
        
        # 🚀 GENERATE INVESTMENT SIGNALS 🚀
        # High-impact whale movement analysis for actionable trading ideas
        _generate_investment_signal(whale_data, transaction_data, intelligence_result)
        
        # Log the enhanced analysis
        production_logger.info("Whale Alert transaction analyzed", extra={
            'extra_fields': {
                'tx_hash': transaction_data['tx_hash'],
                'classification': intelligence_result.get('classification', 'UNKNOWN'),
                'confidence': intelligence_result.get('confidence', 0),
                'whale_score': intelligence_result.get('whale_score', 0),
                'phases_completed': len(intelligence_result.get('phase_results', {})),
                'value_usd': transaction_data['value_usd']
            }
        })
        
        # Store in transaction storage if enabled
        if REAL_TIME_ENABLED and transaction_storage.storage_enabled:
            classified_swap = _create_classified_swap_from_intelligence(transaction_data, intelligence_result)
            if classified_swap:
                transaction_storage.store_classified_swap(classified_swap)
        
        # 🚀 NEW: Store whale transaction classification for sentiment analysis
        if REAL_TIME_ENABLED and transaction_storage.storage_enabled:
            transaction_storage.store_whale_transaction(transaction_data, intelligence_result)
        
        # Display the transaction with enhanced classification
        if transaction_data['value_usd'] >= min_transaction_value:
            _display_whale_alert_transaction(transaction_data, intelligence_result)
        
        return True
        
    except Exception as e:
        production_logger.error("Failed to process Whale Alert transaction", 
                              extra={'extra_fields': {'error': str(e), 'whale_data': whale_data}})
        return False

def _display_whale_alert_transaction(transaction_data: dict, intelligence_result: dict):
    """
    Display Whale Alert transaction with 7-phase analysis results.
    
    Args:
        transaction_data (dict): Transaction data
        intelligence_result (dict): 7-phase analysis results
    """
    try:
        classification = intelligence_result.get('classification', 'TRANSFER')
        confidence = intelligence_result.get('confidence', 0)
        whale_score = intelligence_result.get('whale_score', 0)
        
        # Color coding based on classification
        if classification == 'BUY':
            color = GREEN
        elif classification == 'SELL':
            color = RED
        else:
            color = YELLOW
        
        # Enhanced display with 7-phase results
        print(f"{color}[WHALE ALERT - 7-PHASE] {classification} ({confidence:.1%}){END}")
        print(f"  Hash: {transaction_data['tx_hash'][:16]}...")
        print(f"  Value: ${transaction_data['value_usd']:,.0f} {transaction_data.get('symbol', '')}")
        print(f"  Whale Score: {whale_score:.2f}")
        print(f"  From: {transaction_data['from_address'][:10]}...")
        print(f"  To: {transaction_data['to_address'][:10]}...")
        
        # Show evidence from analysis
        evidence = intelligence_result.get('evidence', [])
        if evidence:
            print(f"  Evidence: {evidence[0]}")  # Show top evidence
        
        print()
        
    except Exception as e:
        logger.error(f"Failed to display Whale Alert transaction: {e}")

def print_classified_swap(swap: ClassifiedSwap):
    """Print a classified swap with enhanced formatting."""
    if not monitoring_enabled:
        return
    
    # Skip if below minimum value
    usd_value = float(swap.amount_in_usd) if swap.amount_in_usd else 0
    if usd_value < min_transaction_value:
        return
    
    # Choose color based on classification
    if swap.classification == "BUY":
        header_color = GREEN
        emoji = "🟢"
    elif swap.classification == "SELL":
        header_color = RED
        emoji = "🔴"
    else:
        header_color = YELLOW
        emoji = "🟡"
    
    # Format the header with DEX and chain info
    header = f"{emoji} [{swap.token_in_symbol or 'TOKEN'} → {swap.token_out_symbol or 'TOKEN'}] ${usd_value:,.2f} USD"
    header += f" | {swap.dex.upper()} on {swap.chain.upper()}"
    
    if swap.transaction_hash:
        header += f" | Tx {swap.transaction_hash[:16]}..."
    
    # Print with colors
    print(header_color + BOLD + header + END)
    
    # Print timestamp
    time_str = swap.block_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')
    print(f"  Time: {time_str}")
    
    # Print addresses
    print(f"  From: {swap.sender_address}")
    if swap.recipient_address:
        print(f"  To:   {swap.recipient_address}")
    
    # Print amounts with token symbols
    amount_in_formatted = float(swap.amount_in) / (10 ** (swap.token_in_decimals or 18))
    amount_out_formatted = float(swap.amount_out) / (10 ** (swap.token_out_decimals or 18))
    
    print(f"  Swap: {amount_in_formatted:,.6f} {swap.token_in_symbol or 'TOKEN'} → {amount_out_formatted:,.6f} {swap.token_out_symbol or 'TOKEN'}")
    
    # Print classification with confidence
    confidence_text = f"({swap.confidence_score:.1%} confidence)"
    whale_text = " 🐋 WHALE" if swap.is_whale_transaction else ""
    print(header_color + f"  Classification: {swap.classification} {confidence_text}{whale_text}" + END)
    
    # Print method used
    print(f"  Method: {swap.classification_method}")
    
    # Add a blank line
    print()

def run_web3_swap_monitor(chain: str) -> None:
    """Web3 log monitor for Uniswap v2/v3 Swap events with rolling cursors (no placeholders)."""
    try:
        import config.api_keys as api_keys
        from config import settings as cfg
        from utils.real_time_classification import classify_swap_transaction
    except Exception as e:
        print(RED + f"Web3 swap monitor init failed: {e}" + END)
        return
    
    # Build Web3 client per chain
    if chain == 'ethereum':
        rpc_url = api_keys.ETHEREUM_RPC_URL
    elif chain == 'polygon':
        rpc_url = api_keys.POLYGON_RPC_URL
    else:
        print(YELLOW + f"Unsupported chain for Web3 swap monitor: {chain}" + END)
        return
    
    # Detailed Web3 connection with error logging
    try:
        print(f"🔗 Attempting Web3 connection to {chain} (swaps): {rpc_url[:50]}...")
        w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': 10}))
        
        # Test connection with detailed error reporting
        if not w3.is_connected():
            print(RED + f"❌ Web3 connection test failed for {chain} (swaps)" + END)
            try:
                block = w3.eth.get_block('latest')
                print(f"✅ Actually connected! Latest block: {block['number']}")
            except Exception as test_error:
                error_msg = str(test_error)
                print(RED + f"❌ Web3 swap monitor connection error for {chain}:" + END)
                print(RED + f"   Error type: {type(test_error).__name__}" + END)
                print(RED + f"   Error message: {error_msg}" + END)
                
                if "429" in error_msg or "rate limit" in error_msg.lower():
                    print(YELLOW + "   🚨 RATE LIMIT DETECTED - Too many API requests" + END)
                elif "timeout" in error_msg.lower():
                    print(YELLOW + "   ⏱️  TIMEOUT - RPC endpoint not responding" + END)
                elif "unauthorized" in error_msg.lower() or "403" in error_msg:
                    print(YELLOW + "   🔒 UNAUTHORIZED - Check your API key" + END)
                elif "connection" in error_msg.lower():
                    print(YELLOW + "   🌐 CONNECTION ERROR - Network issue" + END)
                
                return
        else:
            print(GREEN + f"✅ Web3 swap monitor connected to {chain}" + END)
    except Exception as init_error:
        print(RED + f"❌ Failed to initialize Web3 swap monitor for {chain}:" + END)
        print(RED + f"   Error: {type(init_error).__name__}: {init_error}" + END)
        return
    
    topic_v2 = cfg.EVENT_SIGNATURES.get('uniswap_v2_swap')
    topic_v3 = cfg.EVENT_SIGNATURES.get('uniswap_v3_swap')
    topic_balancer = cfg.EVENT_SIGNATURES.get('balancer_v2_swap')
    topic_curve = cfg.EVENT_SIGNATURES.get('curve_token_exchange')
    if not topic_v2 or not topic_v3 or not topic_balancer or not topic_curve:
        print(RED + "Missing swap event signatures in settings" + END)
        return
    
    async def process_logs(logs: list[dict], dex: str) -> int:
        processed = 0
        for log in logs:
            try:
                tx_hash = log.get('transactionHash')
                tx_hash_hex_raw = tx_hash.hex() if hasattr(tx_hash, 'hex') else str(tx_hash)
                tx_hash_hex = tx_hash_hex_raw if isinstance(tx_hash_hex_raw, str) and tx_hash_hex_raw.startswith('0x') else f"0x{tx_hash_hex_raw}"
                block_number = log.get('blockNumber')
                data_field = log.get('data')
                data_hex_raw = data_field.hex() if hasattr(data_field, 'hex') else str(data_field)
                data_hex = data_hex_raw if isinstance(data_hex_raw, str) and data_hex_raw.startswith('0x') else f"0x{data_hex_raw}"
                address = log.get('address')
                topics_list = log.get('topics', [])
                # Normalize topics to hex strings with 0x prefix
                norm_topics = []
                for t in topics_list:
                    t_hex = t.hex() if hasattr(t, 'hex') else str(t)
                    norm_topics.append(t_hex if t_hex.startswith('0x') else f"0x{t_hex}")
                raw = {
                    'transactionHash': tx_hash_hex,
                    'blockNumber': block_number,
                    'data': data_hex,
                    'address': address,
                    'topics': norm_topics
                }
                swap = await classify_swap_transaction(raw, chain, dex)
                if swap and swap.amount_in_usd is not None and REAL_TIME_ENABLED and transaction_storage.storage_enabled:
                    try:
                        transaction_storage.store_classified_swap(swap)
                        processed += 1
                    except Exception:
                        pass
            except Exception:
                continue
        return processed
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    last_block = LAST_BLOCK_WEB3_SWAP_BY_CHAIN.get(chain)
    block_stride = 50
    print(BLUE + f"🔄 Starting Web3 swap monitor for {chain}..." + END)
    while not shutdown_flag.is_set() and monitoring_enabled:
        try:
            latest = w3.eth.block_number
            if last_block is None:
                last_block = max(0, latest - 20)
                LAST_BLOCK_WEB3_SWAP_BY_CHAIN[chain] = last_block
            if latest <= last_block:
                time.sleep(1.0)
                continue
            from_block = last_block + 1
            to_block = min(latest, from_block + block_stride)
            v2_filter = {'fromBlock': from_block, 'toBlock': to_block, 'topics': [topic_v2]}
            v3_filter = {'fromBlock': from_block, 'toBlock': to_block, 'topics': [topic_v3]}
            balancer_filter = {'fromBlock': from_block, 'toBlock': to_block, 'topics': [topic_balancer]}
            curve_filter = {'fromBlock': from_block, 'toBlock': to_block, 'topics': [topic_curve]}
            v2_logs = []
            v3_logs = []
            balancer_logs = []
            curve_logs = []
            try:
                v2_logs = w3.eth.get_logs(v2_filter)
            except Exception as e:
                print(YELLOW + f"{chain} v2 get_logs error {from_block}-{to_block}: {e}" + END)
            try:
                v3_logs = w3.eth.get_logs(v3_filter)
            except Exception as e:
                print(YELLOW + f"{chain} v3 get_logs error {from_block}-{to_block}: {e}" + END)
            try:
                balancer_logs = w3.eth.get_logs(balancer_filter)
            except Exception as e:
                print(YELLOW + f"{chain} balancer get_logs error {from_block}-{to_block}: {e}" + END)
            try:
                curve_logs = w3.eth.get_logs(curve_filter)
            except Exception as e:
                print(YELLOW + f"{chain} curve get_logs error {from_block}-{to_block}: {e}" + END)
            total = 0
            if v2_logs:
                total += loop.run_until_complete(process_logs(v2_logs, 'uniswap_v2'))
            if v3_logs:
                total += loop.run_until_complete(process_logs(v3_logs, 'uniswap_v3'))
            if balancer_logs:
                total += loop.run_until_complete(process_logs(balancer_logs, 'balancer_v2'))
            if curve_logs:
                total += loop.run_until_complete(process_logs(curve_logs, 'curve'))
            if total:
                print(GREEN + f"✅ {chain} Web3 swaps processed: {total} (blocks {from_block}-{to_block})" + END)
            LAST_BLOCK_WEB3_SWAP_BY_CHAIN[chain] = to_block
            last_block = to_block
            time.sleep(0.5)
        except Exception as e:
            if not shutdown_flag.is_set():
                print(YELLOW + f"{chain} Web3 swap loop error: {e}" + END)
            time.sleep(1.0)

def run_1inch_monitor(chain: str) -> None:
    """Monitor transactions sent to 1inch routers and classify via Transfer delta."""
    try:
        import config.api_keys as api_keys
        from config import settings as cfg
        from utils.real_time_classification import classifier
    except Exception as e:
        print(RED + f"1inch monitor init failed: {e}" + END)
        return
    
    if chain == 'ethereum':
        rpc_url = api_keys.ETHEREUM_RPC_URL
    elif chain == 'polygon':
        rpc_url = api_keys.POLYGON_RPC_URL
    else:
        print(YELLOW + f"Unsupported chain for 1inch monitor: {chain}" + END)
        return
    
    # Detailed Web3 connection with error logging
    try:
        print(f"🔗 Attempting Web3 connection to {chain} (1inch): {rpc_url[:50]}...")
        w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': 10}))
        
        # Test connection with detailed error reporting
        if not w3.is_connected():
            print(RED + f"❌ Web3 connection test failed for {chain} (1inch)" + END)
            try:
                block = w3.eth.get_block('latest')
                print(f"✅ Actually connected! Latest block: {block['number']}")
            except Exception as test_error:
                error_msg = str(test_error)
                print(RED + f"❌ Web3 1inch monitor connection error for {chain}:" + END)
                print(RED + f"   Error type: {type(test_error).__name__}" + END)
                print(RED + f"   Error message: {error_msg}" + END)
                
                if "429" in error_msg or "rate limit" in error_msg.lower():
                    print(YELLOW + "   🚨 RATE LIMIT DETECTED - Too many API requests" + END)
                    print(YELLOW + "   💡 Solution: Wait a few minutes or use a different RPC provider" + END)
                elif "timeout" in error_msg.lower():
                    print(YELLOW + "   ⏱️  TIMEOUT - RPC endpoint not responding" + END)
                    print(YELLOW + "   💡 Solution: Check internet connection or try a different RPC" + END)
                elif "unauthorized" in error_msg.lower() or "403" in error_msg:
                    print(YELLOW + "   🔒 UNAUTHORIZED - Check your API key" + END)
                    print(YELLOW + f"   💡 Current RPC: {rpc_url[:80]}" + END)
                elif "connection" in error_msg.lower():
                    print(YELLOW + "   🌐 CONNECTION ERROR - Network issue" + END)
                
                return
        else:
            print(GREEN + f"✅ Web3 1inch monitor connected to {chain}" + END)
    except Exception as init_error:
        print(RED + f"❌ Failed to initialize Web3 1inch monitor for {chain}:" + END)
        print(RED + f"   Error: {type(init_error).__name__}: {init_error}" + END)
        return
    
    router_addr = cfg.DEX_CONTRACT_INFO.get(chain, {}).get('1inch_v6_router', {}).get('address')
    if not router_addr:
        print(YELLOW + f"1inch router address not found for {chain}" + END)
        return
    
    router_addr = router_addr.lower()
    
    async def process_1inch_tx(tx_hash: str) -> bool:
        try:
            swap = await classifier.classify_1inch_swap(tx_hash, chain)
            if swap and swap.amount_in_usd is not None and swap.amount_in_usd >= cfg.GLOBAL_USD_THRESHOLD and REAL_TIME_ENABLED and transaction_storage.storage_enabled:
                try:
                    transaction_storage.store_classified_swap(swap)
                    return True
                except Exception:
                    pass
        except Exception:
            pass
        return False
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    last_block = LAST_BLOCK_WEB3_SWAP_BY_CHAIN.get(f"{chain}_1inch")
    block_stride = 20
    print(BLUE + f"🔄 Starting 1inch monitor for {chain}..." + END)
    while not shutdown_flag.is_set() and monitoring_enabled:
        try:
            latest = w3.eth.block_number
            if last_block is None:
                last_block = max(0, latest - 10)
                LAST_BLOCK_WEB3_SWAP_BY_CHAIN[f"{chain}_1inch"] = last_block
            if latest <= last_block:
                time.sleep(2.0)
                continue
            from_block = last_block + 1
            to_block = min(latest, from_block + block_stride)
            
            # Fetch all transactions to 1inch router in this block range
            processed = 0
            for block_num in range(from_block, to_block + 1):
                try:
                    block = w3.eth.get_block(block_num, full_transactions=True)
                    for tx in block.get('transactions', []):
                        if tx.get('to', '').lower() == router_addr:
                            tx_hash = tx['hash'].hex() if hasattr(tx['hash'], 'hex') else str(tx['hash'])
                            if loop.run_until_complete(process_1inch_tx(tx_hash)):
                                processed += 1
                except Exception as e:
                    logger.debug(f"{chain} 1inch block {block_num} error: {e}")
            
            if processed:
                print(GREEN + f"✅ {chain} 1inch processed: {processed} (blocks {from_block}-{to_block})" + END)
            LAST_BLOCK_WEB3_SWAP_BY_CHAIN[f"{chain}_1inch"] = to_block
            last_block = to_block
            time.sleep(1.0)
        except Exception as e:
            if not shutdown_flag.is_set():
                print(YELLOW + f"{chain} 1inch loop error: {e}" + END)
            time.sleep(2.0)

def diagnose_rpc_connections():
    """Test and diagnose all RPC connections before starting monitors."""
    print("\n" + "="*80)
    print("🔍 DIAGNOSING RPC CONNECTIONS")
    print("="*80)
    
    # Check Ethereum RPC
    print(f"\n📡 ETHEREUM RPC Configuration:")
    print(f"   URL: {api_keys.ETHEREUM_RPC_URL}")
    print(f"   Available providers: {len(api_keys.ETHEREUM_RPC_PROVIDERS)}")
    
    try:
        eth_w3 = Web3(Web3.HTTPProvider(api_keys.ETHEREUM_RPC_URL, request_kwargs={'timeout': 5}))
        if eth_w3.is_connected():
            block = eth_w3.eth.block_number
            print(GREEN + f"   ✅ Connected! Latest block: {block}" + END)
        else:
            print(YELLOW + "   ⚠️  Connection test returned False" + END)
            try:
                block = eth_w3.eth.block_number
                print(GREEN + f"   ✅ But we can get blocks: {block}" + END)
            except Exception as e:
                print(RED + f"   ❌ Cannot get blocks: {type(e).__name__}: {str(e)[:100]}" + END)
    except Exception as e:
        print(RED + f"   ❌ Connection failed: {type(e).__name__}: {str(e)[:100]}" + END)
    
    # Check Polygon RPC
    print(f"\n📡 POLYGON RPC Configuration:")
    print(f"   URL: {api_keys.POLYGON_RPC_URL}")
    print(f"   Available providers: {len(api_keys.POLYGON_RPC_PROVIDERS)}")
    
    try:
        poly_w3 = Web3(Web3.HTTPProvider(api_keys.POLYGON_RPC_URL, request_kwargs={'timeout': 5}))
        if poly_w3.is_connected():
            block = poly_w3.eth.block_number
            print(GREEN + f"   ✅ Connected! Latest block: {block}" + END)
        else:
            print(YELLOW + "   ⚠️  Connection test returned False" + END)
            try:
                block = poly_w3.eth.block_number
                print(GREEN + f"   ✅ But we can get blocks: {block}" + END)
            except Exception as e:
                print(RED + f"   ❌ Cannot get blocks: {type(e).__name__}: {str(e)[:100]}" + END)
    except Exception as e:
        print(RED + f"   ❌ Connection failed: {type(e).__name__}: {str(e)[:100]}" + END)
    
    # Check Alchemy API key
    print(f"\n🔑 API KEY STATUS:")
    alchemy_key = api_keys.ALCHEMY_API_KEY
    if alchemy_key and alchemy_key != "YourApiKeyToken" and len(alchemy_key) > 10:
        print(GREEN + f"   ✅ Alchemy API key configured: {alchemy_key[:10]}...{alchemy_key[-4:]}" + END)
    else:
        print(YELLOW + "   ⚠️  Alchemy API key not properly configured" + END)
    
    print("\n" + "="*80 + "\n")

def start_real_time_monitoring():
    """Start real-time DEX monitoring threads."""
    if not REAL_TIME_ENABLED:
        print("⚠️  Real-time monitoring not available")
        return []
    
    # Run diagnostics first
    diagnose_rpc_connections()
    
    real_time_threads = []
    
    try:
        # Start Ethereum monitoring
        ethereum_thread = threading.Thread(
            target=monitor_ethereum_swaps,
            name="EthereumSwapMonitor",
            daemon=True
        )
        ethereum_thread.start()
        real_time_threads.append(ethereum_thread)
        
        # Start Polygon monitoring
        polygon_thread = threading.Thread(
            target=monitor_polygon_swaps,
            name="PolygonSwapMonitor", 
            daemon=True
        )
        polygon_thread.start()
        real_time_threads.append(polygon_thread)
        
        # Start Solana monitoring (websocket-based)
        solana_thread = start_solana_thread()
        if solana_thread:
            real_time_threads.append(solana_thread)
        
        print(f"✅ Started {len(real_time_threads)} real-time monitoring threads")
        
    except Exception as e:
        error_msg = f"❌ Failed to start real-time monitoring: {e}"
        print(error_msg)
        log_error(error_msg)
    
    return real_time_threads

def monitor_ethereum_swaps():
    """Monitor Ethereum Uniswap swaps via Etherscan API."""
    print("🔄 Starting Ethereum swap monitoring...")
    
    try:
        from utils.etherscan_poller import start_ethereum_polling
        
        # Create storage callback
        def storage_callback(classified_swap):
            try:
                # Store in database
                stored = transaction_storage.store_classified_swap(classified_swap)
                
                # Print to console if above threshold
                if classified_swap.amount_in_usd and float(classified_swap.amount_in_usd) >= min_transaction_value:
                    print_classified_swap(classified_swap)
                
                return stored
            except Exception as e:
                print(f"❌ Storage callback error: {e}")
                return False
        
        # Start the async polling loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(start_ethereum_polling(storage_callback))
        
    except ImportError:
        print("⚠️  Etherscan poller not available, using placeholder")
        # Fallback to placeholder
        while not shutdown_flag.is_set() and monitoring_enabled:
            try:
                time.sleep(15)
            except Exception as e:
                if not shutdown_flag.is_set():
                    print(f"❌ Ethereum monitoring error: {e}")
                time.sleep(5)
    except Exception as e:
        print(f"❌ Ethereum monitoring failed: {e}")

def monitor_polygon_swaps():
    """Monitor Polygon Uniswap swaps via Polygonscan API."""
    print("🔄 Starting Polygon swap monitoring...")
    
    try:
        from utils.etherscan_poller import start_polygon_polling
        
        # Create storage callback
        def storage_callback(classified_swap):
            try:
                # Store in database
                stored = transaction_storage.store_classified_swap(classified_swap)
                
                # Print to console if above threshold
                if classified_swap.amount_in_usd and float(classified_swap.amount_in_usd) >= min_transaction_value:
                    print_classified_swap(classified_swap)
                
                return stored
            except Exception as e:
                print(f"❌ Storage callback error: {e}")
                return False
        
        # Start the async polling loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(start_polygon_polling(storage_callback))
        
    except ImportError:
        print("⚠️  Polygonscan poller not available, using placeholder")
        # Fallback to placeholder
        while not shutdown_flag.is_set() and monitoring_enabled:
            try:
                time.sleep(15)
            except Exception as e:
                if not shutdown_flag.is_set():
                    print(f"❌ Polygon monitoring error: {e}")
                time.sleep(5)
    except Exception as e:
        print(f"❌ Polygon monitoring failed: {e}")

def monitor_solana_swaps():
    """Monitor Solana Jupiter swaps via Helius webhooks."""
    print("🔄 Starting Solana swap monitoring...")
    print("⚠️  Solana monitoring requires webhook setup - placeholder for now")
    
    while not shutdown_flag.is_set() and monitoring_enabled:
        try:
            # Placeholder for Helius webhook processing
            # In production, this would set up a Flask webhook endpoint
            time.sleep(10)
            
        except Exception as e:
            if not shutdown_flag.is_set():
                print(f"❌ Solana monitoring error: {e}")
            time.sleep(5)

def clear_screen():
    """Clear the terminal"""
    os.system('clear')

def color_text(text, color=None, bold=False):
    """Return colored text without printing"""
    formatted = ""
    if bold:
        formatted += BOLD
    if color:
        formatted += color
    
    return formatted + text + END

def print_simple_header():
    """Print a simple header"""
    clear_screen()
    print(BLUE + BOLD + "=" * 80 + END)
    print(PURPLE + BOLD + " " * 20 + "CRYPTO WHALE TRANSACTION MONITOR" + END)
    print(BLUE + BOLD + "=" * 80 + END)
    print(YELLOW + f"Minimum Value: ${min_transaction_value:,.2f}" + END)
    print(GREEN + f"Active Monitors: {', '.join([t.name for t in active_threads if t.is_alive()])}" + END)
    print(BLUE + "Press Ctrl+C to exit and view summary" + END)
    print(BLUE + "-" * 80 + END)
    print()  # Add a blank line

def print_transaction(tx_data):
    """Print a transaction with simple color formatting and whale intelligence"""
    # Only print if monitoring is enabled
    if not monitoring_enabled:
        return
        
    # Skip if below minimum value
    usd_value = tx_data.get("usd_value", 0)
    if usd_value < min_transaction_value:
        return
    
    # Run whale intelligence analysis if available
    whale_analysis = None
    if WHALE_INTELLIGENCE_ENABLED:
        try:
            whale_analysis = whale_intelligence_engine.analyze_transaction_comprehensive(tx_data)
            if whale_analysis and whale_analysis.get('confidence', 0) > 0.7:
                # Use whale intelligence classification if confidence is high
                tx_data['classification'] = whale_analysis['classification']
                tx_data['confidence'] = whale_analysis['confidence']
                tx_data['whale_evidence'] = whale_analysis.get('evidence', [])
                tx_data['whale_signals'] = whale_analysis.get('whale_signals', [])
                tx_data['dex_info'] = whale_analysis.get('dex_info', {})
                
                # 🚀 NEW: Store whale transaction classification for sentiment analysis
                if REAL_TIME_ENABLED and transaction_storage.storage_enabled:
                    transaction_storage.store_whale_transaction(tx_data, whale_analysis)
        except Exception as e:
            pass  # Silently continue if whale intelligence fails
    
    # Get transaction details
    tx_hash = tx_data.get("tx_hash", "")
    symbol = tx_data.get("symbol", "")
    from_addr = tx_data.get("from", "")
    to_addr = tx_data.get("to", "")
    classification = tx_data.get("classification", "").upper()
    chain = tx_data.get("blockchain", "").upper() or tx_data.get("source", "").upper()
    amount = tx_data.get("amount", 0)
    
    # Choose color based on classification
    if classification == "BUY":
        header_color = GREEN
    elif classification == "SELL":
        header_color = RED
    else:  # Transfer
        header_color = YELLOW
    
    # Enhanced whale indicator
    whale_indicator = ""
    if whale_analysis and whale_analysis.get('whale_signals'):
        whale_indicator = " 🐋"
    elif usd_value >= 100000:  # $100k+ transactions
        whale_indicator = " 🐋"
    
    # Format the transaction header
    header = f"[{symbol} | ${usd_value:,.2f} USD]{whale_indicator}"
    if tx_data.get("block_number"):
        header += f" Block {tx_data.get('block_number')}"
    if tx_hash:
        header += f" | Tx {tx_hash[:16]}..." if len(tx_hash) > 16 else f" | Tx {tx_hash}"
    
    # Print with colors
    print(header_color + BOLD + header + END)
    
    # Print timestamp if available
    if "timestamp" in tx_data:
        try:
            timestamp = tx_data["timestamp"]
            time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
            print(f"  Time: {time_str}")
        except:
            pass
    
    # Print from/to addresses
    print(f"  From: {from_addr}")
    print(f"  To:   {to_addr}")
    
    # Print amount and classification
    print(f"  Amount: {amount:,.2f} {symbol} (~${usd_value:,.2f} USD)")
    confidence = tx_data.get("confidence", 0)
    confidence_color = GREEN if confidence > 0.8 else YELLOW if confidence > 0.5 else ""
    print(header_color + f"  Classification: {classification} (confidence: {confidence_color}{confidence:.2f}{END if confidence_color else ''})" + END)
    
    # Show DEX information if available
    dex_info = tx_data.get('dex_info', {})
    if dex_info.get('dex'):
        print(f"  DEX: {PURPLE}{dex_info['dex']}{END}")
    
    # Show whale signals if available
    whale_signals = tx_data.get('whale_signals', [])
    if whale_signals:
        print(f"  🐋 Whale Signals:")
        for signal in whale_signals[:2]:  # Show first 2 signals
            print(f"    • {signal}")
    
    # Add a blank line
    print()

def monitor_transactions():
    """Monitor transactions from the dedup cache with improved safety"""
    processed_txs = set()
    
    # Thread safety counter
    safety_counter = 0
    
    while not shutdown_flag.is_set() and monitoring_enabled:
        try:
            # Check shutdown flag first
            if shutdown_flag.is_set() or not monitoring_enabled:
                return
            
            # Safety check - if we've hit errors multiple times
            if safety_counter > 10:
                print(YELLOW + "Transaction monitor reset due to errors" + END)
                safety_counter = 0
                time.sleep(0.5)
                continue
                
            # Look for new transactions safely
            try:
                # First get list of keys (separate step to avoid dictionary iteration issues)
                tx_keys = list(deduped_transactions.keys())
                
                # Now process one at a time
                for tx_key in tx_keys:
                    # Check if still in dict (might have been removed)
                    if tx_key in deduped_transactions:
                        # Get tx safely
                        try:
                            tx = deduped_transactions[tx_key]
                        except KeyError:
                            # Key was removed between listing and access
                            continue
                            
                        # Exit early if shutdown requested
                        if shutdown_flag.is_set() or not monitoring_enabled:
                            return
                            
                        if tx_key not in processed_txs:
                            processed_txs.add(tx_key)
                            try:
                                print_transaction(tx)
                            except Exception as tx_error:
                                # One transaction print failed, but continue
                                if not shutdown_flag.is_set():
                                    print(f"Error printing transaction: {tx_error}")
            except RuntimeError as re:
                # Dictionary changed size error
                safety_counter += 1
                time.sleep(0.05)
                continue
            
            # Keep set size reasonable
            if len(processed_txs) > 5000:
                processed_txs = set(list(processed_txs)[-2000:])
                
            # Reset safety counter on success
            safety_counter = 0
                
            time.sleep(0.05)
            
        except Exception as e:
            if not shutdown_flag.is_set():
                print(f"Error monitoring transactions: {e}")
            # Increment error counter
            safety_counter += 1
            time.sleep(0.1)

# 🚀 PROFESSIONAL MULTI-TOKEN MONITORING SYSTEM
def get_threshold_for_tier(tier: str) -> int:
    """Get USD threshold based on market cap tier"""
    tier_thresholds = {
        'large': 50_000,    # $50K+ for major coins
        'medium': 15_000,   # $15K+ for mid-cap tokens  
        'small': 5_000,     # $5K+ for small-cap tokens
        'micro': 1_000,     # $1K+ for micro-cap tokens
        'emerging': 500     # $500+ for emerging tokens
    }
    return tier_thresholds.get(tier, 1_000)  # Default to $1K

def start_multi_token_monitoring():
    """
    🚀 ENHANCED MULTI-TOKEN MONITORING SYSTEM
    
    Professional whale monitoring across all 109 ERC-20 tokens with:
    - Dynamic thresholds based on token tiers
    - Conservative rate limiting
    - Professional error handling
    - Real-time BUY/SELL detection
    """
    import threading
    import time
    
    print(f"{BLUE}🚀 STARTUP: Starting enhanced multi-token monitoring...{END}")
    
    try:
        # Conservative grouping for rate limiting
        tokens_per_group = 20  # Increase throughput while respecting limits
        # Exclude OXT from monitoring as requested
        filtered_tokens = [t for t in TOP_100_ERC20_TOKENS if t.get('symbol') != 'OXT']
        token_groups = [
            filtered_tokens[i:i + tokens_per_group]
            for i in range(0, len(filtered_tokens), tokens_per_group)
        ]
        
        # 🚀 FULL MONITORING: Monitor ALL 109 tokens (limit removed)
        # token_groups = token_groups[:4]  # REMOVED: No longer limiting to 60 tokens
        
        print(f"{BLUE}🔧 STARTUP: Configured {sum(len(group) for group in token_groups)} tokens in {len(token_groups)} groups{END}")
        
        threads = []
        
        for group_idx, token_group in enumerate(token_groups):
            try:
                thread = threading.Thread(
                    target=monitor_token_group,
                    args=(token_group, group_idx),
                    daemon=True,
                    name=f"TokenGroup-{group_idx}"
                )
                
                thread.start()
                threads.append(thread)
                
                print(f"{GREEN}✅ Started monitoring group {group_idx}: {len(token_group)} tokens{END}")
                
                # Staggered startup
                if group_idx < len(token_groups) - 1:
                    time.sleep(2)
                    
            except Exception as e:
                print(f"{RED}❌ Failed to start group {group_idx}: {e}{END}")
        
        print(f"{GREEN}🎉 Multi-token monitoring started: {sum(len(group) for group in token_groups)} tokens across {len(threads)} groups{END}")
        return threads
        
    except Exception as e:
        print(f"{RED}❌ Critical error starting multi-token monitoring: {e}{END}")


def monitor_token_group(tokens: list, group_id: int):
    """
    🎯 MONITOR SPECIFIC TOKEN GROUP
    
    Professional monitoring of a token group with:
    - Conservative rate limiting (3 seconds between tokens)
    - Comprehensive error handling
    - Real-time whale detection and classification
    - Automatic retries and recovery
    """
    import time
    
    group_name = f"Group-{group_id}"
    cycle_count = 0
    
    print(f"{GREEN}🚀 {group_name}: STARTED with {len(tokens)} tokens{END}")
    print(f"{GREEN}🚀 {group_name}: {', '.join([t['symbol'] for t in tokens[:5]])}{'...' if len(tokens) > 5 else ''}{END}")
    
    while not shutdown_flag.is_set() and monitoring_enabled:
        try:
            cycle_count += 1
            cycle_start = time.time()
            
            print(f"{BLUE}📊 {group_name}: CYCLE {cycle_count} - Processing {len(tokens)} tokens{END}")
            
            for token_idx, token in enumerate(tokens):
                if shutdown_flag.is_set() or not monitoring_enabled:
                    return
                
                try:
                    symbol = token['symbol']
                    address = token['address']
                    tier = token.get('tier', 'medium')
                    threshold_usd = get_threshold_for_tier(tier)
                    
                    print(f"{BLUE}🔍 {group_name}: Fetching {symbol} transactions (threshold: ${threshold_usd:,}){END}")
                    
                    # Fetch recent transactions for this token
                    transactions = fetch_token_transactions(symbol, address, threshold_usd)
                    
                    processed_count = 0
                    for tx in transactions:
                        tx_hash = tx.get('hash', tx.get('transactionHash', ''))
                        
                        # 🔧 CRITICAL: Use pipeline-level deduplication
                        if tx_hash and not is_transaction_already_processed(tx_hash):
                            # Enhanced transaction data
                            enhanced_tx = {
                                **tx,
                                'token_symbol': symbol,
                                'token_address': address,
                                'market_cap_tier': tier,
                                'whale_threshold': threshold_usd,
                                'chain': 'ethereum'
                            }
                            
                            # Process through whale intelligence
                            try:
                                display_transaction(enhanced_tx)
                                processed_count += 1
                            except Exception as e:
                                print(f"{RED}❌ {group_name}: Processing error for {symbol}: {e}{END}")
                        elif tx_hash:
                            # Transaction already processed - skip silently
                            pass
                    
                    if processed_count > 0:
                        print(f"{GREEN}✅ {group_name}: {symbol} - {processed_count} transactions processed{END}")
                    else:
                        print(f"{BLUE}⏸️ {group_name}: {symbol} - No qualifying transactions{END}")
                        
                except Exception as e:
                    print(f"{RED}❌ {group_name}: Error with {symbol}: {e}{END}")
                    continue
                
                # Conservative rate limiting
                if token_idx < len(tokens) - 1:
                    time.sleep(3)  # 3 seconds between tokens
            
            cycle_duration = time.time() - cycle_start
            print(f"{GREEN}✅ {group_name}: Cycle {cycle_count} completed in {cycle_duration:.1f}s{END}")
            
            # Wait between cycles (staggered by group ID)
            cycle_wait = 45 + (group_id * 8)  # shorter cycle spacing
            time.sleep(cycle_wait)
            
        except Exception as e:
            print(f"{RED}❌ {group_name}: Cycle error: {e}{END}")
            time.sleep(30)  # Wait before retrying


def fetch_token_transactions(symbol: str, contract_address: str, threshold_usd: float) -> list:
    """
    🔍 FETCH TOKEN TRANSACTIONS
    
    Fetch recent transactions for a specific ERC-20 token with:
    - Etherscan API integration
    - USD value filtering
    - Error handling and retries
    """
    try:
        from chains.ethereum import fetch_erc20_transfers
        from data.tokens import TOKEN_PRICES
        
        # Get token price for USD calculation
        price = TOKEN_PRICES.get(symbol, 0)
        if price == 0:
            print(f"{RED}⚠️ No price data for {symbol} - skipping{END}")
            return []
        
        # Fetch transfers using existing ethereum chain function
        transfers = fetch_erc20_transfers(contract_address, sort="desc")
        if not transfers:
            return []
        
        # Filter by USD threshold
        qualified_transactions = []
        for tx in transfers[:20]:  # Check last 20 transactions
            try:
                # Calculate USD value
                token_info = next((t for t in TOP_100_ERC20_TOKENS if t['symbol'] == symbol), None)
                if not token_info:
                    continue
                    
                decimals = token_info.get('decimals', 18)
                raw_value = int(tx.get("value", 0))
                token_amount = raw_value / (10 ** decimals)
                estimated_usd = token_amount * price
                
                if estimated_usd >= threshold_usd:
                    # Add USD value to transaction with multiple field names for compatibility
                    tx['estimated_usd'] = estimated_usd
                    tx['value_usd'] = estimated_usd  # For compatibility
                    tx['usd_value'] = estimated_usd  # For compatibility
                    tx['token_amount'] = token_amount
                    qualified_transactions.append(tx)
                    
            except Exception as e:
                print(f"{RED}❌ Error processing {symbol} transaction: {e}{END}")
                continue
        
        return qualified_transactions
        
    except Exception as e:
        print(f"{RED}❌ Error fetching {symbol} transactions: {e}{END}")
        return []

def start_monitoring_threads():
    """Start all monitoring threads"""
    threads = []
    
    try:
        # 🚀 PRIORITY: Start Web3 event-driven monitors (ethereum & polygon)
        print(BLUE + "🚀 Starting Web3 event-driven monitors..." + END)
        try:
            eth_thread = threading.Thread(
                target=run_web3_transfer_monitor,
                args=('ethereum',),
                daemon=True,
                name="Web3-Ethereum"
            )
            eth_thread.start()
            threads.append(eth_thread)
            print(GREEN + "✅ Web3 Ethereum monitor started" + END)
        except Exception as e:
            print(RED + f"❌ Error starting Web3 Ethereum monitor: {e}" + END)
        try:
            poly_thread = threading.Thread(
                target=run_web3_transfer_monitor,
                args=('polygon',),
                daemon=True,
                name="Web3-Polygon"
            )
            poly_thread.start()
            threads.append(poly_thread)
            print(GREEN + "✅ Web3 Polygon monitor started" + END)
        except Exception as e:
            print(RED + f"❌ Error starting Web3 Polygon monitor: {e}" + END)

        # Start Web3 swap monitors (Uniswap v2/v3) for exact DEX swaps
        try:
            eth_swaps_thread = threading.Thread(
                target=run_web3_swap_monitor,
                args=('ethereum',),
                daemon=True,
                name="Web3-Ethereum-Swaps"
            )
            eth_swaps_thread.start()
            threads.append(eth_swaps_thread)
            print(GREEN + "✅ Web3 Ethereum swaps monitor started" + END)
        except Exception as e:
            print(RED + f"❌ Error starting Web3 Ethereum swaps monitor: {e}" + END)
        try:
            poly_swaps_thread = threading.Thread(
                target=run_web3_swap_monitor,
                args=('polygon',),
                daemon=True,
                name="Web3-Polygon-Swaps"
            )
            poly_swaps_thread.start()
            threads.append(poly_swaps_thread)
            print(GREEN + "✅ Web3 Polygon swaps monitor started" + END)
        except Exception as e:
            print(RED + f"❌ Error starting Web3 Polygon swaps monitor: {e}" + END)
        
        # Start 1inch monitors (Transfer delta analysis)
        try:
            eth_1inch_thread = threading.Thread(
                target=run_1inch_monitor,
                args=('ethereum',),
                daemon=True,
                name="1inch-Ethereum"
            )
            eth_1inch_thread.start()
            threads.append(eth_1inch_thread)
            print(GREEN + "✅ 1inch Ethereum monitor started" + END)
        except Exception as e:
            print(RED + f"❌ Error starting 1inch Ethereum monitor: {e}" + END)
        try:
            poly_1inch_thread = threading.Thread(
                target=run_1inch_monitor,
                args=('polygon',),
                daemon=True,
                name="1inch-Polygon"
            )
            poly_1inch_thread.start()
            threads.append(poly_1inch_thread)
            print(GREEN + "✅ 1inch Polygon monitor started" + END)
        except Exception as e:
            print(RED + f"❌ Error starting 1inch Polygon monitor: {e}" + END)

        # Etherscan fallback: start multi-token polling as a supplementary source
        print(BLUE + "🔧 Starting Etherscan fallback multi-token polling..." + END)
        try:
            multi_token_threads = start_multi_token_monitoring()
            if multi_token_threads:
                threads.extend(multi_token_threads)
                print(GREEN + f"✅ Fallback multi-token monitor started ({len(multi_token_threads)} groups)" + END)
            else:
                print(YELLOW + "⚠️ Fallback multi-token monitor could not be started" + END)
        except Exception as e:
            print(RED + f"❌ Error starting fallback monitor: {e}" + END)
        
        # Try to start Whale Alert monitor
        try:
            whale_thread = start_whale_thread()
            if whale_thread:
                threads.append(whale_thread)
                print(GREEN + "✅ Whale Alert monitor started" + END)
            else:
                print(YELLOW + "⚠️ Whale Alert monitor could not be started" + END)
        except Exception as e:
            print(RED + f"❌ Error starting Whale Alert monitor: {e}" + END)
        
        # Try to start XRP monitor
        try:
            xrp_thread = start_xrp_thread()
            if xrp_thread:
                threads.append(xrp_thread)
                print(GREEN + "✅ XRP monitor started" + END)
            else:
                print(YELLOW + "⚠️ XRP monitor could not be started" + END)
        except Exception as e:
            print(RED + f"❌ Error starting XRP monitor: {e}" + END)
        
        # Try to start Polygon monitor
        try:
            polygon_thread = threading.Thread(
                target=print_new_polygon_transfers,
                daemon=True,
                name="Polygon"
            )
            polygon_thread.start()
            threads.append(polygon_thread)
            print(GREEN + "✅ Polygon monitor started" + END)
        except Exception as e:
            print(RED + f"❌ Error starting Polygon monitor: {e}" + END)
        
        # Try to start Solana monitor
        try:
            solana_thread = start_solana_thread()
            if solana_thread:
                threads.append(solana_thread)
                print(GREEN + "✅ Solana monitor started" + END)
            else:
                print(YELLOW + "⚠️ Solana monitor could not be started" + END)
        except Exception as e:
            print(RED + f"❌ Error starting Solana monitor: {e}" + END)
        
        # Try to start Solana API monitor
        try:
            solana_api_thread = threading.Thread(
                target=print_new_solana_transfers,
                daemon=True,
                name="Solana-API"
            )
            solana_api_thread.start()
            threads.append(solana_api_thread)
            print(GREEN + "✅ Solana API monitor started" + END)
        except Exception as e:
            print(RED + f"❌ Error starting Solana API monitor: {e}" + END)
        
        # Start real-time DEX monitoring
        try:
            real_time_threads = start_real_time_monitoring()
            threads.extend(real_time_threads)
            if real_time_threads:
                print(GREEN + f"✅ Real-time DEX monitoring started ({len(real_time_threads)} threads)" + END)
            else:
                print(YELLOW + "⚠️ Real-time DEX monitoring not available" + END)
        except Exception as e:
            print(RED + f"❌ Error starting real-time monitoring: {e}" + END)
        
        # 🚀 NEW: Start whale sentiment aggregation service
        try:
            if SENTIMENT_AGGREGATION_ENABLED:
                whale_sentiment_aggregator.start(interval_seconds=60)  # Update every minute
                print(GREEN + "✅ Whale sentiment aggregator started (60s intervals)" + END)
            else:
                print(YELLOW + "⚠️ Whale sentiment aggregation not available" + END)
        except Exception as e:
            print(RED + f"❌ Error starting whale sentiment aggregator: {e}" + END)
        
        return threads
        
    except Exception as e:
        print(RED + f"❌ Error starting monitoring threads: {e}" + END)
        traceback.print_exc()
        return []

def print_simple_summary():
    """Print a simple summary that's guaranteed to work even in error conditions"""
    try:
        clear_screen()
        
        # Header
        print(BLUE + BOLD + "=" * 80 + END)
        print(PURPLE + BOLD + " " * 25 + "FINAL ANALYSIS REPORT" + END)
        print(BLUE + BOLD + "=" * 80 + END)
        print()
        
        # Get dedup stats safely
        try:
            dedup_stats = get_stats()
        except Exception as e:
            print(RED + f"Error getting deduplication stats: {e}" + END)
            dedup_stats = {'total_received': 0, 'total_transactions': 0, 'duplicates_caught': 0, 'dedup_ratio': 0}
        
        # Collect token statistics
        token_stats = defaultdict(lambda: {'buys': 0, 'sells': 0, 'transfers': 0, 'volume': 0.0})
        
        # Process Ethereum transactions - safely
        try:
            for symbol, count in etherscan_buy_counts.items():
                token_stats[symbol]['buys'] += count
            for symbol, count in etherscan_sell_counts.items():
                token_stats[symbol]['sells'] += count
        except Exception as e:
            print(RED + f"Error processing Ethereum stats: {e}" + END)
            
        # Process Whale Alert transactions - safely
        try:
            for symbol, count in whale_buy_counts.items():
                token_stats[symbol]['buys'] += count
            for symbol, count in whale_sell_counts.items():
                token_stats[symbol]['sells'] += count
        except Exception as e:
            print(RED + f"Error processing Whale Alert stats: {e}" + END)
            
        # Process Solana transactions - safely
        try:
            for symbol, count in solana_buy_counts.items():
                token_stats[symbol]['buys'] += count
            for symbol, count in solana_sell_counts.items():
                token_stats[symbol]['sells'] += count
        except Exception as e:
            print(RED + f"Error processing Solana stats: {e}" + END)
            
        # Process XRP transactions - safely
        try:
            token_stats['XRP']['buys'] += xrp_buy_counts
            token_stats['XRP']['sells'] += xrp_sell_counts
        except Exception as e:
            print(RED + f"Error processing XRP stats: {e}" + END)
        
        # Make a safe copy of transactions - multiple fallback approaches
        safe_transactions = {}
        try:
            # Try to snapshot the dict - most direct, may fail
            safe_transactions = dict(deduped_transactions)
        except Exception:
            try:
                # Slower but safer approach - copy one at a time
                safe_transactions = {}
                for k in list(deduped_transactions.keys()):
                    try:
                        if k in deduped_transactions:  # Check again in case of deletion
                            safe_transactions[k] = deduped_transactions[k]
                    except:
                        pass  # Skip any keys that cause problems
            except Exception as e:
                print(RED + f"Error creating transaction copy: {e}" + END)
        
        # Calculate volumes from transactions - safely
        token_addresses = defaultdict(set)
        for tx_key, tx in safe_transactions.items():
            try:
                symbol = tx.get('symbol', '')
                if symbol:
                    tx_value = tx.get('usd_value', 0) or tx.get('estimated_usd', 0) or 0
                    token_stats[symbol]['volume'] += tx_value
                    
                    # Count transfers if not already counted as buy/sell
                    if tx.get('classification', '').lower() not in ['buy', 'sell']:
                        token_stats[symbol]['transfers'] += 1
                    
                    # Track addresses
                    if 'from' in tx and tx['from']:
                        token_addresses[symbol].add(tx['from'])
                    if 'to' in tx and tx['to']:
                        token_addresses[symbol].add(tx['to'])
            except Exception:
                continue  # Skip problematic transactions
        
        # Now let's print sections one by one, with error handling for each
        
        try:
            # Print Transaction Statistics
            print(BLUE + BOLD + "1. TRANSACTION STATISTICS" + END)
            print(BLUE + "-" * 80 + END)
            
            # Header row
            print(f"{'COIN':<10} {'BUYS':>8} {'SELLS':>8} {'TRANSFERS':>10} {'TOTAL':>8} {'BUY %':>7} {'SELL %':>7} {'TREND':>6}")
            print(BLUE + "-" * 80 + END)
            
            # Sort tokens by total transactions
            sorted_tokens = sorted(
                [(symbol, stats) for symbol, stats in token_stats.items()],
                key=lambda x: x[1]['buys'] + x[1]['sells'] + x[1]['transfers'],
                reverse=True
            )
            
            # Print each token's stats - line by line to avoid errors
            for symbol, stats in sorted_tokens:
                try:
                    buys = stats['buys']
                    sells = stats['sells']
                    transfers = stats['transfers']
                    total = buys + sells + transfers
                    
                    if total < 1:  # Show all tokens with any transactions
                        continue
                        
                    buy_pct = (buys / total * 100) if total > 0 else 0
                    sell_pct = (sells / total * 100) if total > 0 else 0
                    
                    # Determine trend
                    if buy_pct > sell_pct + 10:
                        trend = "↑"
                        trend_color = GREEN
                    elif sell_pct > buy_pct + 10:
                        trend = "↓"
                        trend_color = RED
                    else:
                        trend = "→"
                        trend_color = YELLOW
                        
                    # First print the basic stats
                    basic_info = f"{symbol:<10} {buys:>8} {sells:>8} {transfers:>10} {total:>8} "
                    print(basic_info, end='')
                    
                    # Then print the percentages and trend with colors
                    print(GREEN + f"{buy_pct:>6.1f}%" + END, end=' ')
                    print(RED + f"{sell_pct:>6.1f}%" + END, end=' ')
                    print(trend_color + f"{trend:>6}" + END)
                except Exception as e:
                    # Skip this token if error
                    continue
        except Exception as e:
            print(RED + f"Error displaying transaction statistics: {e}" + END)
        
        try:
            # Market Momentum
            print()
            print(BLUE + BOLD + "2. MARKET MOMENTUM ANALYSIS" + END)
            print(BLUE + "-" * 80 + END)
            
            # Find tokens with enough volume
            active_tokens = [
                (symbol, stats) for symbol, stats in token_stats.items() 
                if stats['buys'] + stats['sells'] >= 2  # Lowered from 10 to show more tokens
            ]
            
            # Sort by buy percentage for bullish tokens
            bullish_tokens = sorted(
                active_tokens,
                key=lambda x: (x[1]['buys'] / max(1, x[1]['buys'] + x[1]['sells'])),
                reverse=True
            )[:3]
            
            # Sort by sell percentage for bearish tokens
            bearish_tokens = sorted(
                active_tokens,
                key=lambda x: (x[1]['sells'] / max(1, x[1]['buys'] + x[1]['sells'])),
                reverse=True
            )[:3]
            
            # Print bullish tokens
            print(GREEN + "TOP BULLISH TOKENS:" + END)
            for symbol, stats in bullish_tokens:
                try:
                    total = stats['buys'] + stats['sells']
                    if total > 0:
                        buy_pct = (stats['buys'] / total * 100)
                        sell_pct = (stats['sells'] / total * 100)
                        print(f"  • {symbol:<8} Buy: {buy_pct:>6.1f}%  Sell: {sell_pct:>6.1f}%  (Volume: {total:,} transactions)")
                except Exception:
                    continue
            
            # Print bearish tokens
            print()
            print(RED + "TOP BEARISH TOKENS:" + END)
            for symbol, stats in bearish_tokens:
                try:
                    total = stats['buys'] + stats['sells']
                    if total > 0:
                        buy_pct = (stats['buys'] / total * 100)
                        sell_pct = (stats['sells'] / total * 100)
                        print(f"  • {symbol:<8} Buy: {buy_pct:>6.1f}%  Sell: {sell_pct:>6.1f}%  (Volume: {total:,} transactions)")
                except Exception:
                    continue
        except Exception as e:
            print(RED + f"Error displaying market momentum: {e}" + END)
        
        try:
            # Deduplication
            print()
            print(BLUE + BOLD + "3. DEDUPLICATION EFFECTIVENESS" + END)
            print(BLUE + "-" * 80 + END)
            print(f"Total Transactions Processed: {dedup_stats['total_received']:,}")
            print(f"Unique Transactions: {dedup_stats['total_transactions']:,}")
            print(f"Duplicates Prevented: {dedup_stats['duplicates_caught']:,}")
            print(f"Overall Deduplication Rate: {dedup_stats['dedup_ratio']:.1f}%")
        except Exception as e:
            print(RED + f"Error displaying deduplication stats: {e}" + END)
        
        try:
            # Volume Analysis Section (Added)
            print()
            print(BLUE + BOLD + "4. TOKEN VOLUME ANALYSIS" + END)
            print(BLUE + "-" * 80 + END)
            print(f"{'TOKEN':<10} {'VOLUME (USD)':>15} {'UNIQUE TXS':>12} {'ADDRESSES':>12}")
            print(BLUE + "-" * 80 + END)
            
            # Sort by volume
            volume_sorted = sorted(
                [(symbol, stats) for symbol, stats in token_stats.items()],
                key=lambda x: x[1]['volume'],
                reverse=True
            )
            
            for symbol, stats in volume_sorted:
                try:
                    total = stats['buys'] + stats['sells'] + stats['transfers']
                    if stats['volume'] > 0:
                        address_count = len(token_addresses.get(symbol, set()))
                        print(f"{symbol:<10} ${stats['volume']:>14,.2f} {total:>12,} {address_count:>12,}")
                except Exception:
                    continue
        except Exception as e:
            print(RED + f"Error displaying volume analysis: {e}" + END)
        
        # Skip news section if there are issues
        try:
            # Add News Section (Added) - simplified to be more reliable
            print()
            print(BLUE + BOLD + "5. LATEST CRYPTO NEWS" + END)
            print(BLUE + "-" * 80 + END)
            
            # Just use placeholder news which is more reliable
            print(f"{YELLOW}Visit CoinDesk or CoinTelegraph for the latest cryptocurrency news{END}")
            
            # Get top 3 tokens by transaction volume (safely)
            top_tokens = []
            try:
                top_tokens = [symbol for symbol, _ in volume_sorted[:3]]
            except:
                # Fallback to hardcoded popular tokens if needed
                top_tokens = ["BTC", "ETH", "XRP"]
                
            for symbol in top_tokens:
                print(f"\n{PURPLE}For {symbol} news, visit: https://www.coingecko.com/en/coins/{symbol.lower()}{END}")
        except Exception as e:
            print(RED + f"Error displaying news: {e}" + END)
        
        print()
        print(GREEN + "Analysis complete." + END)
        print(BLUE + BOLD + "=" * 80 + END)
    
    except Exception as e:
        # Ultimate fallback - if even the main report fails
        print(RED + f"Error generating summary: {e}" + END)
        print(YELLOW + "Simplified emergency report:" + END)
        try:
            print(f"- Ethereum buy/sell: {sum(etherscan_buy_counts.values())}/{sum(etherscan_sell_counts.values())}")
            print(f"- Whale Alert buy/sell: {sum(whale_buy_counts.values())}/{sum(whale_sell_counts.values())}")
            print(f"- Solana buy/sell: {sum(solana_buy_counts.values())}/{sum(solana_sell_counts.values())}")
            print(f"- XRP buy/sell: {xrp_buy_counts}/{xrp_sell_counts}")
        except:
            print("Could not generate even basic stats.")
        print(BLUE + BOLD + "=" * 80 + END)

def cleanup_threads():
    """Attempt to gracefully stop any running threads"""
    for thread in threading.enumerate():
        if thread != threading.current_thread() and thread.daemon:
            try:
                if hasattr(thread, "_stop"):
                    thread._stop()
            except Exception:
                pass

def simple_signal_handler(signum, frame):
    """Handle Ctrl+C with simple formatting and buffer draining"""
    global monitoring_enabled
    
    # First, acknowledge Ctrl+C visually with countdown
    print(YELLOW + BOLD + "\n\n[CTRL+C] Shutting down..." + END)
    
    # Set flags to prevent new data processing
    monitoring_enabled = False
    shutdown_flag.set()
    
    # 🚀 NEW: Stop whale sentiment aggregator
    try:
        if SENTIMENT_AGGREGATION_ENABLED:
            whale_sentiment_aggregator.stop()
            print(YELLOW + "Whale sentiment aggregator stopped" + END)
    except Exception as e:
        print(RED + f"Error stopping sentiment aggregator: {e}" + END)
    
    try:
        # Short countdown to allow buffer draining
        drain_seconds = 3
        for i in range(drain_seconds, 0, -1):
            print(YELLOW + f"Finishing pending transactions... {i}s" + END, end="\r")
            time.sleep(1)
        print(YELLOW + "Processing complete                      " + END)
        
        # Force stop any potential websocket connections
        for thread in list(threading.enumerate()):
            if thread != threading.current_thread():
                try:
                    # Try to join with timeout
                    if thread.is_alive():
                        thread.join(timeout=0.5)
                except Exception:
                    pass
        
        # More aggressively cleanup threads
        cleanup_threads()
            
        # Final countdown before summary
        final_delay = 1
        print(YELLOW + f"Generating final report in {final_delay}s..." + END)
        time.sleep(final_delay)
        
        # Try to generate summary with a fail-safe
        try:
            print(YELLOW + "Generating final report..." + END)
            print_simple_summary()
            
            # Add error summary at the end
            print_error_summary()
            
        except Exception as e:
            print(RED + f"Error generating summary: {e}" + END)
            print(RED + "Press Ctrl+C again to force exit" + END)
            
            # Wait for another Ctrl+C
            try:
                time.sleep(5)
            except KeyboardInterrupt:
                print(RED + "Force exiting..." + END)
                os._exit(1)
        
        print(YELLOW + "Exiting now..." + END)
        
        # Force immediate exit
        os._exit(0)
        
    except KeyboardInterrupt:
        # Handle double Ctrl+C for force exit
        print(RED + "\nForce exiting immediately..." + END)
        os._exit(1)
    except Exception as e:
        print(RED + f"\nError during shutdown: {e}" + END)
        traceback.print_exc()
        # Force exit even on error
        os._exit(1)

def prompt_for_minimum_value():
    """Prompt user for minimum transaction value"""
    global min_transaction_value
    
    # Check if running in non-interactive mode (e.g., background/daemon)
    import sys
    if not sys.stdin.isatty():
        print(YELLOW + f"Running in non-interactive mode, using default threshold: ${GLOBAL_USD_THRESHOLD:,.2f}" + END)
        min_transaction_value = GLOBAL_USD_THRESHOLD
        return
    
    print(BLUE + BOLD + "\nEnter minimum transaction value to monitor (USD): " + END, end='')
    try:
        value_input = input()
        
        if value_input.strip():
            min_value = float(value_input)
            if min_value > 0:
                min_transaction_value = min_value
            else:
                print(YELLOW + "Value must be greater than 0, using default" + END)
        else:
            print(YELLOW + f"Using default value: ${GLOBAL_USD_THRESHOLD:,.2f}" + END)
    except (ValueError, EOFError):
        print(YELLOW + f"Invalid input or EOF, using default value: ${GLOBAL_USD_THRESHOLD:,.2f}" + END)
        min_transaction_value = GLOBAL_USD_THRESHOLD

def display_transaction(tx_data, enhanced_display=True):
    """
    🐋 PRODUCTION-READY Display transaction with WHALE INTELLIGENCE ANALYSIS
    """
    try:
        # Extract transaction details
        chain = tx_data.get('chain', 'unknown')
        tx_hash = tx_data.get('hash', 'N/A')
        from_addr = tx_data.get('from_address', 'N/A')
        to_addr = tx_data.get('to_address', 'N/A')
        
        # 🔧 CRITICAL FIX: Look for USD value in multiple fields
        value_usd = float(tx_data.get('estimated_usd', 0) or tx_data.get('value_usd', 0) or tx_data.get('usd_value', 0) or 0)
        
        token_symbol = tx_data.get('token_symbol', 'ETH')
        
        # Enforce global minimum before logging or heavy analysis
        if value_usd < min_transaction_value:
            return

        # Initialize transaction-specific logger
        tx_logger = get_transaction_logger(tx_hash)
        tx_logger.info(
            "Transaction display started",
            extra={'extra_fields': {
                'chain': chain,
                'value_usd': value_usd,
                'token_symbol': token_symbol
            }}
        )
        
        # Run PRODUCTION whale intelligence analysis
        whale_result = whale_engine.analyze_transaction_comprehensive(tx_data)
        
        # 🚀 NEW: Store whale transaction classification for sentiment analysis
        if REAL_TIME_ENABLED and transaction_storage.storage_enabled:
            transaction_storage.store_whale_transaction(tx_data, whale_result)
        
        # Extract advanced analysis results
        swap_analysis = {}
        advanced_analysis = {}
        
        # Determine display elements
        whale_score = getattr(whale_result, 'final_whale_score', 0)
        confidence = getattr(whale_result, 'confidence', 0)
        classification = getattr(whale_result, 'classification', ClassificationType.TRANSFER)
        
        # Handle ClassificationType enum - get string value
        if hasattr(classification, 'value'):
            classification_str = classification.value
        else:
            classification_str = str(classification)
        
        # Log analysis results
        tx_logger.info(
            "Whale intelligence analysis completed",
            extra={'extra_fields': {
                'classification': classification_str,
                'confidence': confidence,
                'whale_score': whale_score
            }}
        )
        
        # 🐋 WHALE INDICATOR
        whale_indicator = ""
        if value_usd >= 1000000:  # $1M+
            whale_indicator = "🐋🐋🐋 MEGA WHALE"
        elif value_usd >= 100000:  # $100K+
            whale_indicator = "🐋🐋 WHALE"
        elif value_usd >= 10000:   # $10K+
            whale_indicator = "🐋 MINI WHALE"
        elif whale_score > 60:
            whale_indicator = "🐋 WHALE SIGNALS"
        
        # 🎯 CONFIDENCE COLOR CODING
        if confidence >= 0.8:
            confidence_color = Fore.GREEN
            confidence_label = "HIGH"
        elif confidence >= 0.5:
            confidence_color = Fore.YELLOW
            confidence_label = "MEDIUM"
        else:
            confidence_color = Fore.RED
            confidence_label = "LOW"
        
        # 📊 ADVANCED TRANSACTION CATEGORIZATION
        category_info = ""
        protocol_info = ""
        
        if advanced_analysis:
            transaction_category = advanced_analysis.get('transaction_category', 'UNKNOWN')
            category_confidence = advanced_analysis.get('confidence_score', 0)
            
            # Enhanced category display
            if transaction_category != 'UNKNOWN':
                category_info = f"📊 {transaction_category}"
                if category_confidence >= 0.90:
                    category_info += f" {Fore.GREEN}({category_confidence:.1%}){Style.RESET_ALL}"
                elif category_confidence >= 0.70:
                    category_info += f" {Fore.YELLOW}({category_confidence:.1%}){Style.RESET_ALL}"
                else:
                    category_info += f" {Fore.RED}({category_confidence:.1%}){Style.RESET_ALL}"
            
            # Protocol interaction display
            protocol_interactions = advanced_analysis.get('protocol_interactions', [])
            if protocol_interactions:
                protocols = [p.get('protocol', 'Unknown') for p in protocol_interactions if p.get('protocol')]
                if protocols:
                    protocol_info = f"🏛️ Protocols: {', '.join(set(protocols))}"
            
            # DEX protocol display
            dex_protocol = advanced_analysis.get('dex_protocol')
            if dex_protocol and dex_protocol != 'Unknown DEX':
                protocol_info = f"🔄 DEX: {dex_protocol}"
            
            # Multi-protocol indicator
            if advanced_analysis.get('multi_protocol'):
                protocol_info += " 🌐 MULTI-PROTOCOL"
            
            # MEV signals
            mev_signals = advanced_analysis.get('mev_signals', {})
            mev_indicators = []
            if mev_signals.get('arbitrage'):
                mev_indicators.append("⚡ ARBITRAGE")
            if mev_signals.get('front_running'):
                mev_indicators.append("🏃 FRONT-RUN")
            if mev_signals.get('sandwich_attack'):
                mev_indicators.append("🥪 SANDWICH")
            
            if mev_indicators:
                protocol_info += f" | MEV: {', '.join(mev_indicators)}"
        
        # 🪐 SOLANA-SPECIFIC ENHANCEMENTS
        if chain == 'solana' and advanced_analysis:
            jupiter_info = advanced_analysis.get('jupiter_route_info', {})
            if jupiter_info.get('route_type') == 'MULTI_HOP':
                hop_count = len(jupiter_info.get('hop_details', []))
                protocol_info += f" | 🪐 Jupiter {hop_count}-hop"
            
            # DeFi operations
            defi_ops = advanced_analysis.get('defi_operations', [])
            if len(defi_ops) > 1:
                protocol_info += f" | 🏛️ {len(defi_ops)} DeFi protocols"
        
        # 💎 FLASH LOAN DETECTION
        flash_loan_info = ""
        if advanced_analysis.get('mev_signals', {}).get('has_flash_loans'):
            flash_loan_info = "⚡ FLASH LOAN DETECTED"
        
        # Main transaction display
        print(f"\n{Fore.CYAN}{'='*80}{Style.RESET_ALL}")
        
        # Header with whale indicator
        header = f"🔗 {chain.upper()} Transaction"
        if whale_indicator:
            header += f" | {Fore.RED}{whale_indicator}{Style.RESET_ALL}"
        print(header)
        
        # Transaction hash
        print(f"📄 Hash: {tx_hash[:16]}...{tx_hash[-16:] if len(tx_hash) > 32 else tx_hash}")
        
        # Addresses
        print(f"📤 From: {from_addr[:8]}...{from_addr[-8:] if len(from_addr) > 16 else from_addr}")
        print(f"📥 To:   {to_addr[:8]}...{to_addr[-8:] if len(to_addr) > 16 else to_addr}")
        
        # Value with formatting
        if value_usd >= 1000000:
            value_display = f"💰 Value: ${value_usd:,.0f} ({token_symbol}) {Fore.RED}🔥 MEGA{Style.RESET_ALL}"
        elif value_usd >= 100000:
            value_display = f"💰 Value: ${value_usd:,.0f} ({token_symbol}) {Fore.YELLOW}🔥 LARGE{Style.RESET_ALL}"
        elif value_usd >= 10000:
            value_display = f"💰 Value: ${value_usd:,.0f} ({token_symbol}) {Fore.GREEN}💎{Style.RESET_ALL}"
        else:
            value_display = f"💰 Value: ${value_usd:,.2f} ({token_symbol})"
        
        print(value_display)
        
        # ENHANCED: Advanced analysis results
        if category_info:
            print(f"{category_info}")
        
        if protocol_info:
            print(f"{protocol_info}")
        
        if flash_loan_info:
            print(f"{Fore.MAGENTA}{flash_loan_info}{Style.RESET_ALL}")
        
        # Classification and confidence
        print(f"🎯 Classification: {Fore.CYAN}{classification}{Style.RESET_ALL}")
        print(f"📊 Confidence: {confidence_color}{confidence_label} ({confidence:.1%}){Style.RESET_ALL}")
        
        # Whale score
        if whale_score > 0:
            if whale_score >= 80:
                score_color = Fore.RED
            elif whale_score >= 60:
                score_color = Fore.YELLOW
            else:
                score_color = Fore.GREEN
            print(f"🐋 Whale Score: {score_color}{whale_score:.0f}/100{Style.RESET_ALL}")
        
        # ENHANCED: Whale signals from advanced analysis (robust to object/dict forms)
        whale_signals = []
        if hasattr(whale_result, 'whale_signals'):
            whale_signals = getattr(whale_result, 'whale_signals') or []
        elif isinstance(whale_result, dict):
            whale_signals = whale_result.get('whale_signals', []) or []
            if isinstance(whale_signals, dict):
                whale_signals = whale_signals.get('signals') or list(whale_signals.values())
        if whale_signals:
            print(f"\n🔍 Whale Intelligence Signals:")
            for signal in whale_signals:
                print(f"   • {signal}")
        
        # ENHANCED: Advanced technical details (if high confidence)
        if enhanced_display and advanced_analysis and advanced_analysis.get('confidence_score', 0) > 0.8:
            print(f"\n🔬 Advanced Log Analysis:")
            
            # Liquidity impact
            liquidity_impact = advanced_analysis.get('liquidity_impact', {})
            if liquidity_impact.get('has_liquidity_operations'):
                operations = liquidity_impact.get('operations', [])
                impact_level = liquidity_impact.get('impact_level', 'UNKNOWN')
                print(f"   💧 Liquidity Operations: {len(operations)} ops ({impact_level} impact)")
            
            # Token flow analysis (for Solana)
            if chain == 'solana':
                token_flow = advanced_analysis.get('token_flow_analysis', {})
                if token_flow.get('transfer_count', 0) > 0:
                    transfer_count = token_flow['transfer_count']
                    token_diversity = token_flow.get('token_diversity', 0)
                    print(f"   🔄 Token Flow: {transfer_count} transfers, {token_diversity} unique tokens")
            
            # Decoded events (for EVM)
            if chain in ['ethereum', 'polygon']:
                decoded_events = advanced_analysis.get('decoded_events', {})
                if decoded_events.get('decoded_events'):
                    event_count = len(decoded_events['decoded_events'])
                    print(f"   🔍 Decoded Events: {event_count} contract events")
        
        print(f"{Fore.CYAN}{'='*80}{Style.RESET_ALL}")
        
        # Log display completion
        tx_logger.info("Transaction display completed successfully", 
                      whale_indicator=whale_indicator, confidence_label=confidence_label)
        
        # Store for potential analysis
        if hasattr(display_transaction, 'last_analysis'):
            display_transaction.last_analysis = whale_result
        
    except Exception as e:
        error_msg = f"Error displaying transaction {tx_data.get('hash', 'unknown')}: {e}"
        production_logger.error(
            "Transaction display failed",
            extra={'extra_fields': {
                'transaction_hash': tx_data.get('hash', 'unknown'),
                'error': str(e),
                'stack_trace': traceback.format_exc()
            }}
        )
        print(f"❌ {error_msg}")

def main():
    """Main entry point with simplified error handling"""
    global active_threads
    
    # Register custom signal handler for clean shutdown
    signal.signal(signal.SIGINT, simple_signal_handler)
    signal.signal(signal.SIGTERM, simple_signal_handler)
    
    try:
        # Ask for minimum value
        prompt_for_minimum_value()
        
        # Initialize prices
        print(BLUE + "Initializing token prices..." + END)
        initialize_prices()
        
        # Test connections
        if not test_etherscan_connection():
            print(RED + "Failed to connect to Etherscan API. Continuing without Etherscan." + END)
        
        # Start monitoring threads
        active_threads = start_monitoring_threads()
        
        # Start monitoring in a separate thread
        monitor_thread = threading.Thread(
            target=monitor_transactions,
            daemon=True,
            name="Monitor"
        )
        monitor_thread.start()
        active_threads.append(monitor_thread)
        
        # Print initial header
        print_simple_header()
        
        # Set up a try-finally to ensure proper cleanup even on unexpected exit
        try:
            # Main loop - just keep the main thread alive and check shutdown_flag more frequently
            while not shutdown_flag.is_set():
                time.sleep(0.1)  # Check more frequently
        except KeyboardInterrupt:
            # Direct call to signal handler
            simple_signal_handler(signal.SIGINT, None)
        finally:
            # Make sure shutdown flag is set if we're exiting for any reason
            shutdown_flag.set()
            monitoring_enabled = False
            
    except KeyboardInterrupt:
        # Handle Ctrl+C with our custom handler
        simple_signal_handler(signal.SIGINT, None)
    except Exception as e:
        print(RED + f"Fatal error in main loop: {e}" + END)
        traceback.print_exc()
        # Force exit on unhandled exception
        os._exit(1)

if __name__ == "__main__":
    main()

class EnhancedMonitor:
    """
    Enhanced transaction monitoring system that integrates address enrichment
    and rule-based classification.
    
    This class:
    1. Monitors transactions from various sources
    2. Enriches addresses with metadata from external services
    3. Classifies transactions as buys, sells, or transfers
    4. Provides analytics and alerts
    """
    
    def __init__(
        self,
        redis_url: Optional[str] = None,
        output_dir: str = "output",
        sources: Optional[List[str]] = None
    ):
        """
        Initialize the enhanced monitor
        
        Args:
            redis_url: Redis connection URL
            output_dir: Directory for output files
            sources: Transaction sources to monitor
        """
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        self.output_dir = output_dir
        
        # Initialize the transaction classifier
        self.classifier = TransactionClassifier(redis_url=redis_url)
        
        # Setup transaction sources
        self.sources = sources or ["ethereum", "solana", "polygon", "xrp"]
        
        # Tracking stats
        self.stats = {
            "transactions_processed": 0,
            "buys": 0,
            "sells": 0,
            "transfers": 0,
            "unknown": 0,
            "start_time": datetime.now().isoformat()
        }
    
    async def process_transaction(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single transaction
        
        Args:
            transaction: Transaction data
            
        Returns:
            Dict[str, Any]: Processed transaction with classification
        """
        # Extract transaction fields
        tx_hash = transaction.get("hash") or transaction.get("tx_hash")
        from_address = transaction.get("from_address") or transaction.get("from")
        to_address = transaction.get("to_address") or transaction.get("to")
        chain = transaction.get("chain") or transaction.get("blockchain", "ethereum")
        token = transaction.get("token") or transaction.get("symbol", "")
        amount = float(transaction.get("amount") or transaction.get("value", 0))
        usd_value = float(transaction.get("usd_value") or transaction.get("value_usd", 0))
        timestamp_str = transaction.get("timestamp")
        
        # Process timestamp
        timestamp = None
        if timestamp_str:
            if isinstance(timestamp_str, (int, float)):
                # Unix timestamp
                timestamp = datetime.fromtimestamp(timestamp_str)
            else:
                # ISO string
                try:
                    timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    pass
        
        if not timestamp:
            timestamp = datetime.now()
        
        # Classify the transaction
        result = await self.classifier.classify_transaction(
            tx_hash=tx_hash,
            from_address=from_address,
            to_address=to_address,
            chain=chain,
            token=token,
            amount=amount,
            usd_value=usd_value,
            timestamp=timestamp
        )
        
        # Update stats
        self.stats["transactions_processed"] += 1
        if result.classification == ClassificationType.BUY:
            self.stats["buys"] += 1
        elif result.classification == ClassificationType.SELL:
            self.stats["sells"] += 1
        elif result.classification == ClassificationType.TRANSFER:
            self.stats["transfers"] += 1
        else:
            self.stats["unknown"] += 1
        
        # Generate human-readable summary
        summary = self.classifier.generate_classification_summary(result)
        
        # Prepare the enhanced transaction record
        enhanced_tx = {
            # Original transaction data
            "tx_hash": tx_hash,
            "from_address": from_address,
            "to_address": to_address,
            "chain": chain,
            "token": token,
            "amount": amount,
            "usd_value": usd_value,
            "timestamp": timestamp.isoformat(),
            
            # Classification data
            "classification": result.classification.value,
            "confidence": result.confidence,
            "confidence_level": result.confidence_level.value,
            "rule": result.triggered_rule,
            "explanation": result.explanation,
            
            # Address entity information
            "from_entity": summary["from_entity"],
            "to_entity": summary["to_entity"],
            
            # Summary
            "summary": summary["summary"]
        }
        
        # Log result
        logger.info(f"Transaction {tx_hash}: {enhanced_tx['classification']} "
                   f"(confidence: {enhanced_tx['confidence']:.2f})")
        
        return enhanced_tx
    
    async def process_batch(self, transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process a batch of transactions
        
        Args:
            transactions: List of transaction data
            
        Returns:
            List[Dict[str, Any]]: List of processed transactions
        """
        tasks = []
        for tx in transactions:
            task = self.process_transaction(tx)
            tasks.append(task)
        
        return await asyncio.gather(*tasks)
    
    def save_transactions(self, transactions: List[Dict[str, Any]], file_path: str) -> None:
        """
        Save processed transactions to file
        
        Args:
            transactions: List of processed transactions
            file_path: Output file path
        """
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, "w") as f:
            json.dump(transactions, f, indent=2, default=str)
            
        logger.info(f"Saved {len(transactions)} transactions to {file_path}")
    
    def save_stats(self, file_path: str) -> None:
        """
        Save monitoring stats to file
        
        Args:
            file_path: Output file path
        """
        # Add end time
        self.stats["end_time"] = datetime.now().isoformat()
        self.stats["duration_seconds"] = (
            datetime.fromisoformat(self.stats["end_time"]) - 
            datetime.fromisoformat(self.stats["start_time"])
        ).total_seconds()
        
        # Calculate percentages
        total = self.stats["transactions_processed"]
        if total > 0:
            self.stats["buy_percentage"] = round(self.stats["buys"] / total * 100, 2)
            self.stats["sell_percentage"] = round(self.stats["sells"] / total * 100, 2)
            self.stats["transfer_percentage"] = round(self.stats["transfers"] / total * 100, 2)
            self.stats["unknown_percentage"] = round(self.stats["unknown"] / total * 100, 2)
        
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, "w") as f:
            json.dump(self.stats, f, indent=2)
            
        logger.info(f"Saved monitoring stats to {file_path}")
    
    async def simulate_monitoring(self, num_transactions: int = 10) -> None:
        """
        Simulate transaction monitoring with dummy data
        
        Args:
            num_transactions: Number of dummy transactions to generate
        """
        logger.info(f"Starting simulated monitoring with {num_transactions} dummy transactions")
        
        # Generate dummy transactions
        dummy_transactions = []
        
        # Example addresses (including some known exchanges)
        from_addresses = [
            "0x28c6c06298d514db089934071355e5743bf21d60",  # Binance
            "0xdfd5293d8e347dfe59e90efd55b2956a1343963d",  # Coinbase
            "0x3f5CE5FBFe3E9af3971dD833D26bA9b5C936f0bE",  # Binance
            "0x8315177aB297bA92A06054cE80a67Ed4DBd7ed3a",  # Random
            "0x47ac0Fb4F2D84898e4D9E7b4DaB3C24507a6D503"   # Random
        ]
        
        to_addresses = [
            "0x8315177aB297bA92A06054cE80a67Ed4DBd7ed3a",  # Random
            "0x47ac0Fb4F2D84898e4D9E7b4DaB3C24507a6D503",  # Random
            "0x28c6c06298d514db089934071355e5743bf21d60",  # Binance
            "0xdfd5293d8e347dfe59e90efd55b2956a1343963d",  # Coinbase
            "0x11E4A2A167C614F900BC7784bdF9F373BB189c3f"   # Uniswap Router
        ]
        
        # Generate transactions
        import random
        for i in range(num_transactions):
            from_idx = random.randint(0, len(from_addresses) - 1)
            to_idx = random.randint(0, len(to_addresses) - 1)
            
            # Ensure from and to addresses are different
            while from_idx == to_idx:
                to_idx = random.randint(0, len(to_addresses) - 1)
            
            tx = {
                "tx_hash": f"0x{i:064x}",
                "from_address": from_addresses[from_idx],
                "to_address": to_addresses[to_idx],
                "chain": random.choice(["ethereum", "solana", "polygon", "xrp"]),
                "token": random.choice(["ETH", "BTC", "USDC", "SOL", "XRP"]),
                "amount": round(random.uniform(0.1, 10.0), 4),
                "usd_value": round(random.uniform(100, 50000), 2),
                "timestamp": datetime.now().isoformat()
            }
            
            dummy_transactions.append(tx)
        
        # Process transactions
        processed_transactions = await self.process_batch(dummy_transactions)
        
        # Save results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        transactions_file = os.path.join(self.output_dir, f"transactions_{timestamp}.json")
        stats_file = os.path.join(self.output_dir, f"stats_{timestamp}.json")
        
        self.save_transactions(processed_transactions, transactions_file)
        self.save_stats(stats_file)
        
        # Print summary
        print("\nMonitoring Summary:")
        print(f"Processed {self.stats['transactions_processed']} transactions")
        print(f"Buys: {self.stats['buys']} ({self.stats.get('buy_percentage', 0)}%)")
        print(f"Sells: {self.stats['sells']} ({self.stats.get('sell_percentage', 0)}%)")
        print(f"Transfers: {self.stats['transfers']} ({self.stats.get('transfer_percentage', 0)}%)")
        print(f"Unknown: {self.stats['unknown']} ({self.stats.get('unknown_percentage', 0)}%)")
        
        logger.info("Simulated monitoring completed")
    
    async def close(self):
        """Close resources"""
        await self.classifier.close()

async def main():
    """Main entry point"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Enhanced Whale Transaction Monitor")
    parser.add_argument("--redis-url", type=str, help="Redis URL for caching")
    parser.add_argument("--output-dir", type=str, default="output", help="Output directory")
    parser.add_argument("--num-transactions", type=int, default=10, 
                       help="Number of dummy transactions for simulation")
    
    args = parser.parse_args()
    
    # Create monitor
    monitor = EnhancedMonitor(
        redis_url=args.redis_url,
        output_dir=args.output_dir
    )
    
    try:
        # Run simulation
        await monitor.simulate_monitoring(args.num_transactions)
    finally:
        # Clean up
        await monitor.close()

if __name__ == "__main__":
    asyncio.run(main())
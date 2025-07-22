#!/usr/bin/env python3
"""
🧠 WHALE INTELLIGENCE ENGINE - Production-Grade Multi-Phase Transaction Classifier
A Google-level engineered refactoring of the whale transaction analysis system.

This module implements a sophisticated, cost-optimized classification pipeline that
analyzes blockchain transactions through multiple intelligence phases to determine
transaction types (BUY/SELL/TRANSFER) and whale activity patterns.

Architecture:
- Phase 1: CEX Classification (Cost-free, highest priority)
- Phase 2: DEX/DeFi Protocol Analysis (Cost-free, high priority)
- Phase 2.5: Market Data Intelligence (Low-cost, market context)
- Phase 3: Blockchain-Specific Analysis (API calls, medium priority)
- Phase 4: Wallet Behavior Analysis (API calls, medium priority)
- Phase 5: BigQuery Mega Whale Detection (Expensive, last resort)
- Phase 6+: Enhanced API Analysis (Conditional execution)

Author: Whale Transaction Monitor Team
Version: 2.1.0 (Market Data Integration)
"""

import logging
import traceback
import time
import random
from typing import Dict, Any, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from abc import ABC, abstractmethod
import json
import re
from collections import defaultdict, Counter

# Third-party imports
import pandas as pd
import numpy as np
from pydantic import BaseModel, Field, validator
from pydantic.dataclasses import dataclass as pydantic_dataclass

# Project imports
from config.logging_config import get_transaction_logger, production_logger
from config.settings import (
    STABLECOIN_SYMBOLS, 
    CONFIDENCE_WEIGHTS, 
    EVENT_SIGNATURES,
    WHALE_THRESHOLDS,
    CLASSIFICATION_THRESHOLDS,
    DEFI_PROTOCOL_SETTINGS,
    PROTOCOL_CONTRACT_VERIFICATION
)
from utils.bigquery_analyzer import BigQueryAnalyzer
from utils.evm_parser import EVMLogParser
from utils.solana_parser import SolanaParser
from utils.api_integrations import (
    get_moralis_token_metadata,
    get_zerion_portfolio_analysis,
    enhanced_cex_address_matching
)
from data.addresses import (
    known_exchange_addresses,
    DEX_ADDRESSES,
    MARKET_MAKER_ADDRESSES,
    PROTOCOL_ADDRESSES,
    solana_exchange_addresses,
    xrp_exchange_addresses,
    SOLANA_DEX_ADDRESSES,
    ADDRESS_TYPE_CONFIDENCE_MODIFIERS
)
from data.tokens import common_stablecoins
from data.market_makers import MARKET_MAKER_ADDRESSES
from models.classes import DefiLlamaData
from utils.dedup import deduplicate_transactions
from utils.helpers import get_protocol_slug, get_dex_name, is_significant_tvl_movement
from utils.summary import has_been_classified, mark_as_classified

# Market Data Integration
try:
    from opportunity_engine.market_data_provider import MarketDataProvider
    MARKET_DATA_AVAILABLE = True
except ImportError:
    MarketDataProvider = None
    MARKET_DATA_AVAILABLE = False

# Initialize logger
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION AND ENUMS
# =============================================================================

class ClassificationType(Enum):
    """Transaction classification types."""
    # Verified On-Chain Swaps (Highest Accuracy)
    VERIFIED_SWAP_BUY = "VERIFIED_SWAP_BUY"
    VERIFIED_SWAP_SELL = "VERIFIED_SWAP_SELL"
    
    # Non-Trade DeFi Operations
    LIQUIDITY_ADD = "LIQUIDITY_ADD"
    LIQUIDITY_REMOVE = "LIQUIDITY_REMOVE"
    WRAP = "WRAP"
    UNWRAP = "UNWRAP"
    
    # Keep existing types for pipeline compatibility
    BUY = "BUY"
    SELL = "SELL"
    TRANSFER = "TRANSFER"
    STAKING = "STAKING"
    DEFI = "DEFI"
    CONFLICT = "CONFLICT"  # For conflicting BUY/SELL signals
    
    # Moderate confidence classifications
    MODERATE_BUY = "MODERATE_BUY"
    MODERATE_SELL = "MODERATE_SELL"
    
    # Generic & Error States
    TOKEN_TRANSFER = "TOKEN_TRANSFER"
    FAILED_TRANSACTION = "FAILED_TRANSACTION"
    UNKNOWN = "UNKNOWN"


class AnalysisPhase(Enum):
    """Analysis phase identifiers."""
    CEX_CLASSIFICATION = "cex_classification"
    DEX_PROTOCOL = "dex_protocol_classification" 
    STABLECOIN_FLOW = "stablecoin_flow"  # NEW PHASE: Stablecoin flow analysis
    MARKET_DATA_INTELLIGENCE = "market_data_intelligence"
    BLOCKCHAIN_SPECIFIC = "blockchain_specific"
    WALLET_BEHAVIOR = "wallet_behavior"
    BIGQUERY_WHALE = "bigquery_mega_whale"
    MORALIS_ENRICHMENT = "moralis_enrichment"
    ZERION_PORTFOLIO = "zerion_portfolio"
    SUPABASE_DEFI = "supabase_defi"


class WhaleSignalType(Enum):
    """Types of whale signals detected."""
    MEGA_WHALE = "MEGA_WHALE"
    HIGH_VOLUME = "HIGH_VOLUME"
    FREQUENT_TRADER = "FREQUENT_TRADER"
    MARKET_MAKER = "MARKET_MAKER"
    EARLY_INVESTOR = "EARLY_INVESTOR"


# =============================================================================
# DATA MODELS
# =============================================================================

@dataclass
class PhaseResult:
    """Structured result from an analysis phase."""
    classification: ClassificationType = ClassificationType.TRANSFER
    confidence: float = 0.0
    evidence: List[str] = field(default_factory=list)
    whale_signals: List[str] = field(default_factory=list)
    phase: str = ""
    raw_data: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate confidence range."""
        self.confidence = max(0.0, min(1.0, self.confidence))


@dataclass
class BehavioralAnalysis:
    """Behavioral heuristics analysis result."""
    gas_price_analysis: Dict[str, Any] = field(default_factory=dict)
    address_behavior_analysis: Dict[str, Any] = field(default_factory=dict)
    timing_analysis: Dict[str, Any] = field(default_factory=dict)
    confidence_adjustments: List[Dict[str, Any]] = field(default_factory=list)
    total_confidence_boost: float = 0.0


@dataclass
class IntelligenceResult:
    """Final intelligence analysis result."""
    classification: ClassificationType = ClassificationType.TRANSFER
    confidence: float = 0.0
    final_whale_score: float = 0.0
    evidence: List[str] = field(default_factory=list)
    whale_signals: List[str] = field(default_factory=list)
    phase_results: Dict[str, PhaseResult] = field(default_factory=dict)
    master_classifier_reasoning: str = ""
    behavioral_analysis: Optional[BehavioralAnalysis] = None
    phases_completed: int = 0
    cost_optimized: bool = True
    opportunity_signal: Optional[Dict[str, Any]] = None


class OpportunitySignal(BaseModel):
    """Trading opportunity signal model."""
    signal_type: str = Field(..., description="Type of opportunity signal")
    token_address: str = Field(..., description="Token contract address")
    confidence_score: float = Field(..., ge=0.0, le=1.0, description="Signal confidence")
    reasoning: List[str] = Field(default_factory=list, description="Analysis reasoning")
    market_data: Dict[str, Any] = Field(default_factory=dict, description="Market metrics")
    
    @validator('confidence_score')
    def validate_confidence(cls, v):
        return max(0.0, min(1.0, v))


# =============================================================================
# CUSTOM EXCEPTIONS
# =============================================================================

class WhaleEngineError(Exception):
    """Base exception for whale engine errors."""
    pass


class APIIntegrationError(WhaleEngineError):
    """Error in API integration."""
    pass


class ConfigurationError(WhaleEngineError):
    """Configuration related error."""
    pass


class DataValidationError(WhaleEngineError):
    """Data validation error."""
    pass


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def is_valid_ethereum_address(address: str) -> bool:
    """
    Validate if an address string is a proper Ethereum address.
    
    Args:
        address: The address string to validate
        
    Returns:
        bool: True if valid Ethereum address, False otherwise
    """
    if not address or not isinstance(address, str):
        return False
    
    # Check if starts with 0x and has exactly 42 characters
    if not address.startswith("0x") or len(address) != 42:
        return False
    
    # Check if all characters after 0x are valid hexadecimal
    try:
        int(address[2:], 16)
        return True
    except ValueError:
        return False


def normalize_address(address: str) -> str:
    """Normalize address to lowercase for consistent comparison."""
    if not address:
        return ""
    return address.lower().strip()


def create_empty_phase_result(reason: str, phase_name: str = "unknown") -> PhaseResult:
    """Create standardized empty phase result."""
    return PhaseResult(
        classification=ClassificationType.TRANSFER,
        confidence=0.0,
        evidence=[reason],
        whale_signals=[],
        phase=phase_name,
        raw_data={"failure_reason": reason}
    )


# =============================================================================
# ANALYSIS ENGINES
# =============================================================================

class BaseAnalysisEngine(ABC):
    """Abstract base class for analysis engines."""
    
    @abstractmethod
    def analyze(self, *args, **kwargs) -> PhaseResult:
        """Perform analysis and return structured result."""
        pass
    
    @abstractmethod
    def get_engine_name(self) -> str:
        """Get the name of this analysis engine."""
        pass


class CEXClassificationEngine(BaseAnalysisEngine):
    """CEX (Centralized Exchange) classification analysis engine."""
    
    def __init__(self, supabase_client=None):
        self.supabase_client = supabase_client
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def get_engine_name(self) -> str:
        return "CEX Classification Engine"
    
    def analyze(self, from_addr: str, to_addr: str, blockchain: str) -> PhaseResult:
        """
        Analyze transaction for CEX (Centralized Exchange) patterns.
        
        Classification Logic:
        - to_address = CEX → SELL (user selling to exchange)
        - from_address = CEX → BUY (user buying from exchange)
        - High confidence (85-95%) for immediate classification
        
        Args:
            from_addr: Transaction sender address
            to_addr: Transaction receiver address
            blockchain: Blockchain network
            
        Returns:
            PhaseResult with classification results
        """
        try:
            self.logger.debug(f"Analyzing CEX patterns for {from_addr} -> {to_addr}")
            
            # Normalize addresses
            from_addr_norm = normalize_address(from_addr)
            to_addr_norm = normalize_address(to_addr)
            
            evidence = []
            whale_signals = []
            classification = ClassificationType.TRANSFER
            confidence = 0.0
            
            # Check hardcoded CEX addresses first (fastest lookup)
            cex_result = self._check_hardcoded_cex_addresses(from_addr_norm, to_addr_norm)
            if cex_result:
                classification, confidence, evidence = cex_result
                self.logger.info(f"Hardcoded CEX match: {classification.value} at {confidence:.2f}")
                
                return PhaseResult(
                    classification=classification,
                    confidence=confidence,
                    evidence=evidence,
                    whale_signals=whale_signals,
                    phase=AnalysisPhase.CEX_CLASSIFICATION.value,
                    raw_data={"source": "hardcoded_cex"}
                )
            
            # Check Supabase comprehensive database
            if self.supabase_client:
                supabase_result = self._check_supabase_cex_addresses(
                    from_addr_norm, to_addr_norm, blockchain
                )
                if supabase_result:
                    classification, confidence, evidence = supabase_result
                    self.logger.info(f"Supabase CEX match: {classification.value} at {confidence:.2f}")
                    
                    return PhaseResult(
                        classification=classification,
                        confidence=confidence,
                        evidence=evidence,
                        whale_signals=whale_signals,
                        phase=AnalysisPhase.CEX_CLASSIFICATION.value,
                        raw_data={"source": "supabase_cex"}
                    )
            
            # No CEX matches found
            self.logger.debug("No CEX addresses detected")
            return create_empty_phase_result(
                "No CEX addresses detected", 
                AnalysisPhase.CEX_CLASSIFICATION.value
            )
            
        except Exception as e:
            self.logger.error(f"CEX classification failed: {e}")
            return create_empty_phase_result(
                f"CEX analysis error: {str(e)}", 
                AnalysisPhase.CEX_CLASSIFICATION.value
            )
    
    def _check_hardcoded_cex_addresses(self, from_addr: str, to_addr: str) -> Optional[Tuple[ClassificationType, float, List[str]]]:
        """Check hardcoded CEX address lists with DEX exclusion."""
        try:
            # CRITICAL FIX: First check if these are DEX addresses
            dex_protocols = {
                "0x7a250d5630b4cf539739df2c5dacb4c659f2488d": "Uniswap V2 Router",
                "0xe592427a0aece92de3edee1f18e0157c05861564": "Uniswap V3 Router", 
                "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45": "Uniswap V3 Router 2",
                "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f": "SushiSwap Router",
                "0x1111111254fb6c44bac0bed2854e76f90643097d": "1inch V4 Router",
                "0xdef171fe48cf0115b1d80b88dc8eab59176fee57": "ParaSwap V5",
                "0xdef1c0ded9bec7f1a1670819833240f027b25eff": "0x Protocol"
            }
            
            # If either address is a DEX, return None (no CEX classification)
            if from_addr.lower() in dex_protocols or to_addr.lower() in dex_protocols:
                self.logger.debug(f"DEX address detected: {from_addr} -> {to_addr}, skipping CEX classification")
                return None
            
            # Check if from_addr is a known exchange (actual CEX only)
            if from_addr in known_exchange_addresses:
                exchange_name = known_exchange_addresses[from_addr]
                classification = ClassificationType.BUY
                confidence = 0.80
                evidence = [f"Hardcoded CEX: Buying from {exchange_name}"]
                return classification, confidence, evidence
            
            # Check if to_addr is a known exchange (actual CEX only)
            if to_addr in known_exchange_addresses:
                exchange_name = known_exchange_addresses[to_addr]
                classification = ClassificationType.SELL
                confidence = 0.80
                evidence = [f"Hardcoded CEX: Selling to {exchange_name}"]
                return classification, confidence, evidence
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Hardcoded CEX check failed: {e}")
            return None
    
    def _check_supabase_cex_addresses(self, from_addr: str, to_addr: str, blockchain: str) -> Optional[Tuple[ClassificationType, float, List[str]]]:
        """
        ENHANCED: Check Supabase database for CEX addresses with comprehensive data utilization.
        
        NOW LEVERAGES ALL AVAILABLE SUPABASE COLUMNS:
        - analysis_tags JSONB (DeFiLlama data, categories, metadata)
        - entity_name, signal_potential, balance_usd
        - detection_method, last_seen_tx
        """
        if not self.supabase_client:
            return None
        
        try:
            # Query both addresses with COMPLETE data extraction
            addresses_to_check = [addr for addr in [from_addr, to_addr] if addr]
            if not addresses_to_check:
                return None

            # COMPREHENSIVE QUERY: All available columns for maximum intelligence
            response = self.supabase_client.table('addresses')\
                .select("""
                    address, label, address_type, confidence, 
                    entity_name, signal_potential, balance_usd, balance_native,
                    detection_method, last_seen_tx, analysis_tags
                """)\
                .in_('address', addresses_to_check)\
                .eq('blockchain', blockchain)\
                .execute()
            
            if not response.data:
                return None
            
            # Process results with ENHANCED intelligence
            for row in response.data:
                address = row.get('address', '').lower()
                label = row.get('label', '')
                address_type = row.get('address_type', '').lower()
                entity_name = row.get('entity_name', '')
                signal_potential = row.get('signal_potential', '')
                balance_usd = float(row.get('balance_usd', 0) or 0)
                detection_method = row.get('detection_method', '')
                analysis_tags = row.get('analysis_tags') or {}
                base_confidence = float(row.get('confidence', 0.5))
                
                # ENHANCED CEX DETECTION using ALL available data
                is_cex, cex_confidence, cex_evidence = self._comprehensive_cex_detection(
                    address_type, label, entity_name, analysis_tags, signal_potential, detection_method
                )
                
                if is_cex:
                    # Enhanced confidence calculation with balance boost
                    enhanced_confidence = min(0.95, base_confidence + cex_confidence)
                    if balance_usd >= 1_000_000:  # $1M+ balance boost
                        enhanced_confidence = min(0.98, enhanced_confidence + 0.05)
                    
                    # Determine transaction direction
                    if address == from_addr:
                        classification = ClassificationType.BUY
                        direction_evidence = f"CEX → User: Buying from {entity_name or label}"
                    else:  # address == to_addr
                        classification = ClassificationType.SELL
                        direction_evidence = f"User → CEX: Selling to {entity_name or label}"
                    
                    # Comprehensive evidence compilation
                    evidence = [direction_evidence] + cex_evidence
                    if balance_usd >= 100_000:
                        evidence.append(f"High-value CEX address: ${balance_usd:,.0f}")
                    if signal_potential:
                        evidence.append(f"Signal potential: {signal_potential}")
                    
                    return classification, enhanced_confidence, evidence
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Enhanced Supabase CEX check failed: {e}")
            return None

    def _comprehensive_cex_detection(self, address_type: str, label: str, entity_name: str, 
                                   analysis_tags: Dict, signal_potential: str, detection_method: str) -> Tuple[bool, float, List[str]]:
        """
        COMPREHENSIVE CEX detection using ALL available Supabase data sources.
        
        Returns: (is_cex, confidence_boost, evidence_list)
        """
        evidence = []
        confidence_boost = 0.0
        
        # 1. DIRECT ADDRESS TYPE DETECTION
        if any(term in address_type for term in ['exchange', 'cex', 'centralized']):
            evidence.append(f"Address type: {address_type}")
            confidence_boost += 0.25
        
        # 2. ENTITY NAME INTELLIGENCE
        if entity_name:
            cex_entities = [
                'binance', 'coinbase', 'kraken', 'okx', 'kucoin', 'huobi', 'gate.io',
                'crypto.com', 'ftx', 'bybit', 'bitfinex', 'gemini', 'bitstamp'
            ]
            if any(cex in entity_name.lower() for cex in cex_entities):
                evidence.append(f"Known CEX entity: {entity_name}")
                confidence_boost += 0.30
        
        # 3. LABEL PATTERN ANALYSIS
        if label:
            label_lower = label.lower()
            if any(term in label_lower for term in ['exchange', 'trading', 'hot wallet', 'cold wallet']):
                evidence.append(f"CEX label pattern: {label}")
                confidence_boost += 0.20
        
        # 4. ANALYSIS_TAGS INTELLIGENCE (DeFiLlama & custom data)
        if isinstance(analysis_tags, dict):
            # DeFiLlama category analysis
            defillama_category = analysis_tags.get('defillama_category', '').lower()
            if 'cex' in defillama_category or 'exchange' in defillama_category:
                evidence.append(f"DeFiLlama CEX category: {defillama_category}")
                confidence_boost += 0.35
            
            # Custom tags analysis
            tags = analysis_tags.get('tags', [])
            if isinstance(tags, list):
                cex_tags = [tag for tag in tags if any(term in str(tag).lower() for term in ['exchange', 'cex', 'trading'])]
                if cex_tags:
                    evidence.append(f"CEX tags: {', '.join(cex_tags)}")
                    confidence_boost += 0.15
        
        # 5. SIGNAL POTENTIAL ANALYSIS
        if signal_potential and any(term in signal_potential.lower() for term in ['exchange', 'trading', 'liquidity']):
            evidence.append(f"Exchange signal potential: {signal_potential}")
            confidence_boost += 0.10
        
        # 6. DETECTION METHOD INTELLIGENCE
        if detection_method and any(term in detection_method.lower() for term in ['exchange', 'cex', 'trading']):
            evidence.append(f"Exchange detection method: {detection_method}")
            confidence_boost += 0.10
        
        # Determine if this is a CEX (require minimum evidence)
        is_cex = confidence_boost >= 0.20  # Reduced threshold for better detection
        
        return is_cex, confidence_boost, evidence
    
    def _is_cex_address(self, address_type: str, label: str) -> bool:
        """
        Comprehensive CEX detection leveraging ALL possible CEX patterns.
        """
        address_type_lower = address_type.lower()
        label_lower = label.lower()
        
        # Primary CEX keywords
        cex_keywords = [
            'cex', 'exchange', 'centralized_exchange', 'spot_exchange',
            'crypto_exchange', 'trading_platform', 'exchange_wallet'
        ]
        
        for keyword in cex_keywords:
            if keyword in address_type_lower:
                return True
        
        # Major CEX names (comprehensive list)
        major_cexes = [
            'binance', 'coinbase', 'kraken', 'okx', 'okex', 'huobi', 'kucoin',
            'bitfinex', 'gemini', 'bitstamp', 'crypto.com', 'ftx', 'bybit',
            'gate.io', 'mexc', 'ascendex', 'bitget', 'phemex', 'deribit',
            'coincheck', 'bitflyer', 'liquid', 'bitbank', 'zaif', 'dmm',
            'bithumb', 'upbit', 'korbit', 'coinone', 'gopax', 'hanbitco',
            'bittrex', 'poloniex', 'hitbtc', 'cex.io', 'lbank', 'hotbit',
            'probit', 'digifinex', 'coinsbit', 'coinbene', 'bkex', 'indodax',
            'wazirx', 'coinspot', 'btcmarkets', 'swyftx', 'coinjar',
            'mercado', 'bitso', 'ripio', 'foxbit', 'novadax', 'brasilbitcoin'
        ]
        
        # Check label for CEX names
        for cex_name in major_cexes:
            if cex_name in label_lower:
                return True
        
        # CEX-related terms in labels
        exchange_terms = [
            'exchange', 'trading', 'spot', 'margin', 'futures', 'derivatives',
            'custody', 'custodial', 'wallet', 'deposit', 'withdrawal'
        ]
        
        # Only consider it CEX if it has exchange-specific terms AND no DEX indicators
        dex_indicators = ['dex', 'swap', 'uniswap', 'sushiswap', 'pancakeswap', 'amm', 'pool']
        
        has_exchange_term = any(term in label_lower for term in exchange_terms)
        has_dex_indicator = any(indicator in label_lower for indicator in dex_indicators)
        
        if has_exchange_term and not has_dex_indicator:
            # Additional checks for well-known exchange patterns
            if any(term in label_lower for term in ['binance', 'coinbase', 'kraken', 'exchange']):
                return True
        
        return False


# =============================================================================
# STABLECOIN FLOW ANALYSIS ENGINE
# =============================================================================

class StablecoinFlowEngine(BaseAnalysisEngine):
    """Stablecoin flow analysis engine for detecting stable asset movements."""
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def get_engine_name(self) -> str:
        return "Stablecoin Flow Engine"
    
    def analyze(self, from_addr: str, to_addr: str, transaction: Dict[str, Any]) -> PhaseResult:
        """
        Analyze stablecoin flow patterns in the transaction.
        
        Args:
            from_addr: Transaction sender address
            to_addr: Transaction receiver address  
            transaction: Full transaction data
            
        Returns:
            PhaseResult with stablecoin flow analysis
        """
        try:
            evidence = []
            whale_signals = []
            classification = ClassificationType.TRANSFER
            confidence = 0.0
            
            # Check if transaction involves stablecoins
            token_symbol = transaction.get('token_symbol', '').upper()
            token_name = transaction.get('token_name', '').upper()
            
            # Known stablecoin patterns
            stablecoin_symbols = ['USDT', 'USDC', 'DAI', 'BUSD', 'FRAX', 'LUSD', 'MIM', 'TUSD']
            stablecoin_keywords = ['USD', 'STABLE', 'DOLLAR']
            
            is_stablecoin = (
                token_symbol in stablecoin_symbols or
                any(keyword in token_name for keyword in stablecoin_keywords)
            )
            
            if is_stablecoin:
                # Stablecoin detected - analyze flow patterns
                amount_usd = transaction.get('amount_usd', 0)
                
                if amount_usd > 1000000:  # > $1M stablecoin movement
                    classification = ClassificationType.BUY  # Large stablecoin inflow = potential buy preparation
                    confidence = 0.65
                    evidence.append(f"Large stablecoin movement: ${amount_usd:,.0f} {token_symbol}")
                    whale_signals.append("MEGA_STABLECOIN_FLOW")
                elif amount_usd > 100000:  # > $100K stablecoin movement
                    classification = ClassificationType.BUY
                    confidence = 0.45
                    evidence.append(f"Significant stablecoin flow: ${amount_usd:,.0f} {token_symbol}")
                    whale_signals.append("HIGH_STABLECOIN_FLOW")
                else:
                    evidence.append(f"Stablecoin transaction: {token_symbol}")
                    confidence = 0.20
            else:
                evidence.append("No stablecoin flow detected")
            
            return PhaseResult(
                classification=classification,
                confidence=confidence,
                evidence=evidence,
                whale_signals=whale_signals,
                phase=AnalysisPhase.STABLECOIN_FLOW.value,
                raw_data={
                    "is_stablecoin": is_stablecoin,
                    "token_symbol": token_symbol,
                    "amount_usd": transaction.get('amount_usd', 0)
                }
            )
            
        except Exception as e:
            self.logger.error(f"Stablecoin flow analysis failed: {e}")
            return create_empty_phase_result(
                f"Stablecoin analysis error: {str(e)}",
                AnalysisPhase.STABLECOIN_FLOW.value
            )


class DEXProtocolEngine(BaseAnalysisEngine):
    """DEX and DeFi protocol classification analysis engine."""
    
    def __init__(self, supabase_client=None):
        self.supabase_client = supabase_client
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def get_engine_name(self) -> str:
        return "DEX Protocol Engine"
    
    def analyze(self, from_addr: str, to_addr: str, blockchain: str) -> PhaseResult:
        """
        Analyze transaction for DEX and DeFi protocol patterns.
        
        ENHANCED with Protocol Contract vs User Address Detection:
        - Now properly distinguishes between protocol contracts and users holding protocol tokens
        - Implements directional logic based on verified protocol interactions
        - Eliminates false positives from simple user-to-user token transfers
        
        Classification Logic:
        - DEX Router → BUY/SELL (70-80% confidence)
        - Lending Protocol → DEFI (80% confidence)
        - Liquid Staking → STAKING (85% confidence)
        - Yield Farming → DEFI (75% confidence)
        
        Args:
            from_addr: Transaction sender address
            to_addr: Transaction receiver address
            blockchain: Blockchain network
            
        Returns:
            PhaseResult with classification results
        """
        try:
            self.logger.debug(f"Analyzing DEX/DeFi patterns for {from_addr} -> {to_addr}")
            
            from_addr_norm = normalize_address(from_addr)
            to_addr_norm = normalize_address(to_addr)
            
            # PRIORITY 1: Check for DEX router interactions (most common case)
            dex_result = self._analyze_dex_router_interaction(from_addr_norm, to_addr_norm)
            if dex_result:
                classification, confidence, evidence = dex_result
                return PhaseResult(
                    classification=classification,
                    confidence=confidence,
                    evidence=evidence,
                    whale_signals=[],
                    phase=AnalysisPhase.DEX_PROTOCOL.value,
                    raw_data={"source": "dex_router_analysis", "method": "directional_logic"}
                )
            
            # Check hardcoded DEX addresses
            hardcoded_result = self._check_hardcoded_dex_addresses(from_addr_norm, to_addr_norm)
            if hardcoded_result:
                return hardcoded_result
            
            # Check Supabase for DeFi protocols with ENHANCED protocol contract detection
            if self.supabase_client:
                supabase_result = self._check_supabase_defi_protocols(
                    from_addr_norm, to_addr_norm, blockchain
                )
                if supabase_result:
                    return supabase_result
            
            # No DEX/DeFi matches found
            return create_empty_phase_result(
                "No DEX/DeFi protocols detected", 
                AnalysisPhase.DEX_PROTOCOL.value
            )
            
        except Exception as e:
            self.logger.error(f"DEX protocol analysis failed: {e}")
            return create_empty_phase_result(
                f"DEX analysis error: {str(e)}", 
                AnalysisPhase.DEX_PROTOCOL.value
            )

    def _is_direct_protocol_interaction(self, from_result: Optional[Dict], to_result: Optional[Dict]) -> bool:
        """
        PHASE 1 ENHANCEMENT: Determine if this is a direct protocol interaction.
        
        This method checks if at least one address is confirmed to be a protocol contract/router,
        not just a user holding protocol tokens.
        
        Args:
            from_result: Supabase query result for from_address
            to_result: Supabase query result for to_address
            
        Returns:
            bool: True if this is a direct protocol interaction, False if user-to-user transfer
        """
        try:
            # Check FROM address for protocol contract indicators
            if from_result:
                if self._is_verified_protocol_contract(from_result):
                    self.logger.debug(f"FROM address is verified protocol contract: {from_result.get('address', '')[:10]}...")
                    return True
            
            # Check TO address for protocol contract indicators
            if to_result:
                if self._is_verified_protocol_contract(to_result):
                    self.logger.debug(f"TO address is verified protocol contract: {to_result.get('address', '')[:10]}...")
                    return True
            
            # No verified protocol contracts found
            self.logger.debug("No verified protocol contracts detected - treating as user-to-user transfer")
            return False
            
        except Exception as e:
            self.logger.warning(f"Protocol interaction check failed: {e}")
            return False

    def _is_verified_protocol_contract(self, address_data: Dict[str, Any]) -> bool:
        """
        Check if an address is a verified protocol contract based on multiple indicators.
        
        ENHANCED DETECTION CRITERIA with Configuration-Driven Verification:
        1. address_type contains configurable protocol contract types
        2. analysis_tags contains DeFiLlama verification data (if enabled)
        3. entity_name indicates official protocol contracts
        4. Label patterns indicate router/contract functionality (if enabled)
        
        Args:
            address_data: Dictionary containing address information from Supabase
            
        Returns:
            bool: True if this is a verified protocol contract
        """
        address_type = address_data.get('address_type', '').lower()
        label = address_data.get('label', '').lower()
        entity_name = address_data.get('entity_name', '').lower()
        analysis_tags = address_data.get('analysis_tags') or {}
        
        verification_sources = 0
        verification_evidence = []
        
        # PRIMARY: Check address_type for protocol contract indicators (ALWAYS ENABLED)
        if PROTOCOL_CONTRACT_VERIFICATION.get('enable_address_type_verification', True):
            protocol_contract_types = PROTOCOL_CONTRACT_VERIFICATION.get('protocol_contract_types', [])
            
            for contract_type in protocol_contract_types:
                if contract_type in address_type:
                    verification_sources += 1
                    verification_evidence.append(f"address_type: {contract_type}")
                    break
        
        # SECONDARY: Check analysis_tags for DeFiLlama verification (CONFIGURABLE)
        if PROTOCOL_CONTRACT_VERIFICATION.get('enable_defillama_verification', True):
            if isinstance(analysis_tags, dict):
                # DeFiLlama verified protocols have specific metadata
                defillama_category = analysis_tags.get('defillama_category', '').lower()
                if defillama_category and defillama_category != 'unknown':
                    # If it's in DeFiLlama with a category, it's likely a protocol contract
                    verification_sources += 1
                    verification_evidence.append(f"defillama_category: {defillama_category}")
                
                # Check for official protocol URLs or verification
                if analysis_tags.get('official_url') or analysis_tags.get('verified_contract'):
                    verification_sources += 1
                    verification_evidence.append("defillama_verified_contract")
                
                # Check for protocol-specific tags
                tags = analysis_tags.get('tags', [])
                if isinstance(tags, list):
                    protocol_tags = ['router', 'contract', 'pool', 'vault', 'protocol', 'official']
                    matching_tags = [tag for tag in tags if isinstance(tag, str) and tag.lower() in protocol_tags]
                    if matching_tags:
                        verification_sources += 1
                        verification_evidence.append(f"protocol_tags: {matching_tags}")
        
        # TERTIARY: Check label patterns for contract/router indicators (CONFIGURABLE)
        if PROTOCOL_CONTRACT_VERIFICATION.get('enable_label_pattern_verification', True):
            contract_indicators = PROTOCOL_CONTRACT_VERIFICATION.get('contract_indicator_keywords', [])
            
            for indicator in contract_indicators:
                if indicator in label:
                    # Additional validation: avoid false positives like "pool token" or "vault share"
                    if not any(user_indicator in label for user_indicator in ['token', 'share', 'receipt', 'lp']):
                        verification_sources += 1
                        verification_evidence.append(f"label_pattern: {indicator}")
                        break
        
        # QUATERNARY: Check entity_name for official protocol names with contract suffixes
        if entity_name:
            # Known protocol patterns that indicate contracts
            known_protocol_patterns = [
                'uniswap_v2_router', 'uniswap_v3_router', 'sushiswap_router',
                'curve_pool', 'balancer_vault', 'aave_lending_pool',
                'compound_comptroller', 'yearn_vault', 'convex_booster',
                'stargate_router', 'hop_bridge', 'synapse_bridge'
            ]
            
            for pattern in known_protocol_patterns:
                if pattern in entity_name:
                    verification_sources += 1
                    verification_evidence.append(f"known_protocol: {pattern}")
                    break
        
        # Determine if verification passes based on minimum sources required
        minimum_sources = PROTOCOL_CONTRACT_VERIFICATION.get('minimum_verification_sources', 1)
        is_verified = verification_sources >= minimum_sources
        
        if is_verified:
            self.logger.debug(f"Protocol contract verified with {verification_sources} sources: {verification_evidence}")
        else:
            self.logger.debug(f"Protocol contract verification failed: {verification_sources}/{minimum_sources} sources")
        
        return is_verified
    
    def _check_hardcoded_dex_addresses(self, from_addr: str, to_addr: str) -> Optional[PhaseResult]:
        """Check hardcoded DEX router addresses."""
        try:
            # Known DEX routers with their classifications
            dex_routers = {
                '0x7a250d5630b4cf539739df2c5dacb4c659f2488d': ('Uniswap V2', 'DEX_ROUTER'),
                '0xe592427a0aece92de3edee1f18e0157c05861564': ('Uniswap V3', 'DEX_ROUTER'),
                '0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45': ('Uniswap V3 Router 2', 'DEX_ROUTER'),
                '0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f': ('SushiSwap', 'DEX_ROUTER'),
                '0x111111125421ca6dc452d289314280a0f8842a65': ('1inch V5', 'DEX_AGGREGATOR'),
                '0x1111111254eeb25477b68fb85ed929f73a960582': ('1inch V4', 'DEX_AGGREGATOR'),
                '0x881d40237659c251811cec9c364ef91dc08d300c': ('MetaMask Swap', 'DEX_AGGREGATOR'),
            }
            
            # Check interactions with DEX routers
            for addr in [from_addr, to_addr]:
                if addr in dex_routers:
                    dex_name, dex_type = dex_routers[addr]
                    
                    # ENHANCED: Determine classification with improved directional logic
                    if addr == to_addr:
                        # User sending to DEX = typically selling
                        classification = ClassificationType.SELL
                        evidence = [f"DEX interaction: User → {dex_name} ({dex_type}) - SELLING"]
                    else:
                        # DEX sending to user = typically buy completion
                        classification = ClassificationType.BUY
                        evidence = [f"DEX interaction: {dex_name} → User ({dex_type}) - BUYING"]
                    
                    confidence = 0.80  # Increased confidence for verified DEX routers
                    
                    return PhaseResult(
                        classification=classification,
                        confidence=confidence,
                        evidence=evidence,
                        whale_signals=[],
                        phase=AnalysisPhase.DEX_PROTOCOL.value,
                        raw_data={
                            "dex_name": dex_name,
                            "dex_type": dex_type,
                            "interaction_direction": "to_dex" if addr == to_addr else "from_dex",
                            "is_verified_router": True
                        }
                    )
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Hardcoded DEX check failed: {e}")
            return None
    
    def _check_supabase_defi_protocols(self, from_addr: str, to_addr: str, blockchain: str) -> Optional[PhaseResult]:
        """
        ENHANCED: Check Supabase for DeFi protocol information with improved flexibility.
        
        Now handles multiple address types and provides better classification logic.
        """
        if not self.supabase_client:
            return None
            
        try:
            # Query both addresses with more flexible criteria
            addresses_to_check = [from_addr.lower(), to_addr.lower()]
            
            # Use broader query to catch more matches
            supabase_result = self.supabase_client.table('addresses').select(
                'address, label, address_type, entity_name, confidence, analysis_tags, balance_usd'
            ).in_(
                'address', addresses_to_check
            ).execute()
            
            if not supabase_result.data:
                # Try alternative query with case-insensitive search
                or_conditions = f"address.ilike.%{from_addr[2:].lower()}%,address.ilike.%{to_addr[2:].lower()}%"
                alt_result = self.supabase_client.table('addresses').select(
                    'address, label, address_type, entity_name, confidence, analysis_tags, balance_usd'
                ).or_(or_conditions).limit(20).execute()
                
                if alt_result.data:
                    supabase_result = alt_result
                else:
                    return None
            
            evidence = []
            whale_signals = []
            protocol_interactions = []
            confidence_boost = 0.0
            
            for addr_data in supabase_result.data:
                addr = addr_data.get('address', '').lower()
                label = addr_data.get('label', '')
                addr_type = addr_data.get('address_type', '')
                entity_name = addr_data.get('entity_name', '')
                balance_usd = addr_data.get('balance_usd', 0) or 0
                
                # Determine if this is from or to address
                addr_role = 'from' if addr == from_addr.lower() else 'to'
                
                # Enhanced DeFi protocol detection
                is_protocol = self._is_verified_protocol_contract(addr_data)
                is_whale = self._is_whale_address(addr_data, balance_usd)
                
                if is_protocol:
                    protocol_interactions.append((addr_role, addr_data))
                    evidence.append(f"Supabase: {addr_role} address is verified DeFi protocol ({entity_name or label})")
                    confidence_boost += 0.25
                    
                elif is_whale:
                    whale_signals.append(f"{addr_role.title()} address: {label} (${balance_usd:,.0f})")
                    evidence.append(f"Supabase: {addr_role} address is whale ({label})")
                    confidence_boost += 0.15
                    
                else:
                    # Still valuable - any labeled address provides context
                    if label and 'unknown' not in label.lower():
                        evidence.append(f"Supabase: {addr_role} address labeled as '{label}'")
                        confidence_boost += 0.05
            
            # Determine if this is a direct protocol interaction
            if protocol_interactions:
                # We have at least one verified protocol - this is a real DeFi interaction
                is_direct_interaction = True
                classification, reasoning = self._determine_enhanced_protocol_classification(
                    from_addr, to_addr, protocol_interactions, blockchain
                )
            else:
                # No verified protocols - likely user-to-user transfer even if DeFi tokens involved
                is_direct_interaction = False
                classification = ClassificationType.TRANSFER
                reasoning = "No verified protocol contracts detected - treating as user transfer"
                
                # Reduce confidence significantly for non-protocol interactions
                confidence_boost = min(confidence_boost, 0.10)
            
            # Calculate final confidence
            base_confidence = CLASSIFICATION_THRESHOLDS.get('protocol_interaction_threshold', 0.75) if is_direct_interaction else 0.30
            final_confidence = min(0.95, base_confidence + confidence_boost)
            
            # Apply protocol-specific confidence boost if applicable
            if is_direct_interaction and DEFI_PROTOCOL_SETTINGS.get('enable_directional_logic', True):
                protocol_boost = DEFI_PROTOCOL_SETTINGS.get('protocol_confidence_boost', 0.15)
                final_confidence = min(0.95, final_confidence + protocol_boost)
            
            return PhaseResult(
                classification=classification,
                confidence=final_confidence,
                evidence=evidence,
                whale_signals=whale_signals,
                phase=AnalysisPhase.SUPABASE_DEFI.value,
                raw_data={
                    "supabase_matches": len(supabase_result.data),
                    "protocol_interactions": len(protocol_interactions),
                    "is_direct_protocol_interaction": is_direct_interaction,
                    "reasoning": reasoning
                }
            )
            
        except Exception as e:
            self.logger.error(f"Supabase DeFi protocol check failed: {e}")
            return None
    
    def _is_whale_address(self, addr_data: Dict[str, Any], balance_usd: float) -> bool:
        """Determine if an address represents a whale based on multiple indicators."""
        
        # Check balance threshold
        if balance_usd >= WHALE_THRESHOLDS.get('whale_usd', 1_000_000):
            return True
            
        # Check label patterns
        label = addr_data.get('label', '').lower()
        whale_indicators = [
            'whale', 'large', 'mega', 'high volume', 'exchange', 'binance', 
            'coinbase', 'trading', 'institutional', 'fund'
        ]
        
        for indicator in whale_indicators:
            if indicator in label:
                return True
                
        # Check entity name
        entity_name = addr_data.get('entity_name', '').lower()
        if any(indicator in entity_name for indicator in whale_indicators):
            return True
            
        return False

    def _determine_enhanced_protocol_classification(self, from_addr: str, to_addr: str, protocol_interactions: List[Tuple[str, Dict[str, Any]]], blockchain: str) -> Tuple[ClassificationType, str]:
        """
        PHASE 2 ENHANCEMENT: Determine final classification with sophisticated directional logic.
        
        ENHANCED DIRECTIONAL LOGIC FOR KEY DEFI CATEGORIES:
        
        Liquid Staking (e.g., Lido, mETH):
        - User → Protocol: STAKING (BUY) - User is staking assets
        - Protocol → User: UNSTAKING (SELL) - User is unstaking and receiving assets back
        
        Lending (e.g., Aave, Compound):
        - User → Protocol: DEPOSIT (BUY) - User is supplying assets to the protocol
        - Protocol → User: WITHDRAW (SELL) - User is withdrawing supplied assets
        
        DEXs (e.g., Uniswap, Curve):
        - User → Protocol: SELL - User sends Token A to the router to receive Token B
        - Protocol → User: BUY - Router initiates a transfer to the user (less common)
        
        Bridges (e.g., Stargate, Hop):
        - All bridge interactions should remain TRANSFER (cross-chain movement, not investment position change)
        
        Returns: (final_classification, direction_evidence)
        """
        
        # BRIDGE LOGIC: Always TRANSFER regardless of direction
        if protocol_interactions[0][1].get('protocol_type', '') == "BRIDGE":
            # Check if bridge classification override is enabled
            if DEFI_PROTOCOL_SETTINGS.get('bridge_classification_override', True):
                return ClassificationType.TRANSFER, f"Cross-chain bridge transfer via {protocol_interactions[0][1].get('entity_name', '')}"
            else:
                # Fall back to base classification if override disabled
                return ClassificationType.TRANSFER, f"Bridge interaction via {protocol_interactions[0][1].get('entity_name', '')}"
        
        # LIQUID STAKING LOGIC: Investment behavior analysis
        elif protocol_interactions[0][1].get('protocol_type', '') in ["LIQUID_STAKING", "STAKING"]:
            staking_mapping = DEFI_PROTOCOL_SETTINGS.get('staking_classification_mapping', 'BUY')
            if protocol_interactions[0][0] == 'from':
                if staking_mapping == 'BUY':
                    return ClassificationType.BUY, f"User is staking via {protocol_interactions[0][1].get('entity_name', '')} Protocol"
                else:
                    return ClassificationType.STAKING, f"User is staking via {protocol_interactions[0][1].get('entity_name', '')} Protocol"
            else:
                # Unstaking is generally considered selling regardless of settings
                return ClassificationType.SELL, f"User is unstaking from {protocol_interactions[0][1].get('entity_name', '')}"
        
        # LENDING LOGIC: Capital deployment analysis  
        elif protocol_interactions[0][1].get('protocol_type', '') in ["LENDING", "YIELD_FARMING"]:
            deposit_mapping = DEFI_PROTOCOL_SETTINGS.get('lending_deposit_mapping', 'BUY')
            withdraw_mapping = DEFI_PROTOCOL_SETTINGS.get('lending_withdraw_mapping', 'SELL')
            
            if protocol_interactions[0][0] == 'from':
                if deposit_mapping == 'BUY':
                    return ClassificationType.BUY, f"User is supplying to {protocol_interactions[0][1].get('entity_name', '')} lending pool"
                else:
                    return ClassificationType.DEFI, f"User is supplying to {protocol_interactions[0][1].get('entity_name', '')} lending pool"
            else:
                if withdraw_mapping == 'SELL':
                    return ClassificationType.SELL, f"User is withdrawing from {protocol_interactions[0][1].get('entity_name', '')} lending pool"
                else:
                    return ClassificationType.DEFI, f"User is withdrawing from {protocol_interactions[0][1].get('entity_name', '')} lending pool"
        
        # DEX LOGIC: Trading direction analysis
        elif protocol_interactions[0][1].get('protocol_type', '') in ["DEX", "UNISWAP", "CURVE", "BALANCER", "PANCAKESWAP", "SUSHISWAP"]:
            if protocol_interactions[0][0] == 'from':
                return ClassificationType.SELL, f"User selling via {protocol_interactions[0][1].get('entity_name', '')} DEX"
            else:
                return ClassificationType.BUY, f"User buying via {protocol_interactions[0][1].get('entity_name', '')} DEX"
        
        # DERIVATIVES LOGIC: Trading activity
        elif protocol_interactions[0][1].get('protocol_type', '') in ["DERIVATIVES", "OPTIONS", "SYNTHETICS"]:
            return ClassificationType.BUY, f"User trading derivatives via {protocol_interactions[0][1].get('entity_name', '')}"
        
        # GENERIC DEFI: Use base classification with enhanced directional intelligence
        else:
            if protocol_interactions[0][0] == 'from':
                return ClassificationType.BUY, f"User deploying capital to {protocol_interactions[0][1].get('entity_name', '')}"
            else:
                return ClassificationType.BUY, f"User receiving assets from {protocol_interactions[0][1].get('entity_name', '')}"

    def _extract_protocol_metadata(self, analysis_tags: Dict, entity_name: str) -> Dict[str, Any]:
        """Extract additional protocol metadata from analysis_tags."""
        metadata = {}
        
        if isinstance(analysis_tags, dict):
            # DeFiLlama metadata
            if 'defillama_slug' in analysis_tags:
                metadata['defillama_slug'] = analysis_tags['defillama_slug']
            if 'all_chains' in analysis_tags:
                metadata['supported_chains'] = analysis_tags['all_chains']
            if 'official_url' in analysis_tags:
                metadata['protocol_url'] = analysis_tags['official_url']
            
            # Custom tags
            if 'tags' in analysis_tags:
                metadata['protocol_tags'] = analysis_tags['tags']
        
        return metadata
    
    def _comprehensive_defi_protocol_detection(self, address_type: str, label: str, entity_name: str,
                                             analysis_tags: Dict, signal_potential: str) -> Optional[Tuple[ClassificationType, float, str, List[str]]]:
        """
        COMPREHENSIVE DeFi protocol detection using ALL available intelligence sources.
        
        Returns: (classification, confidence_boost, protocol_type, evidence_list)
        """
        evidence = []
        confidence_boost = 0.0
        protocol_type = "UNKNOWN"
        classification = ClassificationType.TRANSFER
        
        # 1. ANALYSIS_TAGS INTELLIGENCE (Primary source - DeFiLlama data)
        if isinstance(analysis_tags, dict):
            defillama_category = analysis_tags.get('defillama_category', '').lower()
            defillama_slug = analysis_tags.get('defillama_slug', '').lower()
            
            # ENHANCED DEFILLAMA CATEGORY MAPPING with proper BUY/SELL logic
            if defillama_category:
                if 'dex' in defillama_category:
                    classification = ClassificationType.BUY  # DEX interactions are typically swaps (BUY/SELL)
                    protocol_type = "DEX"
                    confidence_boost += 0.30
                    evidence.append(f"DeFiLlama DEX: {defillama_category}")
                    
                elif any(term in defillama_category for term in ['liquid staking', 'staking']):
                    classification = ClassificationType.BUY  # Staking = investment behavior
                    protocol_type = "LIQUID_STAKING"
                    confidence_boost += 0.28
                    evidence.append(f"Liquid staking protocol: {defillama_category}")
                    
                elif 'lending' in defillama_category:
                    classification = ClassificationType.BUY  # Lending = capital deployment
                    protocol_type = "LENDING"
                    confidence_boost += 0.26
                    evidence.append(f"DeFi lending: {defillama_category}")
                    
                elif 'yield' in defillama_category or 'farming' in defillama_category:
                    classification = ClassificationType.BUY  # Yield farming = investment
                    protocol_type = "YIELD_FARMING"
                    confidence_boost += 0.25
                    evidence.append(f"Yield protocol: {defillama_category}")
                    
                elif 'bridge' in defillama_category:
                    classification = ClassificationType.TRANSFER  # Bridges are neutral transfers
                    protocol_type = "BRIDGE"
                    confidence_boost += 0.22
                    evidence.append(f"Cross-chain bridge: {defillama_category}")
                    
                elif any(term in defillama_category for term in ['derivatives', 'options', 'futures']):
                    classification = ClassificationType.BUY  # Derivatives = trading activity
                    protocol_type = "DERIVATIVES"
                    confidence_boost += 0.24
                    evidence.append(f"DeFi derivatives: {defillama_category}")
                    
                elif 'synthetics' in defillama_category:
                    classification = ClassificationType.BUY  # Synthetics = trading
                    protocol_type = "SYNTHETICS"
                    confidence_boost += 0.23
                    evidence.append(f"Synthetic assets: {defillama_category}")
            
            # Slug-based detection for specific protocols
            if defillama_slug and confidence_boost == 0.0:  # Fallback if category didn't match
                protocol_mappings = {
                    'uniswap': (ClassificationType.BUY, 0.32, "UNISWAP", "Uniswap DEX"),
                    'curve': (ClassificationType.BUY, 0.30, "CURVE", "Curve Finance"),
                    'aave': (ClassificationType.BUY, 0.28, "AAVE", "Aave lending"),
                    'compound': (ClassificationType.BUY, 0.28, "COMPOUND", "Compound lending"),
                    'balancer': (ClassificationType.BUY, 0.27, "BALANCER", "Balancer DEX"),
                    'pancakeswap': (ClassificationType.BUY, 0.29, "PANCAKESWAP", "PancakeSwap DEX"),
                    'sushiswap': (ClassificationType.BUY, 0.29, "SUSHISWAP", "SushiSwap DEX")
                }
                
                for slug_key, (cls, conf, ptype, desc) in protocol_mappings.items():
                    if slug_key in defillama_slug:
                        classification = cls
                        protocol_type = ptype
                        confidence_boost += conf
                        evidence.append(f"{desc} detected")
                        break
        
        # 2. ENTITY NAME INTELLIGENCE
        if entity_name and confidence_boost == 0.0:  # Secondary source
            entity_lower = entity_name.lower()
            if any(term in entity_lower for term in ['uniswap', 'dex', 'swap']):
                classification = ClassificationType.BUY
                protocol_type = "DEX"
                confidence_boost += 0.25
                evidence.append(f"DEX entity: {entity_name}")
            elif any(term in entity_lower for term in ['aave', 'compound', 'lending']):
                classification = ClassificationType.BUY
                protocol_type = "LENDING"
                confidence_boost += 0.23
                evidence.append(f"Lending entity: {entity_name}")
        
        # 3. ADDRESS TYPE DETECTION
        if address_type and confidence_boost == 0.0:  # Tertiary source
            if any(term in address_type for term in ['dex', 'defi', 'protocol']):
                classification = ClassificationType.BUY
                protocol_type = "DEFI_PROTOCOL"
                confidence_boost += 0.20
                evidence.append(f"DeFi address type: {address_type}")
        
        # 4. LABEL PATTERN ANALYSIS
        if label and confidence_boost == 0.0:  # Fallback source
            label_lower = label.lower()
            if any(term in label_lower for term in ['router', 'pool', 'vault', 'staking']):
                classification = ClassificationType.BUY
                protocol_type = "DEFI_GENERAL"
                confidence_boost += 0.18
                evidence.append(f"DeFi label pattern: {label}")
        
        # Return result if any protocol detected
        if confidence_boost > 0.0:
            return classification, confidence_boost, protocol_type, evidence
        
        return None

    def _is_likely_contract(self, address: str) -> bool:
        """
        Simple heuristic to determine if an address is likely a smart contract.
        
        This is a basic implementation that checks for patterns commonly 
        associated with contract addresses.
        
        Args:
            address: The address to check
            
        Returns:
            bool: True if likely a contract address
        """
        if not address or len(address) != 42:
            return False
        
        # Convert to lowercase for consistent checking
        addr_lower = address.lower()
        
        # Simple heuristics for contract detection:
        # 1. Addresses ending in many zeros (often deployment addresses)
        if addr_lower.endswith('000000') or addr_lower.endswith('0000'):
            return True
        
        # 2. Addresses with specific patterns often used for contracts
        if any(pattern in addr_lower for pattern in ['dead', 'beef', 'cafe', 'babe']):
            return True
        
        # 3. Very common contract address patterns
        if addr_lower.startswith('0x0000000') or addr_lower.startswith('0xfffffff'):
            return True
        
        return False

    def _analyze_dex_router_interaction(self, from_addr: str, to_addr: str) -> Optional[Tuple[ClassificationType, float, List[str]]]:
        """
        ENHANCED: Analyze DEX router interactions with proper directional logic.
        
        Key improvements:
        - Recognizes router vs user interactions
        - Provides lower but honest confidence (0.60 instead of 0.80)
        - Explains that this requires blockchain analysis for accuracy
        """
        dex_routers = {
            "0x7a250d5630b4cf539739df2c5dacb4c659f2488d": "Uniswap V2 Router",
            "0xe592427a0aece92de3edee1f18e0157c05861564": "Uniswap V3 Router", 
            "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45": "Uniswap V3 Router 2",
            "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f": "SushiSwap Router",
            "0x1111111254fb6c44bac0bed2854e76f90643097d": "1inch V4 Router",
            "0xdef171fe48cf0115b1d80b88dc8eab59176fee57": "ParaSwap V5"
        }
        
        # Check for DEX router interactions
        if from_addr.lower() in dex_routers:
            # Router -> User: Likely buying (receiving tokens from DEX)
            router_name = dex_routers[from_addr.lower()]
            return (
                ClassificationType.BUY,
                0.60,  # Lower confidence - requires event log analysis for certainty
                [f"DEX interaction: {router_name} → User (likely BUYING, needs confirmation)"]
            )
        
        elif to_addr.lower() in dex_routers:
            # User -> Router: Likely selling (sending tokens to DEX)
            router_name = dex_routers[to_addr.lower()]
            return (
                ClassificationType.SELL,
                0.60,  # Lower confidence - requires event log analysis for certainty
                [f"DEX interaction: User → {router_name} (likely SELLING, needs confirmation)"]
            )
        
        return None


# =============================================================================
# MAIN WHALE INTELLIGENCE ENGINE
# =============================================================================

class WhaleIntelligenceEngine:
    """
    🧠 PRODUCTION-GRADE WHALE INTELLIGENCE ENGINE 🧠
    
    A comprehensive, cost-optimized transaction analysis system that integrates
    multiple data sources and analysis engines to classify whale transactions
    with high accuracy and performance.
    
    Features:
    - Modular analysis engines with dependency injection
    - Cost-optimized execution with early exit conditions
    - Comprehensive error handling and logging
    - Structured data models with type safety
    - Production-ready configuration management
    - Real-time market data intelligence integration
    """
    
    def __init__(self):
        """Initialize the whale intelligence engine with all components."""
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
        # Initialize core components
        self.bigquery_analyzer: Optional[BigQueryAnalyzer] = None
        self.evm_parsers: Dict[str, EVMLogParser] = {}
        self.solana_parser: Optional[SolanaParser] = None
        self.supabase_client = None
        
        # Initialize market data provider
        self.market_data_provider: Optional[MarketDataProvider] = None
        
        # Initialize analysis engines
        self.cex_engine: Optional[CEXClassificationEngine] = None
        self.dex_engine: Optional[DEXProtocolEngine] = None
        
        # Configuration
        self.phase_weights = CONFIDENCE_WEIGHTS
        self.confidence_thresholds = CLASSIFICATION_THRESHOLDS
        
        # Initialize all components
        self._initialize_components()
        
        self.logger.info("Whale Intelligence Engine initialized successfully")
    
    def _initialize_components(self) -> None:
        """Initialize all engine components with proper error handling."""
        try:
            # Initialize blockchain parsers
            self._init_blockchain_parsers()
            
            # Initialize database connections
            self._init_database_connections()
            
            # Initialize analysis engines
            self._init_analysis_engines()
            
            # Initialize API integrations
            self._init_api_integrations()
            
        except Exception as e:
            self.logger.error(f"Component initialization failed: {e}")
            raise ConfigurationError(f"Failed to initialize whale engine: {e}")
    
    def _init_blockchain_parsers(self) -> None:
        """Initialize blockchain-specific parsers."""
        try:
            # Initialize EVM parsers for Ethereum and Polygon with proper API credentials
            from config.api_keys import ETHERSCAN_API_KEY
            
            self.evm_parsers['ethereum'] = EVMLogParser('ethereum', ETHERSCAN_API_KEY, 'https://api.etherscan.io/api')
            self.evm_parsers['polygon'] = EVMLogParser('polygon', ETHERSCAN_API_KEY, 'https://api.polygonscan.com/api')
            
            # Initialize Solana parser
            self.solana_parser = SolanaParser()
            
            self.logger.info("Blockchain parsers initialized")
            
        except Exception as e:
            self.logger.warning(f"Blockchain parser initialization failed: {e}")
    
    def _init_database_connections(self) -> None:
        """Initialize database connections."""
        try:
            # Initialize Supabase client
            try:
                from supabase import create_client, Client
                from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
                
                if SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY:
                    self.supabase_client: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
                    self.logger.info("✅ Supabase connection established")
                else:
                    self.logger.warning("⚠️ Supabase credentials not available")
                    
            except Exception as e:
                self.logger.warning(f"Supabase initialization failed: {e}")
            
            # Initialize BigQuery with comprehensive error handling
            try:
                from utils.bigquery_analyzer import bigquery_analyzer
                
                # Test BigQuery initialization and permissions
                if bigquery_analyzer and hasattr(bigquery_analyzer, 'client'):
                    # Test with a simple query to verify permissions
                    test_query = "SELECT 1 as test_column LIMIT 1"
                    result = bigquery_analyzer.client.query(test_query).result()
                    self.bigquery_analyzer = bigquery_analyzer
                    self.logger.info("✅ BigQuery connection successful")
                else:
                    self.bigquery_analyzer = None
                    self.logger.warning("⚠️ BigQuery analyzer not available")
                    
            except Exception as bq_error:
                self.bigquery_analyzer = None
                error_msg = str(bq_error)
                
                if "403" in error_msg or "Access Denied" in error_msg:
                    self.logger.warning("⚠️ BigQuery 403 Access Denied - service account needs 'bigquery.jobs.create' permission")
                    self.logger.warning("⚠️ Continuing with 6-phase analysis (excluding BigQuery)")
                elif "bigquery.jobs.create" in error_msg:
                    self.logger.warning("⚠️ BigQuery permissions issue - service account lacks required permissions")
                    self.logger.warning("⚠️ To fix: Grant 'BigQuery Job User' role to service account")
                else:
                    self.logger.warning(f"⚠️ BigQuery initialization failed: {error_msg}")
                
                self.logger.info("⚠️ System continuing with other phases - BigQuery analysis disabled")
            
        except Exception as e:
            self.logger.warning(f"Database initialization failed: {e}")
    
    def _init_analysis_engines(self) -> None:
        """Initialize analysis engines with dependency injection."""
        try:
            self.cex_engine = CEXClassificationEngine(self.supabase_client)
            self.dex_engine = DEXProtocolEngine(self.supabase_client)
            
            self.logger.info("Analysis engines initialized")
            
        except Exception as e:
            self.logger.error(f"Analysis engine initialization failed: {e}")
            raise ConfigurationError(f"Failed to initialize analysis engines: {e}")
    
    def _init_api_integrations(self) -> None:
        """Initialize API integrations."""
        try:
            # Initialize enhanced API integrations if available
            try:
                from utils.enhanced_api_integrations import EnhancedAPIIntegrations
                self.api_integrations = EnhancedAPIIntegrations()
                self.logger.info("Enhanced API integrations initialized")
            except ImportError:
                self.api_integrations = None
                self.logger.info("Enhanced API integrations not available")
            
            # Initialize market data provider
            if MARKET_DATA_AVAILABLE:
                try:
                    self.market_data_provider = MarketDataProvider()
                    self.logger.info("✅ MarketDataProvider initialized successfully")
                except Exception as e:
                    self.market_data_provider = None
                    self.logger.warning(f"⚠️ MarketDataProvider initialization failed: {e}")
            else:
                self.market_data_provider = None
                self.logger.info("⚠️ MarketDataProvider not available - market intelligence disabled")
                
        except Exception as e:
            self.logger.warning(f"API integration initialization failed: {e}")
    
    def _map_to_final_classification(self, internal_result: 'PhaseResult') -> 'PhaseResult':
        """
        Map internal granular classifications to simplified user-facing output.
        This preserves the sophisticated internal analysis while providing clear,
        actionable classifications for the end user.
        """
        if not internal_result:
            return internal_result
        
        # Mapping from internal classification to final output
        mapping = {
            ClassificationType.VERIFIED_SWAP_BUY: ClassificationType.BUY,
            ClassificationType.VERIFIED_SWAP_SELL: ClassificationType.SELL,
            ClassificationType.LIQUIDITY_ADD: ClassificationType.TOKEN_TRANSFER,
            ClassificationType.LIQUIDITY_REMOVE: ClassificationType.TOKEN_TRANSFER,
            ClassificationType.WRAP: ClassificationType.TOKEN_TRANSFER,
            ClassificationType.UNWRAP: ClassificationType.TOKEN_TRANSFER,
            # All others stay the same
        }
        
        final_classification = mapping.get(internal_result.classification, internal_result.classification)
        
        # Update evidence to reflect the mapping if changed
        evidence = internal_result.evidence.copy()
        if final_classification != internal_result.classification:
            evidence.append(f"Mapped from {internal_result.classification.value} to {final_classification.value}")
        
        # Create new result with mapped classification
        from copy import deepcopy
        mapped_result = deepcopy(internal_result)
        mapped_result.classification = final_classification
        mapped_result.evidence = evidence
        
        return mapped_result
    
    def analyze_transaction_comprehensive(self, transaction: Dict[str, Any]) -> IntelligenceResult:
        """
        🎯 TWO-STAGE ANALYSIS PIPELINE 🎯
        
        Execute a sophisticated two-stage whale intelligence analysis:
        Stage 1: Mandatory core analysis (always executed)
        Stage 2: Conditional deep enrichment (if Stage 1 is ambiguous)
        
        Args:
            transaction: Transaction data dictionary
            
        Returns:
            IntelligenceResult: Comprehensive analysis results
        """
        try:
            # Extract and validate transaction data
            tx_data = self._extract_transaction_data(transaction)
            if not tx_data:
                raise DataValidationError("Invalid transaction data provided")
            
            tx_hash, blockchain, from_addr, to_addr = tx_data
            
            # Initialize transaction logger
            tx_logger = get_transaction_logger(tx_hash)
            
            tx_logger.info(
                "Starting Two-Stage Whale Intelligence Analysis",
                blockchain=blockchain,
                from_address=from_addr,
                to_address=to_addr,
                usd_value=transaction.get('amount_usd', 0)
            )
            
            # Initialize result structure
            result = IntelligenceResult()
            
            # ========== STAGE 1: MANDATORY CORE ANALYSIS ==========
            tx_logger.debug("🔍 STAGE 1: Executing Mandatory Core Analysis")
            
            # Phase 1: Blockchain Specific Analysis (Foundation)
            tx_logger.debug("Phase 1: Blockchain Specific Analysis")
            phase1_result = self._analyze_blockchain_specific(tx_hash, blockchain)
            result.phase_results[AnalysisPhase.BLOCKCHAIN_SPECIFIC.value] = phase1_result
            result.whale_signals.extend(phase1_result.whale_signals)
            
            # Phase 2: Stablecoin Flow Analysis
            tx_logger.debug("Phase 2: Stablecoin Flow Analysis")
            phase2_result = self._analyze_stablecoin_flow(from_addr, to_addr, transaction)
            result.phase_results[AnalysisPhase.STABLECOIN_FLOW.value] = phase2_result
            result.whale_signals.extend(phase2_result.whale_signals)
            
            # Phase 3: CEX Classification
            tx_logger.debug("Phase 3: CEX Classification")
            if self.cex_engine:
                phase3_result = self.cex_engine.analyze(from_addr, to_addr, blockchain)
                result.phase_results[AnalysisPhase.CEX_CLASSIFICATION.value] = phase3_result
                result.whale_signals.extend(phase3_result.whale_signals)
            
            # Phase 4: DEX & DeFi Protocol Classification
            tx_logger.debug("Phase 4: DEX & DeFi Protocol Classification")
            if self.dex_engine:
                phase4_result = self.dex_engine.analyze(from_addr, to_addr, blockchain)
                result.phase_results[AnalysisPhase.DEX_PROTOCOL.value] = phase4_result
                result.whale_signals.extend(phase4_result.whale_signals)
            
            # Phase 5: Wallet Behavioral Analysis
            tx_logger.debug("Phase 5: Wallet Behavioral Analysis")
            phase5_result = self._analyze_wallet_behavior(from_addr, to_addr, transaction)
            result.phase_results[AnalysisPhase.WALLET_BEHAVIOR.value] = phase5_result
            result.whale_signals.extend(phase5_result.whale_signals)
            
            # ========== APPLY FINAL CLASSIFICATION MAPPING ==========
            # Map internal granular classifications to user-facing output
            mapped_phase_results = {}
            for phase_name, phase_result in result.phase_results.items():
                mapped_phase_results[phase_name] = self._map_to_final_classification(phase_result)
            
            # ========== CONDITIONAL CHECKPOINT: Professional Pipeline Routing ==========
            stage1_classification, stage1_confidence, stage1_reasoning = self._evaluate_stage1_results(mapped_phase_results)
            
            tx_logger.debug(
                f"Stage 1 Complete: {stage1_classification.value} (confidence: {stage1_confidence:.2f})"
            )
            tx_logger.debug(f"Stage 1 Reasoning: {stage1_reasoning}")
            
            # PROFESSIONAL PIPELINE ROUTING LOGIC
            should_proceed_to_stage2 = (
                stage1_classification == ClassificationType.TRANSFER or
                stage1_classification == ClassificationType.CONFLICT or
                (stage1_confidence <= 0.75)  # Below early exit threshold
            )
            
            if should_proceed_to_stage2:
                tx_logger.info(f"🔬 PROCEEDING TO STAGE 2: {stage1_reasoning}")
                # Continue to Stage 2 analysis below
            else:
                # Early exit approved for high-confidence, uncontested signals
                tx_logger.info(f"✅ EARLY EXIT APPROVED: {stage1_reasoning}")
                result.classification = stage1_classification
                result.confidence = stage1_confidence
                result.master_classifier_reasoning = stage1_reasoning
                result.final_whale_score = self._calculate_whale_score(result.whale_signals)
                result.phases_completed = len(result.phase_results)
                return result
            
            # ========== STAGE 2: CONDITIONAL DEEP ENRICHMENT ==========
            tx_logger.debug("🔬 STAGE 2: Executing Conditional Deep Enrichment")
            
            # 🧠 SMART TIER 2: API-Only Enrichment (Always run - cheap APIs)
            tx_logger.debug("Phase 6: Zerion Portfolio Analysis")
            phase6_result = self._analyze_zerion_portfolio(from_addr, to_addr, tx_hash)
            result.phase_results[AnalysisPhase.ZERION_PORTFOLIO.value] = phase6_result
            result.whale_signals.extend(phase6_result.whale_signals)
            
            tx_logger.debug("Phase 7: Moralis Enrichment")
            phase7_result = self._analyze_moralis_enrichment(from_addr, to_addr, blockchain)
            result.phase_results[AnalysisPhase.MORALIS_ENRICHMENT.value] = phase7_result
            result.whale_signals.extend(phase7_result.whale_signals)
            
            # 🎯 SMART CHECKPOINT: Check if API enrichment resolved uncertainty
            current_confidence = self._calculate_current_confidence(result.phase_results)
            BIGQUERY_TRIGGER_THRESHOLD = 0.70
            
            if current_confidence < BIGQUERY_TRIGGER_THRESHOLD and self.bigquery_analyzer:
                tx_logger.info(f"🚀 TIER 3 TRIGGERED: Confidence still low ({current_confidence:.2f}) - Activating BigQuery")
                tx_logger.debug("Phase 8: BigQuery Mega Whale Detection")
                phase8_result = self._analyze_bigquery_whale(from_addr, to_addr, blockchain)
                result.phase_results[AnalysisPhase.BIGQUERY_WHALE.value] = phase8_result
                result.whale_signals.extend(phase8_result.whale_signals)
            elif current_confidence >= BIGQUERY_TRIGGER_THRESHOLD:
                tx_logger.info(f"💰 COST OPTIMIZED: API enrichment sufficient ({current_confidence:.2f}) - Skipping BigQuery")
            else:
                tx_logger.info("⚠️ BigQuery not available - Using API-only enrichment")
            
            
            # ========== FINAL CLASSIFICATION ==========
            tx_logger.debug("🎯 Executing Enhanced Master Classification")
            
            # Apply behavioral heuristics across all phases
            behavioral_analysis = self._apply_behavioral_heuristics(transaction, result.phase_results)
            result.behavioral_analysis = behavioral_analysis
            
            # Determine final classification using all available signals
            final_classification, final_confidence, reasoning = self._determine_master_classification(
                result.phase_results, behavioral_analysis
            )
            
            # Finalize results
            result.classification = final_classification
            result.confidence = final_confidence
            result.master_classifier_reasoning = reasoning
            result.final_whale_score = self._calculate_whale_score(result.whale_signals)
            result.phases_completed = len(result.phase_results)
            
            # Log final classification
            tx_logger.master_classification(final_classification.value, final_confidence, reasoning)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Comprehensive analysis failed: {e}")
            self.logger.error(traceback.format_exc())
            
            # Return safe fallback result
            return IntelligenceResult(
                classification=ClassificationType.TRANSFER,
                confidence=0.0,
                evidence=[f"Analysis failed: {str(e)}"],
                master_classifier_reasoning=f"Error during analysis: {str(e)}"
            )

    def _calculate_current_confidence(self, phase_results: Dict[str, PhaseResult]) -> float:
        """
        Calculate the current confidence level for smart BigQuery triggering.
        
        Uses the highest confidence from any phase to determine if additional
        BigQuery analysis is needed.
        """
        if not phase_results:
            return 0.0
        
        max_confidence = 0.0
        for phase_result in phase_results.values():
            if hasattr(phase_result, 'confidence') and phase_result.confidence > max_confidence:
                max_confidence = phase_result.confidence
                
        return max_confidence
    
    def _extract_transaction_data(self, transaction: Dict[str, Any]) -> Optional[Tuple[str, str, str, str]]:
        """Extract and validate core transaction data."""
        try:
            tx_hash = transaction.get('hash', transaction.get('transaction_hash', ''))
            blockchain = transaction.get('blockchain', 'ethereum').lower()
            from_addr = normalize_address(transaction.get('from', transaction.get('from_address', '')))
            to_addr = normalize_address(transaction.get('to', transaction.get('to_address', '')))
            
            if not tx_hash or not from_addr or not to_addr:
                self.logger.warning("Missing required transaction fields")
                return None
            
            return tx_hash, blockchain, from_addr, to_addr
            
        except Exception as e:
            self.logger.error(f"Transaction data extraction failed: {e}")
            return None
    
    def _should_exit_early(self, phase_result: PhaseResult, phase_type: str) -> bool:
        """
        Check if phase result warrants early exit with more permissive thresholds.
        
        Enhanced logic:
        - CEX early exit: 0.75 (reduced from 0.85)
        - DEX/Protocol early exit: 0.70 (reduced from 0.75)
        """
        early_exit_thresholds = {
            "CEX": 0.75,      # Reduced from 0.85
            "DEX": 0.70,      # Reduced from 0.75
            "default": 0.70   # For other phase types
        }
        
        threshold = early_exit_thresholds.get(phase_type, early_exit_thresholds["default"])
        
        return (
            phase_result.classification in [
                ClassificationType.BUY, 
                ClassificationType.SELL,
                ClassificationType.MODERATE_BUY,
                ClassificationType.MODERATE_SELL
            ] and
            phase_result.confidence >= threshold
        )
    
    def _has_conflicting_signals(self, phase_results: Dict[str, PhaseResult]) -> bool:
        """Check for conflicting classification signals between phases."""
        classifications = []
        for result in phase_results.values():
            if result.classification in [ClassificationType.BUY, ClassificationType.SELL] and result.confidence > 0.6:
                classifications.append(result.classification)
        
        # Return True if we have both BUY and SELL signals
        return ClassificationType.BUY in classifications and ClassificationType.SELL in classifications
    
    def _finalize_early_exit(self, result: IntelligenceResult, phase_result: PhaseResult, tx_logger, reason: str) -> IntelligenceResult:
        """
        Finalize result for early exit scenario with enhanced moderate confidence support.
        """
        # Map STAKING and DEFI to BUY for whale monitoring
        final_classification = phase_result.classification
        reasoning_suffix = ""
        
        if final_classification == ClassificationType.STAKING:
            final_classification = ClassificationType.BUY
            reasoning_suffix = " (STAKING mapped to BUY - investment behavior)"
        elif final_classification == ClassificationType.DEFI:
            final_classification = ClassificationType.BUY
            reasoning_suffix = " (DEFI mapped to BUY - protocol interaction)"
        
        # Check if this should be a moderate confidence classification
        if (final_classification in [ClassificationType.BUY, ClassificationType.SELL] and 
            phase_result.confidence < CLASSIFICATION_THRESHOLDS['moderate_signal_threshold']):
            final_classification = (ClassificationType.MODERATE_BUY if final_classification == ClassificationType.BUY 
                                  else ClassificationType.MODERATE_SELL)
            reasoning_suffix += " (Moderate confidence)"
        
        result.classification = final_classification
        result.confidence = phase_result.confidence
        result.final_whale_score = self._calculate_whale_score(result.whale_signals)
        result.master_classifier_reasoning = f"{reason}: {phase_result.evidence[0] if phase_result.evidence else 'Early classification'}{reasoning_suffix}"
        result.phases_completed = len(result.phase_results)
        result.cost_optimized = True
        
        tx_logger.master_classification(final_classification.value, phase_result.confidence, reason)
        return result
    
    def _get_max_confidence(self, phase_results: Dict[str, PhaseResult]) -> float:
        """Get maximum confidence from all phase results."""
        return max([result.confidence for result in phase_results.values()], default=0.0)
    
    def _analyze_blockchain_specific(self, tx_hash: str, blockchain: str) -> PhaseResult:
        """Analyze blockchain-specific transaction patterns."""
        try:
            if blockchain in ['ethereum', 'polygon'] and blockchain in self.evm_parsers:
                return self._analyze_evm_transaction(tx_hash, blockchain)
            elif blockchain == 'solana' and self.solana_parser:
                return self._analyze_solana_transaction(tx_hash)
            else:
                return create_empty_phase_result(
                    f"Unsupported blockchain: {blockchain}",
                    AnalysisPhase.BLOCKCHAIN_SPECIFIC.value
                )
                
        except Exception as e:
            self.logger.error(f"Blockchain-specific analysis failed: {e}")
            return create_empty_phase_result(
                f"Blockchain analysis error: {str(e)}",
                AnalysisPhase.BLOCKCHAIN_SPECIFIC.value
            )
    
    def _analyze_evm_transaction(self, tx_hash: str, blockchain: str) -> PhaseResult:
        """Analyze EVM-based transaction with robust fallback handling."""
        try:
            parser = self.evm_parsers.get(blockchain)
            if not parser:
                return create_empty_phase_result(
                    f"No parser available for {blockchain}",
                    AnalysisPhase.BLOCKCHAIN_SPECIFIC.value
                )
            
            # Parse transaction logs and data (now has internal resilience)
            parse_result = parser.analyze_dex_swap(tx_hash)
            
            if not parse_result:
                return create_empty_phase_result(
                    "No DEX activity detected in blockchain analysis",
                    AnalysisPhase.BLOCKCHAIN_SPECIFIC.value
                )
            
            # Convert parse result to PhaseResult
            classification_str = parse_result.get('direction', parse_result.get('classification', 'TRANSFER'))
            
            # Handle direction vs classification mapping
            if classification_str.upper() in ['BUY', 'SELL', 'TRANSFER']:
                classification = ClassificationType(classification_str.upper())
            else:
                classification = ClassificationType.TRANSFER
            
            confidence = parse_result.get('confidence', 0.0)
            evidence = parse_result.get('evidence', [])
            whale_signals = parse_result.get('whale_signals', [])
            
            # Add analysis method to evidence if available
            analysis_method = parse_result.get('analysis_method', 'unknown')
            if analysis_method != 'unknown':
                evidence.append(f"Blockchain analysis method: {analysis_method}")
            
            self.logger.info(f"✅ Blockchain analysis complete for {tx_hash}: {classification.value} (confidence: {confidence:.2f}, method: {analysis_method})")
            
            return PhaseResult(
                classification=classification,
                confidence=confidence,
                evidence=evidence,
                whale_signals=whale_signals,
                phase=AnalysisPhase.BLOCKCHAIN_SPECIFIC.value,
                raw_data=parse_result
            )
            
        except Exception as e:
            self.logger.error(f"EVM transaction analysis failed: {e}")
            return create_empty_phase_result(
                f"EVM analysis error: {str(e)}",
                AnalysisPhase.BLOCKCHAIN_SPECIFIC.value
            )
    
    def _analyze_solana_transaction(self, tx_hash: str) -> PhaseResult:
        """Analyze Solana transaction."""
        try:
            if not self.solana_parser:
                return create_empty_phase_result(
                    "Solana parser not available",
                    AnalysisPhase.BLOCKCHAIN_SPECIFIC.value
                )
            
            # Parse Solana transaction
            parse_result = self.solana_parser.parse_transaction_comprehensive(tx_hash)
            
            if not parse_result:
                return create_empty_phase_result(
                    "No Solana parsing result",
                    AnalysisPhase.BLOCKCHAIN_SPECIFIC.value
                )
            
            # Convert parse result to PhaseResult
            classification = ClassificationType(parse_result.get('classification', 'TRANSFER'))
            confidence = parse_result.get('confidence', 0.0)
            evidence = parse_result.get('evidence', [])
            whale_signals = parse_result.get('whale_signals', [])
            
            return PhaseResult(
                classification=classification,
                confidence=confidence,
                evidence=evidence,
                whale_signals=whale_signals,
                phase=AnalysisPhase.BLOCKCHAIN_SPECIFIC.value,
                raw_data=parse_result
            )
            
        except Exception as e:
            self.logger.error(f"Solana transaction analysis failed: {e}")
            return create_empty_phase_result(
                f"Solana analysis error: {str(e)}",
                AnalysisPhase.BLOCKCHAIN_SPECIFIC.value
            )
    
    def _analyze_wallet_behavior(self, from_addr: str, to_addr: str, transaction: Dict[str, Any]) -> PhaseResult:
        """Analyze wallet behavior patterns."""
        try:
            # This would integrate with the existing Supabase whale analysis
            # For now, implement basic wallet behavior analysis
            
            evidence = []
            whale_signals = []
            confidence = 0.0
            classification = ClassificationType.TRANSFER
            
            # Check for known whale addresses
            if self.supabase_client:
                blockchain = transaction.get('blockchain', 'ethereum')
                whale_analysis = self._check_whale_addresses(from_addr, to_addr, blockchain)
                
                if whale_analysis:
                    classification, confidence, evidence, whale_signals = whale_analysis
            
            return PhaseResult(
                classification=classification,
                confidence=confidence,
                evidence=evidence,
                whale_signals=whale_signals,
                phase=AnalysisPhase.WALLET_BEHAVIOR.value,
                raw_data={"wallet_analysis": True}
            )
            
        except Exception as e:
            self.logger.error(f"Wallet behavior analysis failed: {e}")
            return create_empty_phase_result(
                f"Wallet analysis error: {str(e)}",
                AnalysisPhase.WALLET_BEHAVIOR.value
            )
    
    def _check_whale_addresses(self, from_addr: str, to_addr: str, blockchain: str) -> Optional[Tuple[ClassificationType, float, List[str], List[str]]]:
        """
        ENHANCED: Check addresses against comprehensive whale database with full intelligence.
        
        NOW LEVERAGES COMPLETE SUPABASE WHALE INTELLIGENCE:
        - analysis_tags JSONB with whale categorization and metadata
        - balance_usd for accurate whale tier classification
        - entity_name, signal_potential for institutional detection
        - detection_method for confidence assessment
        """
        try:
            addresses_to_check = [addr for addr in [from_addr, to_addr] if addr]
            if not addresses_to_check:
                return None
            
            # COMPREHENSIVE WHALE QUERY: Extract ALL whale intelligence
            response = self.supabase_client.table('addresses')\
                .select("""
                    address, label, address_type, confidence,
                    entity_name, signal_potential, balance_usd, balance_native,
                    detection_method, analysis_tags, last_seen_tx
                """)\
                .in_('address', addresses_to_check)\
                .eq('blockchain', blockchain)\
                .execute()
            
            if not response.data:
                return None
            
            evidence = []
            whale_signals = []
            classification = ClassificationType.TRANSFER
            max_confidence = 0.0
            
            # Process each address with ENHANCED whale intelligence
            for row in response.data:
                address = row.get('address', '').lower()
                label = row.get('label', '')
                address_type = row.get('address_type', '').lower()
                entity_name = row.get('entity_name', '')
                balance_usd = float(row.get('balance_usd', 0) or 0)
                balance_native = float(row.get('balance_native', 0) or 0)
                signal_potential = row.get('signal_potential', '')
                detection_method = row.get('detection_method', '')
                analysis_tags = row.get('analysis_tags') or {}
                base_confidence = float(row.get('confidence', 0.5))
                
                # COMPREHENSIVE WHALE DETECTION using ALL data sources
                whale_result = self._comprehensive_whale_detection(
                    address_type, label, entity_name, balance_usd, balance_native,
                    signal_potential, detection_method, analysis_tags
                )
                
                if whale_result:
                    whale_classification, whale_confidence, whale_tier, whale_evidence = whale_result
                    
                    # Enhanced confidence calculation with stacking
                    enhanced_confidence = min(0.95, base_confidence + whale_confidence)
                    
                    # Determine transaction direction for whale flow
                    is_outgoing = (address == from_addr)
                    
                    # Apply whale-flow logic with enhanced intelligence
                    if whale_tier in ["MEGA_WHALE", "ULTRA_WHALE"]:
                        # High-tier whales get stronger classification signals
                        if is_outgoing:
                            classification = ClassificationType.SELL
                            direction_evidence = f"🐋 {whale_tier} SELL: {entity_name or 'Whale'} → Market"
                            enhanced_confidence = min(0.98, enhanced_confidence + 0.05)
                        else:
                            classification = ClassificationType.BUY
                            direction_evidence = f"🐋 Market → {whale_tier} BUY: {entity_name or 'Whale'}"
                            enhanced_confidence = min(0.98, enhanced_confidence + 0.05)
                    else:
                        # Regular whales
                        if is_outgoing:
                            classification = ClassificationType.SELL
                            direction_evidence = f"🐋 Whale SELL: {entity_name or 'Whale'} → Market"
                        else:
                            classification = ClassificationType.BUY
                            direction_evidence = f"🐋 Market → Whale BUY: {entity_name or 'Whale'}"
                    
                    # Compile comprehensive evidence
                    evidence.append(direction_evidence)
                    evidence.extend(whale_evidence)
                    
                    # Generate whale signals with enhanced categorization
                    whale_signals.extend([
                        f"{whale_tier} detected",
                        f"Balance: ${balance_usd:,.0f}" if balance_usd > 0 else "High-value address",
                        f"Signal potential: {signal_potential}" if signal_potential else "Whale activity"
                    ])
                    
                    # Track maximum confidence
                    if enhanced_confidence > max_confidence:
                        max_confidence = enhanced_confidence
            
            # Return result if any whale detected
            if max_confidence > 0.0:
                return classification, max_confidence, evidence, whale_signals
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Enhanced whale check failed: {e}")
            return None

    def _comprehensive_whale_detection(self, address_type: str, label: str, entity_name: str,
                                     balance_usd: float, balance_native: float, signal_potential: str,
                                     detection_method: str, analysis_tags: Dict) -> Optional[Tuple[ClassificationType, float, str, List[str]]]:
        """
        COMPREHENSIVE whale detection using ALL available Supabase intelligence.
        
        Returns: (classification, confidence_boost, whale_tier, evidence_list)
        """
        evidence = []
        confidence_boost = 0.0
        whale_tier = "UNKNOWN"
        
        # 1. BALANCE-BASED WHALE CLASSIFICATION (Primary indicator)
        if balance_usd >= 100_000_000:  # $100M+
            whale_tier = "ULTRA_WHALE"
            confidence_boost += 0.40
            evidence.append(f"Ultra whale: ${balance_usd:,.0f} balance")
        elif balance_usd >= 10_000_000:  # $10M+
            whale_tier = "MEGA_WHALE"
            confidence_boost += 0.35
            evidence.append(f"Mega whale: ${balance_usd:,.0f} balance")
        elif balance_usd >= 1_000_000:  # $1M+
            whale_tier = "WHALE"
            confidence_boost += 0.30
            evidence.append(f"Whale: ${balance_usd:,.0f} balance")
        elif balance_usd >= 100_000:  # $100K+
            whale_tier = "MINI_WHALE"
            confidence_boost += 0.25
            evidence.append(f"Mini whale: ${balance_usd:,.0f} balance")
        
        # 2. ADDRESS TYPE WHALE DETECTION
        if any(term in address_type for term in ['whale', 'high_value', 'institutional']):
            confidence_boost += 0.20
            evidence.append(f"Whale address type: {address_type}")
            if whale_tier == "UNKNOWN":
                whale_tier = "CLASSIFIED_WHALE"
        
        # 3. ENTITY NAME INSTITUTIONAL DETECTION
        if entity_name:
            institutional_indicators = [
                'trading', 'capital', 'fund', 'institutional', 'whale',
                'alameda', 'jump', 'wintermute', 'market maker'
            ]
            if any(indicator in entity_name.lower() for indicator in institutional_indicators):
                confidence_boost += 0.25
                evidence.append(f"Institutional entity: {entity_name}")
                if whale_tier == "UNKNOWN":
                    whale_tier = "INSTITUTIONAL_WHALE"
        
        # 4. ANALYSIS_TAGS WHALE INTELLIGENCE
        if isinstance(analysis_tags, dict):
            # Whale discovery tags
            if 'whale_discovery' in analysis_tags:
                whale_data = analysis_tags['whale_discovery']
                if isinstance(whale_data, dict):
                    discovery_balance = whale_data.get('balance_usd', 0)
                    if discovery_balance >= 1_000_000:
                        confidence_boost += 0.30
                        evidence.append(f"Whale discovery: ${discovery_balance:,.0f}")
            
            # Tags analysis
            tags = analysis_tags.get('tags', [])
            if isinstance(tags, list):
                whale_tags = [tag for tag in tags if any(term in str(tag).lower() for term in ['whale', 'mega', 'ultra', 'high_value'])]
                if whale_tags:
                    confidence_boost += 0.15
                    evidence.append(f"Whale tags: {', '.join(whale_tags)}")
                    if whale_tier == "UNKNOWN":
                        whale_tier = "TAGGED_WHALE"
        
        # 5. SIGNAL POTENTIAL ANALYSIS
        if signal_potential:
            if 'high' in signal_potential.lower():
                confidence_boost += 0.15
                evidence.append(f"High signal potential: {signal_potential}")
            elif any(term in signal_potential.lower() for term in ['whale', 'institutional', 'trading']):
                confidence_boost += 0.12
                evidence.append(f"Whale signal potential: {signal_potential}")
        
        # 6. DETECTION METHOD CONFIDENCE
        if detection_method:
            method_confidence = {
                'whale_intelligence_engine': 0.20,
                'bigquery_whale_detection': 0.18,
                'covalent_whale_discovery': 0.15,
                'manual_whale_classification': 0.25
            }
            for method, boost in method_confidence.items():
                if method in detection_method.lower():
                    confidence_boost += boost
                    evidence.append(f"Detection method: {detection_method}")
                    break
        
        # 7. NATIVE BALANCE FALLBACK (if USD balance not available)
        if balance_usd == 0 and balance_native > 0:
            # Rough estimation based on native balance (chain-specific)
            if balance_native >= 1000:  # 1000+ ETH/SOL etc
                confidence_boost += 0.25
                evidence.append(f"High native balance: {balance_native:.2f}")
                if whale_tier == "UNKNOWN":
                    whale_tier = "NATIVE_WHALE"
        
        # Return result if whale detected (minimum threshold)
        if confidence_boost >= 0.20:  # Lowered threshold for better detection
            return ClassificationType.TRANSFER, confidence_boost, whale_tier, evidence
        
        return None
    
    def _analyze_bigquery_whale(self, from_addr: str, to_addr: str, blockchain: str) -> PhaseResult:
        """
        ENHANCED: BigQuery Mega Whale Detection with comprehensive integration.
        
        Leverages historical blockchain data to identify whale patterns:
        - Transaction volume analysis (high-volume traders)
        - Activity patterns (consistent vs sporadic activity)  
        - Counterparty diversity (institutional vs retail behavior)
        - Historical whale classification based on empirical data
        
        Args:
            from_addr: Transaction sender address
            to_addr: Transaction receiver address
            blockchain: Blockchain network
            
        Returns:
            PhaseResult with comprehensive whale analysis
        """
        if not self.bigquery_analyzer:
            return PhaseResult(
                classification=ClassificationType.TRANSFER,
                confidence=0.0,
                evidence=["BigQuery analyzer not available"],
                whale_signals=[],
                phase=AnalysisPhase.BIGQUERY_WHALE.value,
                raw_data={"status": "unavailable"}
            )
        
        try:
            self.logger.debug(f"Analyzing BigQuery whale patterns for {from_addr} -> {to_addr}")
            
            # Analyze both addresses with comprehensive BigQuery intelligence
            from_whale_data = self._analyze_address_bigquery_patterns(from_addr, blockchain)
            to_whale_data = self._analyze_address_bigquery_patterns(to_addr, blockchain)
            
            evidence = []
            whale_signals = []
            classification = ClassificationType.TRANSFER
            max_confidence = 0.0
            raw_data = {
                "from_whale_analysis": from_whale_data,
                "to_whale_analysis": to_whale_data,
                "bigquery_status": "analyzed"
            }
            
            # Process FROM address whale analysis
            if from_whale_data and from_whale_data.get('is_whale', False):
                whale_score = from_whale_data.get('whale_score', 0.0)
                whale_tier = from_whale_data.get('whale_tier', 'UNKNOWN')
                
                # Enhanced confidence based on whale score
                from_confidence = min(0.88, 0.50 + (whale_score / 100) * 0.38)
                
                classification = ClassificationType.SELL  # FROM whale = selling
                evidence.append(f"BigQuery FROM whale: {whale_tier} (score: {whale_score:.1f})")
                whale_signals.append(f"Historical whale pattern detected in sender")
                
                # Add detailed evidence from BigQuery
                if from_whale_data.get('total_volume_usd'):
                    evidence.append(f"Historical volume: ${from_whale_data['total_volume_usd']:,.0f}")
                if from_whale_data.get('transaction_count'):
                    evidence.append(f"Transaction history: {from_whale_data['transaction_count']} txns")
                
                max_confidence = from_confidence
            
            # Process TO address whale analysis
            if to_whale_data and to_whale_data.get('is_whale', False):
                whale_score = to_whale_data.get('whale_score', 0.0)
                whale_tier = to_whale_data.get('whale_tier', 'UNKNOWN')
                
                # Enhanced confidence based on whale score
                to_confidence = min(0.88, 0.50 + (whale_score / 100) * 0.38)
                
                # If no FROM whale, or TO whale has higher confidence
                if max_confidence == 0.0 or to_confidence > max_confidence:
                    classification = ClassificationType.BUY  # TO whale = buying
                    evidence.append(f"BigQuery TO whale: {whale_tier} (score: {whale_score:.1f})")
                    whale_signals.append(f"Historical whale pattern detected in recipient")
                    
                    # Add detailed evidence from BigQuery
                    if to_whale_data.get('total_volume_usd'):
                        evidence.append(f"Historical volume: ${to_whale_data['total_volume_usd']:,.0f}")
                    if to_whale_data.get('transaction_count'):
                        evidence.append(f"Transaction history: {to_whale_data['transaction_count']} txns")
                    
                    max_confidence = to_confidence
                else:
                    # Both are whales - add TO whale as supplementary evidence
                    evidence.append(f"BigQuery TO whale: {whale_tier} (score: {whale_score:.1f})")
                    whale_signals.append(f"Whale-to-whale transaction detected")
                    # Boost confidence for whale-to-whale
                    max_confidence = min(0.92, max_confidence + 0.08)
            
            # Add comprehensive whale pattern analysis
            combined_patterns = self._analyze_combined_whale_patterns(from_whale_data, to_whale_data)
            if combined_patterns:
                evidence.extend(combined_patterns['evidence'])
                whale_signals.extend(combined_patterns['signals'])
                max_confidence = min(0.95, max_confidence + combined_patterns['confidence_boost'])
                raw_data['combined_analysis'] = combined_patterns
            
            return PhaseResult(
                classification=classification,
                confidence=max_confidence,
                evidence=evidence,
                whale_signals=whale_signals,
                phase=AnalysisPhase.BIGQUERY_WHALE.value,
                raw_data=raw_data
            )
            
        except Exception as e:
            self.logger.warning(f"BigQuery whale analysis failed: {e}")
            return PhaseResult(
                classification=ClassificationType.TRANSFER,
                confidence=0.0,
                evidence=[f"BigQuery analysis error: {str(e)}"],
                whale_signals=[],
                phase=AnalysisPhase.BIGQUERY_WHALE.value,
                raw_data={"status": "error", "error": str(e)}
            )

    def _analyze_address_bigquery_patterns(self, address: str, blockchain: str) -> Optional[Dict[str, Any]]:
        """
        Analyze individual address using BigQuery with comprehensive whale detection.
        
        Returns detailed whale analysis or None if not a whale.
        """
        try:
            # Use the bigquery_analyzer to get comprehensive address analysis
            analysis = self.bigquery_analyzer.analyze_address_whale_patterns(address)
            
            if not analysis:
                return None
            
            # Enhanced whale scoring based on multiple factors
            whale_score = 0.0
            whale_indicators = []
            
            # Volume-based scoring
            total_volume = analysis.get('total_volume_usd', 0)
            if total_volume >= 100_000_000:  # $100M+
                whale_score += 40
                whale_indicators.append(f"Ultra-high volume: ${total_volume:,.0f}")
            elif total_volume >= 10_000_000:  # $10M+
                whale_score += 30
                whale_indicators.append(f"High volume: ${total_volume:,.0f}")
            elif total_volume >= 1_000_000:  # $1M+
                whale_score += 20
                whale_indicators.append(f"Significant volume: ${total_volume:,.0f}")
            
            # Transaction frequency scoring
            tx_count = analysis.get('transaction_count', 0)
            if tx_count >= 10000:
                whale_score += 25
                whale_indicators.append(f"Very active: {tx_count} transactions")
            elif tx_count >= 1000:
                whale_score += 15
                whale_indicators.append(f"Active trader: {tx_count} transactions")
            elif tx_count >= 100:
                whale_score += 10
                whale_indicators.append(f"Regular activity: {tx_count} transactions")
            
            # Counterparty diversity scoring
            unique_counterparties = analysis.get('unique_counterparties', 0)
            if unique_counterparties >= 1000:
                whale_score += 20
                whale_indicators.append(f"Institutional reach: {unique_counterparties} counterparties")
            elif unique_counterparties >= 100:
                whale_score += 15
                whale_indicators.append(f"Broad network: {unique_counterparties} counterparties")
            
            # Time span scoring
            days_active = analysis.get('days_active', 0)
            if days_active >= 365:
                whale_score += 15
                whale_indicators.append(f"Long-term participant: {days_active} days")
            elif days_active >= 90:
                whale_score += 10
                whale_indicators.append(f"Established participant: {days_active} days")
            
            # Determine whale tier
            if whale_score >= 80:
                whale_tier = "MEGA_WHALE"
            elif whale_score >= 60:
                whale_tier = "WHALE"
            elif whale_score >= 40:
                whale_tier = "MINI_WHALE"
            else:
                whale_tier = "HIGH_ACTIVITY"
            
            # Enhanced result compilation
            result = {
                **analysis,  # Include original BigQuery data
                'is_whale': whale_score >= 40,  # Lowered threshold for better detection
                'whale_score': whale_score,
                'whale_tier': whale_tier,
                'whale_indicators': whale_indicators,
                'confidence_score': min(0.85, whale_score / 100)
            }
            
            return result
            
        except Exception as e:
            self.logger.warning(f"BigQuery address analysis failed for {address}: {e}")
            return None

    def _analyze_combined_whale_patterns(self, from_data: Optional[Dict], to_data: Optional[Dict]) -> Optional[Dict[str, Any]]:
        """
        Analyze combined whale patterns for enhanced intelligence.
        
        Returns additional evidence, signals, and confidence boost.
        """
        if not from_data and not to_data:
            return None
        
        evidence = []
        signals = []
        confidence_boost = 0.0
        
        # Whale-to-whale transaction analysis
        if (from_data and from_data.get('is_whale', False) and 
            to_data and to_data.get('is_whale', False)):
            
            evidence.append("Whale-to-whale transaction detected")
            signals.append("Institutional-level activity")
            confidence_boost += 0.10
            
            # Volume correlation analysis
            from_volume = from_data.get('total_volume_usd', 0)
            to_volume = to_data.get('total_volume_usd', 0)
            
            if from_volume >= 10_000_000 and to_volume >= 10_000_000:
                evidence.append("Major whale interaction")
                confidence_boost += 0.05
        
        # Activity pattern analysis
        if from_data or to_data:
            whale_data = from_data or to_data
            avg_tx_size = whale_data.get('avg_transaction_size_usd', 0)
            
            if avg_tx_size >= 100_000:
                evidence.append(f"Large average transaction size: ${avg_tx_size:,.0f}")
                signals.append("Institutional transaction patterns")
                confidence_boost += 0.08
        
        if confidence_boost > 0.0:
            return {
                'evidence': evidence,
                'signals': signals,
                'confidence_boost': confidence_boost
            }
        
        return None
    
    def _check_early_exit_conditions(self, phase_results: Dict[str, PhaseResult], tx_logger) -> Optional[Tuple[ClassificationType, float, str]]:
        """Check if early exit conditions are met based on current results."""
        try:
            # Calculate combined evidence
            buy_evidence = 0.0
            sell_evidence = 0.0
            strong_phases = 0
            
            for phase_name, result in phase_results.items():
                if result.confidence > 0.6:
                    strong_phases += 1
                    
                    if result.classification == ClassificationType.BUY:
                        buy_evidence += result.confidence
                    elif result.classification == ClassificationType.SELL:
                        sell_evidence += result.confidence
            
            max_combined_evidence = max(buy_evidence, sell_evidence)
            
            # Early exit if we have strong evidence from multiple phases
            if max_combined_evidence >= 0.80 and strong_phases >= 2:
                final_classification = ClassificationType.BUY if buy_evidence > sell_evidence else ClassificationType.SELL
                final_confidence = min(0.95, max_combined_evidence)
                
                return (
                    final_classification,
                    final_confidence,
                    f"Combined evidence: {final_classification.value} from {strong_phases} phases"
                )
            
            return None
            
        except Exception as e:
            tx_logger.error(f"Early exit check failed: {e}")
            return None
    
    def _finalize_cost_optimized_exit(self, result: IntelligenceResult, early_exit_result: Tuple[ClassificationType, float, str], tx_logger) -> IntelligenceResult:
        """Finalize result for cost-optimized early exit."""
        classification, confidence, reasoning = early_exit_result
        
        result.classification = classification
        result.confidence = confidence
        result.final_whale_score = self._calculate_whale_score(result.whale_signals)
        result.master_classifier_reasoning = f"Cost-Optimized Early Exit: {reasoning}"
        result.phases_completed = len(result.phase_results)
        result.cost_optimized = True
        
        tx_logger.master_classification(classification.value, confidence, f"Cost-Optimized Early Exit: {reasoning}")
        return result
    
    def _execute_enhanced_api_phases(self, result: IntelligenceResult, from_addr: str, to_addr: str, blockchain: str, tx_hash: str, tx_logger) -> None:
        """Execute enhanced API phases when confidence is still low."""
        try:
            # Phase 6: Moralis enrichment
            if self.api_integrations:
                tx_logger.debug("Executing Phase 6: Moralis Enrichment")
                phase6_result = self._analyze_moralis_enrichment(from_addr, to_addr, blockchain)
                result.phase_results[AnalysisPhase.MORALIS_ENRICHMENT.value] = phase6_result
                
                # Phase 7: Zerion portfolio analysis
                tx_logger.debug("Executing Phase 7: Zerion Portfolio Analysis")
                phase7_result = self._analyze_zerion_portfolio(from_addr, to_addr, tx_hash)
                result.phase_results[AnalysisPhase.ZERION_PORTFOLIO.value] = phase7_result
            
        except Exception as e:
            self.logger.warning(f"Enhanced API phases failed: {e}")
    
    def _analyze_moralis_enrichment(self, from_addr: str, to_addr: str, blockchain: str) -> PhaseResult:
        """Analyze using Moralis API for address enrichment."""
        try:
            # Import and use the enhanced API integrations
            from utils.enhanced_api_integrations import EnhancedAPIIntegrationManager
            
            api_manager = EnhancedAPIIntegrationManager()
            
            # Analyze both addresses
            evidence = []
            whale_signals = []
            confidence = 0.0
            
            for addr_type, addr in [("from", from_addr), ("to", to_addr)]:
                try:
                    # Try to get Moralis wallet history
                    moralis_chain = "eth" if blockchain == "ethereum" else "polygon"
                    response = api_manager.get_moralis_wallet_history(addr, moralis_chain)
                    
                    if response.success and response.data:
                        # Extract useful information
                        history = response.data
                        if isinstance(history, dict):
                            tx_count = history.get('total', 0)
                            if tx_count > 1000:
                                whale_signals.append(f"{addr_type.title()} address: High activity ({tx_count:,} transactions)")
                                confidence += 0.15
                            
                            evidence.append(f"Moralis: {addr_type} address has {tx_count:,} transactions")
                    
                    elif hasattr(response, 'status_code') and response.status_code == 401:
                        # Quota exceeded - still provide basic analysis
                        evidence.append(f"Moralis: {addr_type} address analysis (quota limited)")
                        
                except Exception as e:
                    self.logger.debug(f"Moralis API error for {addr}: {e}")
                    evidence.append(f"Moralis: {addr_type} address checked (API limited)")
            
            # Determine classification based on available data
            classification = ClassificationType.TRANSFER
            if confidence > 0.10:
                classification = ClassificationType.SELL if confidence > 0.20 else ClassificationType.BUY
            
            return PhaseResult(
                classification=classification,
                confidence=min(confidence, 0.35),  # Cap at moderate confidence
                evidence=evidence,
                whale_signals=whale_signals,
                phase=AnalysisPhase.MORALIS_ENRICHMENT.value,
                raw_data={"moralis_analysis": "completed"}
            )
            
        except Exception as e:
            return PhaseResult(
                classification=ClassificationType.TRANSFER,
                confidence=0.0,
                evidence=[f"Moralis analysis error: {str(e)}"],
                whale_signals=[],
                phase=AnalysisPhase.MORALIS_ENRICHMENT.value,
                raw_data={}
            )

    def _analyze_zerion_portfolio(self, from_addr: str, to_addr: str, tx_hash: str) -> PhaseResult:
        """Analyze using Zerion for portfolio insights."""
        try:
            from utils.enhanced_api_integrations import EnhancedAPIIntegrationManager
            
            api_manager = EnhancedAPIIntegrationManager()
            
            evidence = []
            whale_signals = []
            confidence = 0.0
            
            for addr_type, addr in [("from", from_addr), ("to", to_addr)]:
                try:
                    # Get Zerion portfolio data
                    response = api_manager.get_zerion_portfolio(addr)
                    
                    if response.success and response.data:
                        portfolio = response.data
                        if isinstance(portfolio, dict):
                            total_value = portfolio.get('total_value_usd', 0)
                            position_count = portfolio.get('position_count', 0)
                            
                            if total_value > 100_000:  # $100K+ portfolio
                                whale_signals.append(f"{addr_type.title()} address: Large portfolio (${total_value:,.0f})")
                                confidence += 0.20
                            elif total_value > 10_000:  # $10K+ portfolio
                                whale_signals.append(f"{addr_type.title()} address: Medium portfolio (${total_value:,.0f})")
                                confidence += 0.10
                            
                            evidence.append(f"Zerion: {addr_type} portfolio ${total_value:,.0f} ({position_count} positions)")
                    
                    elif hasattr(response, 'status_code') and response.status_code in [401, 429]:
                        # Authentication issue or rate limit - provide basic signal
                        evidence.append(f"Zerion: {addr_type} portfolio analysis (API limited)")
                        
                except Exception as e:
                    self.logger.debug(f"Zerion API error for {addr}: {e}")
                    evidence.append(f"Zerion: {addr_type} portfolio checked (API limited)")
            
            # Determine classification based on portfolio analysis
            classification = ClassificationType.TRANSFER
            if confidence > 0.15:
                classification = ClassificationType.BUY  # Large portfolios tend to be accumulating
            
            return PhaseResult(
                classification=classification,
                confidence=min(confidence, 0.30),  # Cap at moderate confidence
                evidence=evidence,
                whale_signals=whale_signals,
                phase=AnalysisPhase.ZERION_PORTFOLIO.value,
                raw_data={"zerion_analysis": "completed"}
            )
            
        except Exception as e:
            return PhaseResult(
                classification=ClassificationType.TRANSFER,
                confidence=0.0,
                evidence=[f"Zerion analysis error: {str(e)}"],
                whale_signals=[],
                phase=AnalysisPhase.ZERION_PORTFOLIO.value,
                raw_data={}
            )
    
    def _determine_master_classification(self, phase_results: Dict[str, PhaseResult], behavioral_analysis: BehavioralAnalysis) -> Tuple[ClassificationType, float, str]:
        """
        Enhanced master classification with confidence stacking and USD value weighting.
        
        Key improvements:
        - Confidence stacking: Multiple medium signals → High confidence
        - USD value weighting for high-value transactions
        - Moderate confidence classifications (60-80%)
        - Smart aggregation that doesn't just take max confidence
        """
        try:
            # Priority-based classification with enhanced thresholds
            priority_phases = [
                AnalysisPhase.CEX_CLASSIFICATION.value,
                AnalysisPhase.DEX_PROTOCOL.value
            ]
            
            # Extract USD value for weighting
            usd_value = 0
            for phase_result in phase_results.values():
                if hasattr(phase_result, 'raw_data') and phase_result.raw_data:
                    usd_value = max(usd_value, phase_result.raw_data.get('value_usd', 0))
            
            # USD value confidence boost
            usd_boost = 0
            if usd_value >= CLASSIFICATION_THRESHOLDS['usd_value_boost_threshold']:
                usd_boost = CLASSIFICATION_THRESHOLDS['usd_value_boost_amount']
            
            # Check priority phases first with reduced thresholds
            for phase_name in priority_phases:
                if phase_name in phase_results:
                    result = phase_results[phase_name]
                    adjusted_confidence = result.confidence + usd_boost
                    
                    if result.classification in [ClassificationType.BUY, ClassificationType.SELL] and adjusted_confidence >= 0.60:  # Reduced from 0.70
                        # Determine if this should be moderate or high confidence
                        if adjusted_confidence >= CLASSIFICATION_THRESHOLDS['moderate_signal_threshold']:
                            final_classification = result.classification
                        else:
                            # Create moderate confidence classification
                            final_classification = (ClassificationType.MODERATE_BUY if result.classification == ClassificationType.BUY 
                                                  else ClassificationType.MODERATE_SELL)
                        
                        return final_classification, adjusted_confidence, f"Priority phase: {phase_name} (USD boost: +{usd_boost:.2f})"
            
            # Enhanced weighted aggregation with confidence stacking
            return self._execute_enhanced_weighted_aggregation(phase_results, behavioral_analysis, usd_boost, usd_value)
            
        except Exception as e:
            self.logger.error(f"Master classification failed: {e}")
            return ClassificationType.TRANSFER, 0.0, f"Classification error: {str(e)}"
    
    def _execute_enhanced_weighted_aggregation(self, phase_results: Dict[str, PhaseResult], 
                                             behavioral_analysis: BehavioralAnalysis, 
                                             usd_boost: float, usd_value: float) -> Tuple[ClassificationType, float, str]:
        """
        Enhanced weighted aggregation with confidence stacking.
        
        Key innovations:
        - Multiple medium-confidence signals combine multiplicatively
        - USD value and behavioral intelligence factored in
        - Moderate vs high confidence determination
        """
        # Enhanced phase weights (cost-optimized)
        phase_weights = {
            AnalysisPhase.CEX_CLASSIFICATION.value: 0.65,       # Highest weight
            AnalysisPhase.DEX_PROTOCOL.value: 0.60,            # High weight  
            AnalysisPhase.MARKET_DATA_INTELLIGENCE.value: 0.45,  # NEW: Market context weight
            AnalysisPhase.BLOCKCHAIN_SPECIFIC.value: 0.50,     # Medium weight
            AnalysisPhase.WALLET_BEHAVIOR.value: 0.45,         # Medium weight
            AnalysisPhase.BIGQUERY_WHALE.value: 0.70,          # Enhanced weight (comprehensive whale data)
            AnalysisPhase.MORALIS_ENRICHMENT.value: 0.20,      # Low weight
            AnalysisPhase.ZERION_PORTFOLIO.value: 0.30         # Medium-low weight
        }
        
        # Collect signals for confidence stacking
        buy_signals = []
        sell_signals = []
        participating_phases = []
        
        for phase_name, result in phase_results.items():
            if result.confidence <= 0:
                continue
                
            weight = phase_weights.get(phase_name, 0.1)
            adjusted_confidence = result.confidence + usd_boost
            
            # Apply behavioral boost
            if behavioral_analysis and behavioral_analysis.total_confidence_boost > 0:
                adjusted_confidence += behavioral_analysis.total_confidence_boost
            
            participating_phases.append(f"{phase_name}: {result.classification.value} ({adjusted_confidence:.2f})")
            
            # Confidence stacking: collect signals by type
            if result.classification in [ClassificationType.BUY, ClassificationType.MODERATE_BUY]:
                buy_signals.append((adjusted_confidence, weight))
            elif result.classification in [ClassificationType.SELL, ClassificationType.MODERATE_SELL]:
                sell_signals.append((adjusted_confidence, weight))
            elif result.classification in [ClassificationType.STAKING, ClassificationType.DEFI]:
                # Map STAKING and DEFI to BUY
                buy_signals.append((adjusted_confidence, weight))
        
        # Advanced confidence stacking calculation
        buy_confidence = self._calculate_stacked_confidence(buy_signals)
        sell_confidence = self._calculate_stacked_confidence(sell_signals)
        
        # Determine final classification
        if buy_confidence > sell_confidence and buy_confidence >= CLASSIFICATION_THRESHOLDS['aggregation_threshold']:
            final_classification = ClassificationType.BUY
            final_confidence = buy_confidence
            signal_type = "BUY"
        elif sell_confidence > buy_confidence and sell_confidence >= CLASSIFICATION_THRESHOLDS['aggregation_threshold']:
            final_classification = ClassificationType.SELL
            final_confidence = sell_confidence
            signal_type = "SELL"
        else:
            # Apply smart transfer reclassification for high-value transactions
            if usd_value >= 50000:  # High-value transactions deserve more analysis
                return self._smart_high_value_reclassification(usd_value, phase_results, participating_phases)
            
            return ClassificationType.TRANSFER, max(buy_confidence, sell_confidence, 0.3), "Insufficient confidence for directional signal"
        
        # Determine moderate vs high confidence
        if final_confidence >= CLASSIFICATION_THRESHOLDS['moderate_signal_threshold']:
            # High confidence signal
            pass  # Keep as-is
        elif final_confidence >= CLASSIFICATION_THRESHOLDS['medium_confidence']:
            # Moderate confidence signal
            final_classification = (ClassificationType.MODERATE_BUY if final_classification == ClassificationType.BUY 
                                  else ClassificationType.MODERATE_SELL)
        
        # Cap confidence and create reasoning
        final_confidence = min(0.95, final_confidence)
        reasoning = f"Stacked {signal_type} confidence from {len(participating_phases)} phases (USD: ${usd_value:,.0f})"
        
        return final_classification, final_confidence, reasoning
    
    def _calculate_stacked_confidence(self, signals: List[Tuple[float, float]]) -> float:
        """
        Calculate stacked confidence using multiplicative combination.
        
        This prevents simple averaging and allows multiple medium signals 
        to create high confidence classifications.
        """
        if not signals:
            return 0.0
        
        if len(signals) == 1:
            confidence, weight = signals[0]
            return confidence * weight
        
        # Multiplicative confidence stacking with diminishing returns
        combined_confidence = 0
        total_weight = 0
        
        for confidence, weight in signals:
            weighted_confidence = confidence * weight
            combined_confidence += weighted_confidence
            total_weight += weight
        
        # Apply stacking multiplier for multiple signals
        if len(signals) >= 2:
            stacking_bonus = (len(signals) - 1) * CLASSIFICATION_THRESHOLDS['confidence_stacking_multiplier'] * 0.1
            combined_confidence *= (1 + stacking_bonus)
        
        # Normalize by total weight
        if total_weight > 0:
            combined_confidence /= total_weight
        
        return min(0.95, combined_confidence)
    
    def _smart_high_value_reclassification(self, usd_value: float, phase_results: Dict[str, PhaseResult], 
                                         participating_phases: List[str]) -> Tuple[ClassificationType, float, str]:
        """
        Smart reclassification for high-value transactions that defaulted to TRANSFER.
        
        High-value transactions are more likely to be intentional economic actions.
        """
        # For very high value transactions, apply heuristics
        confidence_base = 0.55  # Start with medium confidence
        
        # Value-based confidence scaling
        if usd_value >= 1000000:  # $1M+
            confidence_base = 0.65
        elif usd_value >= 500000:  # $500K+
            confidence_base = 0.60
        
        # Check for any DEX or protocol involvement
        has_defi_involvement = any('DEX' in str(result.evidence) or 'DEFI' in str(result.evidence) 
                                 for result in phase_results.values())
        
        # Check for CEX involvement  
        has_cex_involvement = any('CEX' in str(result.evidence) or 'exchange' in str(result.evidence).lower()
                                for result in phase_results.values())
        
        if has_cex_involvement:
            # High-value CEX interaction likely indicates selling
            return ClassificationType.MODERATE_SELL, confidence_base, f"High-value CEX interaction (${usd_value:,.0f})"
        elif has_defi_involvement:
            # High-value DeFi interaction likely indicates buying/investing
            return ClassificationType.MODERATE_BUY, confidence_base, f"High-value DeFi interaction (${usd_value:,.0f})"
        else:
            # Pure high-value transfer - still classify as moderate buy (accumulation)
            return ClassificationType.MODERATE_BUY, confidence_base * 0.8, f"High-value accumulation signal (${usd_value:,.0f})"
    
    def _apply_behavioral_heuristics(self, transaction: Dict[str, Any], phase_results: Dict[str, PhaseResult]) -> BehavioralAnalysis:
        """
        Enhanced behavioral heuristics with gas price intelligence and contextual analysis.
        
        New features:
        - Gas price urgency analysis for confidence boosting
        - USD value impact on behavior assessment
        - Address interaction patterns
        """
        try:
            analysis = BehavioralAnalysis()
            confidence_adjustments = []
            total_boost = 0.0
            
            # Enhanced gas price analysis
            gas_analysis = self._analyze_enhanced_gas_intelligence(transaction)
            analysis.gas_price_analysis = gas_analysis
            
            # Apply gas urgency boost
            if gas_analysis.get('urgency_level') == 'high':
                gas_boost = CLASSIFICATION_THRESHOLDS['gas_urgency_boost']
                total_boost += gas_boost
                confidence_adjustments.append({
                    'type': 'gas_urgency',
                    'boost': gas_boost,
                    'reason': 'High gas price indicates urgent trading intent'
                })
            elif gas_analysis.get('urgency_level') == 'medium':
                gas_boost = CLASSIFICATION_THRESHOLDS['gas_urgency_boost'] * 0.5
                total_boost += gas_boost
                confidence_adjustments.append({
                    'type': 'gas_urgency',
                    'boost': gas_boost,
                    'reason': 'Medium gas price suggests intentional action'
                })
            
            # Enhanced address behavior analysis
            address_behavior = self._analyze_enhanced_address_behavior(transaction, phase_results)
            analysis.address_behavior_analysis = address_behavior
            
            # Apply address behavior boosts
            if address_behavior.get('is_whale_address'):
                whale_boost = 0.08
                total_boost += whale_boost
                confidence_adjustments.append({
                    'type': 'whale_address',
                    'boost': whale_boost,
                    'reason': 'Whale address involvement increases signal confidence'
                })
            
            if address_behavior.get('is_institutional'):
                institutional_boost = 0.06
                total_boost += institutional_boost
                confidence_adjustments.append({
                    'type': 'institutional',
                    'boost': institutional_boost,
                    'reason': 'Institutional address suggests deliberate action'
                })
            
            # Enhanced timing analysis
            timing_analysis = self._analyze_enhanced_timing_intelligence(transaction)
            analysis.timing_analysis = timing_analysis
            
            # Apply timing boosts
            if timing_analysis.get('is_peak_hours'):
                timing_boost = 0.04
                total_boost += timing_boost
                confidence_adjustments.append({
                    'type': 'peak_hours',
                    'boost': timing_boost,
                    'reason': 'Peak trading hours suggest active trading intent'
                })
            
            # USD value impact analysis
            usd_value = transaction.get('amount_usd', transaction.get('value_usd', 0))
            if usd_value >= 1000000:  # $1M+
                value_boost = 0.10
                total_boost += value_boost
                confidence_adjustments.append({
                    'type': 'mega_value',
                    'boost': value_boost,
                    'reason': f'Mega transaction (${usd_value:,.0f}) indicates high conviction'
                })
            elif usd_value >= 500000:  # $500K+
                value_boost = 0.06
                total_boost += value_boost
                confidence_adjustments.append({
                    'type': 'high_value',
                    'boost': value_boost,
                    'reason': f'High-value transaction (${usd_value:,.0f}) suggests intentional action'
                })
            
            # Finalize analysis
            analysis.confidence_adjustments = confidence_adjustments
            analysis.total_confidence_boost = min(0.25, total_boost)  # Cap at 25% boost
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Behavioral heuristics failed: {e}")
            return BehavioralAnalysis()
    
    def _analyze_enhanced_gas_intelligence(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enhanced gas price analysis for trading intent detection.
        
        High gas prices indicate urgency and conviction in trading decisions.
        """
        try:
            gas_price = transaction.get('gas_price', 0)
            gas_used = transaction.get('gas_used', 0)
            
            if not gas_price or not gas_used:
                return {'urgency_level': 'unknown', 'analysis': 'no_gas_data'}
            
            # Convert gas price to Gwei for analysis
            gas_price_gwei = gas_price / 1e9 if gas_price > 1e9 else gas_price
            
            # Gas urgency thresholds (in Gwei)
            if gas_price_gwei >= 100:  # Very high gas
                urgency_level = 'high'
                analysis = 'urgent_trading_intent'
            elif gas_price_gwei >= 50:   # High gas
                urgency_level = 'medium'
                analysis = 'elevated_trading_intent'
            elif gas_price_gwei >= 20:   # Medium gas
                urgency_level = 'low'
                analysis = 'normal_trading_intent'
            else:
                urgency_level = 'very_low'
                analysis = 'routine_transaction'
            
            return {
                'urgency_level': urgency_level,
                'analysis': analysis,
                'gas_price_gwei': gas_price_gwei,
                'gas_used': gas_used,
                'total_fee_eth': (gas_price * gas_used) / 1e18 if gas_price and gas_used else 0
            }
            
        except Exception as e:
            return {'urgency_level': 'unknown', 'analysis': 'gas_analysis_error', 'error': str(e)}
    
    def _analyze_enhanced_address_behavior(self, transaction: Dict[str, Any], phase_results: Dict[str, PhaseResult]) -> Dict[str, Any]:
        """
        Enhanced address behavior profiling for confidence boosting.
        """
        try:
            from_addr = transaction.get('from', transaction.get('from_address', ''))
            to_addr = transaction.get('to', transaction.get('to_address', ''))
            
            behavior_profile = {
                'is_whale_address': False,
                'is_institutional': False,
                'is_frequent_trader': False,
                'address_confidence': 0.0
            }
            
            # Check whale signals from phase results
            for phase_result in phase_results.values():
                if phase_result.whale_signals:
                    for signal in phase_result.whale_signals:
                        signal_lower = signal.lower()
                        if 'whale' in signal_lower or 'mega' in signal_lower:
                            behavior_profile['is_whale_address'] = True
                        if 'institutional' in signal_lower or 'fund' in signal_lower:
                            behavior_profile['is_institutional'] = True
                        if 'frequent' in signal_lower or 'trader' in signal_lower:
                            behavior_profile['is_frequent_trader'] = True
            
            # Address pattern analysis
            if len(from_addr) == 42 and from_addr.startswith('0x'):  # Valid Ethereum address
                # Simple heuristics for address behavior
                if from_addr.endswith('000000'):  # Often institutional/contract addresses
                    behavior_profile['is_institutional'] = True
                
                # Check if addresses appear to be smart contracts (simplified)
                if self._is_likely_contract(from_addr) or self._is_likely_contract(to_addr):
                    behavior_profile['address_confidence'] = 0.1  # Boost for contract interactions
            
            return behavior_profile
            
        except Exception as e:
            return {'is_whale_address': False, 'is_institutional': False, 'error': str(e)}
    
    def _analyze_enhanced_timing_intelligence(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enhanced timing analysis for market context.
        """
        try:
            timestamp = transaction.get('timestamp', transaction.get('block_timestamp'))
            
            if not timestamp:
                return {'is_peak_hours': False, 'analysis': 'no_timestamp'}
            
            # Convert to datetime if needed
            if isinstance(timestamp, (int, float)):
                from datetime import datetime, timezone
                dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            else:
                dt = timestamp
            
            # Peak trading hours analysis (UTC)
            hour = dt.hour
            
            # Market peak hours: 8-12 UTC (European market) and 14-18 UTC (US market)
            is_peak_hours = (8 <= hour <= 12) or (14 <= hour <= 18)
            
            # Day of week analysis
            weekday = dt.weekday()  # 0 = Monday, 6 = Sunday
            is_business_day = weekday < 5  # Monday-Friday
            
            return {
                'is_peak_hours': is_peak_hours,
                'is_business_day': is_business_day,
                'hour_utc': hour,
                'weekday': weekday,
                'analysis': 'peak_hours' if is_peak_hours else 'off_hours'
            }
            
        except Exception as e:
            return {'is_peak_hours': False, 'analysis': 'timing_analysis_error', 'error': str(e)}
    
    def _calculate_whale_score(self, whale_signals: List[str]) -> float:
        """Calculate comprehensive whale score."""
        try:
            if not whale_signals:
                return 0.0
            
            base_score = 0.0
            
            # Score based on signal types
            for signal in whale_signals:
                signal_lower = signal.lower()
                
                if 'mega_whale' in signal_lower:
                    base_score += 25
                elif 'whale' in signal_lower:
                    base_score += 15
                elif 'high_volume' in signal_lower:
                    base_score += 10
                elif 'market_maker' in signal_lower:
                    base_score += 12
                elif 'frequent_trader' in signal_lower:
                    base_score += 8
                else:
                    base_score += 5
            
            # Multiple signal bonus
            if len(whale_signals) >= 3:
                base_score += 10
            elif len(whale_signals) >= 2:
                base_score += 5
            
            # Cap at 100 with diminishing returns
            return min(100.0, base_score * 0.9)
            
        except Exception as e:
            self.logger.error(f"Whale score calculation error: {e}")
            return 0.0
    
    def _analyze_trading_opportunity(self, transaction: Dict[str, Any], result: IntelligenceResult, tx_logger) -> Optional[OpportunitySignal]:
        """Analyze for trading opportunities using the opportunity engine."""
        try:
            # This would integrate with the existing opportunity engine
            # For now, return None as this requires additional implementation
            return None
            
        except Exception as e:
            self.logger.warning(f"Trading opportunity analysis failed: {e}")
            return None
    
    def _analyze_market_data_intelligence(self, from_addr: str, to_addr: str, blockchain: str) -> PhaseResult:
        """
        NEW: Market Data Intelligence Analysis Phase
        
        Fetches real-time market data for the transacted token to provide:
        - Real-time price validation and USD value refinement
        - 24-hour trading volume analysis
        - Market capitalization context
        - Price volatility assessment
        - Market-driven confidence adjustments
        
        Args:
            from_addr: Transaction sender address
            to_addr: Transaction receiver address  
            blockchain: Blockchain network
            
        Returns:
            PhaseResult with market intelligence data
        """
        try:
            if not self.market_data_provider:
                return create_empty_phase_result(
                    "Market data provider not available",
                    AnalysisPhase.MARKET_DATA_INTELLIGENCE.value
                )
            
            evidence = []
            whale_signals = []
            confidence_adjustments = 0.0
            classification = ClassificationType.TRANSFER
            
            # Get token contract address (prioritize 'to' address for token transfers)
            contract_address = to_addr if to_addr else from_addr
            
            if contract_address and blockchain in ['ethereum', 'polygon', 'bsc', 'arbitrum']:
                self.logger.debug(f"Fetching market data for {contract_address} on {blockchain}")
                
                # Fetch comprehensive market data
                market_data = self.market_data_provider.get_market_data_for_token(
                    contract_address, blockchain
                )
                
                if market_data:
                    # Extract key market metrics
                    current_price = market_data.get('current_price_usd', 0)
                    volume_24h = market_data.get('volume_24h_usd', 0)
                    market_cap = market_data.get('market_cap_usd', 0)
                    volatility = market_data.get('price_volatility_24h', 0)
                    
                    self.logger.info(f"Market data retrieved: Price=${current_price:.6f}, Volume24h=${volume_24h:,.0f}, MCap=${market_cap:,.0f}")
                    
                    # Volume-based confidence adjustments
                    if volume_24h >= 10_000_000:  # $10M+ daily volume
                        confidence_adjustments += 0.15
                        evidence.append(f"High liquidity: ${volume_24h:,.0f} 24h volume")
                        whale_signals.append("HIGH_LIQUIDITY_TOKEN")
                    elif volume_24h >= 1_000_000:  # $1M+ daily volume
                        confidence_adjustments += 0.10
                        evidence.append(f"Good liquidity: ${volume_24h:,.0f} 24h volume")
                    elif volume_24h < 100_000:  # < $100K daily volume
                        confidence_adjustments -= 0.05
                        evidence.append(f"Low liquidity warning: ${volume_24h:,.0f} 24h volume")
                    
                    # Market cap context
                    if market_cap >= 1_000_000_000:  # $1B+ market cap
                        confidence_adjustments += 0.10
                        evidence.append(f"Large cap asset: ${market_cap:,.0f} market cap")
                        whale_signals.append("LARGE_CAP_ASSET")
                    elif market_cap >= 100_000_000:  # $100M+ market cap
                        confidence_adjustments += 0.05
                        evidence.append(f"Mid cap asset: ${market_cap:,.0f} market cap")
                    elif market_cap < 10_000_000:  # < $10M market cap
                        evidence.append(f"Small cap asset: ${market_cap:,.0f} market cap")
                        whale_signals.append("SMALL_CAP_ASSET")
                    
                    # Volatility analysis
                    if volatility > 20:  # High volatility (>20%)
                        confidence_adjustments += 0.08
                        evidence.append(f"High volatility: {volatility:.1f}% (opportunity window)")
                        whale_signals.append("HIGH_VOLATILITY_OPPORTUNITY")
                    elif volatility > 10:  # Medium volatility
                        confidence_adjustments += 0.04
                        evidence.append(f"Medium volatility: {volatility:.1f}%")
                    
                    # Price validation
                    if current_price > 0:
                        evidence.append(f"Real-time price: ${current_price:.6f}")
                    
                    # Determine if this market context suggests trading activity
                    if volume_24h >= 1_000_000 and volatility > 5:
                        # Active trading environment suggests higher likelihood of intentional trades
                        if confidence_adjustments > 0.10:
                            classification = ClassificationType.BUY  # Default to BUY in active markets
                            confidence_adjustments += 0.05  # Bonus for active market context
                    
                else:
                    evidence.append("Token not found in market data")
                    self.logger.debug(f"No market data found for {contract_address}")
            
            else:
                evidence.append(f"Unsupported blockchain for market analysis: {blockchain}")
            
            final_confidence = max(0.0, min(0.85, confidence_adjustments))  # Cap market intelligence confidence
            
            return PhaseResult(
                classification=classification,
                confidence=final_confidence,
                evidence=evidence,
                whale_signals=whale_signals,
                phase=AnalysisPhase.MARKET_DATA_INTELLIGENCE.value,
                raw_data={
                    "market_data_available": self.market_data_provider is not None,
                    "contract_address": contract_address,
                    "blockchain": blockchain,
                    "confidence_adjustments": confidence_adjustments
                }
            )
            
        except Exception as e:
            self.logger.error(f"Market data intelligence analysis failed: {e}")
            return create_empty_phase_result(
                f"Market analysis error: {str(e)}",
                AnalysisPhase.MARKET_DATA_INTELLIGENCE.value
            )

    def _evaluate_stage1_results(self, phase_results: Dict[str, PhaseResult]) -> Tuple[ClassificationType, float, str]:
        """
        ENHANCED PROFESSIONAL-GRADE PIPELINE CONTROL (SMART COST OPTIMIZATION):
        
        This method implements a sophisticated 3-tier analysis strategy:
        1. Detects conflicting BUY/SELL signals between Stage 1 phases
        2. Forces Stage 2 enrichment for TRANSFER results (ambiguous cases)
        3. Forces Stage 2 enrichment for CONFLICT results (competing signals)
        4. 🧠 SMART COST OPTIMIZATION: 3-tier confidence routing
        
        3-TIER COST OPTIMIZATION STRATEGY:
        Tier 1: Early Exit (0.85+ confidence) - Skip all Stage 2 analysis
        Tier 2: API-Only Stage 2 (0.70-0.85 confidence) - Use Zerion + Moralis only
        Tier 3: Full Stage 2 with BigQuery (<0.70 confidence) - Last resort for truly ambiguous cases
        
        Args:
            phase_results: Dictionary of completed Stage 1 phase results
            
        Returns:
            Tuple of (classification, confidence, reasoning) for pipeline routing
        """
        EARLY_EXIT_CONFIDENCE_THRESHOLD = 0.85  # 🔧 SMART: Higher threshold for early exit
        API_ONLY_CONFIDENCE_THRESHOLD = 0.70    # 🔧 SMART: API-only enrichment threshold  
        BIGQUERY_CONFIDENCE_THRESHOLD = 0.70    # 🔧 SMART: BigQuery only below this
        HIGH_CONFIDENCE_THRESHOLD = 0.70
        
        # Priority order for Stage 1 phases
        stage1_phases = [
            AnalysisPhase.BLOCKCHAIN_SPECIFIC.value,
            AnalysisPhase.CEX_CLASSIFICATION.value,
            AnalysisPhase.DEX_PROTOCOL.value,
            AnalysisPhase.STABLECOIN_FLOW.value,
            AnalysisPhase.WALLET_BEHAVIOR.value
        ]
        
        # Track all high-confidence BUY and SELL signals
        buy_signals = []
        sell_signals = []
        max_confidence = 0.0
        best_classification = ClassificationType.TRANSFER
        
        # Analyze all Stage 1 results for conflicts and best classification
        for phase_name in stage1_phases:
            if phase_name in phase_results:
                phase_result = phase_results[phase_name]
                
                # Track highest confidence classification
                if phase_result.confidence > max_confidence:
                    max_confidence = phase_result.confidence
                    best_classification = phase_result.classification
                
                # Track high-confidence directional signals for conflict detection
                if phase_result.confidence >= HIGH_CONFIDENCE_THRESHOLD:
                    if phase_result.classification == ClassificationType.BUY:
                        buy_signals.append((phase_name, phase_result.confidence))
                    elif phase_result.classification == ClassificationType.SELL:
                        sell_signals.append((phase_name, phase_result.confidence))
        
        # CRITICAL CONFLICT DETECTION
        has_conflicting_signals = len(buy_signals) > 0 and len(sell_signals) > 0
        
        if has_conflicting_signals:
            self.logger.warning(
                f"🚨 CONFLICTING SIGNALS DETECTED - Must proceed to Stage 2"
            )
            self.logger.warning(f"   BUY signals: {buy_signals}")
            self.logger.warning(f"   SELL signals: {sell_signals}")
            
            # Return CONFLICT classification to force Stage 2 analysis
            return (
                ClassificationType.CONFLICT, 
                1.0,  # High confidence that there IS a conflict
                f"Conflicting BUY/SELL signals in Stage 1 - {len(buy_signals)} BUY vs {len(sell_signals)} SELL"
            )
        
        # FORCE STAGE 2 FOR TRANSFER/TOKEN_TRANSFER RESULTS (ambiguous cases)
        if best_classification in [ClassificationType.TRANSFER, ClassificationType.TOKEN_TRANSFER]:
            self.logger.info(f"🔍 Stage 1 result: {best_classification.value} - Proceeding to Stage 2 for deeper analysis")
            return (
                best_classification,
                max_confidence,
                f"Stage 1 inconclusive ({best_classification.value}) - Stage 2 enrichment required"
            )
        
        # EARLY EXIT LOGIC: Only for uncontested, high-confidence BUY/SELL
        if (max_confidence > EARLY_EXIT_CONFIDENCE_THRESHOLD and 
            best_classification in [ClassificationType.BUY, ClassificationType.SELL]):
            
            # Verify this is truly uncontested (no competing signals)
            competing_signals = buy_signals if best_classification == ClassificationType.SELL else sell_signals
            
            if len(competing_signals) == 0:
                self.logger.info(
                    f"✅ EARLY EXIT APPROVED: {best_classification.value} "
                    f"(confidence: {max_confidence:.2f}, uncontested)"
                )
                return (
                    best_classification,
                    max_confidence,
                    f"High-confidence, uncontested {best_classification.value} from Stage 1"
                )
        
        # DEFAULT: Proceed to Stage 2 for all other cases
        reasoning = f"Stage 1 confidence ({max_confidence:.2f}) below threshold or contested - Stage 2 required"
        self.logger.info(f"🔬 Proceeding to Stage 2: {reasoning}")
        
        return (best_classification, max_confidence, reasoning)
    
    def _analyze_stablecoin_flow(self, from_addr: str, to_addr: str, transaction: Dict[str, Any]) -> PhaseResult:
        """
        Analyze stablecoin flow patterns to detect buy/sell signals.
        
        Args:
            from_addr: Source address
            to_addr: Destination address  
            transaction: Transaction data
            
        Returns:
            PhaseResult with stablecoin flow analysis
        """
        try:
            # Use dedicated stablecoin engine
            stablecoin_engine = StablecoinFlowEngine()
            return stablecoin_engine.analyze(from_addr, to_addr, transaction)
                
        except Exception as e:
            self.logger.error(f"Stablecoin flow analysis failed: {e}")
            return PhaseResult(
                classification=ClassificationType.TRANSFER,
                confidence=0.0,
                evidence=[f"Stablecoin analysis error: {str(e)}"],
                whale_signals=[],
                phase=AnalysisPhase.STABLECOIN_FLOW.value,
                raw_data={}
            )

    def _calculate_current_confidence(self, phase_results: Dict[str, PhaseResult]) -> float:
        """Calculate the current confidence level based on the highest confidence in the results."""
        return max([result.confidence for result in phase_results.values()], default=0.0)


# =============================================================================
# MAIN FUNCTIONS FOR BACKWARD COMPATIBILITY
# =============================================================================

def enhanced_cex_address_matching(from_addr: str, to_addr: str, blockchain: str = "ethereum") -> Tuple[Optional[str], float, List[str]]:
    """
    Enhanced CEX address matching for backward compatibility.
    
    Args:
        from_addr: Sender address
        to_addr: Receiver address
        blockchain: Blockchain network
        
    Returns:
        Tuple of (classification, confidence_score, evidence_sources)
    """
    try:
        engine = WhaleIntelligenceEngine()
        cex_engine = CEXClassificationEngine(engine.supabase_client)
        
        result = cex_engine.analyze(from_addr, to_addr, blockchain)
        
        classification = result.classification.value if result.classification != ClassificationType.TRANSFER else None
        
        return classification, result.confidence, result.evidence
        
    except Exception as e:
        logger.error(f"Enhanced CEX matching failed: {e}")
        return None, 0.0, [f"Error: {str(e)}"]


def comprehensive_stablecoin_analysis(transaction_data: Dict[str, Any]) -> Tuple[Optional[str], float, List[str]]:
    """
    Comprehensive stablecoin analysis for backward compatibility.
    
    Args:
        transaction_data: Transaction data dictionary
        
    Returns:
        Tuple of (classification, confidence, evidence)
    """
    try:
        # This would be implemented based on the existing stablecoin analysis logic
        # For now, return basic analysis
        return None, 0.0, ["Stablecoin analysis not implemented"]
        
    except Exception as e:
        logger.error(f"Stablecoin analysis failed: {e}")
        return None, 0.0, [f"Error: {str(e)}"]


def process_and_enrich_transaction(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Universal transaction processor and enricher using the Production-Ready WhaleIntelligenceEngine
    
    This function is called by the enhanced_monitor.py main loop to process every transaction
    with the full whale intelligence pipeline. Now includes production-grade structured logging.
    
    Args:
        event: Transaction event data from various blockchain sources
        
    Returns:
        Enriched transaction data with whale intelligence analysis or None if processing fails
    """
    try:
        # Extract transaction hash for logging
        tx_hash = event.get('tx_hash', event.get('hash', 'unknown'))
        
        # Initialize transaction-aware structured logger
        tx_logger = get_transaction_logger(tx_hash, trace_id=f"monitor_{int(time.time())}")
        
        tx_logger.info(
            "Processing transaction in enhanced monitor",
            blockchain=event.get('blockchain', 'unknown'),
            value_usd=event.get('estimated_usd', event.get('value_usd', 0)),
            symbol=event.get('symbol', 'unknown')
        )
        
        # Initialize whale intelligence engine (production-ready instance)
        whale_engine = WhaleIntelligenceEngine()
        
        # Convert event to standardized transaction format for the whale intelligence engine
        transaction_data = {
            'hash': tx_hash,
            'blockchain': event.get('blockchain', 'ethereum'),
            'from': event.get('from', event.get('from_address', '')),
            'to': event.get('to', event.get('to_address', '')),
            'amount_usd': event.get('estimated_usd', event.get('value_usd', 0)),
            'usd_value': event.get('estimated_usd', event.get('value_usd', 0)),
            'token_symbol': event.get('symbol', ''),
            'block_number': event.get('block_number', 0),
            'timestamp': event.get('timestamp', int(time.time())),
            'source': 'enhanced_monitor'
        }
        
        tx_logger.debug(
            "Transaction data prepared for whale intelligence analysis",
            from_address=transaction_data['from'],
            to_address=transaction_data['to'],
            formatted_data_keys=list(transaction_data.keys())
        )
        
        # Run FULL PRODUCTION-READY comprehensive analysis
        result = whale_engine.analyze_transaction_comprehensive(transaction_data)
        
        if not result:
            tx_logger.warning("Whale intelligence analysis returned no result")
            return None
        
        # Extract key whale intelligence results
        classification = result.classification.value
        confidence = result.confidence
        whale_score = result.final_whale_score
        whale_signals = result.whale_signals
        reasoning = result.master_classifier_reasoning
        
        # Map moderate confidence classifications to standard format for backward compatibility
        display_classification = classification
        if classification in ['MODERATE_BUY', 'MODERATE_SELL']:
            # Keep the moderate classification but note the confidence level
            base_classification = classification.replace('MODERATE_', '')
            display_classification = f"{base_classification}_MODERATE"
        
        tx_logger.info(
            "Whale intelligence analysis complete",
            final_classification=display_classification,
            final_confidence=confidence,
            final_whale_score=whale_score,
            whale_signals_count=len(whale_signals)
        )
        
        # Create enriched transaction data with enhanced classification metadata
        enriched = {
            # Core classification results (enhanced)
            'classification': display_classification,
            'confidence': confidence,
            'whale_score': whale_score,
            'is_whale_transaction': whale_score >= 60,  # Adjusted threshold
            
            # Enhanced classification metadata
            'classification_type': 'enhanced_intelligence_v2',
            'confidence_tier': 'HIGH' if confidence >= 0.80 else 'MODERATE' if confidence >= 0.60 else 'LOW',
            'is_moderate_confidence': classification.startswith('MODERATE_'),
            'usd_value_analyzed': transaction_data.get('amount_usd', 0),
            'phases_completed': result.phases_completed if hasattr(result, 'phases_completed') else 0,
            'cost_optimized_analysis': result.cost_optimized if hasattr(result, 'cost_optimized') else False,
            
            # Whale intelligence signals
            'whale_signals': whale_signals,
            'whale_signal_count': len(whale_signals),
            'has_mega_whale_signals': any('MEGA' in signal or 'mega' in signal.lower() for signal in whale_signals),
            'has_institutional_signals': any('institutional' in signal.lower() or 'fund' in signal.lower() for signal in whale_signals),
            
            # Enhanced reasoning and evidence
            'reasoning': reasoning,
            'master_classifier_reasoning': reasoning,
            'evidence_summary': '; '.join(str(evidence) for evidence in result.evidence) if result.evidence else '',
            
            # Backward compatibility fields
            'symbol': transaction_data.get('token_symbol', event.get('symbol', '')),
            'blockchain': transaction_data.get('blockchain', 'ethereum'),
            'from_address': transaction_data.get('from', ''),
            'to_address': transaction_data.get('to', ''),
            'transaction_hash': transaction_data.get('hash', ''),
        }
        
        tx_logger.debug(
            "Transaction enrichment complete",
            enriched_keys=list(enriched.keys()),
            has_whale_signals=len(whale_signals) > 0,
            is_whale_transaction=enriched['is_whale_transaction']
        )
        
        return enriched
        
    except Exception as e:
        # Create fallback logger if tx_logger doesn't exist
        if 'tx_logger' not in locals():
            tx_logger = get_transaction_logger(event.get('tx_hash', 'unknown'))
        
        tx_logger.error(
            "Transaction processing failed in enhanced monitor",
            error_message=str(e),
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc()
        )
        
        # Return minimal enrichment to prevent monitor crashes
        return {
            'classification': 'TRANSFER',
            'confidence_score': 0.1,
            'is_whale_transaction': False,
            'whale_classification': "Processing Error",
            'whale_signals': [f"Error: {str(e)}"],
            'advanced_analysis': {'error': str(e)},
            'enrichment_data': {},
            'processing_metadata': {
                'whale_intelligence_engine': 'error_fallback',
                'error': str(e)
            }
        }


def transaction_classifier(from_addr: str, to_addr: str, symbol: str, amount: float, blockchain: str = "ethereum") -> tuple:
    """
    Legacy function for backward compatibility
    """
    try:
        # Create mock transaction data
        transaction_data = {
            'chain': blockchain,
            'from_address': from_addr,
            'to_address': to_addr,
            'value_usd': amount,
            'token_symbol': symbol
        }
        
        # Use comprehensive analysis
        whale_engine = WhaleIntelligenceEngine()
        result = whale_engine.analyze_transaction_comprehensive(transaction_data)
        
        classification = result.classification.value
        confidence = result.confidence
        
        return classification, confidence
        
    except Exception as e:
        logger.error(f"Error in legacy transaction_classifier: {e}")
        return 'TRANSFER', 0.5


def classify_xrp_transaction(tx_data: Dict[str, Any]) -> tuple:
    """
    XRP transaction classification function
    
    Args:
        tx_data: XRP transaction data
        
    Returns:
        Tuple of (classification, confidence)
    """
    try:
        # Convert XRP data to standard format
        transaction_data = {
            'chain': 'xrp',
            'hash': tx_data.get('hash', ''),
            'from_address': tx_data.get('Account', ''),
            'to_address': tx_data.get('Destination', ''),
            'value_usd': float(tx_data.get('amount_usd', 0)),
            'token_symbol': 'XRP'
        }
        
        # Use comprehensive analysis
        whale_engine = WhaleIntelligenceEngine()
        result = whale_engine.analyze_transaction_comprehensive(transaction_data)
        
        classification = result.classification.value
        confidence = result.confidence
        
        return classification, confidence
        
    except Exception as e:
        logger.error(f"Error in XRP transaction classification: {e}")
        return 'TRANSFER', 0.5


def analyze_address_characteristics(address: str, blockchain: str = "ethereum") -> Dict[str, Any]:
    """
    Analyze address characteristics for enhanced classification
    
    Args:
        address: Address to analyze
        blockchain: Blockchain network
        
    Returns:
        Dictionary of address characteristics
    """
    try:
        # Create mock transaction for address analysis
        transaction_data = {
            'chain': blockchain,
            'from_address': address,
            'to_address': '',
            'value_usd': 0,
            'token_symbol': ''
        }
        
        # Use whale intelligence engine for analysis
        whale_engine = WhaleIntelligenceEngine()
        result = whale_engine.analyze_transaction_comprehensive(transaction_data)
        
        return {
            'address': address,
            'blockchain': blockchain,
            'whale_score': result.final_whale_score,
            'historical_data': result.phase_results.get('wallet_behavior', PhaseResult()).__dict__,
            'moralis_data': result.phase_results.get('moralis_enrichment', PhaseResult()).__dict__,
            'characteristics': result.whale_signals
        }
        
    except Exception as e:
        logger.error(f"Error analyzing address characteristics: {e}")
        return {
            'address': address,
            'blockchain': blockchain,
            'whale_score': 0,
            'error': str(e)
        }


def enhanced_solana_classification(owner: str, prev_owner: Optional[str], amount_change: float, tx_hash: str, token: str, source: str = "solana") -> tuple:
    """
    Enhanced Solana transaction classification function
    
    Args:
        owner: Current token owner address
        prev_owner: Previous token owner address (can be None)
        amount_change: Change in token amount
        tx_hash: Transaction hash
        token: Token symbol
        source: Source identifier (default: "solana")
        
    Returns:
        Tuple of (classification, confidence) where:
        - classification: "buy", "sell", or "transfer"
        - confidence: confidence score (0-10)
    """
    try:
        # Create transaction data for comprehensive analysis
        transaction_data = {
            'chain': 'solana',
            'blockchain': 'solana',
            'hash': tx_hash,
            'from_address': prev_owner or '',
            'to_address': owner,
            'value_usd': abs(amount_change) * 1000,  # Rough estimate for analysis
            'token_symbol': token,
            'amount_change': amount_change
        }
        
        # Use the whale intelligence engine for comprehensive analysis
        whale_engine = WhaleIntelligenceEngine()
        result = whale_engine.analyze_transaction_comprehensive(transaction_data)
        
        # Extract classification and confidence
        classification = result.classification.value.lower()
        confidence_score = result.confidence
        
        # Map classification to expected format
        if classification in ['dex_swap_buy', 'buy']:
            final_classification = 'buy'
        elif classification in ['dex_swap_sell', 'sell']:
            final_classification = 'sell'
        else:
            final_classification = 'transfer'
        
        # Convert confidence to 0-10 scale (expected by the calling code)
        final_confidence = min(10, max(0, confidence_score * 10))
        
        # Simple heuristic based on amount change direction if comprehensive analysis fails
        if final_confidence < 1:
            if amount_change > 0:
                final_classification = 'buy'
                final_confidence = 3
            elif amount_change < 0:
                final_classification = 'sell' 
                final_confidence = 3
            else:
                final_classification = 'transfer'
                final_confidence = 2
        
        return final_classification, final_confidence
        
    except Exception as e:
        logger.error(f"Error in enhanced_solana_classification: {e}")
        
        # Fallback classification based on amount change
        if amount_change > 0:
            return 'buy', 2
        elif amount_change < 0:
            return 'sell', 2
        else:
            return 'transfer', 1


# =============================================================================
# MODULE INITIALIZATION
# =============================================================================

# Initialize the main engine instance for use by other modules
try:
    whale_engine = WhaleIntelligenceEngine()
    logger.info("Whale Intelligence Engine module initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize whale engine: {e}")
    whale_engine = None

# Initialize global whale intelligence engine instance for backward compatibility
whale_intelligence_engine = whale_engine

# Export main classes and functions
__all__ = [
    'WhaleIntelligenceEngine',
    'ClassificationType', 
    'AnalysisPhase',
    'PhaseResult',
    'IntelligenceResult',
    'OpportunitySignal',
    'enhanced_cex_address_matching',
    'comprehensive_stablecoin_analysis',
    'process_and_enrich_transaction',
    'transaction_classifier',
    'classify_xrp_transaction',
    'analyze_address_characteristics',
    'enhanced_solana_classification',
    'whale_engine',
    'whale_intelligence_engine'
] 
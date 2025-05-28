import re
from collections import defaultdict
from data.addresses import (
    known_exchange_addresses,
    DEX_ADDRESSES,
    MARKET_MAKER_ADDRESSES,
    solana_exchange_addresses,
    xrp_exchange_addresses,
    SOLANA_DEX_ADDRESSES,
)
from models.classes import DefiLlamaData
from utils.dedup import deduplicate_transactions
from utils.helpers import get_protocol_slug, get_dex_name, is_significant_tvl_movement
from utils.summary import has_been_classified, mark_as_classified
# In classification.py (at the very top), normalize your known exchange dict:
from data import addresses as raw_addresses




# classification.py

from data.addresses import known_exchange_addresses
from data.market_makers import MARKET_MAKER_ADDRESSES

# 1) Combine all known exchange + market maker addresses into ONE set
ALL_EXCHANGES_AND_MARKET_MAKERS = set(
    addr.lower() for addr in known_exchange_addresses.keys()
).union(
    addr.lower() for addr in MARKET_MAKER_ADDRESSES.keys()
)

known_exchange_addresses = {k.lower(): v for k, v in raw_addresses.known_exchange_addresses.items()}


def classify_transaction(tx):
    """
    Improved classification that uses both direct exchange checks and additional heuristics.
    Returns a tuple: (classification, confidence_score)
    """
    # Get sender and receiver addresses in lowercase.
    from_addr = tx.get("from", "").lower()
    to_addr = tx.get("to", "").lower()

    # Use your pre-merged set of known exchange and market maker addresses.
    # (Assuming ALL_EXCHANGES_AND_MARKET_MAKERS is already imported and defined.)
    if from_addr in ALL_EXCHANGES_AND_MARKET_MAKERS and to_addr not in ALL_EXCHANGES_AND_MARKET_MAKERS:
        return ("sell", 4)
    elif to_addr in ALL_EXCHANGES_AND_MARKET_MAKERS and from_addr not in ALL_EXCHANGES_AND_MARKET_MAKERS:
        return ("buy", 4)
    
    # If direct matches are not found, use additional heuristics:
    from_chars = analyze_address_characteristics(from_addr)
    to_chars = analyze_address_characteristics(to_addr)
    
    # Start with a base score of 0.
    score = 0

    # Add weight if the sender's address has exchange-like characteristics.
    if from_chars.get("is_exchange", False):
        score += 2
    # Subtract weight if the receiver's address has exchange-like characteristics.
    if to_chars.get("is_exchange", False):
        score -= 2

    # Bonus: if the USD value of the transaction is high, add a slight extra weight.
    try:
        usd_value = float(tx.get("usd_value", 0))
    except ValueError:
        usd_value = 0
    if usd_value > 3000:
        score += 1

    # Determine the classification based on the score.
    # A positive score means the transaction is likely a sell (exiting an address with exchange traits),
    # a negative score means it is likely a buy.
    if score >= 2:
        return ("sell", score)
    elif score <= -2:
        return ("buy", abs(score))
    else:
        # If the score is near 0, consider it a transfer.
        return ("transfer", 0)



def analyze_address_characteristics(address: str) -> dict:
    """
    Analyzes an address to detect exchange-like characteristics.
    Uses length heuristics and keyword matching without altering method names.
    """
    characteristics = {"is_exchange": False, "confidence": 0}
    addr_lower = address.lower()

    # Heuristic for Ethereum addresses
    if address.startswith("0x") and len(address) == 42:
        characteristics["confidence"] += 1

    # Heuristic for XRP addresses
    if address.startswith("r") and len(address) == 34:
        characteristics["confidence"] += 1

    # Check for common exchange keywords in the address string.
    exchange_keywords = [
        "exchange", "binance", "kraken", "coinbase", "huobi",
        "okex", "bitfinex", "bittrex", "kucoin", "bitstamp", "gemini"
    ]
    for keyword in exchange_keywords:
        if keyword in addr_lower:
            characteristics["confidence"] += 1

    # Mark as exchange-like if confidence is high.
    if characteristics["confidence"] >= 2:
        characteristics["is_exchange"] = True

    return characteristics

def classify_erc20_transfer(tx_from: str, tx_to: str, token_symbol: str) -> str:
    """
    Enhanced ERC-20 transfer classification with proper address handling.
    
    Args:
        tx_from: From address
        tx_to: To address
        token_symbol: Token symbol
        
    Returns:
        str: Classification ("buy", "sell", or "transfer")
    """
    # Standardize addresses to lowercase
    from_addr = tx_from.lower() if tx_from else ""
    to_addr = tx_to.lower() if tx_to else ""
    
    # Skip if addresses are invalid
    if not from_addr or not to_addr:
        return "transfer"
    
    # Direct exchange address check
    if from_addr in known_exchange_addresses:
        return "buy"
    if to_addr in known_exchange_addresses:
        return "sell"
        
    # Check DEX addresses
    if from_addr in DEX_ADDRESSES or to_addr in DEX_ADDRESSES:
        return "transfer"  # DEX interactions are treated as transfers
        
    # Analyze address characteristics
    from_chars = analyze_address_characteristics(from_addr)
    to_chars = analyze_address_characteristics(to_addr)
    
    if from_chars["is_exchange"] and not to_chars["is_exchange"]:
        return "buy"
    elif to_chars["is_exchange"] and not from_chars["is_exchange"]:
        return "sell"
        
    return "transfer"

def transaction_classifier(tx_from: str, tx_to: str, token_symbol: str = None, 
                          amount: float = None, tx_hash: str = None, source: str = None) -> tuple:
    # Standardize addresses
    from_addr = tx_from.lower() if tx_from else ""
    to_addr = tx_to.lower() if tx_to else ""
    
    # Skip if addresses are invalid
    if not from_addr or not to_addr:
        return ("transfer", 1)
    
    # First, check if both sides are exchanges (internal transfer)
    if from_addr in known_exchange_addresses and to_addr in known_exchange_addresses:
        return ("transfer", 3)
    
    # Check exchange addresses - higher confidence classification
    if from_addr in known_exchange_addresses and to_addr not in known_exchange_addresses:
        return ("buy", 4)  # Exchange sending to user = buy
    
    if to_addr in known_exchange_addresses and from_addr not in known_exchange_addresses:
        return ("sell", 4)  # User sending to exchange = sell
    
    # Check for market maker addresses
    if from_addr in MARKET_MAKER_ADDRESSES:
        return ("buy", 3)  # Market maker providing liquidity
    if to_addr in MARKET_MAKER_ADDRESSES:
        return ("sell", 3)  # Market maker absorbing liquidity
    
    # Use heuristic analysis for non-exchange addresses
    from_chars = analyze_address_characteristics(from_addr)
    to_chars = analyze_address_characteristics(to_addr)
    
    # More sophisticated scoring based on address characteristics
    confidence_score = 0
    classification = "transfer"  # Default to transfer instead of unknown
    
    if from_chars.get("is_exchange", False) and not to_chars.get("is_exchange", False):
        confidence_score += 2
        classification = "buy"
    elif to_chars.get("is_exchange", False) and not from_chars.get("is_exchange", False):
        confidence_score += 2
        classification = "sell"
    
    # Consider value - larger transactions might be more likely for institutional selling
    try:
        usd_value = float(amount) if amount is not None else 0
    except (ValueError, AttributeError):
        usd_value = 0

    # Boost confidence for large transactions
    if usd_value > 50000:
        confidence_score += 1

    # Add pattern analysis
    if classification == "unknown" or confidence_score < 2:
        # If either address has exchange-like characteristics but isn't a known exchange
        if from_chars.get("is_exchange", False) or to_chars.get("is_exchange", False):
            confidence_score = max(confidence_score, 2)
            
            if from_chars.get("is_exchange", False):
                classification = "buy"
            else:
                classification = "sell"
        else:
            classification = "transfer"
            confidence_score = max(confidence_score, 1)

    return (classification, confidence_score)





def enhanced_solana_classification(owner, prev_owner=None, amount_change=0, tx_hash=None, token=None, source=None):
    """
    Enhanced Solana transaction classification with additional heuristics and confidence scoring.
    Combines known Solana DEX/exchange addresses, basic flow heuristics, and placeholders
    for additional temporal or behavioral analysis.
    """
    # Skip if already classified
    if token and tx_hash and has_been_classified(token, tx_hash):
        return "already_classified", 0
        
    confidence_score = 0
    classification = "transfer"

    # Combine known Solana DEX and exchange addresses.
    all_solana_addresses = {**SOLANA_DEX_ADDRESSES, **solana_exchange_addresses}
    owner_is_known = owner in all_solana_addresses
    prev_owner_is_known = prev_owner in all_solana_addresses if prev_owner else False

    # Basic exchange flow heuristic.
    if owner_is_known:
        if amount_change > 0:
            classification = "sell"
            confidence_score += 2
        elif amount_change < 0:
            classification = "buy"
            confidence_score += 2

    # DEX interaction heuristic.
    if owner in SOLANA_DEX_ADDRESSES:
        confidence_score += 1

    # Previous owner heuristic.
    if prev_owner_is_known:
        confidence_score += 1

    # Record classification if we have tx_hash
    if tx_hash and token and classification != "already_classified" and confidence_score >= 2:
        mark_as_classified(token, tx_hash, classification, source)

    # Return transfer if confidence is too low.
    if confidence_score < 2:
        classification = "transfer"
        
    return classification, confidence_score

def classify_xrp_transaction(txn, source=None):
    """
    Enhanced XRP transaction classification using additional heuristics.
    Checks known exchange addresses and applies address characteristic analysis.
    """
    try:
        from_addr = txn.get("Account", "")
        to_addr = txn.get("Destination", "")
        tx_hash = txn.get("hash", "")
        try:
            raw_amount = txn.get("Amount", 0)
            # If raw_amount is a string, strip any unwanted characters:
            raw_amount = str(raw_amount).strip()
            amount = float(raw_amount) / 1_000_000
        except (ValueError, TypeError):
            amount = 0
        
        # Skip if already classified
        if tx_hash and has_been_classified("XRP", tx_hash):
            return "already_classified", 0

        # Direct match with known exchange addresses.
        if from_addr in xrp_exchange_addresses and to_addr not in xrp_exchange_addresses:
            classification = "buy"
            confidence = 3
        elif to_addr in xrp_exchange_addresses and from_addr not in xrp_exchange_addresses:
            classification = "sell"
            confidence = 3
        else:
            # Analyze address characteristics.
            from_chars = analyze_address_characteristics(from_addr)
            to_chars = analyze_address_characteristics(to_addr)
            
            if from_chars["is_exchange"] and not to_chars["is_exchange"]:
                classification = "buy"
                confidence = 2
            elif to_chars["is_exchange"] and not from_chars["is_exchange"]:
                classification = "sell"
                confidence = 2
            else:
                classification = "transfer"
                confidence = 1

        # Record classification if we have tx_hash
        if tx_hash and classification != "already_classified" and confidence >= 2:
            mark_as_classified("XRP", tx_hash, classification, source)
            
        return classification, amount

    except Exception as e:
        print(f"Error in classify_xrp_transaction: {e}")
        return "transfer", 0
    
# Optional: Address clustering method (method name preserved) for additional network analysis.
def cluster_related_addresses(transactions, time_window=3600):
    """
    Clusters addresses that interact frequently (e.g., via known exchange addresses).
    """
    clusters = defaultdict(set)
    for tx in transactions:
        from_addr = tx.get("from")
        to_addr = tx.get("to")
        if from_addr and from_addr in known_exchange_addresses:
            clusters[from_addr].add(to_addr)
        if to_addr and to_addr in known_exchange_addresses:
            clusters[to_addr].add(from_addr)
    return clusters

def generate_unique_key(event):
    """
    Creates a unique identifier for each event or transaction. 
    For EVM-based chains, (tx_hash, log_index) is sufficient.
    For Solana, use (signature, instruction_index) or just signature if instruction_index is not available.
    For Tron, (tx_hash,) is typically enough.
    """
    chain = event.get('blockchain', '').lower()

    if chain in ['ethereum', 'bsc', 'polygon']:
        return (event['tx_hash'], event.get('log_index', 0))
    elif chain == 'solana':
        return (event['signature'], event.get('instruction_index', 0))
    elif chain == 'tron':
        return (event['tx_hash'],)
    else:
        return (event['tx_hash'], event.get('log_index', 0))

# ============================================================================
# ADVANCED WHALE IDENTIFICATION HEURISTICS FOR PHASE 2
# ============================================================================

from typing import Dict, List, Optional, Any, Tuple
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def score_address_as_whale(address: str, 
                          transaction_data: Optional[List[Dict]] = None,
                          analytics_platform_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Analyzes an address and assigns a whale score based on multiple heuristics.
    
    Args:
        address: The blockchain address to analyze
        transaction_data: Optional list of transaction data for the address
        analytics_platform_data: Optional data from analytics platforms (Nansen, Dune, etc.)
    
    Returns:
        Dictionary containing whale_score (0.0-1.0) and positive_whale_signals
    """
    whale_score = 0.0
    positive_whale_signals = []
    confidence_factors = []
    
    # Normalize address
    address = address.lower()
    
    # 1. Analytics Platform Label-Based Scoring (High Weight)
    if analytics_platform_data:
        platform_score, platform_signals = _score_analytics_platform_labels(analytics_platform_data)
        whale_score += platform_score * 0.4  # 40% weight for platform data
        positive_whale_signals.extend(platform_signals)
        confidence_factors.append(f"analytics_platforms_{len(analytics_platform_data)}")
    
    # 2. Transaction Volume and Pattern Analysis (High Weight)
    if transaction_data:
        volume_score, volume_signals = _score_transaction_volume_patterns(transaction_data)
        whale_score += volume_score * 0.3  # 30% weight for transaction patterns
        positive_whale_signals.extend(volume_signals)
        confidence_factors.append("transaction_data_available")
    
    # 3. Address Characteristics Analysis (Medium Weight)
    char_score, char_signals = _score_address_characteristics(address)
    whale_score += char_score * 0.15  # 15% weight for address characteristics
    positive_whale_signals.extend(char_signals)
    
    # 4. Known Exchange/Entity Cross-Reference (Medium Weight)
    entity_score, entity_signals = _score_known_entity_patterns(address)
    whale_score += entity_score * 0.15  # 15% weight for entity patterns
    positive_whale_signals.extend(entity_signals)
    
    # Normalize score to 0.0-1.0 range
    whale_score = min(1.0, max(0.0, whale_score))
    
    # Determine whale classification
    whale_classification = _classify_whale_tier(whale_score, positive_whale_signals)
    
    return {
        'address': address,
        'whale_score': round(whale_score, 3),
        'whale_classification': whale_classification,
        'positive_whale_signals': positive_whale_signals,
        'confidence_factors': confidence_factors,
        'signal_count': len(positive_whale_signals),
        'analysis_timestamp': datetime.now().isoformat()
    }


def _score_analytics_platform_labels(analytics_data: Dict[str, Any]) -> Tuple[float, List[str]]:
    """Score based on analytics platform labels and data."""
    score = 0.0
    signals = []
    
    # Nansen data scoring
    if 'nansen_data' in analytics_data:
        nansen = analytics_data['nansen_data']
        label = nansen.get('nansen_label', '').lower()
        
        if 'smart money' in label:
            score += 0.8
            signals.append('nansen_smart_money_label')
        elif 'whale' in label:
            score += 0.9
            signals.append('nansen_whale_label')
        elif 'fund' in label or 'institution' in label:
            score += 0.7
            signals.append('nansen_institutional_label')
        
        # Portfolio value scoring
        portfolio_value = nansen.get('portfolio_value', 0)
        if portfolio_value > 10_000_000:  # $10M+
            score += 0.3
            signals.append('nansen_high_portfolio_value')
        elif portfolio_value > 1_000_000:  # $1M+
            score += 0.2
            signals.append('nansen_medium_portfolio_value')
        
        # Win rate scoring
        win_rate = nansen.get('win_rate', 0)
        if win_rate > 0.7:  # 70%+ win rate
            score += 0.2
            signals.append('nansen_high_win_rate')
    
    # Dune Analytics data scoring
    dune_data_count = sum(1 for key in analytics_data.keys() if 'dune' in key.lower())
    if dune_data_count > 0:
        # Multiple Dune queries flagging the address
        if dune_data_count >= 3:
            score += 0.6
            signals.append('multiple_dune_whale_queries')
        elif dune_data_count >= 2:
            score += 0.4
            signals.append('dune_whale_queries')
        else:
            score += 0.2
            signals.append('dune_query_match')
    
    # Glassnode data scoring
    if 'glassnode_data' in analytics_data:
        glassnode = analytics_data['glassnode_data']
        category = glassnode.get('glassnode_category', '').lower()
        
        if 'whale' in category:
            score += 0.8
            signals.append('glassnode_whale_category')
        
        balance_usd = glassnode.get('balance_usd', 0)
        if balance_usd > 50_000_000:  # $50M+
            score += 0.4
            signals.append('glassnode_ultra_high_balance')
        elif balance_usd > 10_000_000:  # $10M+
            score += 0.3
            signals.append('glassnode_high_balance')
    
    # Arkham Intelligence data scoring
    if 'arkham_data' in analytics_data:
        arkham = analytics_data['arkham_data']
        entity_type = arkham.get('entity_type', '').lower()
        
        if 'whale' in entity_type or 'large trader' in entity_type:
            score += 0.7
            signals.append('arkham_whale_entity')
        
        activity_score = arkham.get('activity_score', 0)
        if activity_score > 0.8:
            score += 0.2
            signals.append('arkham_high_activity')
    
    return min(1.0, score), signals


def _score_transaction_volume_patterns(transaction_data: List[Dict]) -> Tuple[float, List[str]]:
    """Score based on transaction volume and patterns."""
    score = 0.0
    signals = []
    
    if not transaction_data:
        return score, signals
    
    # Calculate transaction metrics
    total_volume_usd = 0
    high_value_tx_count = 0
    unique_tokens = set()
    defi_interactions = 0
    exchange_interactions = 0
    
    for tx in transaction_data:
        # Volume analysis
        usd_value = float(tx.get('usd_value', 0))
        total_volume_usd += usd_value
        
        if usd_value > 100_000:  # $100k+ transactions
            high_value_tx_count += 1
        
        # Token diversity
        token_symbol = tx.get('token_symbol')
        if token_symbol:
            unique_tokens.add(token_symbol)
        
        # DeFi interaction detection
        to_addr = tx.get('to', '').lower()
        from_addr = tx.get('from', '').lower()
        
        if to_addr in DEX_ADDRESSES or from_addr in DEX_ADDRESSES:
            defi_interactions += 1
        
        if (to_addr in known_exchange_addresses or from_addr in known_exchange_addresses):
            exchange_interactions += 1
    
    # Volume-based scoring
    if total_volume_usd > 100_000_000:  # $100M+ total volume
        score += 0.9
        signals.append('ultra_high_volume_usd')
    elif total_volume_usd > 10_000_000:  # $10M+ total volume
        score += 0.7
        signals.append('high_volume_usd')
    elif total_volume_usd > 1_000_000:  # $1M+ total volume
        score += 0.4
        signals.append('medium_volume_usd')
    
    # High-value transaction frequency
    if high_value_tx_count > 50:
        score += 0.3
        signals.append('frequent_large_transactions')
    elif high_value_tx_count > 10:
        score += 0.2
        signals.append('regular_large_transactions')
    
    # Token diversity (whales often trade multiple assets)
    if len(unique_tokens) > 20:
        score += 0.2
        signals.append('high_token_diversity')
    elif len(unique_tokens) > 10:
        score += 0.1
        signals.append('medium_token_diversity')
    
    # DeFi sophistication
    if defi_interactions > 100:
        score += 0.2
        signals.append('heavy_defi_usage')
    elif defi_interactions > 20:
        score += 0.1
        signals.append('regular_defi_usage')
    
    # Exchange interaction patterns
    if exchange_interactions > 50:
        score += 0.1
        signals.append('frequent_exchange_interactions')
    
    return min(1.0, score), signals


def _score_address_characteristics(address: str) -> Tuple[float, List[str]]:
    """Score based on address characteristics and patterns."""
    score = 0.0
    signals = []
    
    # Address pattern analysis
    if address.startswith('0x'):
        # Ethereum-style address analysis
        
        # Check for vanity address patterns (often used by whales/institutions)
        if _is_vanity_address(address):
            score += 0.1
            signals.append('vanity_address_pattern')
        
        # Check for contract address patterns
        if _appears_to_be_contract(address):
            score += 0.05
            signals.append('contract_address_pattern')
    
    return score, signals


def _score_known_entity_patterns(address: str) -> Tuple[float, List[str]]:
    """Score based on known entity patterns and relationships."""
    score = 0.0
    signals = []
    
    # Check against known whale addresses (if we have a curated list)
    if address in known_exchange_addresses:
        # Exchange addresses are not whales in the traditional sense
        score -= 0.5
        signals.append('known_exchange_address')
    
    # Check against market maker addresses
    if address in MARKET_MAKER_ADDRESSES:
        score += 0.3  # Market makers can be considered institutional whales
        signals.append('known_market_maker')
    
    # Check for patterns similar to known whale addresses
    whale_pattern_score = _check_whale_address_patterns(address)
    if whale_pattern_score > 0:
        score += whale_pattern_score
        signals.append('whale_address_pattern_match')
    
    return score, signals


def _classify_whale_tier(whale_score: float, signals: List[str]) -> str:
    """Classify whale tier based on score and signals."""
    signal_count = len(signals)
    
    if whale_score >= 0.8 and signal_count >= 5:
        return 'ultra_whale'
    elif whale_score >= 0.6 and signal_count >= 4:
        return 'major_whale'
    elif whale_score >= 0.4 and signal_count >= 3:
        return 'whale'
    elif whale_score >= 0.3 and signal_count >= 2:
        return 'potential_whale'
    else:
        return 'not_whale'


def _is_vanity_address(address: str) -> bool:
    """Check if address appears to be a vanity address."""
    if not address.startswith('0x') or len(address) != 42:
        return False
    
    # Look for patterns like repeated characters, sequences, or meaningful hex
    hex_part = address[2:].lower()
    
    # Check for repeated patterns
    for i in range(len(hex_part) - 3):
        if hex_part[i:i+4] == hex_part[i:i+4][0] * 4:  # 4 same characters
            return True
    
    # Check for sequential patterns
    sequences = ['0123', '1234', '2345', '3456', '4567', '5678', '6789', '789a', '89ab', '9abc', 'abcd', 'bcde', 'cdef']
    for seq in sequences:
        if seq in hex_part:
            return True
    
    return False


def _appears_to_be_contract(address: str) -> bool:
    """Basic heuristic to check if address might be a contract."""
    # This is a simple heuristic - in practice, you'd query the blockchain
    # Contract addresses often have specific patterns or are in known ranges
    if not address.startswith('0x'):
        return False
    
    # Very basic heuristic - contracts often have addresses starting with certain patterns
    # This would need to be enhanced with actual contract verification
    return False


def _check_whale_address_patterns(address: str) -> float:
    """Check for patterns similar to known whale addresses."""
    # This would implement pattern matching against known whale address characteristics
    # For now, return 0 as this requires a curated dataset of whale addresses
    return 0.0


def identify_whales_from_analytics_batch(platform_data: List[Dict[str, Any]], 
                                       platform_name: str) -> List[Dict[str, Any]]:
    """
    Processes a batch of analytics platform data to identify whale addresses.
    
    Args:
        platform_data: List of address records from analytics platform
        platform_name: Name of the source platform
    
    Returns:
        List of whale addresses with associated data
    """
    whale_addresses = []
    
    for record in platform_data:
        address = record.get('address')
        if not address or not address.startswith('0x'):
            continue
        
        # Apply platform-specific whale identification logic
        is_whale = False
        whale_confidence = 0.0
        whale_tags = []
        
        if platform_name.lower() == 'dune':
            # Dune-specific whale identification
            whale_score = record.get('whale_score', 0)
            total_volume = record.get('total_volume', 0)
            
            if whale_score > 0.7 or total_volume > 10_000_000:
                is_whale = True
                whale_confidence = max(whale_score, min(1.0, total_volume / 100_000_000))
                whale_tags.append('dune_high_score')
        
        elif platform_name.lower() == 'nansen':
            # Nansen-specific whale identification
            label = record.get('nansen_label', '').lower()
            portfolio_value = record.get('portfolio_value', 0)
            
            if 'whale' in label or 'smart money' in label or portfolio_value > 5_000_000:
                is_whale = True
                whale_confidence = 0.8 if 'whale' in label else 0.6
                whale_tags.append('nansen_whale_label')
        
        elif platform_name.lower() == 'glassnode':
            # Glassnode-specific whale identification
            category = record.get('glassnode_category', '').lower()
            balance_usd = record.get('balance_usd', 0)
            
            if 'whale' in category or balance_usd > 10_000_000:
                is_whale = True
                whale_confidence = 0.7
                whale_tags.append('glassnode_whale_category')
        
        if is_whale:
            whale_addresses.append({
                'address': address.lower(),
                'platform_source': platform_name,
                'whale_confidence': whale_confidence,
                'whale_tags': whale_tags,
                'platform_data': record,
                'identified_at': datetime.now().isoformat()
            })
    
    logger.info(f"Identified {len(whale_addresses)} whale addresses from {platform_name} batch of {len(platform_data)} records")
    return whale_addresses


def get_advanced_address_tags(address: str, 
                            analytics_data: Optional[Dict[str, Any]] = None,
                            transaction_data: Optional[List[Dict]] = None) -> List[str]:
    """
    Enhanced address tagging that incorporates whale identification and analytics data.
    
    Args:
        address: The address to analyze
        analytics_data: Optional analytics platform data
        transaction_data: Optional transaction history
    
    Returns:
        List of tags for the address
    """
    tags = []
    
    # Get existing classification tags
    existing_tags = _get_existing_address_tags(address)
    tags.extend(existing_tags)
    
    # Apply whale scoring
    whale_analysis = score_address_as_whale(address, transaction_data, analytics_data)
    
    # Add whale-related tags
    whale_classification = whale_analysis.get('whale_classification')
    if whale_classification != 'not_whale':
        tags.append(f'whale_{whale_classification}')
    
    whale_score = whale_analysis.get('whale_score', 0)
    if whale_score > 0.8:
        tags.append('high_confidence_whale')
    elif whale_score > 0.6:
        tags.append('probable_whale')
    elif whale_score > 0.4:
        tags.append('possible_whale')
    
    # Add analytics platform tags
    if analytics_data:
        for platform, data in analytics_data.items():
            if 'nansen' in platform.lower():
                tags.append('nansen_tracked')
            elif 'dune' in platform.lower():
                tags.append('dune_identified')
            elif 'glassnode' in platform.lower():
                tags.append('glassnode_tracked')
            elif 'arkham' in platform.lower():
                tags.append('arkham_tracked')
    
    # Add signal-based tags
    signals = whale_analysis.get('positive_whale_signals', [])
    for signal in signals:
        if 'high_volume' in signal:
            tags.append('high_volume_trader')
        elif 'defi' in signal:
            tags.append('defi_power_user')
        elif 'institutional' in signal:
            tags.append('institutional_actor')
    
    # Remove duplicates and return
    return list(set(tags))


def _get_existing_address_tags(address: str) -> List[str]:
    """Get existing tags for an address from current classification system."""
    tags = []
    
    # Check existing address classifications
    if address in known_exchange_addresses:
        tags.append('exchange')
        tags.append(f'exchange_{known_exchange_addresses[address]}')
    
    if address in DEX_ADDRESSES:
        tags.append('dex')
        tags.append(f'dex_{DEX_ADDRESSES[address]}')
    
    if address in MARKET_MAKER_ADDRESSES:
        tags.append('market_maker')
        tags.append(f'mm_{MARKET_MAKER_ADDRESSES[address]}')
    
    return tags
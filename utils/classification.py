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
    
    # First, if both sides are exchanges, we don't want to count this as a client trade.
    if from_addr in known_exchange_addresses and to_addr in known_exchange_addresses:
        return ("transfer", 0)
    
    # If the tokens come from an exchange address (and go to a non-exchange),
    # that means the exchange is sending tokens to the client → client is buying.
    if from_addr in known_exchange_addresses and to_addr not in known_exchange_addresses:
        return ("buy", 4)
    
    # If the tokens go to an exchange address (and come from a non-exchange),
    # then the client is selling tokens into the exchange.
    if to_addr in known_exchange_addresses and from_addr not in known_exchange_addresses:
        return ("sell", 4)
    
    # Check for market maker addresses
    if from_addr in MARKET_MAKER_ADDRESSES:
        return ("buy", 3)
    if to_addr in MARKET_MAKER_ADDRESSES:
        return ("sell", 3)
    
    # Fallback: use additional heuristics.
    confidence_score = 0
    classification = "unknown"
    
    from_chars = analyze_address_characteristics(from_addr)
    to_chars = analyze_address_characteristics(to_addr)
    
    if from_chars.get("is_exchange", False) and not to_chars.get("is_exchange", False):
        confidence_score += 2
        classification = "buy"
    elif to_chars.get("is_exchange", False) and not from_chars.get("is_exchange", False):
        confidence_score += 2
        classification = "sell"
    
    try:
        usd_value = float(amount) if amount is not None else 0
    except (ValueError, AttributeError):
        usd_value = 0

    if usd_value > 3000:
        confidence_score += 1

    # Fallback for transfers or unknown classifications
    if classification == "unknown" or confidence_score < 1:
        # Simple heuristic: for transfers, just default to "buy"
        classification = "buy"
        confidence_score = 1
        
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

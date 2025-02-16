from typing import Tuple, Optional
from data.addresses import (
    known_exchange_addresses,
    DEX_ADDRESSES,
    MARKET_MAKER_ADDRESSES,
    solana_exchange_addresses,
    xrp_exchange_addresses
)
from models.classes import DefiLlamaData
from data.addresses import SOLANA_DEX_ADDRESSES
from utils.helpers import get_protocol_slug, get_dex_name, is_significant_tvl_movement

def classify_erc20_transfer(tx_from, tx_to, token_symbol):
    """Enhanced ERC-20 transfer classification"""
    from_lower = tx_from.lower()
    to_lower = tx_to.lower()
    
    # Direct address checks
    if from_lower in known_exchange_addresses:
        return "buy"
    if to_lower in known_exchange_addresses:
        return "sell"
        
    # Pattern matching
    exchange_patterns = {
        "binance", "coinbase", "kraken", "huobi", "okex", "ftx", 
        "bitfinex", "bittrex", "poloniex", "kucoin", "gemini",
        "exchange", "gate.io", "bybit", "bitstamp", "uniswap"
    }
    
    from_is_exchange = any(ex in from_lower for ex in exchange_patterns)
    to_is_exchange = any(ex in to_lower for ex in exchange_patterns)
    
    if from_is_exchange and not to_is_exchange:
        return "buy"
    elif to_is_exchange and not from_is_exchange:
        return "sell"
    
    return "transfer"  # Changed from "unknown" to "transfer"

def classify_xrp_transaction(txn):
    """Strict XRP transaction classification"""
    try:
        from_addr = txn.get("Account", "")
        to_addr = txn.get("Destination", "")
        amount = float(txn.get("Amount", "0")) / 1_000_000  # Convert to XRP
        
        # Check known exchange addresses first
        from_is_exchange = from_addr in xrp_exchange_addresses
        to_is_exchange = to_addr in xrp_exchange_addresses
        
        if from_is_exchange and not to_is_exchange:
            return "buy", amount
        elif to_is_exchange and not from_is_exchange:
            return "sell", amount
            
        # Look for clear exchange patterns
        if any(ex in from_addr.lower() for ex in ["exchange", "binance", "kraken", "coinbase"]):
            return "buy", amount
        elif any(ex in to_addr.lower() for ex in ["exchange", "binance", "kraken", "coinbase"]):
            return "sell", amount
            
        # If no clear classification, mark as transfer
        return "transfer", amount
            
    except Exception as e:
        print(f"Error in XRP classification: {e}")
        return "transfer", amount
    

def transaction_classifier(tx_from, tx_to, token_symbol=None, amount=None):
    """Unified transaction classification with confidence scoring"""
    confidence_score = 0
    classification = "unknown"
    
    # Basic exchange address check
    if tx_from.lower() in known_exchange_addresses:
        confidence_score += 2
        classification = "buy"
    elif tx_to.lower() in known_exchange_addresses:
        confidence_score += 2
        classification = "sell"

    # Check DEX interaction
    if tx_from.lower() in DEX_ADDRESSES or tx_to.lower() in DEX_ADDRESSES:
        confidence_score += 1
        dex_name = get_dex_name(tx_from) or get_dex_name(tx_to)
        if dex_name:
            # Initialize DeFiLlama client once needed
            defillama = DefiLlamaData()
            dex_data = defillama.get_dex_volume(dex_name)
            if dex_data and dex_data.get('dailyVolume', 0) > 1000000:
                confidence_score += 1

    # Check market maker interaction
    if tx_from.lower() in MARKET_MAKER_ADDRESSES or tx_to.lower() in MARKET_MAKER_ADDRESSES:
        confidence_score += 1

    # Protocol TVL check if we have token symbol and amount
    if token_symbol and amount:
        protocol_slug = get_protocol_slug(token_symbol)
        if protocol_slug:
            defillama = DefiLlamaData()
            protocol_data = defillama.get_protocol_tvl(protocol_slug)
            if protocol_data and is_significant_tvl_movement(protocol_data, amount):
                confidence_score += 1

    # Final classification
    if confidence_score >= 3:
        return classification, confidence_score
    elif confidence_score >= 1:
        return f"probable_{classification}", confidence_score
    else:
        return "unknown", confidence_score
    

def enhanced_solana_classification(owner, prev_owner=None, amount_change=0):
    """Enhanced Solana transaction classification with confidence scoring"""
    confidence_score = 0
    classification = "transfer"
    
    # Combine all Solana addresses
    all_solana_addresses = {
        **SOLANA_DEX_ADDRESSES,
        **solana_exchange_addresses
    }
    
    # Check if addresses are known
    owner_is_known = owner in all_solana_addresses
    prev_owner_is_known = prev_owner in all_solana_addresses if prev_owner else False
    
    # Basic exchange flow heuristic
    if owner_is_known and amount_change > 0:
        classification = "sell"
        confidence_score += 2
    elif owner_is_known and amount_change < 0:
        classification = "buy"
        confidence_score += 2
        
    # DEX interaction heuristic
    if owner in SOLANA_DEX_ADDRESSES:
        confidence_score += 1
        
    # Previous owner heuristic
    if prev_owner_is_known:
        confidence_score += 1
        
    # Return transfer if low confidence
    if confidence_score < 2:
        classification = "transfer"
        
    return classification, confidence_score
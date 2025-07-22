import requests
import time
from typing import Dict, List, Optional
from config.api_keys import ETHERSCAN_API_KEY
from config.settings import (
    GLOBAL_USD_THRESHOLD,
    last_processed_block,
    etherscan_buy_counts,
    etherscan_sell_counts,
    print_lock
)
from data.tokens import TOKENS_TO_MONITOR, TOKEN_PRICES
from utils.classification_final import WhaleIntelligenceEngine, comprehensive_stablecoin_analysis
from utils.base_helpers import safe_print, log_error
from utils.summary import record_transfer
from utils.summary import has_been_classified, mark_as_classified
from data.market_makers import MARKET_MAKER_ADDRESSES, FILTER_SETTINGS
from utils.dedup import deduplicator, get_dedup_stats, deduped_transactions, handle_event


def fetch_erc20_transfers(contract_address, sort="desc"):
    
    url = "https://api.etherscan.io/api"
    params = {
        "module": "account",
        "action": "tokentx",
        "contractaddress": contract_address,
        "startblock": 0,
        "endblock": 99999999,
        "sort": sort,
        "apikey": ETHERSCAN_API_KEY
    }
    try:
        safe_print(f"\n📡 Fetching ERC-20 transfers for contract: {contract_address}")
        safe_print(f"Full URL: {url}?{'&'.join([f'{k}={v}' for k,v in params.items()])}")
        
        r = requests.get(url, params=params, timeout=20)
        data = r.json()
        
        if data.get("status") == "1":
            transfers = data.get("result", [])
            safe_print(f"✅ Found {len(transfers)} transfers")
            # Print a sample transaction
            if transfers:
                sample = transfers[0]
                safe_print(f"Sample transfer value: {sample.get('value', 'N/A')}")
                safe_print(f"Sample transfer block: {sample.get('blockNumber', 'N/A')}")
                
        else:
            msg = data.get("message", "No message")
            safe_print(f"❌ Etherscan API error: {msg}")
            # Print the full response for debugging
            safe_print(f"Full response: {data}")
        return data.get("result", [])
    
    except Exception as e:
        error_msg = f"❌ Error fetching transfers: {str(e)}"
        safe_print(error_msg)
        log_error(error_msg)
        return []


# In ethereum.py

def print_new_erc20_transfers():
    """Print new ERC-20 transfers with fixed transaction classification"""
    global last_processed_block
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    safe_print(f"\n[{current_time}] 🔍 Checking ERC-20 transfers...")
    
    for symbol, info in TOKENS_TO_MONITOR.items():
        contract = info["contract"]
        decimals = info["decimals"]
        price = TOKEN_PRICES.get(symbol, 0)
        
        if price == 0:
            safe_print(f"Skipping {symbol} - no price data")
            continue
            
        transfers = fetch_erc20_transfers(contract, sort="desc")
        if not transfers:
            continue
            
        new_transfers = []
        for tx in transfers:
            block_num = int(tx["blockNumber"])
            if block_num <= last_processed_block.get(symbol, 0):
                break
            new_transfers.append(tx)
            
        if new_transfers:
            highest_block = max(int(t["blockNumber"]) for t in new_transfers)
            last_processed_block[symbol] = max(last_processed_block.get(symbol, 0), highest_block)
            
        for tx in reversed(new_transfers):
            try:
                raw_value = int(tx["value"])
                token_amount = raw_value / (10 ** decimals)
                estimated_usd = token_amount * price
                
                if estimated_usd >= GLOBAL_USD_THRESHOLD:
                    from_addr = tx["from"]
                    to_addr = tx["to"]
                    tx_hash = tx["hash"]
                    
                    # Create event for deduplication
                    event = {
                        "blockchain": "ethereum",
                        "tx_hash": tx_hash,
                        "from": from_addr,
                        "to": to_addr,
                        "symbol": symbol,
                        "amount": token_amount,
                        "estimated_usd": estimated_usd,
                        "block_number": int(tx["blockNumber"])
                    }

                    # Process through the universal processor
                    from utils.classification_final import process_and_enrich_transaction
                    
                    enriched_transaction = process_and_enrich_transaction(event)
                    
                    # Add to deduplication system for main monitor display
                    if enriched_transaction:
                        # Add enriched data back to the event
                        event.update({
                            'classification': enriched_transaction.get('classification', 'UNKNOWN'),
                            'confidence': enriched_transaction.get('confidence_score', 0),
                            'whale_signals': enriched_transaction.get('whale_signals', []),
                            'whale_score': enriched_transaction.get('whale_score', 0),
                            'is_whale_transaction': enriched_transaction.get('is_whale_transaction', False),
                            'usd_value': estimated_usd,
                            'source': 'etherscan'
                        })
                        
                        # Add to main monitoring system
                        handle_event(event)
                    
                    classification = enriched_transaction.get('classification', 'UNKNOWN').lower()
                    confidence = enriched_transaction.get('confidence_score', 0)

                    # Always update counters regardless of confidence
                    if classification == "buy":
                        etherscan_buy_counts[symbol] += 1
                    elif classification == "sell":
                        etherscan_sell_counts[symbol] += 1
                    
                    timestamp = int(tx["timeStamp"])
                    human_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
                    
                    # Print with enhanced information
                    whale_indicator = " 🐋" if enriched_transaction.get('is_whale_transaction') else ""
                    safe_print(f"\n[{symbol} | ${estimated_usd:,.2f} USD] Block {tx['blockNumber']} | Tx {tx_hash}{whale_indicator}")
                    safe_print(f"  Time: {human_time}")
                    safe_print(f"  From: {from_addr}")
                    safe_print(f"  To:   {to_addr}")
                    safe_print(f"  Amount: {token_amount:,.2f} {symbol} (~${estimated_usd:,.2f} USD)")
                    safe_print(f"  Classification: {classification.upper()} (confidence: {confidence:.2f})")
                    
                    if enriched_transaction.get('whale_classification'):
                        safe_print(f"  Whale Analysis: {enriched_transaction['whale_classification']}")
                    
                    # Record transfer for volume tracking
                    record_transfer(symbol, token_amount, from_addr, to_addr, tx_hash)
                    
                    # TODO: Store enriched_transaction in Supabase here
                    
            except Exception as e:
                error_msg = f"Error processing {symbol} transfer: {str(e)}"
                safe_print(error_msg)
                log_error(error_msg)
                continue


# chains/ethereum.py
# Add this at the end of the file or update the existing function

def test_etherscan_connection():
    """Test Etherscan API connection"""
    url = "https://api.etherscan.io/api"
    params = {
        "module": "stats",
        "action": "ethsupply",
        "apikey": ETHERSCAN_API_KEY
    }
    try:
        safe_print("Testing Etherscan API connection...")
        r = requests.get(url, params=params, timeout=20)
        data = r.json()
        if data.get("status") == "1":
            safe_print("✅ Etherscan API connection successful")
            return True
        else:
            safe_print(f"❌ Etherscan API error: {data.get('message', 'No message')}")
            return False
    except Exception as e:
        error_msg = f"❌ Error connecting to Etherscan: {e}"
        safe_print(error_msg)
        log_error(error_msg)
        return False
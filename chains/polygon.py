import time
import requests

from config.api_keys import POLYGONSCAN_API_KEY
from config.settings import (
    polygon_last_processed_block,
    print_lock,
    GLOBAL_USD_THRESHOLD
)
from data.tokens import POLYGON_TOKENS_TO_MONITOR, TOKEN_PRICES
from utils.base_helpers import safe_print, log_error

def fetch_polygon_erc20_transfers(contract_address, start_block, sort="asc"):
    """
    Fetch ERC-20 transfer events for a token contract on Polygon.
    """
    base_url = "https://api.polygonscan.com/api"
    results = []
    page = 1
    offset = 100 

    while True:
        params = {
            "module": "account",
            "action": "tokentx",
            "contractaddress": contract_address,
            "startblock": start_block,
            "endblock": 99999999,
            "page": page,
            "offset": offset,
            "sort": sort,
            "apikey": POLYGONSCAN_API_KEY
        }
        try:
            resp = requests.get(base_url, params=params, timeout=15)
            resp.raise_for_status()
        except requests.RequestException as e:
            error_msg = f"[Polygon] API request error for token {contract_address}: {e}"
            safe_print(error_msg)
            log_error(error_msg)
            break

        data = resp.json()
        if data.get("status") != "1" or not data.get("result"):
            break

        transfers = data.get("result", [])
        results.extend(transfers)

        if len(transfers) < offset:
            break
        
        page += 1
        time.sleep(0.2) 
    
    return results

def print_new_polygon_transfers():
    """
    Polls and prints new ERC-20 transfers for monitored Polygon tokens.
    This function will be updated in Phase 2 to call the universal processor.
    """
    global polygon_last_processed_block
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    safe_print(f"\n[{current_time}] 🔍 Checking Polygon ERC-20 transfers...")

    for symbol, info in POLYGON_TOKENS_TO_MONITOR.items():
        contract = info["contract"]
        decimals = info["decimals"]
        price = TOKEN_PRICES.get(symbol, 0)

        if price == 0:
            safe_print(f"Skipping {symbol} on Polygon - no price data")
            continue

        last_block = polygon_last_processed_block.get(symbol, 0)
        
        # Initialize last block if it's 0
        if last_block == 0:
            try:
                proxy_url = "https://api.polygonscan.com/api"
                params = {"module": "proxy", "action": "eth_blockNumber", "apikey": POLYGONSCAN_API_KEY}
                resp = requests.get(proxy_url, params=params, timeout=10)
                data = resp.json()
                if resp.status_code == 200 and "result" in data:
                    last_block = int(data["result"], 16)
                    polygon_last_processed_block[symbol] = last_block
                    safe_print(f"Initialized last block for {symbol} on Polygon to {last_block}")
            except Exception as e:
                error_msg = f"[Polygon] Warning: Could not fetch latest block number for {symbol}. Error: {e}"
                safe_print(error_msg)
                log_error(error_msg)
        
        start_block = last_block + 1
        transfers = fetch_polygon_erc20_transfers(contract, start_block)
        
        if not transfers:
            continue
            
        new_transfers = transfers # Already filtered by start_block in API call
        
        if new_transfers:
            highest_block = max(int(t["blockNumber"]) for t in new_transfers)
            polygon_last_processed_block[symbol] = max(polygon_last_processed_block.get(symbol, 0), highest_block)
            
        for tx in new_transfers:
            try:
                raw_value = int(tx["value"])
                token_amount = raw_value / (10 ** decimals)
                estimated_usd = token_amount * price
                
                if estimated_usd >= GLOBAL_USD_THRESHOLD:
                    from_addr = tx["from"]
                    to_addr = tx["to"]
                    tx_hash = tx["hash"]
                    
                    event = {
                        "blockchain": "polygon",
                        "tx_hash": tx_hash,
                        "from": from_addr,
                        "to": to_addr,
                        "symbol": symbol,
                        "amount": token_amount,
                        "estimated_usd": estimated_usd,
                        "block_number": int(tx["blockNumber"]),
                        "raw_tx": tx,
                        "decimals": decimals,
                    }
                    
                    # Process through the universal processor
                    from utils.classification_final import process_and_enrich_transaction
                    
                    enriched_transaction = process_and_enrich_transaction(event)
                    
                    if enriched_transaction:
                        # Add enriched data back to the event for main monitor display
                        event.update({
                            'classification': enriched_transaction.get('classification', 'UNKNOWN'),
                            'confidence': enriched_transaction.get('confidence_score', 0),
                            'whale_signals': enriched_transaction.get('whale_signals', []),
                            'whale_score': enriched_transaction.get('whale_score', 0),
                            'is_whale_transaction': enriched_transaction.get('is_whale_transaction', False),
                            'usd_value': estimated_usd,
                            'source': 'polygonscan'
                        })
                        
                        # Add to main monitoring system
                        from utils.dedup import handle_event
                        handle_event(event)
                        
                        classification = enriched_transaction.get('classification', 'UNKNOWN').lower()
                        
                        # Update counters
                        from config.settings import polygon_buy_counts, polygon_sell_counts
                        if classification == "buy":
                            polygon_buy_counts[symbol] += 1
                        elif classification == "sell":
                            polygon_sell_counts[symbol] += 1
                        
                        timestamp = int(tx["timeStamp"])
                        human_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
                        
                        # Print with enhanced information
                        whale_indicator = " 🐋" if enriched_transaction.get('is_whale_transaction') else ""
                        safe_print(f"\n[POLYGON - {symbol} | ${estimated_usd:,.2f} USD] Block {tx['blockNumber']} | Tx {tx_hash}{whale_indicator}")
                        safe_print(f"  Time: {human_time}")
                        safe_print(f"  From: {from_addr}")
                        safe_print(f"  To:   {to_addr}")
                        safe_print(f"  Amount: {token_amount:,.2f} {symbol} (~${estimated_usd:,.2f} USD)")
                        safe_print(f"  Classification: {classification.upper()} (confidence: {enriched_transaction['confidence']:2f})")
                        
                        if enriched_transaction.get('whale_classification'):
                            safe_print(f"  Whale Analysis: {enriched_transaction['whale_classification']}")
                        
                        # TODO: Store enriched_transaction in Supabase here

            except Exception as e:
                error_msg = f"Error processing {symbol} transfer on Polygon: {str(e)}"
                safe_print(error_msg)
                log_error(error_msg)
                continue
        
        time.sleep(0.2) # Rate limit between tokens
        
def test_polygonscan_connection():
    """Test Polygonscan API connection"""
    url = "https://api.polygonscan.com/api"
    params = {
        "module": "stats",
        "action": "maticprice",
        "apikey": POLYGONSCAN_API_KEY
    }
    try:
        safe_print("Testing Polygonscan API connection...")
        r = requests.get(url, params=params, timeout=20)
        data = r.json()
        if data.get("status") == "1":
            safe_print("✅ Polygonscan API connection successful")
            return True
        else:
            safe_print(f"❌ Polygonscan API error: {data.get('message', 'No message')}")
            return False
    except Exception as e:
        error_msg = f"❌ Error connecting to Polygonscan: {e}"
        safe_print(error_msg)
        log_error(error_msg)
        return False 
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
from utils.classification import transaction_classifier
from utils.base_helpers import safe_print
from utils.summary import record_transfer


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
        safe_print(f"❌ Error fetching transfers: {str(e)}")
        return []


def print_new_erc20_transfers():
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
            raw_value = int(tx["value"])
            token_amount = raw_value / (10 ** decimals)
            estimated_usd = token_amount * price
            
                    # In print_new_erc20_transfers function, replace the classification section with:
        if estimated_usd >= GLOBAL_USD_THRESHOLD:
            tx_from = tx["from"]
            tx_to = tx["to"]
            
            # Use the enhanced classifier
            classification, confidence = transaction_classifier(
                tx_from=tx_from,
                tx_to=tx_to,
                token_symbol=symbol,
                amount=token_amount
            )
            
            if "probable" not in classification and confidence >= 3:
                if classification == "buy":
                    etherscan_buy_counts[symbol] += 1
                elif classification == "sell":
                    etherscan_sell_counts[symbol] += 1
                
                record_transfer(symbol, token_amount, tx["from"], tx["to"])


            
            timestamp = int(tx["timeStamp"])
            human_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
            # Print with enhanced information
            safe_print(f"\n[{symbol} | ${estimated_usd:,.2f} USD] Block {tx['blockNumber']} | Tx {tx['hash']}")
            safe_print(f"  Time: {human_time}")
            safe_print(f"  From: {tx_from}")
            safe_print(f"  To:   {tx_to}")
            safe_print(f"  Amount: {token_amount:,.2f} {symbol} (~${estimated_usd:,.2f} USD)")
            safe_print(f"  Classification: {classification} (confidence: {confidence})")


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
        safe_print(f"❌ Error connecting to Etherscan: {e}")
        return False
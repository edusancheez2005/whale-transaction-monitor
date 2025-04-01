import re
import time
from datetime import datetime

def parse_transaction_log(log_text):
    """Parse transaction log data into structured format"""
    transactions = []
    
    # Pattern to match transaction entries
    pattern = r'\[(\w+) \| \$([\d,]+\.\d+) USD\] Block (\d+) \| Tx (0x[a-f0-9]+)\s+Time: (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+From: (0x[a-f0-9]+)\s+To:\s+(0x[a-f0-9]+)\s+Amount: ([\d,]+\.?\d*) (\w+) \(~\$([\d,]+\.\d+) USD\)\s+Classification: (\w+) \(confidence: (\d+)\)'
    
    # Find all matches
    matches = re.findall(pattern, log_text)
    
    for match in matches:
        symbol, usd_str, block, tx_hash, time_str, from_addr, to_addr, amount_str, token, usd_value_str, classification, confidence = match
        
        # Convert timestamp
        timestamp = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S').timestamp()
        
        # Clean numeric values
        usd_value = float(usd_value_str.replace(',', ''))
        amount = float(amount_str.replace(',', ''))
        
        # Create transaction object
        tx = {
            "blockchain": "ethereum",
            "tx_hash": tx_hash,
            "from": from_addr,
            "to": to_addr,
            "symbol": symbol,
            "amount": amount,
            "usd_value": usd_value,
            "block_number": int(block),
            "timestamp": timestamp,
            "classification": classification.lower(),
            "confidence": int(confidence)
        }
        
        transactions.append(tx)
    
    return transactions
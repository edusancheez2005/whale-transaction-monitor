#!/usr/bin/env python3
"""
script to remove duplicate transactions from supabase
"""

import os
import sys
from supabase import create_client, Client
from config import api_keys

def cleanup_duplicates():
    """remove duplicate transactions keeping highest confidence entries"""
    
    print("connecting to supabase...")
    supabase: Client = create_client(
        api_keys.SUPABASE_URL,
        api_keys.SUPABASE_SERVICE_ROLE_KEY
    )
    
    print("fetching all transactions...")
    result = supabase.table('whale_transactions')\
        .select('*')\
        .order('transaction_hash')\
        .execute()
    
    transactions = result.data
    print(f"total transactions: {len(transactions)}")
    
    # deduplicate: keep highest confidence for each hash
    unique_txs = {}
    duplicates_found = 0
    
    for tx in transactions:
        tx_hash = tx['transaction_hash']
        
        if tx_hash not in unique_txs:
            unique_txs[tx_hash] = tx
        else:
            duplicates_found += 1
            existing = unique_txs[tx_hash]
            
            # keep the one with higher confidence + whale_score
            current_score = float(tx.get('confidence', 0)) + float(tx.get('whale_score', 0))
            existing_score = float(existing.get('confidence', 0)) + float(existing.get('whale_score', 0))
            
            if current_score > existing_score:
                unique_txs[tx_hash] = tx
    
    print(f"unique transactions: {len(unique_txs)}")
    print(f"duplicates found: {duplicates_found}")
    
    if duplicates_found == 0:
        print("no duplicates found!")
        return
    
    # confirm before proceeding
    response = input(f"\nremove {duplicates_found} duplicates? (yes/no): ")
    if response.lower() != 'yes':
        print("operation cancelled")
        return
    
    print("\ndeleting all transactions...")
    supabase.table('whale_transactions').delete().neq('id', -1).execute()
    
    print("re-inserting unique transactions...")
    batch_size = 100
    unique_list = list(unique_txs.values())
    
    for i in range(0, len(unique_list), batch_size):
        batch = unique_list[i:i+batch_size]
        
        # remove id field to let supabase generate new ones
        for tx in batch:
            if 'id' in tx:
                del tx['id']
        
        supabase.table('whale_transactions').insert(batch).execute()
        print(f"  inserted batch {i//batch_size + 1}/{(len(unique_list)-1)//batch_size + 1}")
    
    print(f"\ncleanup complete!")
    print(f"  before: {len(transactions)} transactions")
    print(f"  after: {len(unique_txs)} transactions")
    print(f"  removed: {duplicates_found} duplicates")

if __name__ == "__main__":
    try:
        cleanup_duplicates()
    except Exception as e:
        print(f"error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


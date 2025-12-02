#!/usr/bin/env python3
"""
🐋 WHALE PERSPECTIVE MIGRATION SCRIPT

This script migrates existing whale_transactions to use the new whale perspective columns.
It identifies the actual whale (non-CEX party) and properly classifies transactions.

Usage:
    python migrate_whale_perspective.py [--dry-run] [--limit N]
    
Options:
    --dry-run: Preview changes without updating database
    --limit N: Process only first N transactions (for testing)
"""

import sys
import argparse
from typing import Dict, Optional
from supabase import create_client
from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
from data.addresses import DEX_ADDRESSES

TRADE_COUNTERPARTY_TYPES = {'CEX', 'DEX'}

# Initialize Supabase
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def determine_whale_perspective(from_addr: str, to_addr: str, blockchain: str) -> Dict:
    """
    Determine whale perspective for a transaction.
    
    Returns dict with:
    - whale_address: The actual whale (non-CEX party)
    - counterparty_address: The CEX/DEX they traded with
    - counterparty_type: CEX, DEX, or EOA
    - is_cex_transaction: Boolean flag
    """
    try:
        # Query address types from Supabase
        from_data = None
        to_data = None
        
        if from_addr:
            result = supabase.table('addresses')\
                .select('address, address_type, label, entity_name')\
                .eq('address', from_addr.lower())\
                .eq('blockchain', blockchain)\
                .execute()
            from_data = result.data[0] if result.data else None
        
        if to_addr:
            result = supabase.table('addresses')\
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
        from_is_cex = from_type in ['CEX Wallet', 'exchange', 'Exchange Wallet'] or \
                      'binance' in from_label.lower() or 'coinbase' in from_label.lower() or \
                      'kraken' in from_label.lower() or 'okx' in from_label.lower()
        to_is_cex = to_type in ['CEX Wallet', 'exchange', 'Exchange Wallet'] or \
                    'binance' in to_label.lower() or 'coinbase' in to_label.lower() or \
                    'kraken' in to_label.lower() or 'okx' in to_label.lower()
        from_is_dex = from_type in ['DEX', 'dex_router', 'DEX Router'] or from_addr.lower() in DEX_ADDRESSES
        to_is_dex = to_type in ['DEX', 'dex_router', 'DEX Router'] or to_addr.lower() in DEX_ADDRESSES
        
        # Determine whale and counterparty
        if from_is_cex and not to_is_cex:
            # CEX → User: User is the whale (receiving/buying)
            return {
                'whale_address': to_addr,
                'counterparty_address': from_addr,
                'counterparty_type': 'CEX',
                'is_cex_transaction': True,
                'from_label': from_label,
                'to_label': to_label
            }
        elif to_is_cex and not from_is_cex:
            # User → CEX: User is the whale (sending/selling)
            return {
                'whale_address': from_addr,
                'counterparty_address': to_addr,
                'counterparty_type': 'CEX',
                'is_cex_transaction': True,
                'from_label': from_label,
                'to_label': to_label
            }
        elif from_is_dex and not to_is_dex:
            # DEX → User: User is the whale (receiving/buying)
            return {
                'whale_address': to_addr,
                'counterparty_address': from_addr,
                'counterparty_type': 'DEX',
                'is_cex_transaction': False,
                'from_label': from_label,
                'to_label': to_label
            }
        elif to_is_dex and not from_is_dex:
            # User → DEX: User is the whale (sending/selling)
            return {
                'whale_address': from_addr,
                'counterparty_address': to_addr,
                'counterparty_type': 'DEX',
                'is_cex_transaction': False,
                'from_label': from_label,
                'to_label': to_label
            }
        elif from_is_cex and to_is_cex:
            # CEX → CEX: Internal transfer
            return {
                'whale_address': None,
                'counterparty_address': None,
                'counterparty_type': 'CEX_INTERNAL',
                'is_cex_transaction': True,
                'from_label': from_label,
                'to_label': to_label
            }
        else:
            # Wallet → Wallet: from_address is initiator
            return {
                'whale_address': from_addr,
                'counterparty_address': to_addr,
                'counterparty_type': 'EOA',
                'is_cex_transaction': False,
                'from_label': from_label,
                'to_label': to_label
            }
    
    except Exception as e:
        print(f"   ⚠️  Error determining perspective: {e}")
        # Fallback
        return {
            'whale_address': from_addr,
            'counterparty_address': to_addr,
            'counterparty_type': 'EOA',
            'is_cex_transaction': False,
            'from_label': '',
            'to_label': ''
        }

def classify_from_whale_perspective(
    whale_address: Optional[str],
    from_address: Optional[str],
    to_address: Optional[str],
    counterparty_type: Optional[str],
    original_classification: str
) -> str:
    """Recompute BUY/SELL/TRANSFER/DEFI based on token flow."""
    whale_addr = (whale_address or '').lower()
    from_addr = (from_address or '').lower()
    to_addr = (to_address or '').lower()
    counterparty = (counterparty_type or '').upper()
    is_trade = counterparty in TRADE_COUNTERPARTY_TYPES

    if not whale_addr:
        return original_classification

    if whale_addr == to_addr:
        return 'BUY' if is_trade else 'TRANSFER'

    if whale_addr == from_addr:
        return 'SELL' if is_trade else 'TRANSFER'

    if original_classification == 'DEFI':
        return 'DEFI'

    if is_trade:
        return original_classification if original_classification in {'BUY', 'SELL'} else 'DEFI'

    if original_classification == 'TRANSFER':
        return 'TRANSFER'

    return 'DEFI'


def migrate_transactions(dry_run: bool = False, limit: Optional[int] = None):
    """
    Migrate all existing whale_transactions to use new whale perspective columns.
    """
    print("=" * 80)
    print("🐋 WHALE PERSPECTIVE MIGRATION")
    print("=" * 80)
    print()
    
    if dry_run:
        print("🔍 DRY RUN MODE - No changes will be made to database")
        print()
    
    # Fetch transactions that need migration (where whale_address is NULL)
    print("📊 Fetching transactions...")
    
    # First, get total count
    count_result = supabase.table('whale_transactions')\
        .select('id', count='exact')\
        .is_('whale_address', 'null')\
        .execute()
    total_count = count_result.count
    print(f"   Total transactions needing migration: {total_count}")
    
    # Fetch in batches to handle large datasets
    batch_size = 1000
    transactions = []
    
    if limit:
        # User specified a limit
        print(f"   User limit: {limit} transactions")
        result = supabase.table('whale_transactions')\
            .select('id, transaction_hash, from_address, to_address, blockchain, classification, token_symbol, usd_value')\
            .is_('whale_address', 'null')\
            .order('timestamp', desc=True)\
            .limit(limit)\
            .execute()
        transactions = result.data
    else:
        # Fetch all in batches
        offset = 0
        while True:
            print(f"   Fetching batch {offset // batch_size + 1} (records {offset} to {offset + batch_size})...")
            result = supabase.table('whale_transactions')\
                .select('id, transaction_hash, from_address, to_address, blockchain, classification, token_symbol, usd_value')\
                .is_('whale_address', 'null')\
                .order('timestamp', desc=True)\
                .range(offset, offset + batch_size - 1)\
                .execute()
            
            if not result.data:
                break
            
            transactions.extend(result.data)
            offset += batch_size
            
            # Stop if we got less than batch_size (last batch)
            if len(result.data) < batch_size:
                break
    
    print(f"   Found {len(transactions)} transactions to migrate")
    print()
    
    if not transactions:
        print("✅ No transactions to migrate!")
        return
    
    # Statistics
    stats = {
        'total': len(transactions),
        'cex_transactions': 0,
        'dex_transactions': 0,
        'eoa_transactions': 0,
        'cex_internal': 0,
        'updated': 0,
        'skipped': 0,
        'errors': 0
    }
    
    # Process each transaction
    print("🔄 Processing transactions...")
    print()
    
    for i, tx in enumerate(transactions, 1):
        tx_hash = tx['transaction_hash']
        from_addr = tx['from_address']
        to_addr = tx['to_address']
        blockchain = tx['blockchain']
        classification = tx['classification']
        token_symbol = tx['token_symbol']
        usd_value = tx['usd_value']
        
        # Show progress every 10 transactions
        if i % 10 == 0:
            print(f"   Progress: {i}/{len(transactions)} ({(i/len(transactions)*100):.1f}%)")
        
        try:
            # Determine whale perspective
            perspective = determine_whale_perspective(from_addr, to_addr, blockchain)
            
            # Track stats
            if perspective['counterparty_type'] == 'CEX':
                stats['cex_transactions'] += 1
            elif perspective['counterparty_type'] == 'DEX':
                stats['dex_transactions'] += 1
            elif perspective['counterparty_type'] == 'CEX_INTERNAL':
                stats['cex_internal'] += 1
                stats['skipped'] += 1
                print(f"   ⏭️  Skipping CEX internal: {tx_hash[:16]}...")
                continue
            else:
                stats['eoa_transactions'] += 1
            
            # Update data
            update_data = {
                'whale_address': perspective['whale_address'],
                'counterparty_address': perspective['counterparty_address'],
                'counterparty_type': perspective['counterparty_type'],
                'is_cex_transaction': perspective['is_cex_transaction'],
                'from_label': perspective['from_label'],
                'to_label': perspective['to_label'],
                'classification': classify_from_whale_perspective(
                    perspective['whale_address'],
                    from_addr,
                    to_addr,
                    perspective['counterparty_type'],
                    classification
                )
            }
            
            if not dry_run:
                # Update the transaction
                supabase.table('whale_transactions')\
                    .update(update_data)\
                    .eq('id', tx['id'])\
                    .execute()
                stats['updated'] += 1
            else:
                # Just show what would be updated
                whale_short = perspective['whale_address'][:10] if perspective['whale_address'] else 'N/A'
                print(f"   Would update: {token_symbol:6s} ${usd_value:10,.0f} | {classification:8s} → {update_data['classification']:8s} | Whale: {whale_short}... | Type: {perspective['counterparty_type']}")
                stats['updated'] += 1
        
        except Exception as e:
            print(f"   ❌ Error processing {tx_hash[:16]}...: {e}")
            stats['errors'] += 1
    
    print()
    print("=" * 80)
    print("📊 MIGRATION SUMMARY")
    print("=" * 80)
    print(f"Total transactions:     {stats['total']}")
    print(f"✅ Updated:              {stats['updated']}")
    print(f"⏭️  Skipped (CEX internal): {stats['skipped']}")
    print(f"❌ Errors:               {stats['errors']}")
    print()
    print("Transaction Types:")
    print(f"   CEX transactions:    {stats['cex_transactions']}")
    print(f"   DEX transactions:    {stats['dex_transactions']}")
    print(f"   EOA transactions:    {stats['eoa_transactions']}")
    print(f"   CEX internal:        {stats['cex_internal']}")
    print()
    
    if dry_run:
        print("🔍 This was a DRY RUN - no changes were made")
        print("   Run without --dry-run to apply changes")
    else:
        print("✅ Migration complete!")
        print()
        print("Next steps:")
        print("1. Verify data: SELECT whale_address, counterparty_type, COUNT(*) FROM whale_transactions GROUP BY whale_address, counterparty_type;")
        print("2. Update your Sonar UI to query by whale_address")
        print("3. Add filter to exclude CEX addresses from whale lists")
    print()

def reclassify_transactions(dry_run: bool = False, limit: Optional[int] = None):
    """
    Recompute BUY/SELL/TRANSFER classifications for existing rows using whale perspective.
    """
    print("=" * 80)
    print("🔁 RECLASSIFYING EXISTING TRANSACTIONS")
    print("=" * 80)
    if dry_run:
        print("🔍 DRY RUN MODE - No database updates will be made\n")
    
    select_fields = 'id, transaction_hash, whale_address, from_address, to_address, counterparty_type, classification, token_symbol, usd_value'
    batch_size = 1000
    offset = 0
    processed = 0
    changed = 0
    
    while True:
        query = supabase.table('whale_transactions')\
            .select(select_fields)\
            .order('timestamp', desc=True)
        
        if limit:
            query = query.range(offset, min(offset + batch_size - 1, limit - 1))
        else:
            query = query.range(offset, offset + batch_size - 1)
        
        result = query.execute()
        rows = result.data or []
        if not rows:
            break
        
        for row in rows:
            processed += 1
            whale_address = row.get('whale_address')
            if not whale_address:
                continue
            
            new_classification = classify_from_whale_perspective(
                whale_address,
                row.get('from_address'),
                row.get('to_address'),
                row.get('counterparty_type'),
                row.get('classification', 'TRANSFER')
            )
            
            if new_classification == row.get('classification'):
                continue
            
            changed += 1
            if dry_run:
                print(f"   Would update {row['transaction_hash'][:10]}...: {row['classification']} → {new_classification}")
            else:
                supabase.table('whale_transactions')\
                    .update({'classification': new_classification})\
                    .eq('id', row['id'])\
                    .execute()
            
            if limit and processed >= limit:
                break
        
        offset += batch_size
        if limit and processed >= limit:
            break
    
    print(f"\nProcessed: {processed:,}")
    print(f"Updated : {changed:,}")
    if dry_run:
        print("🔍 DRY RUN COMPLETE - No database changes were made\n")
    else:
        print("✅ Reclassification complete!\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Migrate whale transactions to use whale perspective')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without updating database')
    parser.add_argument('--limit', type=int, help='Process only first N transactions')
    parser.add_argument('--reclassify', action='store_true', help='Recompute BUY/SELL classifications for all records')
    
    args = parser.parse_args()
    
    try:
        migrate_transactions(dry_run=args.dry_run, limit=args.limit)
        if args.reclassify:
            reclassify_transactions(dry_run=args.dry_run, limit=args.limit)
    except KeyboardInterrupt:
        print("\n\n⚠️  Migration interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Migration failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


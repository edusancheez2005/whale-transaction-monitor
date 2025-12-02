#!/usr/bin/env python3
"""
🛡️ NEAR-DUPLICATE CLEANUP SCRIPT
===================================

Cleans up existing near-duplicates in the whale_transactions table.

This script detects and removes:
- Mirror trades (BUY/SELL pairs within 10 seconds)
- Transfer shadows (BUY/TRANSFER or SELL/TRANSFER pairs)
- Counterparty mismatches (same trade reported differently)

Strategy: Keeps the EARLIEST transaction with HIGHEST confidence
"""

import sys
from datetime import datetime, timedelta
from collections import defaultdict
from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
from supabase import create_client

# Import detection logic from enhanced_monitor
from enhanced_monitor import (
    is_usd_value_match,
    detect_duplicate_pattern,
    NEAR_DUPE_TIME_WINDOW,
    NEAR_DUPE_SAFEGUARD_USD,
    should_merge_cross_entity
)


def cleanup_near_duplicates(dry_run=True, batch_size=1000):
    """
    Clean up near-duplicate transactions from the database.
    
    Args:
        dry_run: If True, only report what would be deleted (don't actually delete)
        batch_size: Number of transactions to process per batch
    """
    supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    
    print("\n" + "="*80)
    print("🛡️ NEAR-DUPLICATE CLEANUP")
    print("="*80)
    print(f"Mode: {'DRY RUN (no changes)' if dry_run else 'LIVE (will delete duplicates)'}")
    print(f"Batch size: {batch_size}")
    print("="*80 + "\n")
    
    # Get all transactions grouped by whale+token
    print("📊 Fetching ALL transactions from database (may take a minute)...")
    
    # Fetch all transactions in batches (Supabase has 1000 row limit per query)
    all_transactions = []
    offset = 0
    page_size = 1000
    
    while True:
        result = supabase.table('whale_transactions')\
            .select('transaction_hash, whale_address, token_symbol, classification, usd_value, timestamp, counterparty_type, counterparty_address, is_cex_transaction, confidence, from_address')\
            .order('timestamp', desc=False)\
            .range(offset, offset + page_size - 1)\
            .execute()
        
        if not result.data:
            break
        
        all_transactions.extend(result.data)
        offset += page_size
        
        # Print progress
        print(f"   Fetched {len(all_transactions):,} transactions...", end='\r')
        
        # If we got fewer than page_size, we're done
        if len(result.data) < page_size:
            break
    
    print(f"   Fetched {len(all_transactions):,} transactions...") # Final newline
    
    if not all_transactions:
        print("❌ No transactions found")
        return
    
    total_txs = len(all_transactions)
    print(f"✅ Loaded {total_txs:,} transactions total\n")
    
    # Use all_transactions instead of result.data from here on
    result.data = all_transactions
    
    # Normalize whale addresses for compatibility
    for tx in result.data:
        tx['whale_address'] = tx.get('whale_address') or tx.get('from_address', '')
    
    # Group by token symbol (enables cross-whale detection)
    print("🔍 Grouping transactions by token symbol...")
    groups = defaultdict(list)
    
    for tx in result.data:
        token = tx.get('token_symbol', '').upper()
        if not token:
            continue
        groups[token].append(tx)
    
    print(f"✅ Found {len(groups):,} token groups\n")
    
    # Detect duplicates
    print("🔍 Detecting near-duplicates...")
    duplicates_to_delete = []
    stats = defaultdict(int)
    
    for token, txs in groups.items():
        if len(txs) < 2:
            continue
        
        # Sort by timestamp
        txs.sort(key=lambda x: x.get('timestamp', ''))
        
        i = 0
        while i < len(txs):
            current = txs[i]
            current_timestamp = datetime.fromisoformat(current['timestamp'].replace('Z', '+00:00'))
            
            # Skip transactions with NULL usd_value
            if current.get('usd_value') is None:
                i += 1
                continue
            
            current_usd = float(current.get('usd_value', 0))
            
            # Skip safeguarded large transactions
            if current_usd > NEAR_DUPE_SAFEGUARD_USD:
                i += 1
                continue
            
            # Skip DEFI transactions
            if current.get('classification') == 'DEFI':
                i += 1
                continue
            
            # Check next transactions within time window
            j = i + 1
            while j < len(txs):
                next_tx = txs[j]
                next_timestamp = datetime.fromisoformat(next_tx['timestamp'].replace('Z', '+00:00'))
                
                time_diff = (next_timestamp - current_timestamp).total_seconds()
                
                # Stop if outside time window
                if time_diff > NEAR_DUPE_TIME_WINDOW:
                    break
                
                # Skip if next transaction has NULL usd_value
                if next_tx.get('usd_value') is None:
                    j += 1
                    continue
                
                next_usd = float(next_tx.get('usd_value', 0))
                
                # Check USD match
                if not is_usd_value_match(current_usd, next_usd):
                    j += 1
                    continue
                
                # Check pattern
                reason = detect_duplicate_pattern(
                    current.get('classification', ''),
                    next_tx.get('classification', ''),
                    current.get('counterparty_type', 'EOA'),
                    next_tx.get('counterparty_type', 'EOA'),
                    current.get('is_cex_transaction', False),
                    next_tx.get('is_cex_transaction', False)
                )
                
                if not reason:
                    j += 1
                    continue
                
                current_entry = {
                    'whale_address': current.get('whale_address'),
                    'counterparty_address': current.get('counterparty_address'),
                    'counterparty_type': current.get('counterparty_type', 'EOA'),
                    'is_cex_transaction': current.get('is_cex_transaction', False)
                }
                next_entry = {
                    'whale_address': next_tx.get('whale_address'),
                    'counterparty_address': next_tx.get('counterparty_address'),
                    'counterparty_type': next_tx.get('counterparty_type', 'EOA'),
                    'is_cex_transaction': next_tx.get('is_cex_transaction', False)
                }
                
                if not should_merge_cross_entity(current_entry, next_entry, reason):
                    j += 1
                    continue
                
                # Duplicate found! Keep the one with higher confidence
                current_confidence = float(current.get('confidence', 0))
                next_confidence = float(next_tx.get('confidence', 0))
                
                if next_confidence > current_confidence:
                    # Delete current, keep next
                    duplicates_to_delete.append({
                        'tx_hash': current['transaction_hash'],
                        'reason': reason,
                        'kept_tx': next_tx['transaction_hash'],
                        'time_diff': time_diff,
                        'usd_value': current_usd,
                        'token': token,
                        'pattern': f"{current.get('classification')} + {next_tx.get('classification')}"
                    })
                    stats[reason] += 1
                    stats['total'] += 1
                    # Move to next transaction
                    i = j
                    break
                else:
                    # Delete next, keep current
                    duplicates_to_delete.append({
                        'tx_hash': next_tx['transaction_hash'],
                        'reason': reason,
                        'kept_tx': current['transaction_hash'],
                        'time_diff': time_diff,
                        'usd_value': next_usd,
                        'token': token,
                        'pattern': f"{current.get('classification')} + {next_tx.get('classification')}"
                    })
                    stats[reason] += 1
                    stats['total'] += 1
                    # Remove from list and continue
                    txs.pop(j)
                    continue
                
                j += 1
            
            i += 1
    
    print(f"✅ Found {len(duplicates_to_delete):,} duplicates\n")
    
    # Print statistics
    if stats:
        print("📊 DUPLICATE BREAKDOWN:")
        print("-" * 80)
        for reason, count in sorted(stats.items()):
            if reason != 'total':
                print(f"  {reason:30s}: {count:,}")
        print("-" * 80)
        print(f"  {'TOTAL':30s}: {stats['total']:,}")
        print()
    
    # Show sample duplicates
    if duplicates_to_delete:
        print("📋 SAMPLE DUPLICATES (first 10):")
        print("-" * 80)
        for i, dupe in enumerate(duplicates_to_delete[:10], 1):
            print(f"\n{i}. Reason: {dupe['reason']}")
            print(f"   Delete: {dupe['tx_hash']}")
            print(f"   Keep:   {dupe['kept_tx']}")
            print(f"   Token:  {dupe['token']}")
            print(f"   USD:    ${dupe['usd_value']:,.2f}")
            print(f"   Time:   {dupe['time_diff']:.1f}s apart")
            print(f"   Pattern: {dupe['pattern']}")
        print()
    
    # Delete duplicates
    if duplicates_to_delete:
        if dry_run:
            print("🔍 DRY RUN - No changes made")
            print(f"   Would delete {len(duplicates_to_delete):,} transactions")
        else:
            print(f"🗑️  Deleting {len(duplicates_to_delete):,} duplicate transactions...")
            
            deleted = 0
            failed = 0
            
            for dupe in duplicates_to_delete:
                try:
                    supabase.table('whale_transactions')\
                        .delete()\
                        .eq('transaction_hash', dupe['tx_hash'])\
                        .execute()
                    deleted += 1
                    
                    if deleted % 100 == 0:
                        print(f"   Deleted {deleted:,}/{len(duplicates_to_delete):,}...")
                        
                except Exception as e:
                    print(f"   ❌ Failed to delete {dupe['tx_hash']}: {e}")
                    failed += 1
            
            print(f"\n✅ Deleted {deleted:,} duplicates")
            if failed > 0:
                print(f"❌ Failed {failed:,} deletions")
    
    print("\n" + "="*80)
    print("✅ CLEANUP COMPLETE")
    print("="*80)
    
    return stats


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Clean up near-duplicate whale transactions')
    parser.add_argument('--live', action='store_true', help='Actually delete duplicates (default is dry-run)')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for processing')
    
    args = parser.parse_args()
    
    try:
        cleanup_near_duplicates(dry_run=not args.live, batch_size=args.batch_size)
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


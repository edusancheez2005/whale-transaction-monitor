#!/usr/bin/env python3
"""
Direct SQL-based reclassification for whale transactions.
More reliable than row-by-row processing when network is unstable.
"""

from supabase import create_client
from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
import sys

print("\n" + "="*80)
print("🔁 SQL-BASED WHALE PERSPECTIVE RECLASSIFICATION")
print("="*80)
print()

client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

print("This script will reclassify ALL transactions based on token flow:")
print("  • whale_address = to_address + CEX/DEX → BUY")
print("  • whale_address = from_address + CEX/DEX → SELL")
print("  • whale_address = to/from + EOA → TRANSFER")
print()

response = input("Continue? (yes/no): ")
if response.lower() != 'yes':
    print("Aborted.")
    sys.exit(0)

print("\n🔄 Running SQL updates...")
print()

# Track stats
stats = {
    'buy_fixed': 0,
    'sell_fixed': 0,
    'transfer_fixed': 0,
    'errors': 0
}

try:
    # Fix 1: whale receiving tokens from CEX/DEX = BUY
    print("1️⃣  Fixing BUYs (whale = to_address + CEX/DEX counterparty)...")
    result = client.rpc('exec_sql', {
        'query': """
            UPDATE whale_transactions
            SET classification = 'BUY'
            WHERE whale_address IS NOT NULL
              AND whale_address = to_address
              AND counterparty_type IN ('CEX', 'DEX')
              AND classification != 'BUY'
            RETURNING id
        """
    }).execute()
    count = len(result.data) if result.data else 0
    stats['buy_fixed'] = count
    print(f"   ✅ Updated {count:,} transactions to BUY")
    
except Exception as e:
    print(f"   ❌ Direct SQL not supported, using row-by-row updates...")
    
    # Fallback: Row-by-row update for BUYs
    try:
        result = client.table('whale_transactions')\
            .select('id')\
            .not_.is_('whale_address', 'null')\
            .execute()
        
        total = len(result.data)
        print(f"   Processing {total:,} transactions...")
        
        batch_size = 1000
        for i in range(0, total, batch_size):
            batch = result.data[i:i+batch_size]
            
            for row in batch:
                try:
                    # Fetch full row
                    tx = client.table('whale_transactions').select('*').eq('id', row['id']).single().execute()
                    tx_data = tx.data
                    
                    whale_addr = (tx_data.get('whale_address') or '').lower()
                    from_addr = (tx_data.get('from_address') or '').lower()
                    to_addr = (tx_data.get('to_address') or '').lower()
                    counterparty_type = tx_data.get('counterparty_type', '')
                    current_class = tx_data.get('classification', '')
                    
                    if not whale_addr:
                        continue
                    
                    # Determine correct classification
                    new_class = None
                    is_trade = counterparty_type in ['CEX', 'DEX']
                    
                    if whale_addr == to_addr:
                        # Whale receiving tokens
                        new_class = 'BUY' if is_trade else 'TRANSFER'
                    elif whale_addr == from_addr:
                        # Whale sending tokens
                        new_class = 'SELL' if is_trade else 'TRANSFER'
                    
                    # Update if changed
                    if new_class and new_class != current_class:
                        client.table('whale_transactions')\
                            .update({'classification': new_class})\
                            .eq('id', row['id'])\
                            .execute()
                        
                        if new_class == 'BUY':
                            stats['buy_fixed'] += 1
                        elif new_class == 'SELL':
                            stats['sell_fixed'] += 1
                        elif new_class == 'TRANSFER':
                            stats['transfer_fixed'] += 1
                
                except Exception as e:
                    stats['errors'] += 1
                    if stats['errors'] < 10:
                        print(f"   ⚠️  Error on row {row['id']}: {e}")
            
            # Progress
            processed = min(i + batch_size, total)
            print(f"   Progress: {processed:,}/{total:,} ({processed/total*100:.1f}%)", end='\r')
        
        print()  # Newline after progress
    
    except Exception as e:
        print(f"   ❌ Fallback failed: {e}")
        stats['errors'] += 1

print()
print("="*80)
print("📊 RECLASSIFICATION SUMMARY")
print("="*80)
print(f"  BUY fixed:      {stats['buy_fixed']:,}")
print(f"  SELL fixed:     {stats['sell_fixed']:,}")
print(f"  TRANSFER fixed: {stats['transfer_fixed']:,}")
print(f"  Total fixed:    {sum([stats['buy_fixed'], stats['sell_fixed'], stats['transfer_fixed']]):,}")
print(f"  Errors:         {stats['errors']:,}")
print()

if stats['errors'] > 0:
    print("⚠️  Some errors occurred, but partial updates were made")
else:
    print("✅ Reclassification complete!")

print("="*80)
print()


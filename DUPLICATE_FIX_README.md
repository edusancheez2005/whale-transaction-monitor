# duplicate transaction fix

## the problem

you were seeing duplicate large transactions like:
- wbtc $286,257,695 appearing twice
- dai $100,000,000 appearing twice
- matic $38,760,611 appearing twice

**root cause:** the same transaction was being captured by multiple data sources (whale alert, etherscan, etc.) and stored multiple times in your supabase database without proper deduplication.

## what i fixed

### 1. api endpoint deduplication
**file:** `api/whale_intelligence_api.py`

added deduplication logic to both endpoints:
- `/whale-signals` - now removes duplicate transaction hashes before aggregating
- `/token/{symbol}/activity` - now returns only unique transactions

**how it works:**
- when querying transactions from supabase, the code now:
  1. groups transactions by `transaction_hash`
  2. keeps only the one with highest confidence + whale_score
  3. removes all duplicates before displaying

### 2. storage layer (already working)
**file:** `enhanced_monitor.py`

the storage layer already uses `upsert` with `on_conflict='transaction_hash'`:
```python
result = self.supabase.table('whale_transactions').upsert(
    whale_data,
    on_conflict='transaction_hash'
).execute()
```

this prevents new duplicates from being stored, but won't remove existing ones.

### 3. cleanup script
**file:** `run_cleanup_duplicates.py`

created a script to remove existing duplicates from your database.

## how to fix your existing data

### option 1: run the cleanup script (recommended)

```bash
python run_cleanup_duplicates.py
```

this will:
1. count existing duplicates
2. ask for confirmation
3. remove duplicates, keeping the highest confidence entry for each transaction
4. show before/after stats

### option 2: let it fix itself over time

new transactions won't be duplicated thanks to the `upsert` logic. old duplicates will just stay in the database but won't be shown in the ui anymore thanks to the api deduplication.

## verify the fix

after running the cleanup script, check your dashboard:
- the "top 10 largest (24h)" should show unique transactions only
- no more duplicate wbtc, dai, or matic entries

## technical details

### why duplicates happened

your monitoring system has multiple threads:
1. **whale alert websocket** - captures large transactions
2. **etherscan api** - monitors ethereum transfers
3. **other sources** - polygon, solana, etc.

when a large transaction occurs (like wbtc $286m), multiple sources detect it:
- whale alert captures it
- etherscan captures it
- both try to store it in supabase

### why the fix works

**before:**
```
supabase query → 1000 transactions (with duplicates)
                ↓
           display all
```

**after:**
```
supabase query → 1000 transactions
                ↓
           deduplicate by hash (keeps highest confidence)
                ↓
           display unique only
```

## future prevention

the fix ensures that:
1. **storage layer:** uses upsert to update existing entries instead of creating duplicates
2. **query layer:** deduplicates results before displaying
3. **database:** you can add a unique constraint on `transaction_hash` column (optional)

### optional: add database constraint

to prevent duplicates at the database level, run this sql in your supabase dashboard:

```sql
create unique index if not exists idx_whale_transactions_hash_unique 
on whale_transactions(transaction_hash);
```

**warning:** only run this after cleaning up existing duplicates!

## summary

- **immediate fix:** the api now deduplicates automatically
- **long-term fix:** run `python run_cleanup_duplicates.py` to clean existing data
- **future prevention:** upsert logic + optional database constraint

your "top 10 largest" table should now show unique transactions only.


# whale perspective fix - complete solution

## the problem (explained)

**before:** every whale address showed ONLY buys OR sells, never both

**why:** your classification was stored at transaction level, not from whale's perspective:
- binance sends tokens → marked as "BUY" 
- when you click on binance address → ALL transactions show "BUY"
- but binance is NOT a whale, it's just a conduit!

## the solution (professional-grade)

### 1. database schema ✅ DONE

added 6 new columns to `whale_transactions`:
```sql
- whale_address         TEXT    -- The actual whale (non-CEX party)
- counterparty_address  TEXT    -- CEX/DEX they traded with
- counterparty_type     TEXT    -- 'CEX', 'DEX', 'EOA'
- is_cex_transaction    BOOLEAN -- Flag for filtering
- from_label           TEXT    -- Address label (for reference)
- to_label             TEXT    -- Address label (for reference)
```

### 2. whale perspective logic ✅ DONE

**new classification rules:**

| from → to | whale_address | counterparty | classification |
|-----------|---------------|--------------|----------------|
| CEX → User | User (to) | CEX (from) | BUY |
| User → CEX | User (from) | CEX (to) | SELL |
| DEX → User | User (to) | DEX (from) | BUY |
| User → DEX | User (from) | DEX (to) | SELL |
| CEX → CEX | NULL | NULL | SKIP (internal) |
| Wallet → Wallet | from | to | TRANSFER |

**implemented in:** `enhanced_monitor.py` → `_determine_whale_perspective()`

### 3. automatic population ✅ DONE

all NEW transactions will automatically populate the whale perspective columns

**modified:** `store_whale_transaction()` in `enhanced_monitor.py`

### 4. migration script ✅ DONE

migrate existing transactions to fix historical data

**file:** `migrate_whale_perspective.py`

## how to run everything

### step 1: test the migration (dry run)

```bash
# preview changes without updating database
python migrate_whale_perspective.py --dry-run --limit 20

# this will show you:
# - which transactions will be updated
# - who the actual whales are
# - cex vs dex vs eoa breakdown
```

### step 2: run the full migration

```bash
# migrate all existing transactions
python migrate_whale_perspective.py

# this updates your entire database (~5-10 minutes depending on size)
```

### step 3: verify the data

```sql
-- check whale distribution
SELECT 
    whale_address,
    counterparty_type,
    COUNT(*) as tx_count,
    SUM(usd_value) as total_volume
FROM whale_transactions
WHERE whale_address IS NOT NULL
GROUP BY whale_address, counterparty_type
ORDER BY total_volume DESC
LIMIT 20;

-- check for CEX addresses (should be empty in whale_address column)
SELECT COUNT(*) 
FROM whale_transactions t
JOIN addresses a ON t.whale_address = a.address
WHERE a.address_type IN ('CEX Wallet', 'exchange');
-- should return 0!
```

### step 4: update sonar queries

**old query (broken):**
```sql
SELECT * FROM whale_transactions
WHERE from_address = '0x21a3...5549'  -- binance address
   OR to_address = '0x21a3...5549'
```

**new query (correct):**
```sql
-- get transactions for a REAL whale
SELECT * FROM whale_transactions
WHERE whale_address = '0xabc...def'  -- actual whale address
  AND counterparty_type IN ('CEX', 'DEX')  -- real trades only
  AND classification IN ('BUY', 'SELL')
ORDER BY timestamp DESC;

-- exclude CEX addresses from whale list
SELECT DISTINCT whale_address, 
       SUM(CASE WHEN classification = 'BUY' THEN usd_value ELSE 0 END) as buy_volume,
       SUM(CASE WHEN classification = 'SELL' THEN usd_value ELSE 0 END) as sell_volume
FROM whale_transactions
WHERE whale_address NOT IN (
    SELECT address FROM addresses 
    WHERE address_type IN ('CEX Wallet', 'exchange', 'DEX')
)
GROUP BY whale_address
HAVING SUM(usd_value) > 100000
ORDER BY (buy_volume + sell_volume) DESC;
```

## expected results

### before (broken) ❌
```
whale: 0x21a3...5549 (binance)
├── usdt    $650,000   BUY
├── api3    $163,692   BUY
├── imx     $161,535   BUY
└── axs     $112,553   BUY

net flow: +$1,261,773  (all positive)
```

### after (correct) ✅
```
cex address excluded from whale list

real whale: 0xabc...def
├── eth     $500,000   BUY   (from binance)
├── link    $200,000   BUY   (from uniswap)
├── uni     $150,000   SELL  (to binance)
├── aave    $100,000   SELL  (to coinbase)
└── snx     $50,000    BUY   (from binance)

net flow: +$600,000  (mixed buy/sell)
```

## testing

run a quick test to verify everything works:

```bash
# test with a known address
python3 << 'EOF'
from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
from supabase import create_client

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# check a real whale vs cex address
test_addresses = [
    "0x21a31ee1afc51d94c2efccaa2092ad1028285549",  # binance (should be excluded)
    # add a real whale address from your data
]

for addr in test_addresses:
    result = supabase.table('whale_transactions')\
        .select('token_symbol, classification, usd_value, whale_address, counterparty_type')\
        .eq('whale_address', addr)\
        .limit(5)\
        .execute()
    
    print(f"\naddress: {addr[:10]}...")
    print(f"transactions as whale: {len(result.data)}")
    for tx in result.data[:3]:
        print(f"  {tx['token_symbol']:6s} ${tx['usd_value']:10,.0f} {tx['classification']:8s}")

EOF
```

## troubleshooting

**issue:** migration fails with "column does not exist"
**fix:** make sure you ran the SQL schema updates in supabase first

**issue:** all whale_address values are NULL
**fix:** run the migration script: `python migrate_whale_perspective.py`

**issue:** sonar still shows binance as whale
**fix:** update sonar queries to:
1. use `whale_address` instead of `from_address/to_address`
2. exclude addresses where `address_type IN ('CEX Wallet', 'exchange')`

**issue:** some whales still show only one direction
**fix:** check if they're actually CEX addresses:
```sql
SELECT a.label, a.address_type, a.entity_name
FROM addresses a
WHERE a.address = 'YOUR_ADDRESS';
```

## status

- [x] database schema updated
- [x] whale perspective logic implemented
- [x] auto-population for new transactions
- [x] migration script created
- [ ] run migration on existing data
- [ ] update sonar ui queries
- [ ] verify results

## next steps

1. run: `python migrate_whale_perspective.py --dry-run` (test)
2. run: `python migrate_whale_perspective.py` (real migration)
3. update sonar queries to use `whale_address`
4. celebrate! 🎉


# 🛡️ Near-Duplicate Fix - Quick Start Guide

## What Was Fixed

Your system was experiencing **near-duplicates** that bypassed exact-hash matching:
- ❌ BUY + SELL pairs (same trade, 5 seconds apart)
- ❌ BUY + TRANSFER pairs (double reporting)
- ❌ Same trade with inconsistent metadata

## What's New

✅ **Real-time suppression** - Duplicates blocked before storage  
✅ **Pattern detection** - 4 duplicate patterns caught  
✅ **Two-layer checking** - Memory cache + database lookback  
✅ **Cleanup script** - Remove existing duplicates  
✅ **Full logging** - Track every suppression  
✅ **Safeguards** - Never dedupe $5M+ or DEFI trades  

---

## Step 1: Clean Existing Duplicates

### Preview what will be cleaned:
```bash
python cleanup_near_duplicates.py
```

This shows you:
- How many duplicates exist
- Breakdown by pattern type
- Sample duplicates (first 10)
- **NO CHANGES MADE** (dry run)

### Actually clean the database:
```bash
python cleanup_near_duplicates.py --live
```

Expected output:
```
🛡️ NEAR-DUPLICATE CLEANUP
Mode: LIVE (will delete duplicates)
✅ Loaded 5,234 transactions
✅ Found 892 unique whale+token pairs
🔍 Detecting near-duplicates...
✅ Found 127 duplicates

📊 DUPLICATE BREAKDOWN:
  mirror_direction          : 45
  transfer_shadow           : 38
  counterparty_mismatch     : 32
  cex_flag_mismatch         : 12
  TOTAL                     : 127

🗑️  Deleting 127 duplicate transactions...
✅ Deleted 127 duplicates
✅ CLEANUP COMPLETE
```

---

## Step 2: Restart Monitor

The new code is active, but you need to restart:

```bash
# Stop current monitor
pkill -f enhanced_monitor.py

# Start with new duplicate suppression
cd /Users/edusanchez/OneDrive/Desktop/whale-transaction-monitor
python enhanced_monitor.py
```

---

## Step 3: Verify It's Working

### Check real-time logs:
```bash
tail -f logs/production.log | grep duplicate
```

You should see lines like:
```json
{"message": "Near-duplicate suppressed - not storing", 
 "reason": "mirror_direction", 
 "pattern": "BUY + SELL"}
```

### Check statistics:
```bash
python -c "from enhanced_monitor import get_near_dupe_stats; import json; print(json.dumps(get_near_dupe_stats(), indent=2))"
```

Expected output:
```json
{
  "cache_hits": 23,
  "db_hits": 5,
  "suppressed": 28,
  "reason_mirror_direction": 12,
  "reason_transfer_shadow": 10,
  "reason_counterparty_mismatch": 6,
  "cache_size": 450,
  "cache_keys": 15
}
```

### Verify in Supabase:

Run this query to check for remaining duplicates:
```sql
SELECT 
  whale_address, 
  token_symbol, 
  classification,
  ROUND(usd_value) as usd,
  DATE_TRUNC('minute', timestamp) as minute,
  COUNT(*) as count
FROM whale_transactions
WHERE timestamp > NOW() - INTERVAL '1 hour'
GROUP BY whale_address, token_symbol, classification, ROUND(usd_value), DATE_TRUNC('minute', timestamp)
HAVING COUNT(*) > 1
ORDER BY count DESC;
```

✅ **Should return 0 rows** (no duplicates)

---

## Configuration (Optional)

If you want to tune the system, edit the top of `enhanced_monitor.py`:

```python
# Time window for duplicates (seconds)
NEAR_DUPE_TIME_WINDOW = 10  # Default: 10s

# USD tolerance (absolute)
NEAR_DUPE_USD_THRESHOLD = 5.0  # Default: $5

# Percentage tolerance
NEAR_DUPE_PERCENTAGE_THRESHOLD = 0.0015  # Default: 0.15%
```

### When to Adjust:

| Problem | Solution |
|---------|----------|
| Still seeing duplicates | Increase `NEAR_DUPE_TIME_WINDOW` to 15-20 |
| Legit trades suppressed | Decrease `NEAR_DUPE_USD_THRESHOLD` to 2-3 |
| High-frequency false positives | Decrease `NEAR_DUPE_TIME_WINDOW` to 5 |

---

## Monitoring

### Daily Health Check:
```bash
# Count suppressions today
grep "Near-duplicate suppressed" logs/production.log | wc -l

# View suppression reasons
grep "Near-duplicate detected" logs/production.log | \
  grep -oP "reason['\"]:\s*['\"][^'\"]*" | sort | uniq -c
```

### Weekly Cleanup (if needed):
```bash
# Check for any duplicates that slipped through
python cleanup_near_duplicates.py

# Clean if any found
python cleanup_near_duplicates.py --live
```

---

## What Each File Does

| File | Purpose |
|------|---------|
| `enhanced_monitor.py` | **Main engine** - Real-time duplicate detection built-in |
| `cleanup_near_duplicates.py` | **Cleanup script** - Remove existing duplicates from DB |
| `NEAR_DUPLICATE_SYSTEM.md` | **Full docs** - Complete technical documentation |
| `DUPLICATE_FIX_QUICKSTART.md` | **This file** - Quick start guide |

---

## Troubleshooting

### ❌ "No module named 'enhanced_monitor'"

**Solution**: Run cleanup from the project directory:
```bash
cd /Users/edusanchez/OneDrive/Desktop/whale-transaction-monitor
python cleanup_near_duplicates.py
```

### ❌ Still seeing duplicates

**Solutions**:
1. Check if monitor was restarted: `ps aux | grep enhanced_monitor`
2. Check configuration values in `enhanced_monitor.py`
3. Run cleanup script: `python cleanup_near_duplicates.py --live`
4. Check logs: `tail -100 logs/production.log | grep duplicate`

### ❌ Legitimate trades being suppressed

**Solutions**:
1. Check logs to see suppression reason
2. Adjust thresholds (see Configuration above)
3. Check if USD values are truly different

### ❌ Performance issues

**Solutions**:
1. Reduce `NEAR_DUPE_CACHE_SIZE` to 25
2. Reduce `NEAR_DUPE_DB_LOOKBACK` to 100
3. Check memory usage: `ps aux | grep enhanced_monitor`

---

## Expected Results

### Before Fix:
```
Whale 0xabc...123: 
  - USDC BUY $100,000 (10:30:05)
  - USDC SELL $100,000 (10:30:08)  ← DUPLICATE
  - ETH BUY $50,000 (10:35:12)
  - ETH TRANSFER $50,000 (10:35:15)  ← DUPLICATE
```

### After Fix:
```
Whale 0xabc...123:
  - USDC BUY $100,000 (10:30:05)  ✅ Kept (higher confidence)
  - ETH BUY $50,000 (10:35:12)    ✅ Kept (earlier timestamp)
```

---

## Summary

✅ **Installed**: Near-duplicate suppression system  
✅ **Automatic**: Works in real-time, no manual intervention  
✅ **Safe**: $5M+ and DEFI trades never touched  
✅ **Logged**: Every decision tracked for audit  
✅ **Tunable**: Adjust thresholds as needed  

**Next Steps**:
1. Run cleanup script (`cleanup_near_duplicates.py --live`)
2. Restart monitor
3. Monitor for 24 hours
4. Check daily stats

For detailed info, see `NEAR_DUPLICATE_SYSTEM.md`

---

**Installation Date**: 2025-10-29  
**Status**: Production Ready ✅  
**Questions**: Check logs first, then review full docs


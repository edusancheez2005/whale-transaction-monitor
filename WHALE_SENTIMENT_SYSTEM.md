# 🐋 Whale Sentiment Analysis System

## Overview

This system provides real-time whale sentiment analysis by tracking large cryptocurrency transactions, classifying them as BUY/SELL, and aggregating the data to show which tokens whales are accumulating vs dumping.

## Architecture

```
Whale Transactions → Classification → Database Storage → Aggregation → UI/Dashboard
```

## Key Components

### 1. **Enhanced Monitor Integration** (`enhanced_monitor.py`)
- **Modified**: Stores whale transaction classifications in `whale_transactions` table
- **Added**: Token symbol extraction logic with fallbacks
- **Added**: Auto-start of sentiment aggregator
- **Storage**: Only BUY/SELL classifications (skips TRANSFER)

### 2. **Sentiment Aggregator** (`whale_sentiment_aggregator.py`)
- **Runs**: Every 60 seconds
- **Calculates**: Buy/sell ratios for last 2 hours
- **Provides**: Real-time sentiment scores
- **Outputs**: Console summaries and API data

### 3. **Database Schema** (`whale_sentiment_tables.sql`)
- **Main table**: `whale_transactions` (already created)
- **Optional table**: `whale_sentiment_aggregated` (for UI persistence)
- **Views**: Bullish/bearish token rankings

### 4. **Test Suite** (`test_whale_sentiment.py`)
- **Validates**: All system components
- **Tests**: Database connectivity and data flow

## How It Works

### Step 1: Transaction Classification
```python
# Every whale transaction gets analyzed
intelligence_result = whale_engine.analyze_transaction_comprehensive(tx_data)

# Only BUY/SELL classifications are stored
if classification in ['BUY', 'SELL']:
    transaction_storage.store_whale_transaction(tx_data, intelligence_result)
```

### Step 2: Token Symbol Extraction
```python
# Multi-fallback approach
symbol = (tx_data.get('symbol') or 
         tx_data.get('token_symbol') or 
         common_tokens.get(token_address) or
         blockchain_default)
```

### Step 3: Real-time Aggregation
```sql
-- Every minute, calculate sentiment
SELECT 
    token_symbol,
    COUNT(CASE WHEN classification = 'BUY' THEN 1 END) as buys,
    COUNT(CASE WHEN classification = 'SELL' THEN 1 END) as sells,
    ROUND(buys * 100.0 / (buys + sells), 2) as buy_percentage
FROM whale_transactions 
WHERE timestamp >= NOW() - INTERVAL '2 hours'
GROUP BY token_symbol
ORDER BY buy_percentage DESC;
```

## Key Metrics

### Sentiment Scores
- **Buy Percentage**: `(buys / total_directional) * 100`
- **Sell Percentage**: `(sells / total_directional) * 100`
- **Sentiment Score**: `buy_percentage - sell_percentage`
- **Volume-Weighted**: Uses USD values for weighting

### Classification Types
- ✅ **BUY**: Whale is accumulating the token
- ❌ **SELL**: Whale is dumping the token
- ⏸️ **TRANSFER**: Ignored (wallet-to-wallet moves)

## Usage

### Start the System
```bash
# Start monitoring with sentiment analysis
python enhanced_monitor.py
```

### Test the System
```bash
# Verify everything works
python test_whale_sentiment.py
```

### View Live Sentiment
The aggregator automatically prints sentiment summaries every minute:
```
🐋 WHALE SENTIMENT ANALYSIS (Last 2 Hours)
TOKEN      BUYS  SELLS   BUY%  SELL%       VOLUME   SENTIMENT
ETH          12      3  80.0%  20.0%  $2,450,000   🟢  +60.0
USDC          5      8  38.5%  61.5%  $1,200,000   🔴  -23.0
BTC           7      2  77.8%  22.2%    $890,000   🟢  +55.6
```

## API Queries for UI

### Get Bullish Tokens (Last 2 Hours)
```sql
SELECT 
    token_symbol,
    COUNT(CASE WHEN classification = 'BUY' THEN 1 END) as buys,
    COUNT(CASE WHEN classification = 'SELL' THEN 1 END) as sells,
    ROUND(COUNT(CASE WHEN classification = 'BUY' THEN 1 END) * 100.0 / COUNT(*), 2) as buy_percentage,
    SUM(usd_value) as total_volume
FROM whale_transactions 
WHERE timestamp >= NOW() - INTERVAL '2 hours'
    AND classification IN ('BUY', 'SELL')
GROUP BY token_symbol
HAVING COUNT(*) >= 3
ORDER BY buy_percentage DESC
LIMIT 10;
```

### Get Bearish Tokens (Last 2 Hours)
```sql
-- Same query but ORDER BY sell_percentage DESC
```

### Get Token Trend (Any Token)
```sql
SELECT 
    token_symbol,
    COUNT(CASE WHEN classification = 'BUY' THEN 1 END) as buys,
    COUNT(CASE WHEN classification = 'SELL' THEN 1 END) as sells,
    ROUND(COUNT(CASE WHEN classification = 'BUY' THEN 1 END) * 100.0 / COUNT(*), 2) as buy_percentage,
    SUM(usd_value) as total_volume
FROM whale_transactions 
WHERE timestamp >= NOW() - INTERVAL '2 hours'
    AND classification IN ('BUY', 'SELL')
    AND token_symbol = 'ETH'  -- Replace with any token
GROUP BY token_symbol;
```

## Integration with Your UI

### Real-time Updates
1. **Polling**: Query the database every 30-60 seconds
2. **WebSocket**: Listen for new whale transactions
3. **Server-Sent Events**: Stream sentiment updates

### Display Components
- **Bullish Panel**: Top tokens with high buy %
- **Bearish Panel**: Top tokens with high sell %
- **Live Feed**: Real-time whale transactions
- **Charts**: Historical sentiment trends

### Sample React/JavaScript
```javascript
// Fetch bullish tokens
const response = await supabase
  .from('whale_transactions')
  .select('token_symbol, classification, usd_value')
  .gte('timestamp', new Date(Date.now() - 2*60*60*1000).toISOString())
  .in('classification', ['BUY', 'SELL']);

// Calculate sentiment
const sentiment = calculateSentiment(response.data);
```

## Configuration

### Time Windows
- Default: **2 hours** (configurable)
- Supported: 1h, 2h, 6h, 24h

### Minimum Thresholds
- **Transactions**: 3+ for trending
- **USD Value**: $1,000+ for whale status

### Update Frequency
- **Aggregation**: Every 60 seconds
- **UI Updates**: Every 30-60 seconds recommended

## Monitoring

### Logs
- Sentiment calculations logged with structured data
- Transaction storage success/failure tracking
- Performance metrics (processing time)

### Health Checks
- Database connectivity
- Classification success rates
- Data freshness

## Troubleshooting

### No Sentiment Data
1. Check if enhanced monitor is running
2. Verify whale transactions are being classified
3. Ensure Supabase connection is working

### Missing Token Symbols
1. Add more token address mappings
2. Improve symbol extraction logic
3. Check transaction data quality

### Performance Issues
1. Add database indexes
2. Limit time window queries
3. Cache aggregated results

## Next Steps

1. **UI Integration**: Connect your React/Vue app to the database
2. **Real-time Updates**: Implement WebSocket or polling
3. **Historical Charts**: Add trend visualization
4. **Alerts**: Notify on sentiment changes
5. **Advanced Metrics**: Add volume-weighted indicators

---

🚀 **Your whale sentiment analysis system is now ready!** 

Run `python enhanced_monitor.py` to start monitoring whale sentiment in real-time. 
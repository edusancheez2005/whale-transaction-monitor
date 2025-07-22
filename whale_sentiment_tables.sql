-- 🐋 WHALE SENTIMENT ANALYSIS TABLES
-- Additional table for storing aggregated sentiment data (optional for UI persistence)

-- Optional table for pre-calculated sentiment aggregations
CREATE TABLE IF NOT EXISTS whale_sentiment_aggregated (
    id SERIAL PRIMARY KEY,
    token_symbol TEXT NOT NULL,
    time_window TEXT NOT NULL, -- e.g., '1h', '2h', '6h', '24h'
    buy_count INTEGER DEFAULT 0,
    sell_count INTEGER DEFAULT 0,
    buy_percentage DECIMAL(5,2) DEFAULT 0,
    sell_percentage DECIMAL(5,2) DEFAULT 0,
    volume_weighted_buy_percentage DECIMAL(5,2) DEFAULT 0,
    total_volume_usd DECIMAL(20,2) DEFAULT 0,
    sentiment_score DECIMAL(6,2) DEFAULT 0, -- buy_percentage - sell_percentage
    volume_sentiment_score DECIMAL(6,2) DEFAULT 0, -- volume-weighted sentiment
    avg_confidence DECIMAL(3,2) DEFAULT 0,
    avg_whale_score DECIMAL(5,2) DEFAULT 0,
    total_transactions INTEGER DEFAULT 0,
    calculated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_whale_sentiment_token_time ON whale_sentiment_aggregated(token_symbol, time_window);
CREATE INDEX IF NOT EXISTS idx_whale_sentiment_calculated_at ON whale_sentiment_aggregated(calculated_at);
CREATE INDEX IF NOT EXISTS idx_whale_sentiment_buy_percentage ON whale_sentiment_aggregated(buy_percentage);
CREATE INDEX IF NOT EXISTS idx_whale_sentiment_sell_percentage ON whale_sentiment_aggregated(sell_percentage);

-- View for latest sentiment per token (most recent calculation)
CREATE OR REPLACE VIEW whale_sentiment_latest AS
SELECT DISTINCT ON (token_symbol, time_window) 
    token_symbol,
    time_window,
    buy_count,
    sell_count,
    buy_percentage,
    sell_percentage,
    volume_weighted_buy_percentage,
    total_volume_usd,
    sentiment_score,
    volume_sentiment_score,
    avg_confidence,
    avg_whale_score,
    total_transactions,
    calculated_at
FROM whale_sentiment_aggregated
ORDER BY token_symbol, time_window, calculated_at DESC;

-- View for top bullish tokens (2h window)
CREATE OR REPLACE VIEW whale_sentiment_bullish_2h AS
SELECT 
    token_symbol,
    buy_count,
    sell_count,
    buy_percentage,
    sell_percentage,
    volume_weighted_buy_percentage,
    total_volume_usd,
    sentiment_score,
    total_transactions,
    calculated_at
FROM whale_sentiment_latest
WHERE time_window = '2h' 
    AND total_transactions >= 3
ORDER BY buy_percentage DESC, total_volume_usd DESC
LIMIT 10;

-- View for top bearish tokens (2h window)
CREATE OR REPLACE VIEW whale_sentiment_bearish_2h AS
SELECT 
    token_symbol,
    buy_count,
    sell_count,
    buy_percentage,
    sell_percentage,
    volume_weighted_buy_percentage,
    total_volume_usd,
    sentiment_score,
    total_transactions,
    calculated_at
FROM whale_sentiment_latest
WHERE time_window = '2h' 
    AND total_transactions >= 3
ORDER BY sell_percentage DESC, total_volume_usd DESC
LIMIT 10;

-- Function to clean old aggregated data (run periodically)
CREATE OR REPLACE FUNCTION clean_old_whale_sentiment(days_to_keep INTEGER DEFAULT 7)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM whale_sentiment_aggregated 
    WHERE calculated_at < NOW() - INTERVAL '1 day' * days_to_keep;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- API queries you can use in your UI:

-- Query 1: Get latest bullish tokens (2h window)
/*
SELECT * FROM whale_sentiment_bullish_2h;
*/

-- Query 2: Get latest bearish tokens (2h window) 
/*
SELECT * FROM whale_sentiment_bearish_2h;
*/

-- Query 3: Get real-time sentiment for a specific token
/*
SELECT 
    token_symbol,
    COUNT(CASE WHEN classification = 'BUY' THEN 1 END) as buys,
    COUNT(CASE WHEN classification = 'SELL' THEN 1 END) as sells,
    ROUND(COUNT(CASE WHEN classification = 'BUY' THEN 1 END) * 100.0 / COUNT(*), 2) as buy_percentage,
    ROUND(COUNT(CASE WHEN classification = 'SELL' THEN 1 END) * 100.0 / COUNT(*), 2) as sell_percentage,
    SUM(usd_value) as total_volume
FROM whale_transactions 
WHERE timestamp >= NOW() - INTERVAL '2 hours'
    AND classification IN ('BUY', 'SELL')
    AND token_symbol = 'YOUR_TOKEN_SYMBOL'
GROUP BY token_symbol;
*/

-- Query 4: Get trending tokens by volume in last hour
/*
SELECT 
    token_symbol,
    COUNT(*) as total_transactions,
    SUM(usd_value) as total_volume,
    COUNT(CASE WHEN classification = 'BUY' THEN 1 END) as buys,
    COUNT(CASE WHEN classification = 'SELL' THEN 1 END) as sells
FROM whale_transactions 
WHERE timestamp >= NOW() - INTERVAL '1 hour'
    AND classification IN ('BUY', 'SELL')
GROUP BY token_symbol
HAVING COUNT(*) >= 3
ORDER BY total_volume DESC
LIMIT 20;
*/ 
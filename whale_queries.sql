-- =====================================================================
-- WHALE DISCOVERY SQL QUERIES FOR SUPABASE DATABASE
-- =====================================================================
-- Updated for actual 'addresses' table schema
-- These queries identify whale addresses based on various criteria
-- =====================================================================

-- QUERY 1: High Balance Whales (addresses with significant USD balance)
-- =====================================================================
SELECT 
    address,
    blockchain,
    source,
    label,
    confidence,
    balance_usd,
    balance_native,
    address_type,
    signal_potential,
    entity_name,
    analysis_tags,
    updated_at
FROM addresses 
WHERE 
    balance_usd > 50000  -- $50K+ threshold
    AND confidence > 0.6
ORDER BY balance_usd DESC
LIMIT 1000;

-- QUERY 2: Transaction Volume Whales (high activity addresses)
-- =====================================================================
SELECT 
    address,
    blockchain,
    source,
    label,
    confidence,
    balance_usd,
    address_type,
    signal_potential,
    analysis_tags->>'total_transaction_value_usd' as total_tx_value_usd,
    analysis_tags->>'transaction_count' as transaction_count,
    last_seen_tx,
    updated_at
FROM addresses 
WHERE 
    (analysis_tags->>'total_transaction_value_usd')::numeric > 100000  -- $100K+ in transactions
    OR (analysis_tags->>'transaction_count')::int > 100  -- 100+ transactions
    OR address_type IN ('whale', 'high_activity', 'exchange', 'defi_whale')
ORDER BY balance_usd DESC NULLS LAST
LIMIT 1000;

-- QUERY 3: Covalent-Discovered Whales (from your new integration)
-- =====================================================================
SELECT 
    address,
    blockchain,
    source,
    label,
    confidence,
    balance_usd,
    address_type,
    signal_potential,
    analysis_tags->>'discovery_method' as discovery_method,
    analysis_tags->>'token_address' as token_address,
    analysis_tags,
    updated_at
FROM addresses 
WHERE 
    source LIKE 'covalent%'
    AND (
        balance_usd > 10000 
        OR (analysis_tags->>'balance_usd')::numeric > 10000
    )
ORDER BY confidence DESC, balance_usd DESC
LIMIT 1000;

-- QUERY 4: Multi-Source Verified Whales (addresses found by multiple sources)
-- =====================================================================
WITH whale_counts AS (
    SELECT 
        address,
        COUNT(DISTINCT source) as source_count,
        MAX(confidence) as max_confidence,
        MAX(balance_usd) as max_balance_usd,
        array_agg(DISTINCT source) as sources,
        array_agg(DISTINCT label) as labels,
        array_agg(DISTINCT address_type) as address_types
    FROM addresses 
    WHERE confidence > 0.5
    GROUP BY address
    HAVING COUNT(DISTINCT source) >= 2  -- Found by at least 2 sources
)
SELECT 
    wc.*,
    a.blockchain,
    a.label,
    a.address_type,
    a.signal_potential,
    a.entity_name,
    a.analysis_tags,
    a.updated_at
FROM whale_counts wc
JOIN addresses a ON a.address = wc.address 
    AND a.confidence = wc.max_confidence
ORDER BY wc.source_count DESC, wc.max_balance_usd DESC
LIMIT 500;

-- QUERY 5: High Confidence Whales (regardless of source)
-- =====================================================================
SELECT 
    address,
    blockchain,
    source,
    label,
    confidence,
    balance_usd,
    balance_native,
    address_type,
    signal_potential,
    entity_name,
    analysis_tags,
    updated_at
FROM addresses 
WHERE 
    confidence >= 0.8  -- High confidence only
ORDER BY confidence DESC, balance_usd DESC NULLS LAST
LIMIT 500;

-- QUERY 6: Token Whale Holders (specific to token holdings)
-- =====================================================================
SELECT 
    address,
    blockchain,
    source,
    label,
    confidence,
    balance_usd,
    address_type,
    analysis_tags->>'token_address' as token_address,
    analysis_tags->>'token_symbol' as token_symbol,
    analysis_tags->>'balance_usd' as token_balance_usd,
    analysis_tags->>'balance' as token_balance,
    analysis_tags,
    updated_at
FROM addresses 
WHERE 
    label ILIKE '%token%whale%'
    OR label ILIKE '%token%holder%'
    OR analysis_tags->>'token_address' IS NOT NULL
    OR address_type = 'token_whale'
ORDER BY balance_usd DESC NULLS LAST
LIMIT 1000;

-- QUERY 7: Recent Whale Activity (addresses updated in last 7 days)
-- =====================================================================
SELECT 
    address,
    blockchain,
    source,
    label,
    confidence,
    balance_usd,
    address_type,
    signal_potential,
    entity_name,
    last_balance_check,
    updated_at
FROM addresses 
WHERE 
    updated_at >= NOW() - INTERVAL '7 days'
    AND (
        confidence > 0.7 
        OR balance_usd > 25000
        OR address_type IN ('whale', 'mega_whale', 'defi_whale')
    )
ORDER BY updated_at DESC, confidence DESC
LIMIT 1000;

-- QUERY 8: Whale Summary Statistics
-- =====================================================================
SELECT 
    'Total Addresses' as metric,
    COUNT(*) as value
FROM addresses 

UNION ALL

SELECT 
    'High Confidence Addresses (>0.6)' as metric,
    COUNT(*) as value
FROM addresses 
WHERE confidence > 0.6

UNION ALL

SELECT 
    'High Value Whales ($100K+)' as metric,
    COUNT(*) as value
FROM addresses 
WHERE balance_usd > 100000

UNION ALL

SELECT 
    'Medium Value Whales ($50K+)' as metric,
    COUNT(*) as value
FROM addresses 
WHERE balance_usd > 50000

UNION ALL

SELECT 
    'Covalent Discovered Addresses' as metric,
    COUNT(*) as value
FROM addresses 
WHERE source LIKE 'covalent%'

UNION ALL

SELECT 
    'Multi-Source Verified Addresses' as metric,
    COUNT(DISTINCT address) as value
FROM (
    SELECT address
    FROM addresses 
    GROUP BY address
    HAVING COUNT(DISTINCT source) >= 2
) multi_source

UNION ALL

SELECT 
    'Blockchains with Addresses' as metric,
    COUNT(DISTINCT blockchain) as value
FROM addresses 

UNION ALL

SELECT 
    'Data Sources Used' as metric,
    COUNT(DISTINCT source) as value
FROM addresses 

UNION ALL

SELECT 
    'Total USD Value Tracked' as metric,
    ROUND(SUM(balance_usd)::numeric, 2) as value
FROM addresses 
WHERE balance_usd IS NOT NULL;

-- QUERY 9: Top Whales by Blockchain
-- =====================================================================
SELECT 
    blockchain,
    COUNT(*) as address_count,
    COUNT(*) FILTER (WHERE balance_usd > 50000) as whale_count,
    AVG(confidence) as avg_confidence,
    SUM(balance_usd) as total_value_usd,
    MAX(balance_usd) as max_balance_usd,
    array_agg(DISTINCT source) as data_sources,
    array_agg(DISTINCT address_type) FILTER (WHERE address_type IS NOT NULL) as address_types
FROM addresses 
WHERE confidence > 0.5
GROUP BY blockchain
ORDER BY whale_count DESC, total_value_usd DESC;

-- QUERY 10: Export All Whale Data (for analysis)
-- =====================================================================
SELECT 
    address,
    blockchain,
    source,
    label,
    confidence,
    balance_usd,
    balance_native,
    address_type,
    signal_potential,
    entity_name,
    detection_method,
    analysis_tags,
    last_seen_tx,
    last_balance_check,
    created_at,
    updated_at,
    
    -- Calculated whale categories
    CASE 
        WHEN balance_usd > 10000000 THEN 'Mega Whale (>$10M)'
        WHEN balance_usd > 1000000 THEN 'Large Whale ($1M-$10M)'
        WHEN balance_usd > 100000 THEN 'Medium Whale ($100K-$1M)'
        WHEN balance_usd > 50000 THEN 'Small Whale ($50K-$100K)'
        WHEN confidence > 0.8 THEN 'High Confidence Address'
        WHEN address_type IN ('whale', 'defi_whale', 'exchange') THEN 'Categorized Whale'
        ELSE 'Standard Address'
    END as whale_category,
    
    -- Risk/potential assessment
    CASE 
        WHEN signal_potential = 'high' THEN 'High Potential'
        WHEN signal_potential = 'medium' THEN 'Medium Potential'
        WHEN confidence > 0.8 AND balance_usd > 100000 THEN 'Verified High Value'
        WHEN balance_usd > 50000 THEN 'High Value'
        ELSE 'Standard'
    END as risk_category
    
FROM addresses 
WHERE 
    confidence > 0.5
    OR balance_usd > 25000
    OR address_type IN ('whale', 'mega_whale', 'defi_whale', 'exchange')
ORDER BY balance_usd DESC NULLS LAST, confidence DESC;

-- QUERY 11: Whale Detection Methods Analysis
-- =====================================================================
SELECT 
    detection_method,
    source,
    COUNT(*) as address_count,
    COUNT(*) FILTER (WHERE balance_usd > 50000) as whale_count,
    AVG(confidence) as avg_confidence,
    AVG(balance_usd) as avg_balance_usd,
    SUM(balance_usd) as total_value_usd
FROM addresses 
WHERE detection_method IS NOT NULL
GROUP BY detection_method, source
ORDER BY whale_count DESC, total_value_usd DESC;

-- QUERY 12: Top Individual Whales
-- =====================================================================
SELECT 
    address,
    blockchain,
    source,
    label,
    confidence,
    balance_usd,
    address_type,
    signal_potential,
    entity_name,
    detection_method,
    last_balance_check,
    updated_at
FROM addresses 
WHERE balance_usd IS NOT NULL
ORDER BY balance_usd DESC
LIMIT 100;

-- QUERY 13: Covalent API Results Summary
-- =====================================================================
SELECT 
    'Covalent Addresses Total' as metric,
    COUNT(*) as count,
    SUM(balance_usd) as total_usd_value,
    AVG(balance_usd) as avg_usd_value,
    AVG(confidence) as avg_confidence
FROM addresses 
WHERE source LIKE 'covalent%'

UNION ALL

SELECT 
    'Covalent High Value ($50K+)' as metric,
    COUNT(*) as count,
    SUM(balance_usd) as total_usd_value,
    AVG(balance_usd) as avg_usd_value,
    AVG(confidence) as avg_confidence
FROM addresses 
WHERE source LIKE 'covalent%' AND balance_usd > 50000;

-- =====================================================================
-- USAGE INSTRUCTIONS:
-- =====================================================================
-- 1. Run QUERY 8 first to get overall statistics
-- 2. Run QUERY 13 to see Covalent API results specifically  
-- 3. Use QUERY 1-7 to find specific types of whales
-- 4. Use QUERY 10 to export all whale data for external analysis
-- 5. Use QUERY 12 to see your top individual whales
-- 6. Adjust balance_usd thresholds based on your whale definition
-- ===================================================================== 
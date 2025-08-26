#!/usr/bin/env python3
"""
Database Setup for Real-Time Transaction Monitoring

This script creates the necessary Supabase table for storing classified swap transactions
from the real-time market flow engine.
"""

import sys
import os
from datetime import datetime

# Add config path
sys.path.append(os.path.join(os.path.dirname(__file__), 'config'))

try:
    from supabase import create_client, Client
    import api_keys
except ImportError as e:
    print(f"❌ Missing required packages. Install with: pip install supabase")
    sys.exit(1)

def create_transaction_monitoring_table():
    """Create the transaction monitoring table in Supabase."""
    
    # Initialize Supabase client
    supabase: Client = create_client(
        api_keys.SUPABASE_URL, 
        api_keys.SUPABASE_SERVICE_ROLE_KEY
    )
    
    # SQL to create the transaction monitoring table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS transaction_monitoring (
        id BIGSERIAL PRIMARY KEY,
        transaction_hash TEXT NOT NULL UNIQUE,
        block_number BIGINT NOT NULL,
        block_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
        chain TEXT NOT NULL CHECK (chain IN ('ethereum', 'polygon', 'solana')),
        dex TEXT NOT NULL CHECK (dex IN ('uniswap_v2', 'uniswap_v3', 'jupiter')),
        
        -- Token Information
        token_in_address TEXT NOT NULL,
        token_out_address TEXT NOT NULL,
        token_in_symbol TEXT,
        token_out_symbol TEXT,
        token_in_decimals INTEGER,
        token_out_decimals INTEGER,
        
        -- Transaction Amounts
        amount_in NUMERIC NOT NULL,
        amount_out NUMERIC NOT NULL,
        amount_in_usd NUMERIC,
        amount_out_usd NUMERIC,
        
        -- Classification
        classification TEXT NOT NULL CHECK (classification IN ('BUY', 'SELL', 'UNKNOWN')),
        confidence_score NUMERIC DEFAULT 1.0 CHECK (confidence_score >= 0 AND confidence_score <= 1),
        
        -- Enrichment Data
        sender_address TEXT NOT NULL,
        recipient_address TEXT,
        is_whale_transaction BOOLEAN DEFAULT FALSE,
        whale_classification TEXT,
        
        -- Price Data
        token_price_usd NUMERIC,
        gas_used BIGINT,
        gas_price NUMERIC,
        transaction_fee_usd NUMERIC,
        
        -- Metadata
        raw_log_data JSONB,
        classification_method TEXT DEFAULT 'stablecoin_heuristic',
        processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        
        -- Indexes for performance
        CONSTRAINT unique_tx_hash UNIQUE (transaction_hash)
    );
    
    -- Create indexes for efficient querying
    CREATE INDEX IF NOT EXISTS idx_transaction_monitoring_chain_dex ON transaction_monitoring (chain, dex);
    CREATE INDEX IF NOT EXISTS idx_transaction_monitoring_block_timestamp ON transaction_monitoring (block_timestamp DESC);
    CREATE INDEX IF NOT EXISTS idx_transaction_monitoring_classification ON transaction_monitoring (classification);
    CREATE INDEX IF NOT EXISTS idx_transaction_monitoring_whale ON transaction_monitoring (is_whale_transaction) WHERE is_whale_transaction = TRUE;
    CREATE INDEX IF NOT EXISTS idx_transaction_monitoring_token_in ON transaction_monitoring (token_in_address);
    CREATE INDEX IF NOT EXISTS idx_transaction_monitoring_token_out ON transaction_monitoring (token_out_address);
    CREATE INDEX IF NOT EXISTS idx_transaction_monitoring_amount_usd ON transaction_monitoring (amount_in_usd DESC) WHERE amount_in_usd IS NOT NULL;
    
    -- Create a view for easy querying of recent high-value transactions
    CREATE OR REPLACE VIEW recent_whale_swaps AS
    SELECT 
        transaction_hash,
        block_timestamp,
        chain,
        dex,
        token_in_symbol,
        token_out_symbol,
        amount_in_usd,
        amount_out_usd,
        classification,
        sender_address,
        is_whale_transaction
    FROM transaction_monitoring 
    WHERE 
        block_timestamp >= NOW() - INTERVAL '24 hours'
        AND (amount_in_usd >= 100000 OR amount_out_usd >= 100000 OR is_whale_transaction = TRUE)
    ORDER BY block_timestamp DESC;
    
    -- Create a view for market sentiment analysis
    CREATE OR REPLACE VIEW market_sentiment_hourly AS
    SELECT 
        DATE_TRUNC('hour', block_timestamp) as hour,
        chain,
        token_in_address,
        token_in_symbol,
        COUNT(*) as total_swaps,
        COUNT(*) FILTER (WHERE classification = 'BUY') as buy_count,
        COUNT(*) FILTER (WHERE classification = 'SELL') as sell_count,
        SUM(amount_in_usd) FILTER (WHERE classification = 'BUY') as total_buy_volume_usd,
        SUM(amount_in_usd) FILTER (WHERE classification = 'SELL') as total_sell_volume_usd,
        ROUND(
            (COUNT(*) FILTER (WHERE classification = 'BUY')::NUMERIC / 
             NULLIF(COUNT(*), 0)) * 100, 2
        ) as buy_percentage
    FROM transaction_monitoring 
    WHERE 
        block_timestamp >= NOW() - INTERVAL '7 days'
        AND amount_in_usd IS NOT NULL
    GROUP BY 
        DATE_TRUNC('hour', block_timestamp),
        chain,
        token_in_address,
        token_in_symbol
    HAVING COUNT(*) >= 5  -- Only include tokens with meaningful activity
    ORDER BY hour DESC, total_buy_volume_usd DESC NULLS LAST;
    """
    
    try:
        # Execute the SQL
        result = supabase.rpc('exec_sql', {'sql': create_table_sql}).execute()
        print("✅ Successfully created transaction_monitoring table and views")
        return True
        
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        # Try alternative method using direct SQL execution
        try:
            # Split the SQL into individual statements
            statements = [stmt.strip() for stmt in create_table_sql.split(';') if stmt.strip()]
            
            for stmt in statements:
                if stmt:
                    supabase.postgrest.rpc('exec_sql', {'sql': stmt}).execute()
            
            print("✅ Successfully created transaction_monitoring table using alternative method")
            return True
            
        except Exception as e2:
            print(f"❌ Alternative method also failed: {e2}")
            print("\n📝 Manual SQL to run in Supabase SQL Editor:")
            print("=" * 60)
            print(create_table_sql)
            print("=" * 60)
            return False

def test_table_creation():
    """Test that the table was created successfully."""
    try:
        supabase: Client = create_client(
            api_keys.SUPABASE_URL, 
            api_keys.SUPABASE_SERVICE_ROLE_KEY
        )
        
        # Try to query the table
        result = supabase.table('transaction_monitoring').select('*').limit(1).execute()
        print("✅ Table query test successful")
        
        # Test the views
        result = supabase.table('recent_whale_swaps').select('*').limit(1).execute()
        print("✅ recent_whale_swaps view test successful")
        
        result = supabase.table('market_sentiment_hourly').select('*').limit(1).execute()
        print("✅ market_sentiment_hourly view test successful")
        
        return True
        
    except Exception as e:
        print(f"❌ Table test failed: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Setting up Transaction Monitoring Database...")
    print(f"📊 Supabase URL: {api_keys.SUPABASE_URL}")
    
    if create_transaction_monitoring_table():
        print("\n🧪 Testing table creation...")
        if test_table_creation():
            print("\n🎉 Database setup complete! Ready for real-time transaction monitoring.")
        else:
            print("\n⚠️  Table created but testing failed. Check Supabase dashboard.")
    else:
        print("\n❌ Database setup failed. Please run the SQL manually in Supabase.") 
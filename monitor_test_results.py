#!/usr/bin/env python3
"""
Real-time monitoring script for Enhanced Monitor 5-minute test.
Checks database for new whale transactions and sentiment data.
"""

import time
import sys
from datetime import datetime, timedelta
from supabase import create_client
import os
from collections import defaultdict, Counter

# Import configuration from the project
from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

def init_supabase():
    """Initialize Supabase client."""
    try:
        return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    except Exception as e:
        print(f"❌ Supabase connection failed: {e}")
        return None

def get_recent_transactions(supabase, minutes_ago=5):
    """Get transactions from the last N minutes."""
    try:
        cutoff_time = datetime.utcnow() - timedelta(minutes=minutes_ago)
        cutoff_str = cutoff_time.isoformat()
        
        response = supabase.table('whale_transactions')\
            .select('*')\
            .gte('created_at', cutoff_str)\
            .order('created_at', desc=True)\
            .execute()
        
        return response.data if response.data else []
    except Exception as e:
        print(f"❌ Error fetching transactions: {e}")
        return []

def get_sentiment_data(supabase, minutes_ago=5):
    """Get sentiment aggregation data."""
    try:
        cutoff_time = datetime.utcnow() - timedelta(minutes=minutes_ago)
        cutoff_str = cutoff_time.isoformat()
        
        response = supabase.table('whale_sentiment_aggregated')\
            .select('*')\
            .gte('updated_at', cutoff_str)\
            .order('buy_sell_ratio', desc=True)\
            .execute()
        
        return response.data if response.data else []
    except Exception as e:
        print(f"❌ Error fetching sentiment data: {e}")
        return []

def analyze_transactions(transactions):
    """Analyze transaction patterns."""
    if not transactions:
        return {}
    
    # Classification analysis
    classifications = Counter(tx.get('classification', 'UNKNOWN') for tx in transactions)
    
    # Token analysis
    tokens = Counter(tx.get('token_symbol', 'UNKNOWN') for tx in transactions)
    
    # Chain analysis
    chains = Counter(tx.get('blockchain', 'UNKNOWN') for tx in transactions)
    
    # Confidence analysis
    confidences = [tx.get('confidence', 0) for tx in transactions]
    avg_confidence = sum(confidences) / len(confidences) if confidences else 0
    
    # BigQuery usage analysis
    bigquery_used = sum(1 for tx in transactions if tx.get('phases_completed', 0) >= 8)
    bigquery_percentage = (bigquery_used / len(transactions)) * 100 if transactions else 0
    
    return {
        'total_transactions': len(transactions),
        'classifications': dict(classifications),
        'top_tokens': dict(tokens.most_common(5)),
        'chains': dict(chains),
        'avg_confidence': avg_confidence,
        'bigquery_usage': {
            'used': bigquery_used,
            'percentage': bigquery_percentage
        }
    }

def display_results(cycle, transactions, sentiment_data, analysis):
    """Display formatted results."""
    print(f"\n{'='*60}")
    print(f"📊 ENHANCED MONITOR TEST - CYCLE {cycle}")
    print(f"⏰ Time: {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*60}")
    
    if not transactions:
        print("⏳ No new transactions detected yet...")
        return
    
    print(f"🔍 TRANSACTION ANALYSIS:")
    print(f"  Total Transactions: {analysis['total_transactions']}")
    print(f"  Average Confidence: {analysis['avg_confidence']:.2f}")
    print(f"  BigQuery Usage: {analysis['bigquery_usage']['used']}/{analysis['total_transactions']} ({analysis['bigquery_usage']['percentage']:.1f}%)")
    
    print(f"\n📈 CLASSIFICATIONS:")
    for classification, count in analysis['classifications'].items():
        percentage = (count / analysis['total_transactions']) * 100
        print(f"  {classification}: {count} ({percentage:.1f}%)")
    
    print(f"\n🪙 TOP TOKENS:")
    for token, count in analysis['top_tokens'].items():
        print(f"  {token}: {count} transactions")
    
    print(f"\n⛓️ BLOCKCHAINS:")
    for chain, count in analysis['chains'].items():
        print(f"  {chain}: {count} transactions")
    
    if sentiment_data:
        print(f"\n💰 WHALE SENTIMENT (Top 5):")
        for item in sentiment_data[:5]:
            token = item.get('token_symbol', 'UNKNOWN')
            ratio = item.get('buy_sell_ratio', 0)
            buys = item.get('total_buys', 0)
            sells = item.get('total_sells', 0)
            print(f"  {token}: Ratio {ratio:.2f} ({buys} buys, {sells} sells)")
    else:
        print(f"\n💰 WHALE SENTIMENT: No aggregated data yet")
    
    print(f"\n🎯 RECENT TRANSACTIONS:")
    for i, tx in enumerate(transactions[:3], 1):
        hash_short = tx.get('transaction_hash', 'N/A')[:10] + '...'
        classification = tx.get('classification', 'UNKNOWN')
        token = tx.get('token_symbol', 'N/A')
        confidence = tx.get('confidence', 0)
        print(f"  {i}. {hash_short} | {classification} | {token} | {confidence:.2f}")

def main():
    """Main monitoring loop."""
    print("🚀 Enhanced Monitor Test Results - Real-time Monitoring")
    print("📊 Checking database every 60 seconds for 5 minutes...")
    
    supabase = init_supabase()
    if not supabase:
        print("❌ Cannot connect to Supabase. Exiting.")
        return
    
    start_time = datetime.now()
    cycle = 1
    
    try:
        while True:
            # Check if 5 minutes have passed
            elapsed = datetime.now() - start_time
            if elapsed.total_seconds() > 300:  # 5 minutes
                print(f"\n⏰ 5-minute test completed!")
                break
            
            # Fetch data
            transactions = get_recent_transactions(supabase, minutes_ago=5)
            sentiment_data = get_sentiment_data(supabase, minutes_ago=5)
            analysis = analyze_transactions(transactions)
            
            # Display results
            display_results(cycle, transactions, sentiment_data, analysis)
            
            cycle += 1
            time.sleep(60)  # Wait 1 minute
            
    except KeyboardInterrupt:
        print(f"\n⚠️ Monitoring stopped by user")
    
    # Final summary
    print(f"\n🎯 FINAL TEST SUMMARY")
    print(f"⏰ Test duration: {elapsed}")
    transactions = get_recent_transactions(supabase, minutes_ago=5)
    analysis = analyze_transactions(transactions)
    
    if transactions:
        print(f"✅ Total transactions captured: {analysis['total_transactions']}")
        print(f"✅ Average confidence: {analysis['avg_confidence']:.2f}")
        print(f"✅ BigQuery cost optimization: {100-analysis['bigquery_usage']['percentage']:.1f}% savings")
        print(f"✅ Classifications: {analysis['classifications']}")
    else:
        print("⚠️ No transactions were captured during the test")

if __name__ == "__main__":
    main() 
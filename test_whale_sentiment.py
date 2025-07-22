#!/usr/bin/env python3
"""
🐋 TEST WHALE SENTIMENT SYSTEM 🐋

Test script to verify the whale sentiment aggregation system is working properly.
"""

import time
from whale_sentiment_aggregator import whale_sentiment_aggregator

def test_sentiment_aggregator():
    """Test the whale sentiment aggregator functionality."""
    print("🐋 Testing Whale Sentiment Aggregator...")
    print("="*60)
    
    # Test 1: Initialize aggregator
    print("Test 1: Initializing aggregator...")
    try:
        print(f"✅ Supabase connected: {whale_sentiment_aggregator.supabase is not None}")
    except Exception as e:
        print(f"❌ Initialization failed: {e}")
        return False
    
    # Test 2: Get current sentiment data
    print("\nTest 2: Getting current sentiment data...")
    try:
        sentiment_data = whale_sentiment_aggregator.get_token_sentiment(hours=2)
        print(f"✅ Retrieved {len(sentiment_data)} tokens with sentiment data")
        
        if sentiment_data:
            print("\nTop 3 tokens by activity:")
            for i, token in enumerate(sentiment_data[:3]):
                print(f"  {i+1}. {token['token_symbol']}: "
                      f"{token['buys']} buys, {token['sells']} sells "
                      f"({token['buy_percentage']:.1f}% buy ratio)")
        else:
            print("ℹ️  No sentiment data available yet (system may be new)")
    except Exception as e:
        print(f"❌ Failed to get sentiment data: {e}")
        return False
    
    # Test 3: Get bullish tokens
    print("\nTest 3: Getting bullish tokens...")
    try:
        bullish_tokens = whale_sentiment_aggregator.get_bullish_tokens(hours=2, min_transactions=1)
        print(f"✅ Retrieved {len(bullish_tokens)} bullish tokens")
        
        if bullish_tokens:
            print("Top bullish tokens:")
            for token in bullish_tokens[:3]:
                print(f"  • {token['token_symbol']}: {token['buy_percentage']:.1f}% buys")
    except Exception as e:
        print(f"❌ Failed to get bullish tokens: {e}")
        return False
    
    # Test 4: Get bearish tokens
    print("\nTest 4: Getting bearish tokens...")
    try:
        bearish_tokens = whale_sentiment_aggregator.get_bearish_tokens(hours=2, min_transactions=1)
        print(f"✅ Retrieved {len(bearish_tokens)} bearish tokens")
        
        if bearish_tokens:
            print("Top bearish tokens:")
            for token in bearish_tokens[:3]:
                print(f"  • {token['token_symbol']}: {token['sell_percentage']:.1f}% sells")
    except Exception as e:
        print(f"❌ Failed to get bearish tokens: {e}")
        return False
    
    # Test 5: Print sentiment summary
    print("\nTest 5: Printing sentiment summary...")
    try:
        whale_sentiment_aggregator.print_sentiment_summary()
        print("✅ Sentiment summary printed successfully")
    except Exception as e:
        print(f"❌ Failed to print sentiment summary: {e}")
        return False
    
    print("\n" + "="*60)
    print("🎉 All tests passed! Whale sentiment system is working properly.")
    return True

def test_enhanced_monitor_integration():
    """Test that enhanced monitor can import and use the sentiment system."""
    print("\n🔗 Testing Enhanced Monitor Integration...")
    print("="*60)
    
    try:
        # Test import
        from enhanced_monitor import SENTIMENT_AGGREGATION_ENABLED, transaction_storage
        print(f"✅ Sentiment aggregation enabled: {SENTIMENT_AGGREGATION_ENABLED}")
        
        # Test transaction storage has the new method
        if hasattr(transaction_storage, 'store_whale_transaction'):
            print("✅ Transaction storage has whale transaction method")
        else:
            print("❌ Transaction storage missing whale transaction method")
            return False
        
        # Test token symbol extraction
        if hasattr(transaction_storage, '_extract_token_symbol'):
            print("✅ Token symbol extraction method available")
            
            # Test extraction with sample data
            test_tx = {
                'symbol': 'ETH',
                'blockchain': 'ethereum',
                'tx_hash': '0x123...'
            }
            symbol = transaction_storage._extract_token_symbol(test_tx)
            print(f"✅ Token symbol extraction test: {symbol}")
        else:
            print("❌ Token symbol extraction method missing")
            return False
        
    except Exception as e:
        print(f"❌ Enhanced monitor integration test failed: {e}")
        return False
    
    print("✅ Enhanced monitor integration working properly")
    return True

def main():
    """Main test function."""
    print("🚀 WHALE SENTIMENT SYSTEM TEST SUITE")
    print("="*60)
    
    # Run tests
    test1_passed = test_sentiment_aggregator()
    test2_passed = test_enhanced_monitor_integration()
    
    # Summary
    print("\n" + "="*60)
    print("📊 TEST RESULTS SUMMARY")
    print("="*60)
    print(f"Sentiment Aggregator: {'✅ PASS' if test1_passed else '❌ FAIL'}")
    print(f"Enhanced Monitor Integration: {'✅ PASS' if test2_passed else '❌ FAIL'}")
    
    if test1_passed and test2_passed:
        print("\n🎉 ALL TESTS PASSED!")
        print("\nYour whale sentiment system is ready to use!")
        print("\nNext steps:")
        print("1. Run 'python enhanced_monitor.py' to start monitoring")
        print("2. Wait for whale transactions to be classified")
        print("3. Check the sentiment summary every minute")
        print("4. Integrate the API queries into your UI")
    else:
        print("\n⚠️  SOME TESTS FAILED")
        print("Please check the error messages above and fix any issues.")
    
    print("\n" + "="*60)

if __name__ == "__main__":
    main() 
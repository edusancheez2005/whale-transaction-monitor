#!/usr/bin/env python3
"""
Debug DEX Classification Logic
"""

import sys
sys.path.append('.')

from utils.classification_final import WhaleIntelligenceEngine, DEXProtocolEngine

def test_specific_transaction():
    """Test the specific transaction that's showing wrong evidence."""
    
    # Transaction: Router → User (should be BUY)
    transaction_data = {
        'hash': '0x3bab9af9fd799b179543f16aaa7ba2f4f9af8e1e06392950182a52448d07d714',
        'from': '0x7a250d5630b4cf539739df2c5dacb4c659f2488d',  # Uniswap Router
        'to': '0xcc9a0c905bb88b84b744fed69490c8f97ad3a585',    # User
        'value': '10000000000000000000',
        'blockchain': 'ethereum'
    }
    
    print("=== TESTING ROUTER → USER TRANSACTION ===")
    print(f"FROM (Router): {transaction_data['from']}")
    print(f"TO (User): {transaction_data['to']}")
    print()
    
    # Test DEX engine directly
    dex_engine = DEXProtocolEngine()
    dex_result = dex_engine.analyze(
        transaction_data['from'], 
        transaction_data['to'], 
        transaction_data['blockchain']
    )
    
    print("=== DEX ENGINE RESULT ===")
    print(f"Classification: {dex_result.classification.value}")
    print(f"Confidence: {dex_result.confidence:.2f}")
    print(f"Evidence: {dex_result.evidence}")
    print()
    
    # Test individual methods
    print("=== TESTING INDIVIDUAL METHODS ===")
    
    # Method 1: _analyze_dex_router_interaction
    router_result = dex_engine._analyze_dex_router_interaction(
        transaction_data['from'], 
        transaction_data['to']
    )
    print(f"Router method: {router_result}")
    
    # Method 2: _check_hardcoded_dex_addresses
    hardcoded_result = dex_engine._check_hardcoded_dex_addresses(
        transaction_data['from'], 
        transaction_data['to']
    )
    print(f"Hardcoded method: {hardcoded_result}")
    
    return dex_result

def test_user_to_router():
    """Test User → Router transaction (should be SELL)."""
    
    print("\n" + "="*50)
    print("=== TESTING USER → ROUTER TRANSACTION ===")
    
    # Transaction: User → Router (should be SELL)
    transaction_data = {
        'from': '0x3e6e7953dd30f30a31cb14beff7fefa34d0e64e5',  # User
        'to': '0x7a250d5630b4cf539739df2c5dacb4c659f2488d',    # Uniswap Router
        'blockchain': 'ethereum'
    }
    
    print(f"FROM (User): {transaction_data['from']}")
    print(f"TO (Router): {transaction_data['to']}")
    print()
    
    # Test DEX engine directly
    dex_engine = DEXProtocolEngine()
    dex_result = dex_engine.analyze(
        transaction_data['from'], 
        transaction_data['to'], 
        transaction_data['blockchain']
    )
    
    print("=== DEX ENGINE RESULT ===")
    print(f"Classification: {dex_result.classification.value}")
    print(f"Confidence: {dex_result.confidence:.2f}")
    print(f"Evidence: {dex_result.evidence}")
    
    return dex_result

if __name__ == "__main__":
    try:
        # Test both directions
        result1 = test_specific_transaction()
        result2 = test_user_to_router()
        
        print("\n" + "="*50)
        print("=== SUMMARY ===")
        print("Both tests should show different classifications:")
        print(f"Router → User: {result1.classification.value} (should be BUY)")
        print(f"User → Router: {result2.classification.value} (should be SELL)")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc() 
#!/usr/bin/env python3
"""
Test Suite for DeFi Protocol Context Intelligence & Directional Logic Enhancement

This test suite verifies the implementation of the enhanced DEXProtocolEngine
with protocol contract detection and directional logic.

Author: DeFi Intelligence Enhancement Team
Version: 1.0.0
"""

import sys
import os
import unittest
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any, List

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.classification_final import DEXProtocolEngine, ClassificationType, PhaseResult, AnalysisPhase


class TestDeFiProtocolContextIntelligence(unittest.TestCase):
    """Test suite for DeFi Protocol Context Intelligence enhancements."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_supabase_client = Mock()
        self.engine = DEXProtocolEngine(self.mock_supabase_client)
        
        # Test addresses
        self.user_address = "0x1234567890123456789012345678901234567890"
        self.protocol_contract = "0x7a250d5630b4cf539739df2c5dacb4c659f2488d"  # Uniswap V2 Router
        self.meth_token_holder = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        
    def test_protocol_contract_verification_address_type(self):
        """Test protocol contract verification using address_type field."""
        # Test data with protocol_contract address_type
        address_data = {
            'address': self.protocol_contract,
            'address_type': 'protocol_router',
            'label': 'Uniswap V2 Router',
            'entity_name': 'uniswap_v2_router',
            'analysis_tags': {}
        }
        
        result = self.engine._is_verified_protocol_contract(address_data)
        self.assertTrue(result, "Should identify protocol_router as verified contract")
    
    def test_protocol_contract_verification_defillama(self):
        """Test protocol contract verification using DeFiLlama data."""
        # Test data with DeFiLlama category
        address_data = {
            'address': self.protocol_contract,
            'address_type': 'unknown',
            'label': 'DEX Router',
            'entity_name': 'uniswap',
            'analysis_tags': {
                'defillama_category': 'dex',
                'defillama_slug': 'uniswap',
                'official_url': 'https://uniswap.org'
            }
        }
        
        result = self.engine._is_verified_protocol_contract(address_data)
        self.assertTrue(result, "Should identify DeFiLlama verified protocol as contract")
    
    def test_protocol_contract_verification_negative(self):
        """Test that user addresses are NOT identified as protocol contracts."""
        # Test data for regular user address
        address_data = {
            'address': self.user_address,
            'address_type': 'user',
            'label': 'User Wallet',
            'entity_name': '',
            'analysis_tags': {}
        }
        
        result = self.engine._is_verified_protocol_contract(address_data)
        self.assertFalse(result, "Should NOT identify user address as protocol contract")
    
    def test_direct_protocol_interaction_positive(self):
        """Test direct protocol interaction detection with verified contract."""
        # Mock Supabase response with protocol contract
        from_result = {
            'address': self.protocol_contract,
            'address_type': 'dex_router',
            'label': 'Uniswap V2 Router',
            'entity_name': 'uniswap_v2_router',
            'analysis_tags': {'defillama_category': 'dex'}
        }
        
        to_result = {
            'address': self.user_address,
            'address_type': 'user',
            'label': 'User Wallet',
            'entity_name': '',
            'analysis_tags': {}
        }
        
        result = self.engine._is_direct_protocol_interaction(from_result, to_result)
        self.assertTrue(result, "Should detect direct protocol interaction with verified contract")
    
    def test_direct_protocol_interaction_negative(self):
        """Test that user-to-user transfers are NOT classified as protocol interactions."""
        # Mock Supabase response with two user addresses
        from_result = {
            'address': self.user_address,
            'address_type': 'user',
            'label': 'User Wallet 1',
            'entity_name': '',
            'analysis_tags': {}
        }
        
        to_result = {
            'address': self.meth_token_holder,
            'address_type': 'user', 
            'label': 'mETH Token Holder',
            'entity_name': '',
            'analysis_tags': {}
        }
        
        result = self.engine._is_direct_protocol_interaction(from_result, to_result)
        self.assertFalse(result, "Should NOT detect protocol interaction between users")
    
    def test_enhanced_directional_logic_liquid_staking(self):
        """Test enhanced directional logic for liquid staking protocols."""
        # Test staking operation (user -> protocol)
        classification, evidence = self.engine._determine_enhanced_protocol_classification(
            ClassificationType.BUY, "LIQUID_STAKING", True, "Lido"
        )
        
        self.assertEqual(classification, ClassificationType.BUY)
        self.assertIn("staking via Lido", evidence)
        
        # Test unstaking operation (protocol -> user)
        classification, evidence = self.engine._determine_enhanced_protocol_classification(
            ClassificationType.BUY, "LIQUID_STAKING", False, "Lido"
        )
        
        self.assertEqual(classification, ClassificationType.SELL)
        self.assertIn("unstaking from Lido", evidence)
    
    def test_enhanced_directional_logic_lending(self):
        """Test enhanced directional logic for lending protocols."""
        # Test deposit operation (user -> protocol)
        classification, evidence = self.engine._determine_enhanced_protocol_classification(
            ClassificationType.BUY, "LENDING", True, "Aave"
        )
        
        self.assertEqual(classification, ClassificationType.BUY)
        self.assertIn("supplying to Aave", evidence)
        
        # Test withdrawal operation (protocol -> user)
        classification, evidence = self.engine._determine_enhanced_protocol_classification(
            ClassificationType.BUY, "LENDING", False, "Aave"
        )
        
        self.assertEqual(classification, ClassificationType.SELL)
        self.assertIn("withdrawing from Aave", evidence)
    
    def test_enhanced_directional_logic_dex(self):
        """Test enhanced directional logic for DEX protocols."""
        # Test selling operation (user -> DEX)
        classification, evidence = self.engine._determine_enhanced_protocol_classification(
            ClassificationType.BUY, "DEX", True, "Uniswap"
        )
        
        self.assertEqual(classification, ClassificationType.SELL)
        self.assertIn("selling via Uniswap", evidence)
        
        # Test buying operation (DEX -> user)
        classification, evidence = self.engine._determine_enhanced_protocol_classification(
            ClassificationType.BUY, "DEX", False, "Uniswap"
        )
        
        self.assertEqual(classification, ClassificationType.BUY)
        self.assertIn("buying via Uniswap", evidence)
    
    def test_bridge_classification_override(self):
        """Test that bridge interactions are classified as TRANSFER."""
        classification, evidence = self.engine._determine_enhanced_protocol_classification(
            ClassificationType.BUY, "BRIDGE", True, "Stargate"
        )
        
        self.assertEqual(classification, ClassificationType.TRANSFER)
        self.assertIn("bridge transfer", evidence)
    
    @patch('utils.classification_final.DEFI_PROTOCOL_SETTINGS', {
        'require_verified_contracts': True,
        'enable_directional_logic': True,
        'protocol_confidence_boost': 0.15,
        'bridge_classification_override': True
    })
    def test_supabase_defi_protocols_with_verification(self):
        """Test full Supabase DeFi protocol analysis with verification."""
        # Mock Supabase response
        mock_response = Mock()
        mock_response.data = [
            {
                'address': self.protocol_contract,
                'label': 'Uniswap V2 Router',
                'address_type': 'dex_router',
                'confidence': 0.8,
                'entity_name': 'uniswap_v2_router',
                'signal_potential': 'high',
                'balance_usd': 1000000,
                'balance_native': 100,
                'detection_method': 'defillama_verified',
                'analysis_tags': {
                    'defillama_category': 'dex',
                    'defillama_slug': 'uniswap'
                },
                'last_seen_tx': 'recent'
            }
        ]
        
        self.mock_supabase_client.table.return_value.select.return_value.in_.return_value.eq.return_value.execute.return_value = mock_response
        
        result = self.engine._check_supabase_defi_protocols(
            self.user_address, self.protocol_contract, "ethereum"
        )
        
        self.assertIsNotNone(result, "Should return classification result")
        self.assertIn(result.classification, [ClassificationType.BUY, ClassificationType.SELL])
        self.assertGreater(result.confidence, 0.7, "Should have high confidence for verified protocol")
        self.assertTrue(result.raw_data.get('is_verified_protocol', False))
    
    def test_user_to_user_transfer_classification(self):
        """Test that user-to-user transfers are correctly classified as TRANSFER."""
        # Mock Supabase response with two user addresses (no protocol contracts)
        mock_response = Mock()
        mock_response.data = [
            {
                'address': self.user_address,
                'label': 'User Wallet 1',
                'address_type': 'user',
                'confidence': 0.5,
                'entity_name': '',
                'signal_potential': 'low',
                'balance_usd': 50000,
                'balance_native': 10,
                'detection_method': 'manual',
                'analysis_tags': {},
                'last_seen_tx': 'recent'
            },
            {
                'address': self.meth_token_holder,
                'label': 'mETH Token Holder',
                'address_type': 'user',
                'confidence': 0.5,
                'entity_name': '',
                'signal_potential': 'low', 
                'balance_usd': 75000,
                'balance_native': 15,
                'detection_method': 'manual',
                'analysis_tags': {},
                'last_seen_tx': 'recent'
            }
        ]
        
        self.mock_supabase_client.table.return_value.select.return_value.in_.return_value.eq.return_value.execute.return_value = mock_response
        
        result = self.engine._check_supabase_defi_protocols(
            self.user_address, self.meth_token_holder, "ethereum"
        )
        
        self.assertIsNotNone(result, "Should return classification result")
        self.assertEqual(result.classification, ClassificationType.TRANSFER)
        self.assertLess(result.confidence, 0.3, "Should have low confidence for user transfer")
        self.assertIn("user-to-user transfer", result.evidence[0].lower())
        self.assertFalse(result.raw_data.get('protocol_interaction', True))
    
    def test_comprehensive_defi_protocol_detection(self):
        """Test comprehensive DeFi protocol detection logic."""
        # Test with DeFiLlama DEX category
        result = self.engine._comprehensive_defi_protocol_detection(
            'dex_router', 'Uniswap Router', 'uniswap', 
            {'defillama_category': 'dex', 'defillama_slug': 'uniswap'}, 'high'
        )
        
        self.assertIsNotNone(result, "Should detect DeFi protocol")
        classification, confidence_boost, protocol_type, evidence = result
        
        self.assertEqual(protocol_type, "DEX")
        self.assertGreater(confidence_boost, 0.25)
        self.assertTrue(any("DeFiLlama DEX" in ev for ev in evidence))
    
    def run_comprehensive_test_suite(self):
        """Run all tests and provide summary."""
        print("🧪 Running DeFi Protocol Context Intelligence Test Suite...")
        print("=" * 70)
        
        test_methods = [method for method in dir(self) if method.startswith('test_')]
        passed = 0
        failed = 0
        
        for test_method in test_methods:
            try:
                print(f"📋 Running {test_method}...")
                getattr(self, test_method)()
                print(f"✅ {test_method} - PASSED")
                passed += 1
            except Exception as e:
                print(f"❌ {test_method} - FAILED: {str(e)}")
                failed += 1
        
        print("=" * 70)
        print(f"📊 Test Results: {passed} passed, {failed} failed")
        
        if failed == 0:
            print("🎉 ALL TESTS PASSED! DeFi Protocol Context Intelligence is working correctly.")
        else:
            print("⚠️  Some tests failed. Please review the implementation.")
        
        return failed == 0


def main():
    """Main test execution function."""
    print("🚀 DeFi Protocol Context Intelligence & Directional Logic Test Suite")
    print("📅 Testing implementation of Claude Sonnet 4 Enhancement Prompt")
    print("")
    
    # Create test instance and run comprehensive suite
    test_suite = TestDeFiProtocolContextIntelligence()
    test_suite.setUp()
    
    success = test_suite.run_comprehensive_test_suite()
    
    if success:
        print("\n✨ Enhancement Implementation: SUCCESS")
        print("🎯 Key Features Verified:")
        print("   ✓ Protocol contract vs user address detection")
        print("   ✓ Direct protocol interaction verification")
        print("   ✓ Enhanced directional logic for DeFi categories")
        print("   ✓ User-to-user transfer classification")
        print("   ✓ Configuration-driven verification system")
        return 0
    else:
        print("\n❌ Enhancement Implementation: ISSUES DETECTED")
        return 1


if __name__ == "__main__":
    exit(main()) 
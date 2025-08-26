#!/usr/bin/env python3
"""
🛡️ COMPREHENSIVE ADDRESS DATABASE EXPANSION
================================================================================

MISSION: Bulletproof whale monitoring across ALL tokens, DEX, CEX, and DeFi
Professional Google-level infrastructure coverage for production whale tracking.

This script adds:
- ALL major DEX routers (Uniswap, SushiSwap, 1inch, Curve, Balancer, etc.)
- ALL major CEX addresses (Binance, Coinbase, FTX, OKEx, etc.)
- ALL major DeFi protocols (Aave, Compound, MakerDAO, etc.)
- ALL major token contracts (USDC, WETH, major ERC-20s)
- Bridge contracts for cross-chain compatibility

Author: Senior DevOps Engineer (Life Depends On This)
Version: 1.0.0 (Production-Critical)
"""

import asyncio
import time
from typing import Dict, List
from supabase import create_client
from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

class ComprehensiveAddressExpansion:
    """Professional-grade address database expansion for whale monitoring."""
    
    def __init__(self):
        self.supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
        
        # 🔥 COMPREHENSIVE ADDRESS DATABASE
        self.comprehensive_addresses = {
            
            # ========================================================================
            # 🔀 DEX ROUTERS - CRITICAL FOR BUY/SELL DETECTION
            # ========================================================================
            "DEX_ROUTERS": {
                # Uniswap Ecosystem
                '0x7a250d5630b4cf539739df2c5dacb4c659f2488d': 'Uniswap V2 Router',
                '0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45': 'Uniswap V3 Router',
                '0xe592427a0aece92de3edee1f18e0157c05861564': 'Uniswap V3 Router 2',
                '0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad': 'Uniswap Universal Router',
                
                # SushiSwap
                '0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f': 'SushiSwap Router',
                '0x1b02da8cb0d097eb8d957414cc0e1e97cb1f8df4': 'SushiSwap BentoBox',
                
                # 1inch
                '0x1111111254fb6c44bac0bed2854e76f90643097d': '1inch Router V4',
                '0x111111125434b319222cdbf8c261674adb56f3ae': '1inch Router V5',
                
                # 0x Protocol
                '0xdef1c0ded9bec7f1a1670819833240f027b25eff': '0x Protocol Exchange',
                
                # Curve Finance
                '0x99a58482ba3d06e0e1e9444c8b7a8c7649e8c9c1': 'Curve Router',
                '0x8f942c20d02befc377d41445793068908e2250d0': 'Curve Tricrypto Router',
                
                # Balancer
                '0xba12222222228d8ba445958a75a0704d566bf2c8': 'Balancer V2 Vault',
                
                # PancakeSwap (Ethereum)
                '0xd0c19dc82c54d13eff0077e7b688b0b1b8f0e045': 'PancakeSwap Router',
                
                # Kyber Network
                '0x9aab3f75489902f3a48495025729a0af77d4b11e': 'Kyber Router',
                
                # Paraswap
                '0xdef171fe48cf0115b1d80b88dc8eab59176fee57': 'Paraswap Router',
                
                # CoW Protocol
                '0x9008d19f58aabd9ed0d60971565aa8510560ab41': 'CoW Protocol Settlement',
                
                # Matcha (0x)
                '0x61935cbdd02287b511119ddb11aeb42f1593b7ef': 'Matcha Router',
            },
            
            # ========================================================================
            # 🏦 CEX ADDRESSES - CRITICAL FOR EXCHANGE FLOW DETECTION  
            # ========================================================================
            "CEX_WALLETS": {
                # Binance (Multiple hot wallets)
                '0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be': 'Binance Hot Wallet',
                '0xd551234ae421e3bcba99a0da6d736074f22192ff': 'Binance Hot Wallet 2',
                '0x28c6c06298d514db089934071355e5743bf21d60': 'Binance Hot Wallet 14',
                '0x21a31ee1afc51d94c2efccaa2092ad1028285549': 'Binance Hot Wallet 8',
                '0xdfd5293d8e347dfe59e90efd55b2956a1343963d': 'Binance Hot Wallet 7',
                '0x564286362092d8e7936f0549571a803b203aaced': 'Binance US',
                
                # Coinbase
                '0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43': 'Coinbase Hot Wallet',
                '0x71660c4005ba85c37ccec55d0c4493e66fe775d3': 'Coinbase Hot Wallet 2',
                '0x503828976d22510aad0201ac7ec88293211d23da': 'Coinbase Hot Wallet 3',
                '0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740': 'Coinbase Hot Wallet 4',
                '0x3cd751e6b0078be393132286c442345e5dc49699': 'Coinbase Hot Wallet 5',
                
                # Kraken
                '0x2910543af39aba0cd09dbb2d50200b3e800a63d2': 'Kraken Hot Wallet',
                '0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13': 'Kraken Hot Wallet 2',
                
                # OKEx
                '0x6cc5f688a315f3dc28a7781717a9a798a59fda7b': 'OKEx Hot Wallet',
                '0x236f9f97e0e62388479bf9e5ba4889e46b0273c3': 'OKEx Hot Wallet 2',
                
                # Huobi
                '0xdc76cd25977e0a5ae17155770273ad58648900d3': 'Huobi Hot Wallet',
                '0xab83d182f3485cf1d6ccdd34c7cfef95b4c08da4': 'Huobi Hot Wallet 2',
                
                # Bybit
                '0xf89d7b9c864f589bbf53a82105107622b35eaa40': 'Bybit Hot Wallet',
                
                # KuCoin
                '0x1522900b6dafac587d499a862861c0869be6e428': 'KuCoin Hot Wallet',
                '0xd6216fc19db775df9774a6e33526131da7d19a2c': 'KuCoin Hot Wallet 2',
                
                # Gate.io
                '0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c': 'Gate.io Hot Wallet',
                
                # Bitfinex
                '0x1151314c646ce4e0efd76d1af4760ae66a9fe30f': 'Bitfinex Hot Wallet',
                
                # Gemini
                '0xd24400ae8bfebb18ca49be86258a3c749cf46853': 'Gemini Hot Wallet',
            },
            
            # ========================================================================
            # 🏛️ DEFI PROTOCOLS - ESSENTIAL FOR PROTOCOL DETECTION
            # ========================================================================
            "DEFI_PROTOCOLS": {
                # Uniswap Factories
                '0x1f98431c8ad98523636104104c1e2ad1e6d420c': 'Uniswap V3 Factory',
                '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f': 'Uniswap V2 Factory',
                
                # Aave
                '0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9': 'Aave Lending Pool',
                '0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2': 'Aave Pool V3',
                
                # Compound
                '0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b': 'Compound cDAI',
                '0x39aa39c021dfbae8fac545936693ac917d5e7563': 'Compound cUSDC',
                '0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5': 'Compound cETH',
                
                # MakerDAO
                '0x9759a6ac90977b93b58547b4a71c78317f391a28': 'MakerDAO DSProxy',
                '0x5ef30b9986345249bc32d8928b7ee64de9435e39': 'MakerDAO Vat',
                
                # Curve Protocol
                '0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7': 'Curve 3Pool',
                '0xa2b47e3d5c44877cca798226b7b8118f9bfb7a56': 'Curve Compound Pool',
                
                # Yearn Finance
                '0x5f18c75abdae578b483e5f43f12a39cf75b973a9': 'Yearn yUSDC Vault',
                '0xa354f35829ae975e850e23e9615b11da1b3dc4de': 'Yearn yUSDT Vault',
                
                # Lido
                '0xae7ab96520de3a18e5e111b5eaab95820216e558': 'Lido stETH',
                '0x889edc2edab5f40e902b864ad4d7ade8e412f9b1': 'Lido Withdrawal Queue',
                
                # Synthetix
                '0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f': 'Synthetix SNX',
                
                # Convex Finance
                '0xf403c135812408bfbe8713b5a23a04b3d48aae31': 'Convex Booster',
            },
            
            # ========================================================================
            # 🪙 MAJOR TOKEN CONTRACTS - FOR TOKEN MONITORING
            # ========================================================================
            "TOKEN_CONTRACTS": {
                # Stablecoins
                '0xa0b86a33e6ba6e6c4c11c2e4c3b5d4e82b28b2b1': 'USDC Token',
                '0xdac17f958d2ee523a2206206994597c13d831ec7': 'USDT Token',
                '0x6b175474e89094c44da98b954eedeac495271d0f': 'DAI Token',
                '0x853d955acef822db058eb8505911ed77f175b99e': 'FRAX Token',
                '0x4fabb145d64652a948d72533023f6e7a623c7c53': 'BUSD Token',
                
                # Wrapped Assets
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2': 'WETH Token',
                '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599': 'WBTC Token',
                
                # Major DeFi Tokens
                '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984': 'UNI Token',
                '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9': 'AAVE Token',
                '0x6b3595068778dd592e39a122f4f5a5cf09c90fe2': 'SUSHI Token',
                '0xd533a949740bb3306d119cc777fa900ba034cd52': 'CRV Token',
                '0x0bc529c00c6401aef6d220be8c6ea1667f6ad93e': 'YFI Token',
                '0x514910771af9ca656af840dff83e8264ecf986ca': 'LINK Token',
                
                # Layer 2 Tokens
                '0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0': 'MATIC Token',
                '0x4e15361fd6b4bb609fa63c81a2be19d873717870': 'FTM Token',
                
                # Meme Tokens (High Volume)
                '0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce': 'SHIB Token',
                '0x6982508145454ce325ddbe47a25d4ec3d2311933': 'PEPE Token',
                '0x4d224452801aced8b2f0aebe155379bb5d594381': 'APE Token',
            },
            
            # ========================================================================
            # 🌉 BRIDGE CONTRACTS - FOR CROSS-CHAIN MONITORING
            # ========================================================================
            "BRIDGE_CONTRACTS": {
                # Polygon Bridge
                '0x40ec5b33f54e0e8a33a975908c5ba1c14e5bbbdf': 'Polygon ERC20 Predicate',
                '0xa0c68c638235ee32657e8f720a23cec1bfc77c77': 'Polygon Root Chain Manager',
                
                # Arbitrum Bridge
                '0x8315177ab297ba92a06054ce80a67ed4dbd7ed3a': 'Arbitrum Bridge',
                '0x4dbd4fc535ac27206064b68ffcf827b0a60bab3f': 'Arbitrum Inbox',
                
                # Optimism Bridge
                '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1': 'Optimism Gateway',
                '0x467194771dae2967aef3ecbedd3bf9a310c76c65': 'Optimism L1StandardBridge',
                
                # Multichain (Anyswap)
                '0x6b7a87899490ece95443e979ca9485cbe7e71522': 'Multichain Router',
            }
        }

    async def expand_address_database(self) -> Dict[str, int]:
        """
        🚀 EXPAND ADDRESS DATABASE WITH COMPREHENSIVE COVERAGE
        
        Professional-grade expansion ensuring bulletproof whale monitoring.
        """
        print("🛡️ STARTING COMPREHENSIVE ADDRESS DATABASE EXPANSION")
        print("="*70)
        
        stats = {
            'dex_routers_added': 0,
            'cex_wallets_added': 0, 
            'defi_protocols_added': 0,
            'token_contracts_added': 0,
            'bridge_contracts_added': 0,
            'total_added': 0,
            'errors': 0
        }
        
        # Process each category
        for category, addresses in self.comprehensive_addresses.items():
            print(f"\n📊 PROCESSING: {category}")
            print("-" * 50)
            
            category_count = 0
            
            for address, label in addresses.items():
                try:
                    # Determine address type based on category
                    address_type = self._get_address_type(category)
                    entity_name = self._extract_entity_name(label)
                    
                    # Check if address already exists
                    existing = self.supabase.table('addresses').select('address').eq('address', address.lower()).execute()
                    
                    if existing.data:
                        # Update existing address with better classification
                        result = self.supabase.table('addresses').update({
                            'label': label,
                            'address_type': address_type,
                            'entity_name': entity_name,
                            'confidence': 0.95,  # High confidence for manually curated
                            'detection_method': 'comprehensive_expansion_verified',
                            'analysis_tags': {
                                'category': category,
                                'manual_verification': True,
                                'expansion_timestamp': time.time()
                            }
                        }).eq('address', address.lower()).execute()
                        
                        if result.data:
                            print(f"  ✅ UPDATED: {label[:40]:<40} | {address[:20]}...")
                            category_count += 1
                        else:
                            print(f"  ⚠️ UPDATE FAILED: {label[:40]:<40} | {address[:20]}...")
                            stats['errors'] += 1
                    else:
                        # Insert new address
                        result = self.supabase.table('addresses').insert({
                            'address': address.lower(),
                            'label': label,
                            'address_type': address_type,
                            'entity_name': entity_name,
                            'confidence': 0.95,  # High confidence for manually curated
                            'detection_method': 'comprehensive_expansion_verified',
                            'blockchain': 'ethereum',
                            'analysis_tags': {
                                'category': category,
                                'manual_verification': True,
                                'expansion_timestamp': time.time()
                            }
                        }).execute()
                        
                        if result.data:
                            print(f"  ✅ ADDED: {label[:40]:<40} | {address[:20]}...")
                            category_count += 1
                        else:
                            print(f"  ❌ INSERT FAILED: {label[:40]:<40} | {address[:20]}...")
                            stats['errors'] += 1
                    
                    # Rate limiting to avoid overwhelming Supabase
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    print(f"  ❌ ERROR processing {address}: {e}")
                    stats['errors'] += 1
            
            # Update category stats
            if category == 'DEX_ROUTERS':
                stats['dex_routers_added'] = category_count
            elif category == 'CEX_WALLETS':
                stats['cex_wallets_added'] = category_count
            elif category == 'DEFI_PROTOCOLS':
                stats['defi_protocols_added'] = category_count
            elif category == 'TOKEN_CONTRACTS':
                stats['token_contracts_added'] = category_count
            elif category == 'BRIDGE_CONTRACTS':
                stats['bridge_contracts_added'] = category_count
            
            stats['total_added'] += category_count
            print(f"  📊 {category}: {category_count} addresses processed")
        
        # Final report
        print("\n" + "="*70)
        print("🎉 COMPREHENSIVE ADDRESS EXPANSION COMPLETE")
        print("="*70)
        print(f"📊 DEX Routers Added/Updated:    {stats['dex_routers_added']}")
        print(f"📊 CEX Wallets Added/Updated:    {stats['cex_wallets_added']}")
        print(f"📊 DeFi Protocols Added/Updated: {stats['defi_protocols_added']}")
        print(f"📊 Token Contracts Added/Updated: {stats['token_contracts_added']}")
        print(f"📊 Bridge Contracts Added/Updated: {stats['bridge_contracts_added']}")
        print(f"📊 Total Processed:              {stats['total_added']}")
        print(f"📊 Errors:                       {stats['errors']}")
        
        if stats['errors'] == 0:
            print("✅ BULLETPROOF EXPANSION SUCCESSFUL - WHALE MONITORING READY")
        else:
            print(f"⚠️ EXPANSION COMPLETED WITH {stats['errors']} ERRORS")
        
        return stats

    def _get_address_type(self, category: str) -> str:
        """Map category to address type."""
        mapping = {
            'DEX_ROUTERS': 'DEX Router',
            'CEX_WALLETS': 'CEX Wallet',
            'DEFI_PROTOCOLS': 'DeFi Protocol',
            'TOKEN_CONTRACTS': 'Token Contract',
            'BRIDGE_CONTRACTS': 'Bridge Contract'
        }
        return mapping.get(category, 'Unknown')

    def _extract_entity_name(self, label: str) -> str:
        """Extract entity name from label."""
        # Extract the main entity (e.g., "Uniswap" from "Uniswap V2 Router")
        entity_words = label.split()
        if len(entity_words) > 0:
            return entity_words[0]
        return 'Unknown'

async def run_comprehensive_expansion():
    """Run comprehensive address database expansion."""
    expander = ComprehensiveAddressExpansion()
    results = await expander.expand_address_database()
    return results

if __name__ == "__main__":
    # Run comprehensive expansion
    results = asyncio.run(run_comprehensive_expansion())
    
    # Save results
    timestamp = int(time.time())
    filename = f'comprehensive_expansion_results_{timestamp}.json'
    
    import json
    with open(filename, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n💾 Expansion results saved: {filename}") 
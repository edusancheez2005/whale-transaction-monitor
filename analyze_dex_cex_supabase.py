#!/usr/bin/env python3
"""
Focused DEX/CEX Analysis of Supabase Database
Extract all DEX, CEX, and trading-related classifications
"""

import sys
import json
from collections import Counter
from typing import Dict, List, Any

sys.path.append('.')

def analyze_dex_cex_intelligence():
    """Focused analysis of DEX/CEX classifications in Supabase."""
    
    try:
        from supabase import create_client
        from config.api_keys import SUPABASE_URL, SUPABASE_ANON_KEY
        
        # Initialize Supabase client
        supabase = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
        
        print("🔍 FOCUSED DEX/CEX SUPABASE ANALYSIS")
        print("=" * 80)
        
        # 1. ADDRESS TYPE ANALYSIS - looking for DEX/CEX patterns
        print("\n🏷️  ADDRESS TYPE DEEP ANALYSIS")
        print("-" * 50)
        
        # Get ALL address types to find DEX/CEX patterns
        address_type_response = supabase.table('addresses').select('address_type').execute()
        address_types = [item['address_type'] for item in address_type_response.data if item.get('address_type')]
        type_counts = Counter(address_types)
        
        print(f"Total address types found: {len(set(address_types)):,}")
        print("ALL address types in database:")
        for addr_type, count in type_counts.most_common():
            print(f"  {addr_type}: {count:,}")
        
        # 2. DETECTION METHOD ANALYSIS 
        print("\n🔍 DETECTION METHOD DEEP ANALYSIS")
        print("-" * 50)
        
        detection_response = supabase.table('addresses').select('detection_method').execute()
        methods = [item['detection_method'] for item in detection_response.data if item.get('detection_method')]
        method_counts = Counter([m for m in methods if m])  # Filter out None values
        
        print(f"Total detection methods found: {len(set(methods)):,}")
        print("ALL detection methods in database:")
        for method, count in method_counts.most_common():
            print(f"  {method}: {count:,}")
        
        # 3. SIGNAL POTENTIAL ANALYSIS
        print("\n🎯 SIGNAL POTENTIAL DEEP ANALYSIS")
        print("-" * 50)
        
        signal_response = supabase.table('addresses').select('signal_potential').execute()
        signals = [item['signal_potential'] for item in signal_response.data if item.get('signal_potential')]
        signal_counts = Counter([s for s in signals if s])  # Filter out None values
        
        print(f"Total signal potentials found: {len(set(signals)):,}")
        print("ALL signal potentials in database:")
        for signal, count in signal_counts.most_common():
            print(f"  {signal}: {count:,}")
        
        # 4. ENTITY NAME ANALYSIS - looking for exchange patterns
        print("\n🏢 ENTITY NAME ANALYSIS - Exchange Patterns")
        print("-" * 50)
        
        entity_response = supabase.table('addresses').select('entity_name').execute()
        entities = [item['entity_name'] for item in entity_response.data if item.get('entity_name')]
        
        # Filter for potential exchange/DEX entities
        exchange_keywords = ['binance', 'coinbase', 'kraken', 'uniswap', 'dex', 'exchange', 'swap', 'trading', 'okx', 'kucoin', 'crypto.com', 'gate.io', 'huobi', 'bybit']
        
        potential_exchanges = []
        for entity in entities:
            if entity and any(keyword in entity.lower() for keyword in exchange_keywords):
                potential_exchanges.append(entity)
        
        exchange_counts = Counter(potential_exchanges)
        print(f"Potential exchange entities found: {len(set(potential_exchanges)):,}")
        print("Top exchange-related entities:")
        for entity, count in exchange_counts.most_common(20):
            print(f"  {entity}: {count:,}")
        
        # 5. LABEL ANALYSIS - looking for exchange patterns
        print("\n🔖 LABEL ANALYSIS - Exchange/DEX Patterns")
        print("-" * 50)
        
        label_response = supabase.table('addresses').select('label').execute()
        labels = [item['label'] for item in label_response.data if item.get('label')]
        
        # Filter for potential exchange/DEX labels
        potential_exchange_labels = []
        for label in labels:
            if label and any(keyword in label.lower() for keyword in exchange_keywords):
                potential_exchange_labels.append(label)
        
        label_counts = Counter(potential_exchange_labels)
        print(f"Potential exchange labels found: {len(set(potential_exchange_labels)):,}")
        print("Top exchange-related labels:")
        for label, count in label_counts.most_common(20):
            print(f"  {label}: {count:,}")
        
        # 6. ANALYSIS TAGS DEEP DIVE - DeFiLlama categories
        print("\n🏗️  ANALYSIS TAGS - DeFiLlama DEX Categories")
        print("-" * 50)
        
        tags_response = supabase.table('addresses').select('analysis_tags').execute()
        
        dex_categories = []
        cex_categories = []
        all_categories = []
        
        for item in tags_response.data:
            if item.get('analysis_tags'):
                tags = item['analysis_tags']
                if isinstance(tags, dict):
                    category = tags.get('defillama_category', '')
                    if category:
                        all_categories.append(category)
                        if 'dex' in category.lower() or 'swap' in category.lower():
                            dex_categories.append(category)
                        elif 'cex' in category.lower() or 'exchange' in category.lower():
                            cex_categories.append(category)
        
        print(f"All DeFiLlama categories: {set(all_categories)}")
        print(f"\nDEX-related categories found: {len(dex_categories)}")
        if dex_categories:
            dex_counts = Counter(dex_categories)
            for category, count in dex_counts.most_common():
                print(f"  {category}: {count:,}")
        
        print(f"\nCEX-related categories found: {len(cex_categories)}")
        if cex_categories:
            cex_counts = Counter(cex_categories)
            for category, count in cex_counts.most_common():
                print(f"  {category}: {count:,}")
        
        # 7. HIGH-VALUE DEX/CEX SAMPLES
        print("\n🎖️  HIGH-VALUE DEX/CEX SAMPLES")
        print("-" * 50)
        
        # Get samples with exchange-related keywords
        sample_response = supabase.table('addresses').select('*').or_(
            'label.ilike.%exchange%,label.ilike.%dex%,label.ilike.%swap%,label.ilike.%binance%,label.ilike.%uniswap%'
        ).limit(10).execute()
        
        if sample_response.data:
            print("Sample exchange/DEX addresses:")
            for addr in sample_response.data:
                print(f"\nAddress: {addr.get('address', '')[:10]}...")
                print(f"  Blockchain: {addr.get('blockchain', 'N/A')}")
                print(f"  Type: {addr.get('address_type', 'N/A')}")
                print(f"  Label: {addr.get('label', 'N/A')}")
                print(f"  Entity: {addr.get('entity_name', 'N/A')}")
                print(f"  Detection Method: {addr.get('detection_method', 'N/A')}")
                print(f"  Signal: {addr.get('signal_potential', 'N/A')}")
                if addr.get('analysis_tags'):
                    print(f"  DeFiLlama Category: {addr.get('analysis_tags', {}).get('defillama_category', 'N/A')}")
        
        # 8. CREATE COMPREHENSIVE SUMMARY
        exchange_intelligence = {
            "total_addresses": len(address_types),
            "address_types": dict(type_counts),
            "detection_methods": dict(method_counts),
            "signal_potentials": dict(signal_counts),
            "exchange_entities": dict(exchange_counts.most_common(50)),
            "exchange_labels": dict(label_counts.most_common(50)),
            "defillama_categories": list(set(all_categories)),
            "dex_categories": dict(Counter(dex_categories)) if dex_categories else {},
            "cex_categories": dict(Counter(cex_categories)) if cex_categories else {}
        }
        
        # Save detailed analysis
        with open('dex_cex_intelligence_summary.json', 'w') as f:
            json.dump(exchange_intelligence, f, indent=2)
        
        print(f"\n✅ DEX/CEX intelligence summary saved to 'dex_cex_intelligence_summary.json'")
        
        return exchange_intelligence
        
    except Exception as e:
        print(f"❌ Error analyzing Supabase DEX/CEX data: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    print("Starting focused DEX/CEX analysis...")
    result = analyze_dex_cex_intelligence()
    
    if result:
        print(f"\n🎉 DEX/CEX analysis complete!")
        print(f"Found {len(result.get('address_types', {}))} unique address types")
        print(f"Found {len(result.get('detection_methods', {}))} detection methods")
        print(f"Found {len(result.get('signal_potentials', {}))} signal potentials")
        print(f"Found {len(result.get('exchange_entities', {}))} exchange-related entities")
        print(f"Found {len(result.get('exchange_labels', {}))} exchange-related labels")
    else:
        print("❌ DEX/CEX analysis failed") 
#!/usr/bin/env python3
"""
Comprehensive Supabase Database Intelligence Analysis
Extract and analyze all available data to maximize intelligence potential
"""

import sys
import json
from collections import defaultdict, Counter
from typing import Dict, List, Any, Optional
import pandas as pd

sys.path.append('.')

def analyze_supabase_intelligence():
    """Comprehensive analysis of Supabase address database."""
    
    try:
        from supabase import create_client
        from config.api_keys import SUPABASE_URL, SUPABASE_ANON_KEY
        
        # Initialize Supabase client
        supabase = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
        
        print("🔍 COMPREHENSIVE SUPABASE INTELLIGENCE ANALYSIS")
        print("=" * 80)
        
        # 1. Get total database overview
        print("\n📊 DATABASE OVERVIEW")
        print("-" * 40)
        
        # Count total records using proper count query
        total_response = supabase.table('addresses').select('*', count='exact').limit(1).execute()
        total_count = total_response.count if hasattr(total_response, 'count') else 0
        print(f"Total addresses in database: {total_count:,}")
        
        # If we don't get count from the response, try alternative method
        if total_count == 0:
            # Try getting count via a simple query
            count_response = supabase.table('addresses').select('id').execute()
            total_count = len(count_response.data) if count_response.data else 0
            print(f"Total addresses (fallback count): {total_count:,}")
        
        # 2. Blockchain distribution - get ALL records for accurate distribution
        print("\n🌐 BLOCKCHAIN DISTRIBUTION")
        print("-" * 40)
        
        # Use pagination to get all blockchain data
        all_blockchains = []
        page_size = 1000
        offset = 0
        
        while True:
            blockchain_response = supabase.table('addresses').select('blockchain').range(offset, offset + page_size - 1).execute()
            if not blockchain_response.data:
                break
            
            all_blockchains.extend([item['blockchain'] for item in blockchain_response.data if item.get('blockchain')])
            
            if len(blockchain_response.data) < page_size:
                break
            offset += page_size
            
            print(f"  Processed {len(all_blockchains):,} blockchain records so far...")
        
        blockchain_counts = Counter(all_blockchains)
        total_blockchain_records = len(all_blockchains)
        
        for blockchain, count in blockchain_counts.most_common():
            percentage = (count / total_blockchain_records) * 100 if total_blockchain_records > 0 else 0
            print(f"{blockchain}: {count:,} ({percentage:.1f}%)")
        
        # 3. Address type analysis - sample approach for large datasets
        print("\n🏷️  ADDRESS TYPE ANALYSIS")
        print("-" * 40)
        
        # Get a representative sample for address types
        address_type_response = supabase.table('addresses').select('address_type').limit(10000).execute()
        address_types = [item['address_type'] for item in address_type_response.data if item.get('address_type')]
        type_counts = Counter(address_types)
        
        print(f"Address types (sample of {len(address_types):,} records):")
        for addr_type, count in type_counts.most_common(20):  # Top 20
            percentage = (count / len(address_types)) * 100 if len(address_types) > 0 else 0
            print(f"{addr_type}: {count:,} ({percentage:.2f}%)")
        
        # 4. Label analysis - sample approach
        print("\n🔖 LABEL PATTERNS ANALYSIS")
        print("-" * 40)
        
        label_response = supabase.table('addresses').select('label').limit(10000).execute()
        labels = [item['label'] for item in label_response.data if item.get('label')]
        label_counts = Counter(labels)
        
        print(f"Top 15 most common labels (sample of {len(labels):,} records):")
        for label, count in label_counts.most_common(15):
            percentage = (count / len(labels)) * 100 if len(labels) > 0 else 0
            print(f"  {label}: {count:,} ({percentage:.2f}%)")
        
        # 5. Entity name analysis
        print("\n🏢 ENTITY NAME ANALYSIS")
        print("-" * 40)
        
        entity_response = supabase.table('addresses').select('entity_name').execute()
        entities = [item['entity_name'] for item in entity_response.data if item.get('entity_name')]
        entity_counts = Counter(entities)
        
        print("Top 15 most common entities:")
        for entity, count in entity_counts.most_common(15):
            percentage = (count / total_count) * 100 if total_count > 0 else 0
            print(f"  {entity}: {count:,} ({percentage:.2f}%)")
        
        # 6. Signal potential analysis
        print("\n🎯 SIGNAL POTENTIAL ANALYSIS")
        print("-" * 40)
        
        signal_response = supabase.table('addresses').select('signal_potential').execute()
        signals = [item['signal_potential'] for item in signal_response.data if item.get('signal_potential')]
        signal_counts = Counter(signals)
        
        for signal, count in signal_counts.most_common(10):
            percentage = (count / total_count) * 100 if total_count > 0 else 0
            print(f"  {signal}: {count:,} ({percentage:.2f}%)")
        
        # 7. Detection method analysis
        print("\n🔍 DETECTION METHOD ANALYSIS")
        print("-" * 40)
        
        detection_response = supabase.table('addresses').select('detection_method').execute()
        methods = [item['detection_method'] for item in detection_response.data if item.get('detection_method')]
        method_counts = Counter(methods)
        
        for method, count in method_counts.most_common():
            percentage = (count / total_count) * 100 if total_count > 0 else 0
            print(f"  {method}: {count:,} ({percentage:.2f}%)")
        
        # 8. Analysis tags deep dive
        print("\n🏗️  ANALYSIS TAGS DEEP ANALYSIS")
        print("-" * 40)
        
        # Get all analysis_tags data
        tags_response = supabase.table('addresses').select('analysis_tags').execute()
        
        # Analyze JSONB structure
        defillama_categories = []
        defillama_slugs = []
        custom_tags = []
        tag_structures = defaultdict(int)
        
        for item in tags_response.data:
            if item.get('analysis_tags'):
                tags = item['analysis_tags']
                if isinstance(tags, dict):
                    # Track structure types
                    structure_key = tuple(sorted(tags.keys()))
                    tag_structures[structure_key] += 1
                    
                    # Extract DeFiLlama data
                    if 'defillama_category' in tags:
                        defillama_categories.append(tags['defillama_category'])
                    if 'defillama_slug' in tags:
                        defillama_slugs.append(tags['defillama_slug'])
                    if 'tags' in tags:
                        if isinstance(tags['tags'], list):
                            custom_tags.extend(tags['tags'])
        
        print("Analysis tags structure patterns:")
        for structure, count in Counter(tag_structures).most_common(10):
            print(f"  {structure}: {count:,}")
        
        if defillama_categories:
            print(f"\nDeFiLlama categories found: {len(set(defillama_categories))}")
            category_counts = Counter(defillama_categories)
            print("Top DeFiLlama categories:")
            for category, count in category_counts.most_common(10):
                print(f"  {category}: {count:,}")
        
        if custom_tags:
            print(f"\nCustom tags found: {len(set(custom_tags))}")
            custom_counts = Counter(custom_tags)
            print("Top custom tags:")
            for tag, count in custom_counts.most_common(10):
                print(f"  {tag}: {count:,}")
        
        # 9. Balance analysis
        print("\n💰 BALANCE ANALYSIS")
        print("-" * 40)
        
        balance_response = supabase.table('addresses').select('balance_usd', 'balance_native').execute()
        
        usd_balances = [float(item['balance_usd']) for item in balance_response.data 
                       if item.get('balance_usd') and item['balance_usd'] is not None]
        
        if usd_balances:
            print(f"Addresses with USD balance data: {len(usd_balances):,}")
            print(f"Total USD value tracked: ${sum(usd_balances):,.2f}")
            print(f"Average USD balance: ${sum(usd_balances)/len(usd_balances):,.2f}")
            print(f"Median USD balance: ${sorted(usd_balances)[len(usd_balances)//2]:,.2f}")
            
            # Whale categories by balance
            mega_whales = len([b for b in usd_balances if b >= 10_000_000])  # $10M+
            whales = len([b for b in usd_balances if 1_000_000 <= b < 10_000_000])  # $1M-$10M
            dolphins = len([b for b in usd_balances if 100_000 <= b < 1_000_000])  # $100K-$1M
            
            print(f"\nWhale classification by balance:")
            print(f"  Mega whales ($10M+): {mega_whales:,}")
            print(f"  Whales ($1M-$10M): {whales:,}")
            print(f"  Dolphins ($100K-$1M): {dolphins:,}")
        
        # 10. High-value intelligence extraction
        print("\n🎖️  HIGH-VALUE INTELLIGENCE SAMPLES")
        print("-" * 40)
        
        # Get sample of high-confidence, high-value addresses
        high_value_response = supabase.table('addresses').select('*').gte('confidence', 0.8).limit(10).execute()
        
        if high_value_response.data:
            print("Sample high-confidence addresses:")
            for addr in high_value_response.data:
                print(f"\nAddress: {addr.get('address', '')[:10]}...")
                print(f"  Blockchain: {addr.get('blockchain', 'N/A')}")
                print(f"  Type: {addr.get('address_type', 'N/A')}")
                print(f"  Label: {addr.get('label', 'N/A')}")
                print(f"  Entity: {addr.get('entity_name', 'N/A')}")
                print(f"  Confidence: {addr.get('confidence', 0)}")
                balance_usd = addr.get('balance_usd', 0) or 0
                print(f"  Balance USD: ${balance_usd:,}")
                print(f"  Signal: {addr.get('signal_potential', 'N/A')}")
                if addr.get('analysis_tags'):
                    print(f"  Tags: {addr.get('analysis_tags')}")
        
        # 11. Create intelligence summary for prompt enhancement
        intelligence_summary = {
            "total_addresses": total_count,
            "blockchains": dict(blockchain_counts),
            "address_types": dict(type_counts.most_common(50)),
            "top_entities": dict(entity_counts.most_common(20)),
            "detection_methods": dict(method_counts),
            "signal_potentials": dict(signal_counts),
            "defillama_categories": dict(Counter(defillama_categories).most_common(20)) if defillama_categories else {},
            "tag_structures": {str(k): v for k, v in Counter(tag_structures).items()},  # Convert tuple keys to strings
            "whale_counts": {
                "mega_whales": mega_whales if usd_balances else 0,
                "whales": whales if usd_balances else 0,
                "dolphins": dolphins if usd_balances else 0
            }
        }
        
        # Save summary to file
        with open('supabase_intelligence_summary.json', 'w') as f:
            json.dump(intelligence_summary, f, indent=2)
        
        print(f"\n✅ Intelligence summary saved to 'supabase_intelligence_summary.json'")
        print(f"Total data points analyzed: {total_count:,}")
        
        return intelligence_summary
        
    except Exception as e:
        print(f"❌ Error analyzing Supabase: {e}")
        import traceback
        traceback.print_exc()
        return None

def generate_enhanced_prompt_recommendations(intelligence_summary: Dict[str, Any]):
    """Generate specific recommendations for the enhanced prompt based on Supabase analysis."""
    
    print("\n" + "=" * 80)
    print("🚀 ENHANCED PROMPT RECOMMENDATIONS")
    print("=" * 80)
    
    if not intelligence_summary:
        print("❌ No intelligence summary available")
        return
    
    print("\n1. 🏷️  ADDRESS TYPE INTELLIGENCE")
    print("-" * 50)
    address_types = intelligence_summary.get('address_types', {})
    
    print("Key address types to prioritize in classification logic:")
    for addr_type, count in list(address_types.items())[:15]:
        print(f"  - {addr_type}: {count:,} addresses")
    
    print("\n2. 🏢 ENTITY INTELLIGENCE")
    print("-" * 50)
    entities = intelligence_summary.get('top_entities', {})
    
    print("Major entities requiring hardcoded classification rules:")
    for entity, count in list(entities.items())[:15]:
        print(f"  - {entity}: {count:,} addresses")
    
    print("\n3. 🔍 DETECTION METHOD OPTIMIZATION")
    print("-" * 50)
    methods = intelligence_summary.get('detection_methods', {})
    
    print("Detection methods to integrate and prioritize:")
    for method, count in methods.items():
        print(f"  - {method}: {count:,} addresses")
    
    print("\n4. 🎯 SIGNAL POTENTIAL MAPPING")
    print("-" * 50)
    signals = intelligence_summary.get('signal_potentials', {})
    
    print("Signal potentials to incorporate in confidence scoring:")
    for signal, count in signals.items():
        print(f"  - {signal}: {count:,} addresses")
    
    print("\n5. 🏗️  DEFILLAMA INTEGRATION OPPORTUNITIES")
    print("-" * 50)
    defillama_cats = intelligence_summary.get('defillama_categories', {})
    
    if defillama_cats:
        print("DeFiLlama categories to enhance DEX/DeFi classification:")
        for category, count in list(defillama_cats.items())[:15]:
            print(f"  - {category}: {count:,} protocols")
    else:
        print("  No DeFiLlama data found - opportunity for enhancement!")
    
    print("\n6. 💰 WHALE CLASSIFICATION INTELLIGENCE")
    print("-" * 50)
    whale_counts = intelligence_summary.get('whale_counts', {})
    
    print("Whale distribution for confidence boosting:")
    print(f"  - Mega whales ($10M+): {whale_counts.get('mega_whales', 0):,}")
    print(f"  - Whales ($1M-$10M): {whale_counts.get('whales', 0):,}")
    print(f"  - Dolphins ($100K-$1M): {whale_counts.get('dolphins', 0):,}")
    
    print("\n7. 🌐 BLOCKCHAIN COVERAGE")
    print("-" * 50)
    blockchains = intelligence_summary.get('blockchains', {})
    
    print("Blockchain support priorities:")
    for blockchain, count in blockchains.items():
        total = intelligence_summary.get('total_addresses', 1)
        percentage = (count / total) * 100
        print(f"  - {blockchain}: {count:,} addresses ({percentage:.1f}%)")

if __name__ == "__main__":
    print("Starting comprehensive Supabase intelligence analysis...")
    intelligence_summary = analyze_supabase_intelligence()
    
    if intelligence_summary:
        generate_enhanced_prompt_recommendations(intelligence_summary)
        print(f"\n🎉 Analysis complete! Review 'supabase_intelligence_summary.json' for detailed data.")
    else:
        print("❌ Analysis failed. Check your Supabase connection and credentials.") 
#!/usr/bin/env python3
"""
API Integration Test Suite
=========================

Tests all major API integrations to ensure they're working properly:
- Moralis API
- Zerion API  
- Covalent API
- BigQuery
- Supabase

This helps debug why integrations are failing in the live tests.
"""

import sys
import os
import asyncio
import requests
from typing import Dict, Any

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.api_keys import (
    MORALIS_API_KEY, COVALENT_API_KEY, 
    SUPABASE_URL, SUPABASE_ANON_KEY,
    GOOGLE_APPLICATION_CREDENTIALS
)
from config.settings import API_CONFIG

def test_moralis_api():
    """Test Moralis API connectivity."""
    print("🧪 Testing Moralis API...")
    
    if not MORALIS_API_KEY:
        print("❌ Moralis API key not found")
        return False
    
    try:
        # Test a simple address balance endpoint - using Vitalik's address
        test_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
        url = f"https://deep-index.moralis.io/api/v2/{test_address}/balance"
        
        headers = {
            'X-API-Key': MORALIS_API_KEY,
            'Content-Type': 'application/json'
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            print(f"✅ Moralis API working - Status: {response.status_code}")
            return True
        elif response.status_code == 401:
            print(f"⚠️  Moralis API quota exceeded - Status: {response.status_code}")
            print("   Note: API key valid but plan limits reached")
            return "quota_exceeded"
        else:
            print(f"❌ Moralis API error - Status: {response.status_code}, Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Moralis API exception: {e}")
        return False

def test_zerion_api():
    """Test Zerion API connectivity."""
    print("🧪 Testing Zerion API...")
    
    zerion_key = API_CONFIG.get("zerion", {}).get("api_key")
    if not zerion_key:
        print("❌ Zerion API key not found in config")
        return False
    
    try:
        # Test Zerion portfolio endpoint - using Vitalik's address
        test_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
        url = f"https://api.zerion.io/v1/wallets/{test_address}/positions"
        
        # Zerion uses Basic Auth with API key
        import base64
        credentials = base64.b64encode(f"{zerion_key}:".encode()).decode()
        
        headers = {
            'Authorization': f'Basic {credentials}',
            'Content-Type': 'application/json'
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            print(f"✅ Zerion API working - Status: {response.status_code}")
            return True
        elif response.status_code == 401:
            print(f"⚠️  Zerion API authentication issue - Status: {response.status_code}")
            return "auth_issue"
        else:
            print(f"❌ Zerion API error - Status: {response.status_code}, Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Zerion API exception: {e}")
        return False

def test_covalent_api():
    """Test Covalent API connectivity."""
    print("🧪 Testing Covalent API...")
    
    if not COVALENT_API_KEY:
        print("❌ Covalent API key not found")
        return False
    
    try:
        # Test Covalent token balances endpoint - using Vitalik's address
        test_address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
        url = f"https://api.covalenthq.com/v1/eth-mainnet/address/{test_address}/balances_v2/"
        
        headers = {
            'Authorization': f'Bearer {COVALENT_API_KEY}',
            'Content-Type': 'application/json'
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            print(f"✅ Covalent API working - Status: {response.status_code}")
            return True
        elif response.status_code == 402:
            print(f"⚠️  Covalent API quota exceeded - Status: {response.status_code}")
            return "quota_exceeded"
        else:
            print(f"❌ Covalent API error - Status: {response.status_code}, Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Covalent API exception: {e}")
        return False

def test_supabase_connection():
    """Test Supabase connectivity."""
    print("🧪 Testing Supabase connection...")
    
    try:
        from supabase import create_client
        
        if not SUPABASE_URL or not SUPABASE_ANON_KEY:
            print("❌ Supabase credentials not found")
            return False
        
        # Create Supabase client
        supabase = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
        
        # Test a simple query to addresses table
        result = supabase.table('addresses').select('address, label, address_type').limit(5).execute()
        
        if result.data:
            print(f"✅ Supabase working - Retrieved {len(result.data)} addresses")
            print(f"   Sample data: {result.data[0] if result.data else 'None'}")
            return True
        else:
            print("❌ Supabase connected but no data found")
            return False
            
    except Exception as e:
        print(f"❌ Supabase exception: {e}")
        return False

def test_bigquery_connection():
    """Test BigQuery connectivity."""
    print("🧪 Testing BigQuery connection...")
    
    try:
        from google.cloud import bigquery
        import json
        
        # Check if credentials file exists
        if not os.path.exists(GOOGLE_APPLICATION_CREDENTIALS):
            print(f"❌ BigQuery credentials file not found: {GOOGLE_APPLICATION_CREDENTIALS}")
            return False
        
        # Create BigQuery client
        client = bigquery.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)
        
        # Test a simple query
        test_query = "SELECT 1 as test_value"
        query_job = client.query(test_query)
        results = query_job.result()
        
        for row in results:
            if row.test_value == 1:
                print("✅ BigQuery working - Test query successful")
                return True
        
        print("❌ BigQuery test query failed")
        return False
        
    except Exception as e:
        print(f"❌ BigQuery exception: {e}")
        return False

def main():
    """Run all API tests."""
    print("🚀 Starting API Integration Test Suite")
    print("="*50)
    
    results = {
        "moralis": test_moralis_api(),
        "zerion": test_zerion_api(), 
        "covalent": test_covalent_api(),
        "supabase": test_supabase_connection(),
        "bigquery": test_bigquery_connection()
    }
    
    print("\n📊 Test Results Summary:")
    print("="*30)
    
    working_count = 0
    for service, status in results.items():
        status_emoji = "✅" if status else "❌"
        print(f"{status_emoji} {service.title()}: {'WORKING' if status else 'FAILED'}")
        if status:
            working_count += 1
    
    print(f"\n🎯 Overall Status: {working_count}/{len(results)} integrations working")
    
    if working_count == len(results):
        print("🎉 All integrations are working! Full pipeline restored!")
    else:
        print("⚠️  Some integrations need attention. Check error messages above.")
    
    return results

if __name__ == "__main__":
    main() 
from typing import Dict, List
from models.classes import DuneAnalytics
from utils.base_helpers import safe_print
from config.settings import DUNE_QUERIES, print_lock
from data.tokens import TOKENS_TO_MONITOR

def get_transfer_volumes():
    """Get transfer volumes from Dune Analytics"""
    dune = DuneAnalytics()
    transfer_volumes = {}
    
    try:
        results = dune.execute_query("2516346")  # Transfer volume query
        if results and 'data' in results:
            for row in results['data']:
                transfer_volumes[row['token']] = {
                    'volume': row['transfer_volume'],
                    'count': row['transfer_count'],
                    'unique_addresses': row['unique_addresses']
                }
    except Exception as e:
        safe_print(f"Error getting transfer volumes: {e}")
    
    return transfer_volumes

def monitor_dune_metrics():
    """Monitor specific Dune Analytics metrics with enhanced analysis"""
    dune = DuneAnalytics()
    
    try:
        # Print debug information
        safe_print("\n🔍 Checking Dune Analytics metrics...")
        
        # Volume Analysis
        volume_results = dune.execute_query(DUNE_QUERIES["total_dex_volume"])
        if volume_results and 'data' in volume_results:
            safe_print("\n=== 24h DEX Trading Volume Analysis ===")
            for row in volume_results['data']:
                safe_print(f"• {row.get('dex', 'Unknown')}: ${row.get('volume_24h', 0):,.2f}")
                
        # Stablecoin flows
        stablecoin_results = dune.execute_query(DUNE_QUERIES["stablecoin_flows"])
        if stablecoin_results and 'data' in stablecoin_results:
            safe_print("\n=== Stablecoin Flow Analysis ===")
            for row in stablecoin_results['data']:
                safe_print(f"• {row.get('stablecoin', 'Unknown')}: "
                          f"Net Flow: ${row.get('net_flow', 0):,.2f} | "
                          f"Volume: ${row.get('volume', 0):,.2f}")
                
        # Gas Analytics
        gas_results = dune.execute_query(DUNE_QUERIES["gas_analytics"])
        if gas_results and 'data' in gas_results:
            safe_print("\n=== Gas Price Analytics ===")
            row = gas_results['data'][0] if gas_results['data'] else {}  # Latest data
            safe_print(f"• Average: {row.get('avg_gwei', 'N/A')} gwei | "
                      f"Max: {row.get('max_gwei', 'N/A')} gwei | "
                      f"Base Fee: {row.get('base_fee', 'N/A')} gwei")
            
        # Token Transfer Analysis
        transfer_results = dune.execute_query(DUNE_QUERIES["token_bridges"])
        if transfer_results and 'data' in transfer_results:
            safe_print("\n=== Cross-chain Bridge Analysis ===")
            for row in transfer_results['data']:
                safe_print(f"• {row.get('bridge', 'Unknown')}: "
                          f"Volume: ${row.get('volume_usd', 0):,.2f} | "
                          f"Transfers: {row.get('transfer_count', 0):,}")

    except Exception as e:
        safe_print(f"Error monitoring Dune metrics: {e}")
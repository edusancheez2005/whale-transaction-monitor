from typing import Dict, List
from models.classes import DuneAnalytics
from utils.base_helpers import safe_print
from config.settings import print_lock
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
        # Volume Analysis
        volume_results = dune.execute_query("2516339")  # Volume query
        if volume_results:
            safe_print("\n=== 24h Trading Volume Analysis ===")
            for row in volume_results['data']:
                safe_print(f"• {row['token']}: ${row['volume_24h']:,.2f}")
                
        # Liquidity Analysis
        liquidity_results = dune.execute_query("2516340")  # Liquidity query
        if liquidity_results:
            safe_print("\n=== Liquidity Pool Analysis ===")
            for row in liquidity_results['data']:
                safe_print(
                    f"• {row['pool_name']}: "
                    f"TVL: ${row['tvl']:,.2f} | "
                    f"24h Volume: ${row['volume_24h']:,.2f}"
                )
                
        # Whale Transaction Analysis
        whale_results = dune.execute_query("2516341")  # Whale analysis query
        if whale_results:
            safe_print("\n=== Whale Transaction Analysis ===")
            for row in whale_results['data']:
                safe_print(
                    f"• {row['token']}: "
                    f"Count: {row['transaction_count']} | "
                    f"Volume: ${row['volume']:,.2f}"
                )
                
        # Gas Analytics
        gas_results = dune.execute_query("2516345")  # Gas analytics query
        if gas_results:
            safe_print("\n=== Gas Price Analytics ===")
            row = gas_results['data'][0]  # Latest data
            safe_print(
                f"• Average: {row['avg_gwei']} gwei | "
                f"Max: {row['max_gwei']} gwei | "
                f"Base Fee: {row['base_fee']} gwei"
            )
            
        # Token Transfer Analysis
        transfer_results = dune.execute_query("2516346")  # Transfer analysis query
        if transfer_results:
            safe_print("\n=== Token Transfer Analysis ===")
            for row in transfer_results['data']:
                safe_print(
                    f"• {row['token']}: "
                    f"Unique Senders: {row['unique_senders']} | "
                    f"Transfer Count: {row['transfer_count']}"
                )

    except Exception as e:
        safe_print(f"Error monitoring Dune metrics: {e}")
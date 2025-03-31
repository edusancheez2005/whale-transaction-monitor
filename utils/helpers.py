import os
import sys
import time
import signal
import threading
import traceback
from typing import Dict
from config.settings import print_lock, shutdown_flag
from collections import defaultdict
from utils.base_helpers import safe_print
from config.settings import (
    etherscan_buy_counts,
    etherscan_sell_counts,
    whale_buy_counts,
    whale_sell_counts,
    solana_buy_counts,
    solana_sell_counts,
    xrp_payment_count,
    xrp_total_amount
)
from utils.dedup import deduplicator
from utils.summary import print_final_aggregated_summary
from datetime import datetime, timedelta

transaction_cache = {'token_symbol': {'tx_hash': {'timestamp': datetime, 'amount': float}}}


def safe_shutdown():
    """Safe shutdown function that can be called from anywhere"""
    signal_handler(None, None)


def get_dex_name(address):
    address = address.lower()
    dex_mapping = {
        "0x7a250d5630b4cf539739df2c5dacb4c659f2488d": "uniswap_v2",
        "0xe592427a0aece92de3edee1f18e0157c05861564": "uniswap_v3",
        "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f": "sushiswap",
        "0xdef171fe48cf0115b1d80b88dc8eab59176fee57": "paraswap",
        "0x1111111254fb6c44bac0bed2854e76f90643097d": "1inch"
    }
    return dex_mapping.get(address)

def get_protocol_slug(token_symbol):
    protocol_mapping = {
        "UNI": "uniswap",
        "SUSHI": "sushiswap",
        "AAVE": "aave",
        "COMP": "compound",
        "CRV": "curve",
        "BAL": "balancer",
        "YFI": "yearn",
    }
    return protocol_mapping.get(token_symbol.upper())

def is_significant_tvl_movement(protocol_tvl, amount):
    try:
        if not protocol_tvl or not isinstance(protocol_tvl, dict):
            return False
            
        current_tvl = protocol_tvl.get('tvl', 0)
        if isinstance(current_tvl, (list, dict)):
            if isinstance(current_tvl, dict) and 'total' in current_tvl:
                current_tvl = float(current_tvl['total'])
            else:
                return False
                
        current_tvl = float(current_tvl)
        if current_tvl == 0:
            return False
            
        amount_percentage = (float(amount) / current_tvl) * 100
        return amount_percentage > 0.1
    except Exception as e:
        print(f"Error checking TVL significance: {str(e)}")
        return False


def get_bridge_name(address):
    """Convert address to bridge name for DeFiLlama API"""
    address = address.lower()
    bridge_mapping = {
        "0x3ee18b2214aff97000d974cf647e7c347e8fa585": "wormhole",
        "0x40ec5b33f54e0e8a33a975908c5ba1c14e5bbbdf": "polygon_bridge",
        "0x8ea3156f834a0dcc4d76ce41146ad73942c06f24": "arbitrum_bridge",
        "0x99c9fc46f92e8a1c0dec1b1747d010903e884be1": "optimism_bridge"
    }
    return bridge_mapping.get(address)

def matches_historical_pattern(flow_data, from_addr, to_addr):
    """Analyze if transaction matches historical patterns"""
    try:
        transfers = flow_data.get('data', {}).get('ethereum', {}).get('transfers', [])
        
        # Count how many times these addresses have interacted
        interaction_count = 0
        for transfer in transfers:
            sender = transfer.get('sender', {}).get('address', '').lower()
            receiver = transfer.get('receiver', {}).get('address', '').lower()
            if (sender == from_addr.lower() and receiver == to_addr.lower()) or \
               (sender == to_addr.lower() and receiver == from_addr.lower()):
                interaction_count += 1
        
        # If we've seen these addresses interact before, it's a pattern
        return interaction_count > 0
    except Exception as e:
        print(f"Error analyzing historical pattern: {e}")
        return False





# In utils/helpers.py - Update signal_handler and clean_shutdown functions

# Add these color codes at the top of the file
# ANSI color codes
HEADER = '\033[95m'
BLUE = '\033[94m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
RED = '\033[91m'
BOLD = '\033[1m'
UNDERLINE = '\033[4m'
END = '\033[0m'

def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully with color output"""
    print(f"\n{YELLOW}{BOLD}Initiating shutdown...{END}")
    shutdown_flag.set()
    
    # Clear the screen
    os.system('clear')  # For macOS/Linux
    
    try:
        # Print a colorful header
        print(f"\n{BLUE}{BOLD}{'=' * 80}{END}")
        print(f"{HEADER}{BOLD}{' ' * 30}FINAL ANALYSIS REPORT{END}")
        print(f"{BLUE}{BOLD}{'=' * 80}{END}\n")
        
        # Print the summary
        print_final_aggregated_summary()
        
        print(f"\n{GREEN}Shutdown complete.{END}")
        
        # Force exit
        sys.exit(0)
        
    except Exception as e:
        print(f"\n{RED}Error during shutdown: {e}{END}")
        traceback.print_exc()
        sys.exit(1)

# Register the signal handler
signal.signal(signal.SIGINT, signal_handler)

def clean_shutdown():
    """Safe shutdown function that can be called from anywhere"""
    try:
        # Stop all websocket connections
        for thread in list(threading.enumerate()):  # Create a copy of the thread list
            if thread != threading.current_thread():
                if hasattr(thread, '_stop'):
                    thread._stop()

        # Force clear the screen
        os.system('cls' if os.name == 'nt' else 'clear')
        
        # Give time for screen to clear
        time.sleep(1)
        
        # Make a copy of all data structures that will be used in summary
        with print_lock:
            # Create a copy of all data needed for the summary
            etherscan_buys = dict(etherscan_buy_counts)
            etherscan_sells = dict(etherscan_sell_counts)
            whale_buys = dict(whale_buy_counts)
            whale_sells = dict(whale_sell_counts)
            solana_buys = dict(solana_buy_counts)
            solana_sells = dict(solana_sell_counts)
            
            # Print summary using the copied data
            print("\n" + "=" * 80)
            print(" " * 30 + "FINAL ANALYSIS REPORT")
            print("=" * 80)
            
            # Combine and sort all transaction data
            aggregated_buy = defaultdict(int)
            aggregated_sell = defaultdict(int)
            
            for coin, count in etherscan_buys.items():
                if count > 0:  # Only include non-zero counts
                    aggregated_buy[coin] += count
            for coin, count in etherscan_sells.items():
                if count > 0:
                    aggregated_sell[coin] += count
            for coin, count in whale_buys.items():
                if count > 0:
                    aggregated_buy[coin] += count
            for coin, count in whale_sells.items():
                if count > 0:
                    aggregated_sell[coin] += count
            for coin, count in solana_buys.items():
                if count > 0:
                    aggregated_buy[coin] += count
            for coin, count in solana_sells.items():
                if count > 0:
                    aggregated_sell[coin] += count

            # Calculate summaries
            summaries = []
            for coin in set(list(aggregated_buy.keys()) + list(aggregated_sell.keys())):
                buys = aggregated_buy[coin]
                sells = aggregated_sell[coin]
                total = buys + sells
                if total > 0:  # Only include coins with activity
                    buy_pct = (buys / total * 100) if total else 0
                    summaries.append({
                        'coin': coin,
                        'buys': buys,
                        'sells': sells,
                        'total': total,
                        'buy_pct': buy_pct,
                        'trend': "↑" if buy_pct > 55 else "↓" if buy_pct < 45 else "→"
                    })

            # Sort by total volume
            summaries.sort(key=lambda x: x['total'], reverse=True)

            # Print formatted table
            print("\n{:<8} {:>10} {:>10} {:>10} {:>10} {:>8}".format(
                "COIN", "BUYS", "SELLS", "TOTAL", "BUY %", "TREND"))
            print("-" * 60)

            for summary in summaries:
                print("{:<8} {:>10,d} {:>10,d} {:>10,d} {:>9.1f}% {:>8}".format(
                    summary['coin'],
                    summary['buys'],
                    summary['sells'],
                    summary['total'],
                    summary['buy_pct'],
                    summary['trend']
                ))

            print("\nSession ended:", time.strftime('%Y-%m-%d %H:%M:%S'))
            print("=" * 80)
        
        # Force exit
        os._exit(0)
        
    except Exception as e:
        print(f"Error during shutdown: {e}")
        traceback.print_exc()
        os._exit(1)

def compute_buy_percentage(buys, sells):
    total = buys + sells
    return buys / total if total else 0



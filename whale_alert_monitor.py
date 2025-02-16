#!/usr/bin/env python3
"""
Combined Multi-Chain Coin Monitor with Aggregated Analysis

This script:
 - Opens a Whale Alert websocket for live "whale" transactions
 - Polls the Etherscan API for ERC‑20 token transfers
 - Polls Solscan API for Solana token transfers
 - Opens an XRP Ledger websocket for XRP Payment transactions
 - Classifies transactions as "buy" or "sell" using heuristics
 - Maintains counters for all transaction types
 - Displays aggregated summary and crypto news headlines on exit
"""

import os
import threading
from typing import List
import time
import signal
import threading
from utils.base_helpers import safe_print
from utils.helpers import get_dex_name, safe_shutdown
from chains.ethereum import print_new_erc20_transfers
import signal
import sys

# Local imports
from config.settings import (
    shutdown_flag,
)
from data.tokens import TOKEN_PRICES
from models.classes import BitQueryAPI, CoinGeckoAPI, DuneAnalytics
from utils.base_helpers import safe_print
from utils.helpers import signal_handler
from chains.ethereum import print_new_erc20_transfers, test_etherscan_connection
from chains.solana import start_solana_thread
from chains.whale_alert import start_whale_thread
from chains.xrp import start_xrp_thread
from chains.dune import monitor_dune_metrics
from chains.bitquery import monitor_bitquery_transfers
from models.classes import initialize_prices
from chains.bitquery import monitor_bitquery_transfers
from chains.dune import monitor_dune_metrics
from utils.summary import print_final_aggregated_summary



def initialize_price_updates() -> threading.Thread:
    """Start price update thread"""
    price_thread = threading.Thread(
        target=update_token_prices,
        daemon=True
    )
    price_thread.start()
    return price_thread

def update_token_prices() -> None:
    """Periodically update token prices from CoinGecko"""
    coingecko = CoinGeckoAPI()
    while not shutdown_flag.is_set():
        try:
            global TOKEN_PRICES
            TOKEN_PRICES = coingecko.initialize_prices()
            safe_print("\nToken prices updated from CoinGecko")
            time.sleep(300)  # Update every 5 minutes
        except Exception as e:
            safe_print(f"Error updating prices: {e}")
            time.sleep(60)  # Shorter retry on error

def start_monitoring() -> List[threading.Thread]:
    """Initialize and start all monitoring threads"""
    threads = []
    
    # Start price updates
    threads.append(initialize_price_updates())
    
    # Start chain monitors
    threads.append(start_whale_thread())
    threads.append(start_xrp_thread())
    threads.append(start_solana_thread())
    
    return threads

def main():
    # Register signal handler for SIGINT (Ctrl+C)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    safe_print("Starting Combined Multi-Chain Coin Monitor...")
    safe_print("Press Ctrl+C to exit.\n")

    try:
        # Test initial connections
        if not test_etherscan_connection():
            safe_print("Failed to connect to Etherscan API.")
            return

        # Initialize prices
        global TOKEN_PRICES
        TOKEN_PRICES = initialize_prices()

        # Start monitoring threads
        active_threads = []
        active_threads.append(start_whale_thread())
        active_threads.append(start_xrp_thread())
        active_threads.append(start_solana_thread())

        # Main monitoring loop
        while not shutdown_flag.is_set():
            try:
                print_new_erc20_transfers()
                time.sleep(60)
            except KeyboardInterrupt:
                break
            
    except KeyboardInterrupt:
        signal_handler(signal.SIGINT, None)
    except Exception as e:
        safe_print(f"Error in main loop: {e}")
        signal_handler(signal.SIGINT, None)

if __name__ == "__main__":
    main()
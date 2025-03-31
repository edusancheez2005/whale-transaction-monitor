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
import time
import signal
import sys
from typing import List, Optional
import traceback

# Local imports
from config.settings import (
    shutdown_flag,
)
from utils.base_helpers import safe_print
from utils.helpers import signal_handler
from chains.ethereum import print_new_erc20_transfers, test_etherscan_connection
from chains.whale_alert import start_whale_thread
from chains.xrp import start_xrp_thread 
from chains.solana import start_solana_thread
from models.classes import initialize_prices
from utils.summary import print_final_aggregated_summary
from utils.dedup import EnhancedDeduplication

dedup = EnhancedDeduplication()

def initialize_price_updates() -> threading.Thread:
    """Start price update thread"""
    try:
        from models.classes import CoinGeckoAPI
        price_thread = threading.Thread(
            target=update_token_prices,
            daemon=True,
            name="PriceUpdater"
        )
        price_thread.start()
        return price_thread
    except Exception as e:
        safe_print(f"Error starting price update thread: {e}")
        return None

def update_token_prices() -> None:
    """Periodically update token prices from CoinGecko"""
    try:
        from models.classes import CoinGeckoAPI
        from data.tokens import TOKEN_PRICES
        
        coingecko = CoinGeckoAPI()
        
        while not shutdown_flag.is_set():
            try:
                # Update this line to use get_price instead of initialize_prices
                updated_prices = {}
                for symbol in TOKEN_PRICES.keys():
                    try:
                        price = coingecko.get_price(symbol.lower())
                        if price:
                            updated_prices[symbol] = price
                        else:
                            updated_prices[symbol] = TOKEN_PRICES[symbol]  # Keep old price if update fails
                    except Exception as price_error:
                        safe_print(f"Error updating price for {symbol}: {price_error}")
                        updated_prices[symbol] = TOKEN_PRICES[symbol]
                
                # Update global token prices
                TOKEN_PRICES.update(updated_prices)
                safe_print("\nToken prices updated from CoinGecko")
                time.sleep(300)  # Update every 5 minutes
            except Exception as e:
                safe_print(f"Error updating prices: {e}")
                time.sleep(60)  # Shorter retry on error
    except Exception as e:
        safe_print(f"Critical error in price update thread: {e}")
        traceback.print_exc()

def restart_thread(thread_name: str) -> Optional[threading.Thread]:
    """Restart a specific monitoring thread"""
    safe_print(f"Attempting to restart {thread_name} thread...")
    try:
        if thread_name == "PriceUpdater":
            new_thread = threading.Thread(
                target=update_token_prices,
                daemon=True,
                name="PriceUpdater"
            )
            new_thread.start()
            return new_thread
        elif thread_name == "WhaleAlert":
            return start_whale_thread()
        elif thread_name == "XRP":
            return start_xrp_thread()
        elif thread_name == "Solana":
            return start_solana_thread()
        elif thread_name == "Ethereum":
            new_thread = threading.Thread(
                target=ethereum_monitor_loop,
                daemon=True,
                name="Ethereum"
            )
            new_thread.start()
            return new_thread
    except Exception as e:
        safe_print(f"Error restarting {thread_name} thread: {e}")
    return None

def monitor_thread_status(threads: List[threading.Thread]) -> None:
    """Monitor the health of all running threads"""
    while not shutdown_flag.is_set():
        try:
            for i, thread in enumerate(threads):
                if thread and hasattr(thread, 'name') and not thread.is_alive():
                    safe_print(f"⚠️ Warning: {thread.name} thread died")
                    new_thread = restart_thread(thread.name)
                    if new_thread:
                        threads[i] = new_thread
                        safe_print(f"✓ Restarted {thread.name} thread")
                    else:
                        safe_print(f"❌ Failed to restart {thread.name} thread")
            
            time.sleep(30)  # Check every 30 seconds
            
        except Exception as e:
            safe_print(f"Error in thread monitoring: {e}")
            time.sleep(5)

def ethereum_monitor_loop():
    """Run Ethereum monitoring loop at regular intervals"""
    while not shutdown_flag.is_set():
        try:
            print_new_erc20_transfers()
        except Exception as e:
            safe_print(f"Error in Ethereum monitor: {e}")
        
        # Sleep for 60 seconds before next check
        time.sleep(60)

def initialize_monitoring():
    """Initialize all monitoring systems"""
    safe_print("Starting Combined Multi-Chain Coin Monitor...")
    safe_print("Press Ctrl+C to exit.\n")
    
    # Initialize prices
    safe_print("Initializing token prices...")
    global TOKEN_PRICES
    from data.tokens import TOKEN_PRICES
    TOKEN_PRICES.update(initialize_prices())
    
    # Test connections
    if not test_etherscan_connection():
        safe_print("Failed to connect to Etherscan API.")
        return False
    
    # Initialize deduplication system
    safe_print("Initializing transaction deduplication...")
    
    return True

def start_monitoring_threads():
    """Start all monitoring threads with enhanced error handling"""
    threads = []
    
    try:
        # Start price updates
        price_thread = initialize_price_updates()
        if price_thread:
            threads.append(price_thread)
            safe_print("✓ Price update thread started")
        
        # Start Ethereum monitoring thread
        ethereum_thread = threading.Thread(
            target=ethereum_monitor_loop,
            daemon=True,
            name="Ethereum"
        )
        ethereum_thread.start()
        threads.append(ethereum_thread)
        safe_print("✓ Ethereum monitor started")
        
        # Try to start other chain monitors with fallback options
        try:
            whale_thread = start_whale_thread()
            if whale_thread:
                threads.append(whale_thread)
                safe_print("✓ Whale Alert monitor started")
            else:
                safe_print("⚠️ Whale Alert monitor could not be started")
        except Exception as e:
            safe_print(f"Error starting Whale Alert monitor: {e}")
            safe_print("⚠️ Whale Alert monitor disabled due to error")
        
        try:
            xrp_thread = start_xrp_thread()
            if xrp_thread:
                threads.append(xrp_thread)
                safe_print("✓ XRP monitor started")
            else:
                safe_print("⚠️ XRP monitor could not be started")
        except Exception as e:
            safe_print(f"Error starting XRP monitor: {e}")
            safe_print("⚠️ XRP monitor disabled due to error")
        
        try:
            solana_thread = start_solana_thread()
            if solana_thread:
                threads.append(solana_thread)
                safe_print("✓ Solana monitor started")
            else:
                safe_print("⚠️ Solana monitor could not be started")
        except Exception as e:
            safe_print(f"Error starting Solana monitor: {e}")
            safe_print("⚠️ Solana monitor disabled due to error")
        
        # Add status monitoring thread
        status_thread = threading.Thread(
            target=monitor_thread_status,
            args=(threads,),
            daemon=True,
            name="StatusMonitor"
        )
        status_thread.start()
        
        return threads
    except Exception as e:
        safe_print(f"Error starting monitoring threads: {e}")
        traceback.print_exc()
        return []

def main():
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    safe_print("Starting Combined Multi-Chain Coin Monitor...")
    safe_print("Press Ctrl+C to exit.\n")
    
    try:
        # Initialize systems
        if not initialize_monitoring():
            return
        
        # Start monitoring threads
        active_threads = start_monitoring_threads()
        if not active_threads:
            safe_print("Failed to start monitoring threads")
            return
        
        # Main monitoring loop - just focus on Ethereum transfers
        while not shutdown_flag.is_set():
            try:
                # Keep main thread alive for signal handling
                time.sleep(1)
            except KeyboardInterrupt:
                break
            except Exception as e:
                safe_print(f"Error in main loop: {e}")
                time.sleep(5)  # Brief pause before retrying
                
    except KeyboardInterrupt:
        signal_handler(signal.SIGINT, None)
    except Exception as e:
        safe_print(f"Fatal error in main loop: {e}")
        traceback.print_exc()
        signal_handler(signal.SIGINT, None)

if __name__ == "__main__":
    main()
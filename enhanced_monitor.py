# super_simple_monitor.py

#!/usr/bin/env python3
"""
Super Simple Visual Crypto Transaction Monitor

Features:
- Simple, reliable display for macOS
- Color-coded transactions (buy/sell/transfer)
- Prompt for minimum transaction value
- Clean summary on exit
"""

import os
import sys
import time
import signal
import threading
from collections import defaultdict
import traceback

# Local imports
from config.settings import (
    shutdown_flag,
    GLOBAL_USD_THRESHOLD,
    etherscan_buy_counts,
    etherscan_sell_counts,
    whale_buy_counts,
    whale_sell_counts,
    solana_buy_counts,
    solana_sell_counts,
    xrp_buy_counts,
    xrp_sell_counts
)
from chains.ethereum import print_new_erc20_transfers, test_etherscan_connection
from chains.whale_alert import start_whale_thread
from chains.xrp import start_xrp_thread 
from chains.solana import start_solana_thread
from models.classes import initialize_prices
from utils.dedup import get_stats, deduped_transactions

# Basic colors
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
PURPLE = '\033[95m'
BOLD = '\033[1m'
END = '\033[0m'

# Global settings
min_transaction_value = GLOBAL_USD_THRESHOLD
active_threads = []
monitoring_enabled = True  # Flag to control transaction display

def clear_screen():
    """Clear the terminal"""
    os.system('clear')

def color_text(text, color=None, bold=False):
    """Return colored text without printing"""
    formatted = ""
    if bold:
        formatted += BOLD
    if color:
        formatted += color
    
    return formatted + text + END

def print_simple_header():
    """Print a simple header"""
    clear_screen()
    print(BLUE + BOLD + "=" * 80 + END)
    print(PURPLE + BOLD + " " * 20 + "CRYPTO WHALE TRANSACTION MONITOR" + END)
    print(BLUE + BOLD + "=" * 80 + END)
    print(YELLOW + f"Minimum Value: ${min_transaction_value:,.2f}" + END)
    print(GREEN + f"Active Monitors: {', '.join([t.name for t in active_threads if t.is_alive()])}" + END)
    print(BLUE + "Press Ctrl+C to exit and view summary" + END)
    print(BLUE + "-" * 80 + END)
    print()  # Add a blank line

def print_transaction(tx_data):
    """Print a transaction with simple color formatting"""
    # Only print if monitoring is enabled
    if not monitoring_enabled:
        return
        
    # Skip if below minimum value
    usd_value = tx_data.get("usd_value", 0)
    if usd_value < min_transaction_value:
        return
    
    # Get transaction details
    tx_hash = tx_data.get("tx_hash", "")
    symbol = tx_data.get("symbol", "")
    from_addr = tx_data.get("from", "")
    to_addr = tx_data.get("to", "")
    classification = tx_data.get("classification", "").upper()
    chain = tx_data.get("blockchain", "").upper() or tx_data.get("source", "").upper()
    amount = tx_data.get("amount", 0)
    
    # Choose color based on classification
    if classification == "BUY":
        header_color = GREEN
    elif classification == "SELL":
        header_color = RED
    else:  # Transfer
        header_color = YELLOW
    
    # Format the transaction header
    header = f"[{symbol} | ${usd_value:,.2f} USD]"
    if tx_data.get("block_number"):
        header += f" Block {tx_data.get('block_number')}"
    if tx_hash:
        header += f" | Tx {tx_hash[:16]}..." if len(tx_hash) > 16 else f" | Tx {tx_hash}"
    
    # Print with colors
    print(header_color + BOLD + header + END)
    
    # Print timestamp if available
    if "timestamp" in tx_data:
        try:
            timestamp = tx_data["timestamp"]
            time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
            print(f"  Time: {time_str}")
        except:
            pass
    
    # Print from/to addresses
    print(f"  From: {from_addr}")
    print(f"  To:   {to_addr}")
    
    # Print amount and classification
    print(f"  Amount: {amount:,.2f} {symbol} (~${usd_value:,.2f} USD)")
    confidence = tx_data.get("confidence", 0)
    print(header_color + f"  Classification: {classification} (confidence: {confidence})" + END)
    
    # Add a blank line
    print()

def monitor_transactions():
    """Monitor transactions from the dedup cache with improved safety"""
    processed_txs = set()
    
    # Thread safety counter
    safety_counter = 0
    
    while not shutdown_flag.is_set() and monitoring_enabled:
        try:
            # Check shutdown flag first
            if shutdown_flag.is_set() or not monitoring_enabled:
                return
            
            # Safety check - if we've hit errors multiple times
            if safety_counter > 10:
                print(YELLOW + "Transaction monitor reset due to errors" + END)
                safety_counter = 0
                time.sleep(0.5)
                continue
                
            # Look for new transactions safely
            try:
                # First get list of keys (separate step to avoid dictionary iteration issues)
                tx_keys = list(deduped_transactions.keys())
                
                # Now process one at a time
                for tx_key in tx_keys:
                    # Check if still in dict (might have been removed)
                    if tx_key in deduped_transactions:
                        # Get tx safely
                        try:
                            tx = deduped_transactions[tx_key]
                        except KeyError:
                            # Key was removed between listing and access
                            continue
                            
                        # Exit early if shutdown requested
                        if shutdown_flag.is_set() or not monitoring_enabled:
                            return
                            
                        if tx_key not in processed_txs:
                            processed_txs.add(tx_key)
                            try:
                                print_transaction(tx)
                            except Exception as tx_error:
                                # One transaction print failed, but continue
                                if not shutdown_flag.is_set():
                                    print(f"Error printing transaction: {tx_error}")
            except RuntimeError as re:
                # Dictionary changed size error
                safety_counter += 1
                time.sleep(0.05)
                continue
            
            # Keep set size reasonable
            if len(processed_txs) > 5000:
                processed_txs = set(list(processed_txs)[-2000:])
                
            # Reset safety counter on success
            safety_counter = 0
                
            time.sleep(0.1)
            
        except Exception as e:
            if not shutdown_flag.is_set():
                print(f"Error monitoring transactions: {e}")
            # Increment error counter
            safety_counter += 1
            time.sleep(0.1)

def start_monitoring_threads():
    """Start all monitoring threads"""
    threads = []
    
    try:
        # Start Ethereum monitoring thread
        ethereum_thread = threading.Thread(
            target=print_new_erc20_transfers,
            daemon=True,
            name="Ethereum"
        )
        ethereum_thread.start()
        threads.append(ethereum_thread)
        print(GREEN + "✅ Ethereum monitor started" + END)
        
        # Try to start Whale Alert monitor
        try:
            whale_thread = start_whale_thread()
            if whale_thread:
                threads.append(whale_thread)
                print(GREEN + "✅ Whale Alert monitor started" + END)
            else:
                print(YELLOW + "⚠️ Whale Alert monitor could not be started" + END)
        except Exception as e:
            print(RED + f"❌ Error starting Whale Alert monitor: {e}" + END)
        
        # Try to start XRP monitor
        try:
            xrp_thread = start_xrp_thread()
            if xrp_thread:
                threads.append(xrp_thread)
                print(GREEN + "✅ XRP monitor started" + END)
            else:
                print(YELLOW + "⚠️ XRP monitor could not be started" + END)
        except Exception as e:
            print(RED + f"❌ Error starting XRP monitor: {e}" + END)
        
        # Try to start Solana monitor
        try:
            solana_thread = start_solana_thread()
            if solana_thread:
                threads.append(solana_thread)
                print(GREEN + "✅ Solana monitor started" + END)
            else:
                print(YELLOW + "⚠️ Solana monitor could not be started" + END)
        except Exception as e:
            print(RED + f"❌ Error starting Solana monitor: {e}" + END)
        
        return threads
        
    except Exception as e:
        print(RED + f"❌ Error starting monitoring threads: {e}" + END)
        traceback.print_exc()
        return []

def print_simple_summary():
    """Print a simple summary that's guaranteed to work even in error conditions"""
    try:
        clear_screen()
        
        # Header
        print(BLUE + BOLD + "=" * 80 + END)
        print(PURPLE + BOLD + " " * 25 + "FINAL ANALYSIS REPORT" + END)
        print(BLUE + BOLD + "=" * 80 + END)
        print()
        
        # Get dedup stats safely
        try:
            dedup_stats = get_stats()
        except Exception as e:
            print(RED + f"Error getting deduplication stats: {e}" + END)
            dedup_stats = {'total_received': 0, 'total_transactions': 0, 'duplicates_caught': 0, 'dedup_ratio': 0}
        
        # Collect token statistics
        token_stats = defaultdict(lambda: {'buys': 0, 'sells': 0, 'transfers': 0, 'volume': 0.0})
        
        # Process Ethereum transactions - safely
        try:
            for symbol, count in etherscan_buy_counts.items():
                token_stats[symbol]['buys'] += count
            for symbol, count in etherscan_sell_counts.items():
                token_stats[symbol]['sells'] += count
        except Exception as e:
            print(RED + f"Error processing Ethereum stats: {e}" + END)
            
        # Process Whale Alert transactions - safely
        try:
            for symbol, count in whale_buy_counts.items():
                token_stats[symbol]['buys'] += count
            for symbol, count in whale_sell_counts.items():
                token_stats[symbol]['sells'] += count
        except Exception as e:
            print(RED + f"Error processing Whale Alert stats: {e}" + END)
            
        # Process Solana transactions - safely
        try:
            for symbol, count in solana_buy_counts.items():
                token_stats[symbol]['buys'] += count
            for symbol, count in solana_sell_counts.items():
                token_stats[symbol]['sells'] += count
        except Exception as e:
            print(RED + f"Error processing Solana stats: {e}" + END)
            
        # Process XRP transactions - safely
        try:
            token_stats['XRP']['buys'] += xrp_buy_counts
            token_stats['XRP']['sells'] += xrp_sell_counts
        except Exception as e:
            print(RED + f"Error processing XRP stats: {e}" + END)
        
        # Make a safe copy of transactions - multiple fallback approaches
        safe_transactions = {}
        try:
            # Try to snapshot the dict - most direct, may fail
            safe_transactions = dict(deduped_transactions)
        except Exception:
            try:
                # Slower but safer approach - copy one at a time
                safe_transactions = {}
                for k in list(deduped_transactions.keys()):
                    try:
                        if k in deduped_transactions:  # Check again in case of deletion
                            safe_transactions[k] = deduped_transactions[k]
                    except:
                        pass  # Skip any keys that cause problems
            except Exception as e:
                print(RED + f"Error creating transaction copy: {e}" + END)
        
        # Calculate volumes from transactions - safely
        token_addresses = defaultdict(set)
        for tx_key, tx in safe_transactions.items():
            try:
                symbol = tx.get('symbol', '')
                if symbol:
                    tx_value = tx.get('usd_value', 0) or tx.get('estimated_usd', 0) or 0
                    token_stats[symbol]['volume'] += tx_value
                    
                    # Count transfers if not already counted as buy/sell
                    if tx.get('classification', '').lower() not in ['buy', 'sell']:
                        token_stats[symbol]['transfers'] += 1
                    
                    # Track addresses
                    if 'from' in tx and tx['from']:
                        token_addresses[symbol].add(tx['from'])
                    if 'to' in tx and tx['to']:
                        token_addresses[symbol].add(tx['to'])
            except Exception:
                continue  # Skip problematic transactions
        
        # Now let's print sections one by one, with error handling for each
        
        try:
            # Print Transaction Statistics
            print(BLUE + BOLD + "1. TRANSACTION STATISTICS" + END)
            print(BLUE + "-" * 80 + END)
            
            # Header row
            print(f"{'COIN':<10} {'BUYS':>8} {'SELLS':>8} {'TRANSFERS':>10} {'TOTAL':>8} {'BUY %':>7} {'SELL %':>7} {'TREND':>6}")
            print(BLUE + "-" * 80 + END)
            
            # Sort tokens by total transactions
            sorted_tokens = sorted(
                [(symbol, stats) for symbol, stats in token_stats.items()],
                key=lambda x: x[1]['buys'] + x[1]['sells'] + x[1]['transfers'],
                reverse=True
            )
            
            # Print each token's stats - line by line to avoid errors
            for symbol, stats in sorted_tokens:
                try:
                    buys = stats['buys']
                    sells = stats['sells']
                    transfers = stats['transfers']
                    total = buys + sells + transfers
                    
                    if total < 5:  # Skip tokens with very few transactions
                        continue
                        
                    buy_pct = (buys / total * 100) if total > 0 else 0
                    sell_pct = (sells / total * 100) if total > 0 else 0
                    
                    # Determine trend
                    if buy_pct > sell_pct + 10:
                        trend = "↑"
                        trend_color = GREEN
                    elif sell_pct > buy_pct + 10:
                        trend = "↓"
                        trend_color = RED
                    else:
                        trend = "→"
                        trend_color = YELLOW
                        
                    # First print the basic stats
                    basic_info = f"{symbol:<10} {buys:>8} {sells:>8} {transfers:>10} {total:>8} "
                    print(basic_info, end='')
                    
                    # Then print the percentages and trend with colors
                    print(GREEN + f"{buy_pct:>6.1f}%" + END, end=' ')
                    print(RED + f"{sell_pct:>6.1f}%" + END, end=' ')
                    print(trend_color + f"{trend:>6}" + END)
                except Exception as e:
                    # Skip this token if error
                    continue
        except Exception as e:
            print(RED + f"Error displaying transaction statistics: {e}" + END)
        
        try:
            # Market Momentum
            print()
            print(BLUE + BOLD + "2. MARKET MOMENTUM ANALYSIS" + END)
            print(BLUE + "-" * 80 + END)
            
            # Find tokens with enough volume
            active_tokens = [
                (symbol, stats) for symbol, stats in token_stats.items() 
                if stats['buys'] + stats['sells'] >= 10
            ]
            
            # Sort by buy percentage for bullish tokens
            bullish_tokens = sorted(
                active_tokens,
                key=lambda x: (x[1]['buys'] / max(1, x[1]['buys'] + x[1]['sells'])),
                reverse=True
            )[:3]
            
            # Sort by sell percentage for bearish tokens
            bearish_tokens = sorted(
                active_tokens,
                key=lambda x: (x[1]['sells'] / max(1, x[1]['buys'] + x[1]['sells'])),
                reverse=True
            )[:3]
            
            # Print bullish tokens
            print(GREEN + "TOP BULLISH TOKENS:" + END)
            for symbol, stats in bullish_tokens:
                try:
                    total = stats['buys'] + stats['sells']
                    if total > 0:
                        buy_pct = (stats['buys'] / total * 100)
                        sell_pct = (stats['sells'] / total * 100)
                        print(f"  • {symbol:<8} Buy: {buy_pct:>6.1f}%  Sell: {sell_pct:>6.1f}%  (Volume: {total:,} transactions)")
                except Exception:
                    continue
            
            # Print bearish tokens
            print()
            print(RED + "TOP BEARISH TOKENS:" + END)
            for symbol, stats in bearish_tokens:
                try:
                    total = stats['buys'] + stats['sells']
                    if total > 0:
                        buy_pct = (stats['buys'] / total * 100)
                        sell_pct = (stats['sells'] / total * 100)
                        print(f"  • {symbol:<8} Buy: {buy_pct:>6.1f}%  Sell: {sell_pct:>6.1f}%  (Volume: {total:,} transactions)")
                except Exception:
                    continue
        except Exception as e:
            print(RED + f"Error displaying market momentum: {e}" + END)
        
        try:
            # Deduplication
            print()
            print(BLUE + BOLD + "3. DEDUPLICATION EFFECTIVENESS" + END)
            print(BLUE + "-" * 80 + END)
            print(f"Total Transactions Processed: {dedup_stats['total_received']:,}")
            print(f"Unique Transactions: {dedup_stats['total_transactions']:,}")
            print(f"Duplicates Prevented: {dedup_stats['duplicates_caught']:,}")
            print(f"Overall Deduplication Rate: {dedup_stats['dedup_ratio']:.1f}%")
        except Exception as e:
            print(RED + f"Error displaying deduplication stats: {e}" + END)
        
        try:
            # Volume Analysis Section (Added)
            print()
            print(BLUE + BOLD + "4. TOKEN VOLUME ANALYSIS" + END)
            print(BLUE + "-" * 80 + END)
            print(f"{'TOKEN':<10} {'VOLUME (USD)':>15} {'UNIQUE TXS':>12} {'ADDRESSES':>12}")
            print(BLUE + "-" * 80 + END)
            
            # Sort by volume
            volume_sorted = sorted(
                [(symbol, stats) for symbol, stats in token_stats.items()],
                key=lambda x: x[1]['volume'],
                reverse=True
            )
            
            for symbol, stats in volume_sorted:
                try:
                    total = stats['buys'] + stats['sells'] + stats['transfers']
                    if stats['volume'] > 0:
                        address_count = len(token_addresses.get(symbol, set()))
                        print(f"{symbol:<10} ${stats['volume']:>14,.2f} {total:>12,} {address_count:>12,}")
                except Exception:
                    continue
        except Exception as e:
            print(RED + f"Error displaying volume analysis: {e}" + END)
        
        # Skip news section if there are issues
        try:
            # Add News Section (Added) - simplified to be more reliable
            print()
            print(BLUE + BOLD + "5. LATEST CRYPTO NEWS" + END)
            print(BLUE + "-" * 80 + END)
            
            # Just use placeholder news which is more reliable
            print(f"{YELLOW}Visit CoinDesk or CoinTelegraph for the latest cryptocurrency news{END}")
            
            # Get top 3 tokens by transaction volume (safely)
            top_tokens = []
            try:
                top_tokens = [symbol for symbol, _ in volume_sorted[:3]]
            except:
                # Fallback to hardcoded popular tokens if needed
                top_tokens = ["BTC", "ETH", "XRP"]
                
            for symbol in top_tokens:
                print(f"\n{PURPLE}For {symbol} news, visit: https://www.coingecko.com/en/coins/{symbol.lower()}{END}")
        except Exception as e:
            print(RED + f"Error displaying news: {e}" + END)
        
        print()
        print(GREEN + "Analysis complete." + END)
        print(BLUE + BOLD + "=" * 80 + END)
    
    except Exception as e:
        # Ultimate fallback - if even the main report fails
        print(RED + f"Error generating summary: {e}" + END)
        print(YELLOW + "Simplified emergency report:" + END)
        try:
            print(f"- Ethereum buy/sell: {sum(etherscan_buy_counts.values())}/{sum(etherscan_sell_counts.values())}")
            print(f"- Whale Alert buy/sell: {sum(whale_buy_counts.values())}/{sum(whale_sell_counts.values())}")
            print(f"- Solana buy/sell: {sum(solana_buy_counts.values())}/{sum(solana_sell_counts.values())}")
            print(f"- XRP buy/sell: {xrp_buy_counts}/{xrp_sell_counts}")
        except:
            print("Could not generate even basic stats.")
        print(BLUE + BOLD + "=" * 80 + END)

def cleanup_threads():
    """Attempt to gracefully stop any running threads"""
    for thread in threading.enumerate():
        if thread != threading.current_thread() and thread.daemon:
            try:
                if hasattr(thread, "_stop"):
                    thread._stop()
            except Exception:
                pass

def simple_signal_handler(signum, frame):
    """Handle Ctrl+C with simple formatting and buffer draining"""
    global monitoring_enabled
    
    # First, acknowledge Ctrl+C visually with countdown
    print(YELLOW + BOLD + "\n\n[CTRL+C] Shutting down..." + END)
    
    # Set flags to prevent new data processing
    monitoring_enabled = False
    shutdown_flag.set()
    
    try:
        # Short countdown to allow buffer draining
        drain_seconds = 3
        for i in range(drain_seconds, 0, -1):
            print(YELLOW + f"Finishing pending transactions... {i}s" + END, end="\r")
            time.sleep(1)
        print(YELLOW + "Processing complete                      " + END)
        
        # Force stop any potential websocket connections
        for thread in list(threading.enumerate()):
            if thread != threading.current_thread():
                try:
                    # Try to join with timeout
                    if thread.is_alive():
                        thread.join(timeout=0.5)
                except Exception:
                    pass
        
        # More aggressively cleanup threads
        cleanup_threads()
            
        # Final countdown before summary
        final_delay = 1
        print(YELLOW + f"Generating final report in {final_delay}s..." + END)
        time.sleep(final_delay)
        
        # Try to generate summary with a fail-safe
        try:
            print(YELLOW + "Generating final report..." + END)
            print_simple_summary()
        except Exception as e:
            print(RED + f"Error generating summary: {e}" + END)
            print(RED + "Press Ctrl+C again to force exit" + END)
            
            # Wait for another Ctrl+C
            try:
                time.sleep(5)
            except KeyboardInterrupt:
                print(RED + "Force exiting..." + END)
                os._exit(1)
        
        print(YELLOW + "Exiting now..." + END)
        
        # Force immediate exit
        os._exit(0)
        
    except KeyboardInterrupt:
        # Handle double Ctrl+C for force exit
        print(RED + "\nForce exiting immediately..." + END)
        os._exit(1)
    except Exception as e:
        print(RED + f"\nError during shutdown: {e}" + END)
        traceback.print_exc()
        # Force exit even on error
        os._exit(1)

def prompt_for_minimum_value():
    """Prompt user for minimum transaction value"""
    global min_transaction_value
    
    print(BLUE + BOLD + "\nEnter minimum transaction value to monitor (USD): " + END, end='')
    value_input = input()
    
    try:
        if value_input.strip():
            min_value = float(value_input)
            if min_value > 0:
                min_transaction_value = min_value
            else:
                print(YELLOW + "Value must be greater than 0, using default" + END)
        else:
            print(YELLOW + f"Using default value: ${GLOBAL_USD_THRESHOLD:,.2f}" + END)
    except ValueError:
        print(YELLOW + f"Invalid input, using default value: ${GLOBAL_USD_THRESHOLD:,.2f}" + END)

def main():
    """Main entry point with simplified error handling"""
    global active_threads
    
    # Register custom signal handler for clean shutdown
    signal.signal(signal.SIGINT, simple_signal_handler)
    signal.signal(signal.SIGTERM, simple_signal_handler)
    
    try:
        # Ask for minimum value
        prompt_for_minimum_value()
        
        # Initialize prices
        print(BLUE + "Initializing token prices..." + END)
        initialize_prices()
        
        # Test connections
        if not test_etherscan_connection():
            print(RED + "Failed to connect to Etherscan API." + END)
            return
        
        # Start monitoring threads
        active_threads = start_monitoring_threads()
        
        # Start monitoring in a separate thread
        monitor_thread = threading.Thread(
            target=monitor_transactions,
            daemon=True,
            name="Monitor"
        )
        monitor_thread.start()
        active_threads.append(monitor_thread)
        
        # Print initial header
        print_simple_header()
        
        # Set up a try-finally to ensure proper cleanup even on unexpected exit
        try:
            # Main loop - just keep the main thread alive and check shutdown_flag more frequently
            while not shutdown_flag.is_set():
                time.sleep(0.1)  # Check more frequently
        except KeyboardInterrupt:
            # Direct call to signal handler
            simple_signal_handler(signal.SIGINT, None)
        finally:
            # Make sure shutdown flag is set if we're exiting for any reason
            shutdown_flag.set()
            monitoring_enabled = False
            
    except KeyboardInterrupt:
        # Handle Ctrl+C with our custom handler
        simple_signal_handler(signal.SIGINT, None)
    except Exception as e:
        print(RED + f"Fatal error in main loop: {e}" + END)
        traceback.print_exc()
        # Force exit on unhandled exception
        os._exit(1)

if __name__ == "__main__":
    main()
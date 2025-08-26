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
import asyncio
import json
import logging
import argparse
from datetime import datetime
from typing import Dict, Any, List, Optional

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

# Import the new classification system
from address_enrichment import AddressEnrichmentService, EnrichedAddress, ChainType
from rule_engine import RuleEngine, Transaction, AddressMetadata, ClassificationType
from transaction_classifier import TransactionClassifier

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
    """Monitor transactions from the dedup cache"""
    processed_txs = set()
    
    while not shutdown_flag.is_set() and monitoring_enabled:
        try:
            # Look for new transactions
            for tx_key, tx in list(deduped_transactions.items()):
                if tx_key not in processed_txs:
                    processed_txs.add(tx_key)
                    print_transaction(tx)
            
            # Keep set size reasonable
            if len(processed_txs) > 5000:
                processed_txs = set(list(processed_txs)[-2000:])
                
            time.sleep(0.5)
        except Exception as e:
            print(f"Error monitoring transactions: {e}")
            time.sleep(1)

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
    """Print a simple summary that's guaranteed to work"""
    clear_screen()
    
    # Header
    print(BLUE + BOLD + "=" * 80 + END)
    print(PURPLE + BOLD + " " * 25 + "FINAL ANALYSIS REPORT" + END)
    print(BLUE + BOLD + "=" * 80 + END)
    print()
    
    # Get dedup stats
    dedup_stats = get_stats()
    
    # Collect token statistics
    token_stats = defaultdict(lambda: {'buys': 0, 'sells': 0, 'transfers': 0, 'volume': 0.0})
    
    # Process Ethereum transactions
    for symbol, count in etherscan_buy_counts.items():
        token_stats[symbol]['buys'] += count
    for symbol, count in etherscan_sell_counts.items():
        token_stats[symbol]['sells'] += count
        
    # Process Whale Alert transactions
    for symbol, count in whale_buy_counts.items():
        token_stats[symbol]['buys'] += count
    for symbol, count in whale_sell_counts.items():
        token_stats[symbol]['sells'] += count
        
    # Process Solana transactions
    for symbol, count in solana_buy_counts.items():
        token_stats[symbol]['buys'] += count
    for symbol, count in solana_sell_counts.items():
        token_stats[symbol]['sells'] += count
        
    # Process XRP transactions
    token_stats['XRP']['buys'] += xrp_buy_counts
    token_stats['XRP']['sells'] += xrp_sell_counts
    
    # Calculate volumes from deduped transactions
    for tx_key, tx in deduped_transactions.items():
        symbol = tx.get('symbol', '')
        if symbol:
            token_stats[symbol]['volume'] += tx.get('usd_value', 0)
            
            # Count transfers if not already counted as buy/sell
            if tx.get('classification', '').lower() not in ['buy', 'sell']:
                token_stats[symbol]['transfers'] += 1
    
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
        total = stats['buys'] + stats['sells']
        if total > 0:
            buy_pct = (stats['buys'] / total * 100)
            sell_pct = (stats['sells'] / total * 100)
            print(f"  • {symbol:<8} Buy: {buy_pct:>6.1f}%  Sell: {sell_pct:>6.1f}%  (Volume: {total:,} transactions)")
    
    # Print bearish tokens
    print()
    print(RED + "TOP BEARISH TOKENS:" + END)
    for symbol, stats in bearish_tokens:
        total = stats['buys'] + stats['sells']
        if total > 0:
            buy_pct = (stats['buys'] / total * 100)
            sell_pct = (stats['sells'] / total * 100)
            print(f"  • {symbol:<8} Buy: {buy_pct:>6.1f}%  Sell: {sell_pct:>6.1f}%  (Volume: {total:,} transactions)")
    
    # Deduplication
    print()
    print(BLUE + BOLD + "3. DEDUPLICATION EFFECTIVENESS" + END)
    print(BLUE + "-" * 80 + END)
    print(f"Total Transactions Processed: {dedup_stats['total_received']:,}")
    print(f"Unique Transactions: {dedup_stats['total_transactions']:,}")
    print(f"Duplicates Prevented: {dedup_stats['duplicates_caught']:,}")
    print(f"Overall Deduplication Rate: {dedup_stats['dedup_ratio']:.1f}%")
    
    # Volume Analysis Section (Added)
    print()
    print(BLUE + BOLD + "4. TOKEN VOLUME ANALYSIS" + END)
    print(BLUE + "-" * 80 + END)
    print(f"{'TOKEN':<10} {'VOLUME (USD)':>15} {'UNIQUE TXS':>12} {'ADDRESSES':>12}")
    print(BLUE + "-" * 80 + END)
    
    # Track unique addresses per token
    token_addresses = defaultdict(set)
    for tx_key, tx in deduped_transactions.items():
        symbol = tx.get('symbol', '')
        if symbol:
            if 'from' in tx and tx['from']:
                token_addresses[symbol].add(tx['from'])
            if 'to' in tx and tx['to']:
                token_addresses[symbol].add(tx['to'])
    
    # Sort by volume
    volume_sorted = sorted(
        [(symbol, stats) for symbol, stats in token_stats.items()],
        key=lambda x: x[1]['volume'],
        reverse=True
    )
    
    for symbol, stats in volume_sorted:
        total = stats['buys'] + stats['sells'] + stats['transfers']
        if stats['volume'] > 0:
            address_count = len(token_addresses.get(symbol, set()))
            print(f"{symbol:<10} ${stats['volume']:>14,.2f} {total:>12,} {address_count:>12,}")
    
    # Add News Section (Added)
    print()
    print(BLUE + BOLD + "5. LATEST CRYPTO NEWS" + END)
    print(BLUE + "-" * 80 + END)
    
    # Get top 3 tokens by transaction volume
    top_tokens = [symbol for symbol, _ in volume_sorted[:3]]
    
    # Try to import and use news API functions if available
    try:
        from utils.summary import get_news_for_token
        
        for symbol in top_tokens:
            print(f"\n{PURPLE}📰 Latest news for {symbol}:{END}")
            try:
                news_items = get_news_for_token(symbol)
                if news_items:
                    for item in news_items:
                        print(f"  • {item}")
                else:
                    print(f"  • No recent news found for {symbol}")
            except Exception as e:
                print(f"  • Error fetching news: {str(e)}")
    except ImportError:
        # Fallback news if the proper function isn't available
        print(f"{YELLOW}News API connection not available - showing placeholder news{END}")
        for symbol in top_tokens:
            print(f"\n{PURPLE}📰 Latest news for {symbol}:{END}")
            print(f"  • Visit CoinDesk or CoinTelegraph for the latest {symbol} news")
    
    print()
    print(GREEN + "Analysis complete." + END)
    print(BLUE + BOLD + "=" * 80 + END)

def simple_signal_handler(signum, frame):
    """Handle Ctrl+C with simple formatting"""
    global monitoring_enabled
    
    # Disable transaction monitoring first
    monitoring_enabled = False
    
    print(YELLOW + BOLD + "\n\nShutting down and generating summary..." + END)
    shutdown_flag.set()
    
    try:
        # Give more time for other threads to shut down and for user to see message
        delay_seconds = 15  # You can adjust this value as needed
        print(YELLOW + f"Will display summary in {delay_seconds} seconds..." + END)
        time.sleep(delay_seconds)
        
        # Print final summary
        print_simple_summary()
        
        # Wait a moment before exit
        time.sleep(1)
        
        # Exit cleanly
        sys.exit(0)
    except Exception as e:
        print(RED + f"\nError during shutdown: {e}" + END)
        traceback.print_exc()
        sys.exit(1)

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
    
    # Register custom signal handler
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
        
        # Main loop - just keep the main thread alive
        while not shutdown_flag.is_set():
            time.sleep(0.5)
            
    except KeyboardInterrupt:
        simple_signal_handler(signal.SIGINT, None)
    except Exception as e:
        print(RED + f"Fatal error in main loop: {e}" + END)
        traceback.print_exc()
        simple_signal_handler(signal.SIGINT, None)

if __name__ == "__main__":
    main()

class EnhancedMonitor:
    """
    Enhanced transaction monitoring system that integrates address enrichment
    and rule-based classification.
    
    This class:
    1. Monitors transactions from various sources
    2. Enriches addresses with metadata from external services
    3. Classifies transactions as buys, sells, or transfers
    4. Provides analytics and alerts
    """
    
    def __init__(
        self,
        redis_url: Optional[str] = None,
        output_dir: str = "output",
        sources: Optional[List[str]] = None
    ):
        """
        Initialize the enhanced monitor
        
        Args:
            redis_url: Redis connection URL
            output_dir: Directory for output files
            sources: Transaction sources to monitor
        """
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        self.output_dir = output_dir
        
        # Initialize the transaction classifier
        self.classifier = TransactionClassifier(redis_url=redis_url)
        
        # Setup transaction sources
        self.sources = sources or ["ethereum", "solana", "polygon", "xrp"]
        
        # Tracking stats
        self.stats = {
            "transactions_processed": 0,
            "buys": 0,
            "sells": 0,
            "transfers": 0,
            "unknown": 0,
            "start_time": datetime.now().isoformat()
        }
    
    async def process_transaction(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single transaction
        
        Args:
            transaction: Transaction data
            
        Returns:
            Dict[str, Any]: Processed transaction with classification
        """
        # Extract transaction fields
        tx_hash = transaction.get("hash") or transaction.get("tx_hash")
        from_address = transaction.get("from_address") or transaction.get("from")
        to_address = transaction.get("to_address") or transaction.get("to")
        chain = transaction.get("chain") or transaction.get("blockchain", "ethereum")
        token = transaction.get("token") or transaction.get("symbol", "")
        amount = float(transaction.get("amount") or transaction.get("value", 0))
        usd_value = float(transaction.get("usd_value") or transaction.get("value_usd", 0))
        timestamp_str = transaction.get("timestamp")
        
        # Process timestamp
        timestamp = None
        if timestamp_str:
            if isinstance(timestamp_str, (int, float)):
                # Unix timestamp
                timestamp = datetime.fromtimestamp(timestamp_str)
            else:
                # ISO string
                try:
                    timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    pass
        
        if not timestamp:
            timestamp = datetime.now()
        
        # Classify the transaction
        result = await self.classifier.classify_transaction(
            tx_hash=tx_hash,
            from_address=from_address,
            to_address=to_address,
            chain=chain,
            token=token,
            amount=amount,
            usd_value=usd_value,
            timestamp=timestamp
        )
        
        # Update stats
        self.stats["transactions_processed"] += 1
        if result.classification == ClassificationType.BUY:
            self.stats["buys"] += 1
        elif result.classification == ClassificationType.SELL:
            self.stats["sells"] += 1
        elif result.classification == ClassificationType.TRANSFER:
            self.stats["transfers"] += 1
        else:
            self.stats["unknown"] += 1
        
        # Generate human-readable summary
        summary = self.classifier.generate_classification_summary(result)
        
        # Prepare the enhanced transaction record
        enhanced_tx = {
            # Original transaction data
            "tx_hash": tx_hash,
            "from_address": from_address,
            "to_address": to_address,
            "chain": chain,
            "token": token,
            "amount": amount,
            "usd_value": usd_value,
            "timestamp": timestamp.isoformat(),
            
            # Classification data
            "classification": result.classification.value,
            "confidence": result.confidence,
            "confidence_level": result.confidence_level.value,
            "rule": result.triggered_rule,
            "explanation": result.explanation,
            
            # Address entity information
            "from_entity": summary["from_entity"],
            "to_entity": summary["to_entity"],
            
            # Summary
            "summary": summary["summary"]
        }
        
        # Log result
        logger.info(f"Transaction {tx_hash}: {enhanced_tx['classification']} "
                   f"(confidence: {enhanced_tx['confidence']:.2f})")
        
        return enhanced_tx
    
    async def process_batch(self, transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process a batch of transactions
        
        Args:
            transactions: List of transaction data
            
        Returns:
            List[Dict[str, Any]]: List of processed transactions
        """
        tasks = []
        for tx in transactions:
            task = self.process_transaction(tx)
            tasks.append(task)
        
        return await asyncio.gather(*tasks)
    
    def save_transactions(self, transactions: List[Dict[str, Any]], file_path: str) -> None:
        """
        Save processed transactions to file
        
        Args:
            transactions: List of processed transactions
            file_path: Output file path
        """
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, "w") as f:
            json.dump(transactions, f, indent=2, default=str)
            
        logger.info(f"Saved {len(transactions)} transactions to {file_path}")
    
    def save_stats(self, file_path: str) -> None:
        """
        Save monitoring stats to file
        
        Args:
            file_path: Output file path
        """
        # Add end time
        self.stats["end_time"] = datetime.now().isoformat()
        self.stats["duration_seconds"] = (
            datetime.fromisoformat(self.stats["end_time"]) - 
            datetime.fromisoformat(self.stats["start_time"])
        ).total_seconds()
        
        # Calculate percentages
        total = self.stats["transactions_processed"]
        if total > 0:
            self.stats["buy_percentage"] = round(self.stats["buys"] / total * 100, 2)
            self.stats["sell_percentage"] = round(self.stats["sells"] / total * 100, 2)
            self.stats["transfer_percentage"] = round(self.stats["transfers"] / total * 100, 2)
            self.stats["unknown_percentage"] = round(self.stats["unknown"] / total * 100, 2)
        
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, "w") as f:
            json.dump(self.stats, f, indent=2)
            
        logger.info(f"Saved monitoring stats to {file_path}")
    
    async def simulate_monitoring(self, num_transactions: int = 10) -> None:
        """
        Simulate transaction monitoring with dummy data
        
        Args:
            num_transactions: Number of dummy transactions to generate
        """
        logger.info(f"Starting simulated monitoring with {num_transactions} dummy transactions")
        
        # Generate dummy transactions
        dummy_transactions = []
        
        # Example addresses (including some known exchanges)
        from_addresses = [
            "0x28c6c06298d514db089934071355e5743bf21d60",  # Binance
            "0xdfd5293d8e347dfe59e90efd55b2956a1343963d",  # Coinbase
            "0x3f5CE5FBFe3E9af3971dD833D26bA9b5C936f0bE",  # Binance
            "0x8315177aB297bA92A06054cE80a67Ed4DBd7ed3a",  # Random
            "0x47ac0Fb4F2D84898e4D9E7b4DaB3C24507a6D503"   # Random
        ]
        
        to_addresses = [
            "0x8315177aB297bA92A06054cE80a67Ed4DBd7ed3a",  # Random
            "0x47ac0Fb4F2D84898e4D9E7b4DaB3C24507a6D503",  # Random
            "0x28c6c06298d514db089934071355e5743bf21d60",  # Binance
            "0xdfd5293d8e347dfe59e90efd55b2956a1343963d",  # Coinbase
            "0x11E4A2A167C614F900BC7784bdF9F373BB189c3f"   # Uniswap Router
        ]
        
        # Generate transactions
        import random
        for i in range(num_transactions):
            from_idx = random.randint(0, len(from_addresses) - 1)
            to_idx = random.randint(0, len(to_addresses) - 1)
            
            # Ensure from and to addresses are different
            while from_idx == to_idx:
                to_idx = random.randint(0, len(to_addresses) - 1)
            
            tx = {
                "tx_hash": f"0x{i:064x}",
                "from_address": from_addresses[from_idx],
                "to_address": to_addresses[to_idx],
                "chain": random.choice(["ethereum", "solana", "polygon", "xrp"]),
                "token": random.choice(["ETH", "BTC", "USDC", "SOL", "XRP"]),
                "amount": round(random.uniform(0.1, 10.0), 4),
                "usd_value": round(random.uniform(100, 50000), 2),
                "timestamp": datetime.now().isoformat()
            }
            
            dummy_transactions.append(tx)
        
        # Process transactions
        processed_transactions = await self.process_batch(dummy_transactions)
        
        # Save results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        transactions_file = os.path.join(self.output_dir, f"transactions_{timestamp}.json")
        stats_file = os.path.join(self.output_dir, f"stats_{timestamp}.json")
        
        self.save_transactions(processed_transactions, transactions_file)
        self.save_stats(stats_file)
        
        # Print summary
        print("\nMonitoring Summary:")
        print(f"Processed {self.stats['transactions_processed']} transactions")
        print(f"Buys: {self.stats['buys']} ({self.stats.get('buy_percentage', 0)}%)")
        print(f"Sells: {self.stats['sells']} ({self.stats.get('sell_percentage', 0)}%)")
        print(f"Transfers: {self.stats['transfers']} ({self.stats.get('transfer_percentage', 0)}%)")
        print(f"Unknown: {self.stats['unknown']} ({self.stats.get('unknown_percentage', 0)}%)")
        
        logger.info("Simulated monitoring completed")
    
    async def close(self):
        """Close resources"""
        await self.classifier.close()

async def main():
    """Main entry point"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Enhanced Whale Transaction Monitor")
    parser.add_argument("--redis-url", type=str, help="Redis URL for caching")
    parser.add_argument("--output-dir", type=str, default="output", help="Output directory")
    parser.add_argument("--num-transactions", type=int, default=10, 
                       help="Number of dummy transactions for simulation")
    
    args = parser.parse_args()
    
    # Create monitor
    monitor = EnhancedMonitor(
        redis_url=args.redis_url,
        output_dir=args.output_dir
    )
    
    try:
        # Run simulation
        await monitor.simulate_monitoring(args.num_transactions)
    finally:
        # Clean up
        await monitor.close()

if __name__ == "__main__":
    asyncio.run(main())
import os
import sys
import time
import signal
import threading
import traceback
from typing import Dict, List, Tuple, Optional, Union
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
import ast
import csv
import json
import importlib.util
from pathlib import Path

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


# ============================================================================
# ADDRESS LIST MANAGEMENT FUNCTIONS FOR PHASE 1 INTEGRATION
# ============================================================================

def load_addresses_from_file(filepath: str, dict_name: str) -> Dict[str, str]:
    """
    Safely loads a specific dictionary from data/addresses.py using AST parsing.
    
    Args:
        filepath: Path to the addresses file (e.g., 'data/addresses.py')
        dict_name: Name of the dictionary to extract (e.g., 'known_exchange_addresses')
    
    Returns:
        Dictionary containing the address mappings
    
    Raises:
        FileNotFoundError: If the file doesn't exist
        ValueError: If the dictionary name is not found
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            content = file.read()
        
        # Parse the file content as an AST
        tree = ast.parse(content)
        
        # Find the target dictionary assignment
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == dict_name:
                        # Safely evaluate the dictionary literal
                        if isinstance(node.value, ast.Dict):
                            return ast.literal_eval(node.value)
        
        raise ValueError(f"Dictionary '{dict_name}' not found in {filepath}")
        
    except Exception as e:
        safe_print(f"Error loading addresses from {filepath}: {str(e)}")
        raise


def fetch_and_parse_nemesiaai_data(local_repo_path: str) -> Dict[str, str]:
    """
    Fetches and parses Ethereum exchange addresses from the nemesiaai/crypto-exchange-wallets repository.
    
    Args:
        local_repo_path: Path to local clone/download of the nemesiaai repository
    
    Returns:
        Dictionary of {address: label} mappings
    """
    addresses = {}
    repo_path = Path(local_repo_path)
    
    if not repo_path.exists():
        safe_print(f"Repository path does not exist: {local_repo_path}")
        return addresses
    
    # Target files to parse (adjust based on actual repository structure)
    target_files = [
        "etherscan_labs_labeled_exchange_addresses.js",
        "coincarp_exchange_wallets_ethereum.json",
        "exchange_wallets.json",
        "ethereum_exchange_addresses.json"
    ]
    
    for filename in target_files:
        file_path = repo_path / filename
        if file_path.exists():
            try:
                safe_print(f"Processing {filename}...")
                
                if filename.endswith('.json'):
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Handle different JSON structures
                    if isinstance(data, dict):
                        for addr, label in data.items():
                            if addr and isinstance(addr, str) and addr.startswith('0x'):
                                addresses[addr.lower()] = str(label)
                    elif isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict):
                                addr = item.get('address') or item.get('wallet_address')
                                label = item.get('label') or item.get('exchange') or item.get('name')
                                if addr and label and addr.startswith('0x'):
                                    addresses[addr.lower()] = str(label)
                
                elif filename.endswith('.js'):
                    # Parse JavaScript files containing address objects
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    # Extract JSON-like objects from JavaScript
                    import re
                    json_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
                    matches = re.findall(json_pattern, content)
                    
                    for match in matches:
                        try:
                            # Clean up JavaScript syntax to make it JSON-compatible
                            cleaned = re.sub(r'(\w+):', r'"\1":', match)  # Quote keys
                            cleaned = re.sub(r"'([^']*)'", r'"\1"', cleaned)  # Convert single quotes
                            data = json.loads(cleaned)
                            
                            for addr, label in data.items():
                                if addr and isinstance(addr, str) and addr.startswith('0x'):
                                    addresses[addr.lower()] = str(label)
                        except json.JSONDecodeError:
                            continue
                            
            except Exception as e:
                safe_print(f"Error processing {filename}: {str(e)}")
                continue
    
    safe_print(f"Extracted {len(addresses)} addresses from nemesiaai repository")
    return addresses


def parse_coincarp_csv(csv_filepath: str, address_column: str = 'address', 
                      label_column: str = 'exchange') -> Dict[str, str]:
    """
    Parses CoinCarp CSV files to extract exchange addresses.
    
    Args:
        csv_filepath: Path to the CoinCarp CSV file
        address_column: Name of the column containing addresses
        label_column: Name of the column containing exchange labels
    
    Returns:
        Dictionary of {address: label} mappings
    """
    addresses = {}
    
    try:
        with open(csv_filepath, 'r', encoding='utf-8') as csvfile:
            # Detect delimiter
            sample = csvfile.read(1024)
            csvfile.seek(0)
            sniffer = csv.Sniffer()
            delimiter = sniffer.sniff(sample).delimiter
            
            reader = csv.DictReader(csvfile, delimiter=delimiter)
            
            for row in reader:
                addr = row.get(address_column, '').strip()
                label = row.get(label_column, '').strip()
                
                if addr and label:
                    # Normalize address format
                    if addr.startswith('0x'):  # Ethereum-style
                        addr = addr.lower()
                    addresses[addr] = label
        
        safe_print(f"Extracted {len(addresses)} addresses from {csv_filepath}")
        
    except Exception as e:
        safe_print(f"Error parsing CSV {csv_filepath}: {str(e)}")
    
    return addresses


def merge_address_data(current_dict: Dict[str, str], new_data: Dict[str, str], 
                      conflict_strategy: str = "prefer_existing_if_specific") -> Dict[str, str]:
    """
    Merges new address data with existing address dictionary using intelligent conflict resolution.
    
    Args:
        current_dict: Current address dictionary from data/addresses.py
        new_data: New address data to merge
        conflict_strategy: Strategy for handling conflicts
            - "prefer_existing_if_specific": Keep existing if it's more specific
            - "prefer_new": Always use new label
            - "concatenate_if_different": Combine labels if they differ
            - "always_overwrite_with_por_label": For PoR data, always overwrite
    
    Returns:
        Updated merged dictionary
    """
    merged = current_dict.copy()
    conflicts_logged = []
    additions_logged = []
    
    def is_more_specific(label1: str, label2: str) -> bool:
        """Determines if label1 is more specific than label2"""
        specific_indicators = ['hot', 'cold', 'wallet', 'deposit', 'withdrawal', 'trading']
        label1_lower = label1.lower()
        label2_lower = label2.lower()
        
        label1_specific = any(indicator in label1_lower for indicator in specific_indicators)
        label2_specific = any(indicator in label2_lower for indicator in specific_indicators)
        
        if label1_specific and not label2_specific:
            return True
        elif label2_specific and not label1_specific:
            return False
        else:
            return len(label1) > len(label2)  # Longer labels often more descriptive
    
    for addr, new_label in new_data.items():
        addr_normalized = addr.lower() if addr.startswith('0x') else addr
        
        if addr_normalized not in merged:
            # New address - add it
            merged[addr_normalized] = new_label
            additions_logged.append(f"Added: {addr_normalized} -> {new_label}")
        else:
            # Address exists - handle conflict
            existing_label = merged[addr_normalized]
            
            if existing_label == new_label:
                continue  # No conflict
            
            if conflict_strategy == "prefer_existing_if_specific":
                if is_more_specific(existing_label, new_label):
                    conflicts_logged.append(f"Kept existing: {addr_normalized} -> {existing_label} (vs {new_label})")
                else:
                    merged[addr_normalized] = new_label
                    conflicts_logged.append(f"Updated: {addr_normalized} -> {new_label} (was {existing_label})")
            
            elif conflict_strategy == "prefer_new":
                merged[addr_normalized] = new_label
                conflicts_logged.append(f"Updated: {addr_normalized} -> {new_label} (was {existing_label})")
            
            elif conflict_strategy == "concatenate_if_different":
                if existing_label.lower() != new_label.lower():
                    combined_label = f"{existing_label}|{new_label}"
                    merged[addr_normalized] = combined_label
                    conflicts_logged.append(f"Combined: {addr_normalized} -> {combined_label}")
            
            elif conflict_strategy == "always_overwrite_with_por_label":
                merged[addr_normalized] = new_label
                conflicts_logged.append(f"PoR Override: {addr_normalized} -> {new_label} (was {existing_label})")
    
    # Log summary
    safe_print(f"\nMerge Summary:")
    safe_print(f"  New addresses added: {len(additions_logged)}")
    safe_print(f"  Conflicts resolved: {len(conflicts_logged)}")
    safe_print(f"  Total addresses: {len(merged)}")
    
    if conflicts_logged:
        safe_print(f"\nConflict Details:")
        for conflict in conflicts_logged[:10]:  # Show first 10
            safe_print(f"  {conflict}")
        if len(conflicts_logged) > 10:
            safe_print(f"  ... and {len(conflicts_logged) - 10} more conflicts")
    
    return merged


def merge_manual_por_data(current_dict: Dict[str, str], 
                         por_address_list: List[Tuple[str, str]]) -> Dict[str, str]:
    """
    Merges manually collected Proof-of-Reserve (PoR) addresses with high authority.
    
    Args:
        current_dict: Current address dictionary
        por_address_list: List of (address, label) tuples from PoR disclosures
    
    Returns:
        Updated dictionary with PoR data integrated
    """
    por_data = {}
    for addr, label in por_address_list:
        addr_normalized = addr.lower() if addr.startswith('0x') else addr
        # Mark PoR labels as authoritative
        por_label = f"{label}_por" if not label.endswith('_por') else label
        por_data[addr_normalized] = por_label
    
    return merge_address_data(current_dict, por_data, "always_overwrite_with_por_label")


def pretty_print_address_dict_for_update(updated_dict: Dict[str, str], dict_name: str) -> None:
    """
    Prints the updated dictionary in a clean, Python-formatted string ready for copy-pasting.
    
    Args:
        updated_dict: The updated address dictionary
        dict_name: Name of the dictionary variable
    """
    safe_print(f"\n{'='*80}")
    safe_print(f"UPDATED {dict_name.upper()} FOR data/addresses.py")
    safe_print(f"{'='*80}")
    safe_print(f"\n{dict_name} = {{")
    
    # Sort addresses for consistent output
    sorted_items = sorted(updated_dict.items())
    
    for addr, label in sorted_items:
        safe_print(f'    "{addr}": "{label}",')
    
    safe_print("}")
    safe_print(f"\n{'='*80}")
    safe_print(f"Copy the above dictionary to replace {dict_name} in data/addresses.py")
    safe_print(f"{'='*80}\n")


def validate_address_format(address: str, chain: str = "ethereum") -> bool:
    """
    Validates address format for different blockchain networks.
    
    Args:
        address: The address to validate
        chain: The blockchain network ("ethereum", "solana", "xrp")
    
    Returns:
        True if address format is valid
    """
    if chain.lower() == "ethereum":
        return bool(address and isinstance(address, str) and 
                   address.startswith('0x') and len(address) == 42)
    elif chain.lower() == "solana":
        return bool(address and isinstance(address, str) and 
                   len(address) >= 32 and len(address) <= 44)
    elif chain.lower() == "xrp":
        return bool(address and isinstance(address, str) and 
                   (address.startswith('r') or address.startswith('X')))
    else:
        return bool(address and isinstance(address, str) and len(address) > 10)


# ============================================================================
# DEVELOPER WORKFLOW HELPER FUNCTIONS
# ============================================================================

def execute_nemesiaai_integration_workflow(repo_path: str) -> None:
    """
    Complete workflow for integrating nemesiaai repository data.
    
    Args:
        repo_path: Path to the local nemesiaai repository
    """
    safe_print("Starting nemesiaai integration workflow...")
    
    try:
        # Load current data
        current_known_eth_exchanges = load_addresses_from_file('data/addresses.py', 'known_exchange_addresses')
        safe_print(f"Loaded {len(current_known_eth_exchanges)} existing Ethereum exchange addresses")
        
        # Fetch new data
        nemesiaai_eth_data = fetch_and_parse_nemesiaai_data(repo_path)
        
        if not nemesiaai_eth_data:
            safe_print("No data extracted from nemesiaai repository")
            return
        
        # Merge data
        updated_known_eth_exchanges = merge_address_data(
            current_known_eth_exchanges, 
            nemesiaai_eth_data,
            "prefer_existing_if_specific"
        )
        
        # Print formatted output
        pretty_print_address_dict_for_update(updated_known_eth_exchanges, 'known_exchange_addresses')
        
    except Exception as e:
        safe_print(f"Error in nemesiaai integration workflow: {str(e)}")


def execute_coincarp_integration_workflow(csv_files: List[str]) -> None:
    """
    Complete workflow for integrating CoinCarp CSV data.
    
    Args:
        csv_files: List of paths to CoinCarp CSV files
    """
    safe_print("Starting CoinCarp integration workflow...")
    
    try:
        # Load current data
        current_known_eth_exchanges = load_addresses_from_file('data/addresses.py', 'known_exchange_addresses')
        current_solana_exchanges = load_addresses_from_file('data/addresses.py', 'solana_exchange_addresses')
        
        all_new_data = {}
        
        for csv_file in csv_files:
            safe_print(f"Processing {csv_file}...")
            csv_data = parse_coincarp_csv(csv_file)
            all_new_data.update(csv_data)
        
        if not all_new_data:
            safe_print("No data extracted from CoinCarp CSVs")
            return
        
        # Separate by chain type
        eth_data = {addr: label for addr, label in all_new_data.items() 
                   if validate_address_format(addr, "ethereum")}
        solana_data = {addr: label for addr, label in all_new_data.items() 
                      if validate_address_format(addr, "solana")}
        
        # Merge Ethereum data
        if eth_data:
            updated_eth_exchanges = merge_address_data(current_known_eth_exchanges, eth_data)
            pretty_print_address_dict_for_update(updated_eth_exchanges, 'known_exchange_addresses')
        
        # Merge Solana data
        if solana_data:
            updated_solana_exchanges = merge_address_data(current_solana_exchanges, solana_data)
            pretty_print_address_dict_for_update(updated_solana_exchanges, 'solana_exchange_addresses')
        
    except Exception as e:
        safe_print(f"Error in CoinCarp integration workflow: {str(e)}")



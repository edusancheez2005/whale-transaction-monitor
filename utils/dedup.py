# dedup.py

from typing import Dict, Any, Optional, Tuple
from collections import defaultdict
import time

# In dedup.py - update the TransactionDeduplicator class

class TransactionDeduplicator:
    def __init__(self):
        self.transactions = {}
        self.chain_hashes = defaultdict(set)
        self.address_timestamps = defaultdict(dict)
        self.stats = {
            'total_received': 0,
            'duplicates_caught': 0,
            'circular_flows_caught': 0,  # New counter for circular flows
            'by_chain': defaultdict(lambda: {'total': 0, 'duplicates': 0, 'circular': 0}),
        }

    # In dedup.py - update the generate_key method
    def generate_key(self, event: Dict[str, Any]) -> Tuple:
        """Generate a more robust unique key for transaction deduplication"""
        chain = event.get('blockchain', '').lower()
        tx_hash = event.get('tx_hash', '')
        
        # For Solana, add more components to make the key unique
        if chain == 'solana':
            # Include more data points for Solana
            from_addr = event.get('from', '')
            to_addr = event.get('to', '')
            amount = str(event.get('amount', '0'))
            # Create a more unique composite key
            return (chain, tx_hash, from_addr, to_addr, amount)
        elif chain in ['ethereum', 'bsc', 'polygon']:
            # For EVM chains, use hash and log index
            return (chain, tx_hash, event.get('log_index', 0))
        elif chain == 'xrp':
            # For XRP, use hash and sequence
            return (chain, tx_hash, event.get('sequence', 0))
        else:
            # Default case
            return (chain, tx_hash, event.get('log_index', 0))

    def handle_event(self, event: Dict[str, Any]) -> bool:
        """Process new event with enhanced deduplication"""
        if not event:
            return False

        self.stats['total_received'] += 1
        chain = event.get('blockchain', '').lower()
        self.stats['by_chain'][chain]['total'] += 1

        unique_key = self.generate_key(event)
        
        # Check for direct duplicates
        if unique_key in self.transactions:
            self.stats['duplicates_caught'] += 1
            self.stats['by_chain'][chain]['duplicates'] += 1
            
            # Update classification if needed
            if ('classification' not in self.transactions[unique_key] and 
                'classification' in event):
                self.transactions[unique_key]['classification'] = event['classification']
                
            return False
        
        # Check for circular flows (A->B->C->A) within short time windows
        from_addr = event.get('from', '')
        to_addr = event.get('to', '')
        amount = event.get('amount', 0)
        symbol = event.get('symbol', '')
        
        # Look for matching amount in reverse direction within last hour
        current_time = time.time()
        for key, tx in list(self.transactions.items()):  # Use list() to avoid iteration issues
            tx_time = tx.get('timestamp', 0)
            if current_time - tx_time > 3600:  # Skip transactions older than 1 hour
                continue
                
            # Check if this is part of a circular flow
            # 1. Same symbol
            # 2. Similar amount (within 1%)
            # 3. Either:
            #    a. Reverse direction (from becomes to, to becomes from)
            #    b. Same to address as current from address (chain of transfers)
            if (tx.get('symbol') == symbol and
                abs(tx.get('amount', 0) - amount) / max(0.01, amount) < 0.01 and
                ((tx.get('from') == to_addr and tx.get('to') == from_addr) or  # Reverse
                 (tx.get('to') == from_addr))):  # Chain of transfers
                
                self.stats['circular_flows_caught'] += 1
                self.stats['by_chain'][chain]['circular'] += 1
                return False

        # Add timestamp if not present
        if 'timestamp' not in event:
            event['timestamp'] = current_time
            
        self.transactions[unique_key] = event
        return True

        # In dedup.py - update the get_stats function
    def get_stats(self):
        """Get deduplication statistics with chain breakdown"""
        total_dupes = self.stats['duplicates_caught'] + self.stats['circular_flows_caught']
        chain_stats = {}
        
        # Calculate per-chain deduplication rates
        for chain, stats in self.stats['by_chain'].items():
            if stats['total'] > 0:
                dedup_rate = ((stats['duplicates'] + stats['circular']) / stats['total']) * 100
                chain_stats[chain] = {
                    'total': stats['total'],
                    'duplicates': stats['duplicates'] + stats['circular'],
                    'rate': dedup_rate
                }
        
        return {
            'total_transactions': len(self.transactions),
            'total_received': self.stats['total_received'],
            'duplicates_caught': self.stats['duplicates_caught'],
            'circular_flows_caught': self.stats['circular_flows_caught'],
            'total_duplicates': total_dupes,
            'by_chain': chain_stats,
            'dedup_ratio': (total_dupes / max(1, self.stats['total_received'])) * 100
        }

# Keep the old name for backward compatibility
EnhancedDeduplication = TransactionDeduplicator  # Add this line

# Global instance
deduplicator = TransactionDeduplicator()

# Export functions
def handle_event(event: Dict[str, Any]) -> bool:
    """Process new event and determine if it's unique"""
    return deduplicator.handle_event(event)

def get_stats() -> Dict[str, Any]:
    import copy
    try:
        return deduplicator.get_stats()
    except Exception as e:
        print(f"Error in get_stats: {e}")
        return {
            'total_transactions': 0,
            'total_received': 0,
            'duplicates_caught': 0,
            'circular_flows_caught': 0,
            'total_duplicates': 0,
            'by_chain': {},
            'dedup_ratio': 0
        }

def get_transactions() -> Dict:
    return deduplicator.transactions

# For backward compatibility
get_dedup_stats = get_stats
deduped_transactions = deduplicator.transactions

# Export these for direct access if needed
__all__ = [
    'handle_event',
    'get_stats',
    'get_transactions',
    'get_dedup_stats',
    'deduped_transactions',
    'TransactionDeduplicator',
    'EnhancedDeduplication',  # Add this line
    'deduplicator'
]

def deduplicate_transactions(transactions):
    """
    If duplicates share the same 'hash', keep whichever has higher confidence.
    """
    unique = {}
    for tx in transactions:
        tx_hash = tx.get("hash")
        if tx_hash is None:
            continue  # Skip if missing a hash

        # Check if we already saw a transaction with this hash
        if tx_hash in unique:
            existing = unique[tx_hash]
            if tx.get("confidence", 0) > existing.get("confidence", 1):
                unique[tx_hash] = tx
        else:
            unique[tx_hash] = tx

    return list(unique.values())
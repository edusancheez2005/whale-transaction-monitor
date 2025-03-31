# dedup.py

from typing import Dict, Any, Optional, Tuple
from collections import defaultdict
import time

class TransactionDeduplicator:
    def __init__(self):
        self.transactions = {}
        self.chain_hashes = defaultdict(set)
        self.address_timestamps = defaultdict(dict)
        self.stats = {
            'total_received': 0,
            'duplicates_caught': 0,
            'by_chain': defaultdict(lambda: {'total': 0, 'duplicates': 0}),
        }

    def generate_key(self, event: Dict[str, Any]) -> Tuple:
        chain = event.get('blockchain', '').lower()
        tx_hash = event.get('tx_hash', '')
        
        if chain == 'solana':
            # Use only chain and tx_hash for Solana events.
            return (chain, tx_hash)
        elif chain in ['ethereum', 'bsc', 'polygon']:
            return (chain, tx_hash, event.get('log_index', 0))
        elif chain == 'xrp':
            return (chain, tx_hash, event.get('sequence', 0))
        else:
            return (chain, tx_hash, event.get('log_index', 0))


    def handle_event(self, event: Dict[str, Any]) -> bool:
        """Process new event and determine if it's unique"""
        if not event:
            return False

        self.stats['total_received'] += 1
        chain = event.get('blockchain', '').lower()
        self.stats['by_chain'][chain]['total'] += 1

        unique_key = self.generate_key(event)
        
        if unique_key in self.transactions:
            self.stats['duplicates_caught'] += 1
            self.stats['by_chain'][chain]['duplicates'] += 1
            
            # If the existing event doesn't have a classification but new one does,
            # update the classification
            if ('classification' not in self.transactions[unique_key] and 
                'classification' in event):
                self.transactions[unique_key]['classification'] = event['classification']
                
            return False

        self.transactions[unique_key] = event
        return True

    def get_stats(self) -> Dict[str, Any]:
        """Get deduplication statistics"""
        return {
            'total_transactions': len(self.transactions),
            'total_received': self.stats['total_received'],
            'duplicates_caught': self.stats['duplicates_caught'],
            'by_chain': dict(self.stats['by_chain']),
            'dedup_ratio': (self.stats['duplicates_caught'] / max(1, self.stats['total_received'])) * 100
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
    return deduplicator.get_stats()

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



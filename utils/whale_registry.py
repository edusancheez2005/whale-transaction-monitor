#!/usr/bin/env python3
"""
Whale Wallet Registry
Tracks wallets that consistently make profitable/significant moves
"""

import logging
import json
import os
from typing import Dict, Optional
from datetime import datetime, timedelta
from decimal import Decimal

logger = logging.getLogger(__name__)

class WhaleRegistry:
    """
    Track and learn from whale wallet behavior over time.
    
    Features:
    - Track trade frequency and patterns
    - Calculate "smart money" score
    - Boost confidence for proven performers
    - Detect accumulation/distribution patterns
    """
    
    def __init__(self, persistence_file: str = 'data/whale_registry.json'):
        self.persistence_file = persistence_file
        self.registry = {}  # {address: whale_data}
        self.load_registry()
    
    def load_registry(self):
        """Load whale registry from disk."""
        try:
            if os.path.exists(self.persistence_file):
                with open(self.persistence_file, 'r') as f:
                    self.registry = json.load(f)
                logger.info(f"Loaded {len(self.registry)} tracked whales from registry")
            else:
                logger.info("No existing whale registry found, starting fresh")
        except Exception as e:
            logger.error(f"Failed to load whale registry: {e}")
            self.registry = {}
    
    def save_registry(self):
        """Save whale registry to disk."""
        try:
            os.makedirs(os.path.dirname(self.persistence_file), exist_ok=True)
            with open(self.persistence_file, 'w') as f:
                json.dump(self.registry, f, indent=2, default=str)
            logger.debug(f"Saved {len(self.registry)} whales to registry")
        except Exception as e:
            logger.error(f"Failed to save whale registry: {e}")
    
    def track_transaction(
        self,
        address: str,
        transaction: Dict
    ):
        """
        Track a whale transaction and update registry.
        
        Args:
            address: Wallet address
            transaction: {
                'classification': 'BUY' | 'SELL',
                'token': str,
                'usd_value': float,
                'timestamp': datetime,
                'confidence': float
            }
        """
        address_lower = address.lower()
        
        # Initialize if new whale
        if address_lower not in self.registry:
            self.registry[address_lower] = {
                'address': address_lower,
                'first_seen': datetime.now().isoformat(),
                'last_activity': datetime.now().isoformat(),
                'total_trades': 0,
                'buy_count': 0,
                'sell_count': 0,
                'total_volume_usd': 0.0,
                'tokens_traded': [],
                'smart_money_score': 0.5,  # Start neutral
                'is_proven': False,
                'trade_history': []
            }
        
        whale_data = self.registry[address_lower]
        
        # Update stats
        whale_data['last_activity'] = datetime.now().isoformat()
        whale_data['total_trades'] += 1
        whale_data['total_volume_usd'] += transaction.get('usd_value', 0)
        
        if transaction.get('classification') == 'BUY':
            whale_data['buy_count'] += 1
        elif transaction.get('classification') == 'SELL':
            whale_data['sell_count'] += 1
        
        # Track tokens
        token = transaction.get('token')
        if token and token not in whale_data['tokens_traded']:
            whale_data['tokens_traded'].append(token)
        
        # Add to trade history (keep last 50)
        whale_data['trade_history'].append({
            'timestamp': datetime.now().isoformat(),
            'classification': transaction.get('classification'),
            'token': token,
            'usd_value': transaction.get('usd_value'),
            'confidence': transaction.get('confidence')
        })
        whale_data['trade_history'] = whale_data['trade_history'][-50:]
        
        # Update smart money score
        self._update_smart_money_score(whale_data)
        
        # Mark as proven if meets criteria
        if (
            whale_data['total_trades'] >= 5 and
            whale_data['total_volume_usd'] >= 250_000 and
            whale_data['smart_money_score'] >= 0.65
        ):
            whale_data['is_proven'] = True
        
        # Save to disk periodically
        if whale_data['total_trades'] % 5 == 0:
            self.save_registry()
    
    def _update_smart_money_score(self, whale_data: Dict):
        """
        Calculate "smart money" score based on:
        - Consistency (regular trading)
        - Volume (high value trades)
        - Diversification (trading multiple tokens)
        """
        score = 0.5  # Start neutral
        
        # Factor 1: Trade frequency (max +0.2)
        if whale_data['total_trades'] >= 20:
            score += 0.2
        elif whale_data['total_trades'] >= 10:
            score += 0.1
        
        # Factor 2: Total volume (max +0.2)
        if whale_data['total_volume_usd'] >= 1_000_000:
            score += 0.2
        elif whale_data['total_volume_usd'] >= 500_000:
            score += 0.1
        
        # Factor 3: Token diversification (max +0.1)
        unique_tokens = len(whale_data['tokens_traded'])
        if unique_tokens >= 10:
            score += 0.1
        elif unique_tokens >= 5:
            score += 0.05
        
        whale_data['smart_money_score'] = min(0.99, score)
    
    def get_whale_confidence_boost(self, address: str) -> float:
        """
        Get confidence boost for a whale address.
        
        Returns: 0.0 - 0.2 (boost to add to base confidence)
        """
        address_lower = address.lower()
        
        if address_lower not in self.registry:
            return 0.0
        
        whale_data = self.registry[address_lower]
        
        if whale_data['is_proven']:
            # Proven whales get max boost
            return 0.15
        elif whale_data['total_trades'] >= 5:
            # Active traders get moderate boost
            return 0.08
        elif whale_data['total_trades'] >= 2:
            # New but active get small boost
            return 0.03
        
        return 0.0
    
    def get_whale_info(self, address: str) -> Optional[Dict]:
        """Get whale info if tracked."""
        return self.registry.get(address.lower())
    
    def is_proven_whale(self, address: str) -> bool:
        """Check if address is a proven whale."""
        whale_data = self.registry.get(address.lower())
        return whale_data.get('is_proven', False) if whale_data else False
    
    def get_stats(self) -> Dict:
        """Get registry statistics."""
        proven_count = sum(1 for w in self.registry.values() if w.get('is_proven'))
        total_volume = sum(w.get('total_volume_usd', 0) for w in self.registry.values())
        
        return {
            'total_tracked': len(self.registry),
            'proven_whales': proven_count,
            'total_volume_tracked': total_volume,
            'total_trades_tracked': sum(w.get('total_trades', 0) for w in self.registry.values())
        }


# Global instance
whale_registry = WhaleRegistry()



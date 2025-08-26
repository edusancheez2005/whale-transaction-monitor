"""
Opportunity Engine - Real-Time Whale Transaction Analysis

This module provides sophisticated market analysis capabilities for detecting
high-value trading opportunities based on whale transaction patterns.
"""

from .market_data_provider import MarketDataProvider
from .analyzer import OpportunityAnalyzer
from .models import OpportunitySignal, MarketHeuristics

__all__ = [
    'MarketDataProvider',
    'OpportunityAnalyzer', 
    'OpportunitySignal',
    'MarketHeuristics'
] 
"""
OpportunityAnalyzer - Core trading signal analysis engine

Performs sophisticated technical analysis on market data to identify 
high-value trading opportunities based on whale transaction patterns.
"""

import time
import logging
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple

from .market_data_provider import MarketDataProvider
from .models import (
    OpportunitySignal, MarketHeuristics, TokenInfo, TransactionTrigger,
    SignalType, HeuristicResult
)
from config.settings import GLOBAL_USD_THRESHOLD


class OpportunityAnalyzer:
    """
    Professional-grade opportunity analysis engine.
    
    Applies sophisticated technical analysis heuristics to determine
    if a whale transaction signals a high-value trading opportunity.
    """
    
    def __init__(self, market_data_provider: MarketDataProvider):
        self.market_data_provider = market_data_provider
        self.logger = logging.getLogger(f"{__name__}.OpportunityAnalyzer")
        
        # Analysis configuration
        self.config = {
            # EMA settings
            'ema_period': 20,
            
            # Volume surge thresholds
            'volume_surge_threshold': 2.0,  # 200% of average
            
            # RSI settings
            'rsi_period': 14,
            'rsi_buy_threshold': 75,  # Don't buy if RSI > 75 (overbought)
            'rsi_sell_threshold': 25,  # Don't sell if RSI < 25 (oversold)
            
            # Minimum data points required for analysis
            'min_data_points': 50,
            
            # Confidence scoring weights
            'weights': {
                'price_trend': 0.4,
                'volume_surge': 0.4,
                'rsi_check': 0.2
            },
            
            # More realistic scoring thresholds
            'high_confidence_threshold': 0.8,  # Require 80% instead of 100%
            'moderate_confidence_threshold': 0.6  # Allow moderate signals at 60%
        }
        
        self.logger.info("OpportunityAnalyzer initialized with technical analysis capabilities")
    
    def analyze_opportunity(self, transaction_data: Dict[str, Any]) -> Optional[OpportunitySignal]:
        """
        Main entry point for opportunity analysis.
        
        Args:
            transaction_data: Dict containing transaction details:
                - contract_address: str
                - chain: str
                - classification: str (BUY/SELL/STAKING)
                - value_usd: float
                - hash: str
                - from_address: str
                - to_address: str
                - symbol: str (optional)
                
        Returns:
            OpportunitySignal if a signal is detected, None otherwise
        """
        start_time = time.time()
        
        try:
            # Extract transaction details
            contract_address = transaction_data.get('contract_address', '').strip()
            chain = transaction_data.get('chain', 'ethereum').lower()
            classification = transaction_data.get('classification', '').upper()
            value_usd = float(transaction_data.get('value_usd', 0))
            
            # Validate inputs
            if not contract_address or not classification:
                self.logger.warning("Missing required transaction data")
                return None
            
            # Check threshold
            if value_usd < GLOBAL_USD_THRESHOLD:
                self.logger.debug(f"Transaction value ${value_usd:,.0f} below threshold ${GLOBAL_USD_THRESHOLD:,.0f}")
                return None
            
            # Only analyze BUY/SELL transactions
            if classification not in ['BUY', 'SELL', 'STAKING']:
                self.logger.debug(f"Skipping analysis for classification: {classification}")
                return None
            
            # Map STAKING to BUY for analysis purposes
            analysis_classification = 'BUY' if classification in ['STAKING'] else classification
            
            self.logger.info(f"Analyzing {analysis_classification} opportunity for {contract_address} (${value_usd:,.0f})")
            
            # Create token and transaction objects
            token = TokenInfo(
                symbol=transaction_data.get('symbol'),
                contract_address=contract_address,
                chain=chain,
                decimals=transaction_data.get('decimals')
            )
            
            trigger = TransactionTrigger(
                hash=transaction_data.get('hash', ''),
                from_address=transaction_data.get('from_address', ''),
                to_address=transaction_data.get('to_address', ''),
                value_usd=value_usd,
                classification=classification
            )
            
            # Fetch market data
            market_data = self.market_data_provider.get_market_data_for_token(contract_address, chain)
            if not market_data:
                self.logger.warning(f"Could not fetch market data for {contract_address}")
                return None
            
            # Perform technical analysis
            heuristics = self._perform_technical_analysis(market_data, analysis_classification)
            
            # Generate signal if heuristics pass
            signal = self._generate_signal(token, trigger, heuristics, analysis_classification, market_data)
            
            # Calculate analysis duration
            analysis_duration = (time.time() - start_time) * 1000
            if signal:
                signal.analysis_duration_ms = analysis_duration
            
            return signal
            
        except Exception as e:
            self.logger.error(f"Error analyzing opportunity: {e}", exc_info=True)
            return None
    
    def _perform_technical_analysis(self, market_data: Dict[str, Any], signal_type: str) -> MarketHeuristics:
        """
        Perform comprehensive technical analysis on market data.
        
        Args:
            market_data: Market data from CoinGecko
            signal_type: 'BUY' or 'SELL'
            
        Returns:
            MarketHeuristics object with analysis results
        """
        heuristics = MarketHeuristics()
        
        try:
            prices = market_data.get('prices', [])
            volumes = market_data.get('volumes', [])
            
            if len(prices) < self.config['min_data_points']:
                self.logger.warning(f"Insufficient data points: {len(prices)} < {self.config['min_data_points']}")
                heuristics.insufficient_data = True
                return heuristics
            
            heuristics.data_points_analyzed = len(prices)
            
            # Extract price and volume arrays
            price_data = np.array([p[1] for p in prices])
            volume_data = np.array([v[1] for v in volumes])
            
            current_price = price_data[-1]
            heuristics.current_price = current_price
            
            # Calculate 20-period EMA
            ema_20 = self._calculate_ema(price_data, self.config['ema_period'])
            heuristics.ema_20 = ema_20
            
            # Price trend analysis
            if signal_type == 'BUY':
                heuristics.price_above_ema = (
                    HeuristicResult.PASS if current_price > ema_20 
                    else HeuristicResult.FAIL
                )
            else:  # SELL
                heuristics.price_below_ema = (
                    HeuristicResult.PASS if current_price < ema_20 
                    else HeuristicResult.FAIL
                )
            
            # Volume analysis
            current_volume = volume_data[-1]
            # Use last 24 data points (roughly 2 hours for 5-min data) for recent volume
            recent_volume_avg = np.mean(volume_data[-24:]) if len(volume_data) >= 24 else current_volume
            # Use earlier data for baseline (hours 4-24)
            baseline_volume_avg = np.mean(volume_data[-144:-24]) if len(volume_data) >= 144 else np.mean(volume_data[:-24]) if len(volume_data) > 24 else current_volume
            
            volume_ratio = recent_volume_avg / baseline_volume_avg if baseline_volume_avg > 0 else 1.0
            
            heuristics.current_volume = current_volume
            heuristics.avg_volume_24h = baseline_volume_avg
            heuristics.volume_ratio = volume_ratio
            
            heuristics.volume_surge = (
                HeuristicResult.PASS if volume_ratio >= self.config['volume_surge_threshold']
                else HeuristicResult.FAIL
            )
            
            # RSI analysis
            rsi = self._calculate_rsi(price_data, self.config['rsi_period'])
            heuristics.rsi_value = rsi
            
            if signal_type == 'BUY':
                # For BUY signals, RSI should be below 75 (not overbought)
                heuristics.rsi_check = (
                    HeuristicResult.PASS if rsi < self.config['rsi_buy_threshold']
                    else HeuristicResult.FAIL
                )
            else:  # SELL
                # For SELL signals, RSI should be above 25 (not oversold)
                heuristics.rsi_check = (
                    HeuristicResult.PASS if rsi > self.config['rsi_sell_threshold']
                    else HeuristicResult.FAIL
                )
            
            # Calculate overall scores
            heuristics.buy_score, heuristics.sell_score = self._calculate_scores(heuristics, signal_type)
            
            self.logger.debug(f"Technical analysis complete: Price=${current_price:.6f}, EMA=${ema_20:.6f}, "
                            f"Volume Ratio={volume_ratio:.2f}, RSI={rsi:.1f}")
            
        except Exception as e:
            self.logger.error(f"Error in technical analysis: {e}")
            heuristics.insufficient_data = True
            
        return heuristics
    
    def _calculate_ema(self, prices: np.ndarray, period: int) -> float:
        """Calculate Exponential Moving Average."""
        if len(prices) < period:
            return float(np.mean(prices))
        
        # Use pandas-style EMA calculation
        alpha = 2 / (period + 1)
        ema = prices[0]
        
        for price in prices[1:]:
            ema = alpha * price + (1 - alpha) * ema
            
        return float(ema)
    
    def _calculate_rsi(self, prices: np.ndarray, period: int = 14) -> float:
        """Calculate Relative Strength Index."""
        if len(prices) < period + 1:
            return 50.0  # Neutral RSI for insufficient data
        
        # Calculate price changes
        deltas = np.diff(prices)
        
        # Separate gains and losses
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        # Calculate average gains and losses
        avg_gains = np.mean(gains[-period:])
        avg_losses = np.mean(losses[-period:])
        
        if avg_losses == 0:
            return 100.0
        
        rs = avg_gains / avg_losses
        rsi = 100 - (100 / (1 + rs))
        
        return float(rsi)
    
    def _calculate_scores(self, heuristics: MarketHeuristics, signal_type: str) -> Tuple[float, float]:
        """Calculate buy and sell scores based on heuristics."""
        buy_score = 0.0
        sell_score = 0.0
        
        weights = self.config['weights']
        
        if signal_type == 'BUY':
            # Calculate buy score
            if heuristics.price_above_ema == HeuristicResult.PASS:
                buy_score += weights['price_trend']
            if heuristics.volume_surge == HeuristicResult.PASS:
                buy_score += weights['volume_surge']
            if heuristics.rsi_check == HeuristicResult.PASS:
                buy_score += weights['rsi_check']
        else:  # SELL
            # Calculate sell score
            if heuristics.price_below_ema == HeuristicResult.PASS:
                sell_score += weights['price_trend']
            if heuristics.volume_surge == HeuristicResult.PASS:
                sell_score += weights['volume_surge']
            if heuristics.rsi_check == HeuristicResult.PASS:
                sell_score += weights['rsi_check']
        
        return buy_score, sell_score
    
    def _generate_signal(self, token: TokenInfo, trigger: TransactionTrigger, 
                        heuristics: MarketHeuristics, signal_type: str,
                        market_data: Dict[str, Any]) -> Optional[OpportunitySignal]:
        """
        Generate the final trading signal based on analysis results.
        
        Args:
            token: Token information
            trigger: Transaction that triggered analysis
            heuristics: Technical analysis results
            signal_type: 'BUY' or 'SELL'
            market_data: Raw market data
            
        Returns:
            OpportunitySignal if criteria are met, None otherwise
        """
        if heuristics.insufficient_data:
            self.logger.debug("Insufficient data for signal generation")
            return None
        
        # Determine if signal criteria are met
        score = heuristics.buy_score if signal_type == 'BUY' else heuristics.sell_score
        
        # Use flexible thresholds instead of requiring perfect score
        high_confidence_threshold = self.config['high_confidence_threshold']
        moderate_confidence_threshold = self.config['moderate_confidence_threshold']
        
        if score < moderate_confidence_threshold:
            self.logger.debug(f"Signal criteria not met: score {score:.2f} < required {moderate_confidence_threshold:.2f}")
            return None
        
        # Determine signal type and confidence
        if score >= high_confidence_threshold:
            if signal_type == 'BUY':
                signal_enum = SignalType.HIGH_CONFIDENCE_BUY
            else:
                signal_enum = SignalType.HIGH_CONFIDENCE_SELL
            confidence = min(0.95, 0.75 + (score - high_confidence_threshold) * 0.5)
        elif score >= moderate_confidence_threshold:
            if signal_type == 'BUY':
                signal_enum = SignalType.MODERATE_BUY
            else:
                signal_enum = SignalType.MODERATE_SELL
            confidence = min(0.75, 0.55 + (score - moderate_confidence_threshold) * 0.4)
        else:
            # This shouldn't happen given our logic above, but included for completeness
            signal_enum = SignalType.NO_SIGNAL
            confidence = 0.0
        
        # Generate reasoning
        reasoning = heuristics.get_passing_heuristics(signal_type)
        if not reasoning:
            reasoning = ["All technical indicators align for this signal"]
        
        # Create market data snippet for the response
        market_snippet = {
            'current_price': heuristics.current_price,
            'ema_20': heuristics.ema_20,
            'volume_ratio': heuristics.volume_ratio,
            'rsi': heuristics.rsi_value,
            'data_points': heuristics.data_points_analyzed
        }
        
        signal = OpportunitySignal(
            signal_type=signal_enum,
            confidence_score=confidence,
            token=token,
            trigger_transaction=trigger,
            heuristics=heuristics,
            reasoning=reasoning,
            market_data_snippet=market_snippet
        )
        
        self.logger.info(f"Generated {signal_enum.value} signal with {confidence:.1%} confidence for {token.symbol or token.contract_address}")
        
        return signal 
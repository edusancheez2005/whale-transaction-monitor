#!/usr/bin/env python3
"""
🐋 WHALE SENTIMENT AGGREGATION SERVICE 🐋

Real-time aggregation service that calculates token buy/sell ratios
from whale transaction classifications every minute.

Features:
- Real-time sentiment calculation
- Token trending analysis
- Historical data windowing
- Supabase integration
- Production logging
"""

import time
import threading
import traceback
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from typing import Dict, List, Tuple
from supabase import create_client, Client
import config.api_keys as api_keys
from config.logging_config import production_logger

class WhaleSentimentAggregator:
    """
    Aggregates whale transaction data to provide real-time sentiment analysis.
    """
    
    def __init__(self):
        self.supabase: Client = None
        self.is_running = False
        self.aggregation_thread = None
        self._initialize_supabase()
    
    def _initialize_supabase(self):
        """Initialize Supabase client."""
        try:
            self.supabase = create_client(
                api_keys.SUPABASE_URL,
                api_keys.SUPABASE_SERVICE_ROLE_KEY
            )
            production_logger.info("Whale sentiment aggregator initialized", 
                                 extra={'extra_fields': {'supabase_url': api_keys.SUPABASE_URL}})
        except Exception as e:
            production_logger.error("Failed to initialize Supabase for whale sentiment aggregator",
                                  extra={'extra_fields': {'error': str(e)}})
    
    def get_token_sentiment(self, hours: int = 2) -> List[Dict]:
        """
        Get token sentiment analysis for the specified time window.
        
        Args:
            hours (int): Number of hours to look back
            
        Returns:
            List[Dict]: Token sentiment data sorted by activity
        """
        if not self.supabase:
            return []
        
        try:
            # Calculate time window
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(hours=hours)
            
            # Query whale transactions
            result = self.supabase.table('whale_transactions') \
                .select('token_symbol, classification, usd_value, confidence, whale_score') \
                .gte('timestamp', start_time.isoformat()) \
                .lte('timestamp', end_time.isoformat()) \
                .in_('classification', ['BUY', 'SELL']) \
                .execute()
            
            if not result.data:
                return []
            
            # Aggregate data by token
            token_data = defaultdict(lambda: {
                'buys': 0,
                'sells': 0,
                'total_volume': 0,
                'buy_volume': 0,
                'sell_volume': 0,
                'avg_confidence': 0,
                'avg_whale_score': 0,
                'total_transactions': 0
            })
            
            for tx in result.data:
                symbol = tx['token_symbol']
                classification = tx['classification']
                usd_value = float(tx['usd_value']) if tx['usd_value'] else 0
                confidence = float(tx['confidence']) if tx['confidence'] else 0
                whale_score = float(tx['whale_score']) if tx['whale_score'] else 0
                
                stats = token_data[symbol]
                stats['total_transactions'] += 1
                stats['total_volume'] += usd_value
                stats['avg_confidence'] += confidence
                stats['avg_whale_score'] += whale_score
                
                if classification == 'BUY':
                    stats['buys'] += 1
                    stats['buy_volume'] += usd_value
                elif classification == 'SELL':
                    stats['sells'] += 1
                    stats['sell_volume'] += usd_value
            
            # Calculate final metrics
            sentiment_data = []
            for symbol, stats in token_data.items():
                total_tx = stats['total_transactions']
                total_directional = stats['buys'] + stats['sells']
                
                if total_directional == 0:
                    continue
                
                buy_percentage = (stats['buys'] / total_directional) * 100
                sell_percentage = (stats['sells'] / total_directional) * 100
                
                # Calculate volume-weighted sentiment
                total_directional_volume = stats['buy_volume'] + stats['sell_volume']
                volume_weighted_buy_pct = 0
                if total_directional_volume > 0:
                    volume_weighted_buy_pct = (stats['buy_volume'] / total_directional_volume) * 100
                
                sentiment_data.append({
                    'token_symbol': symbol,
                    'buys': stats['buys'],
                    'sells': stats['sells'],
                    'total_transactions': total_directional,
                    'buy_percentage': round(buy_percentage, 2),
                    'sell_percentage': round(sell_percentage, 2),
                    'volume_weighted_buy_percentage': round(volume_weighted_buy_pct, 2),
                    'total_volume': stats['total_volume'],
                    'buy_volume': stats['buy_volume'],
                    'sell_volume': stats['sell_volume'],
                    'avg_confidence': round(stats['avg_confidence'] / total_tx, 2) if total_tx > 0 else 0,
                    'avg_whale_score': round(stats['avg_whale_score'] / total_tx, 2) if total_tx > 0 else 0,
                    'sentiment_score': round(buy_percentage - sell_percentage, 2),
                    'volume_sentiment_score': round(volume_weighted_buy_pct - (100 - volume_weighted_buy_pct), 2),
                    'last_updated': datetime.now(timezone.utc).isoformat()
                })
            
            # Sort by total transactions (activity level)
            sentiment_data.sort(key=lambda x: x['total_transactions'], reverse=True)
            
            production_logger.info("Token sentiment calculated successfully",
                                 extra={'extra_fields': {
                                     'tokens_analyzed': len(sentiment_data),
                                     'time_window_hours': hours,
                                     'total_transactions': sum(d['total_transactions'] for d in sentiment_data)
                                 }})
            
            return sentiment_data
            
        except Exception as e:
            production_logger.error("Failed to calculate token sentiment",
                                  extra={'extra_fields': {
                                      'error': str(e),
                                      'stack_trace': traceback.format_exc()
                                  }})
            return []
    
    def get_bullish_tokens(self, hours: int = 2, min_transactions: int = 3) -> List[Dict]:
        """Get tokens with highest buy percentage."""
        sentiment_data = self.get_token_sentiment(hours)
        
        # Filter and sort by buy percentage
        bullish_tokens = [
            token for token in sentiment_data 
            if token['total_transactions'] >= min_transactions
        ]
        
        bullish_tokens.sort(key=lambda x: x['buy_percentage'], reverse=True)
        return bullish_tokens[:10]  # Top 10
    
    def get_bearish_tokens(self, hours: int = 2, min_transactions: int = 3) -> List[Dict]:
        """Get tokens with highest sell percentage."""
        sentiment_data = self.get_token_sentiment(hours)
        
        # Filter and sort by sell percentage
        bearish_tokens = [
            token for token in sentiment_data 
            if token['total_transactions'] >= min_transactions
        ]
        
        bearish_tokens.sort(key=lambda x: x['sell_percentage'], reverse=True)
        return bearish_tokens[:10]  # Top 10
    
    def store_aggregated_sentiment(self, sentiment_data: List[Dict]) -> bool:
        """
        Store aggregated sentiment data in Supabase for the UI.
        
        Args:
            sentiment_data: List of token sentiment data
            
        Returns:
            bool: Success status
        """
        if not self.supabase or not sentiment_data:
            return False
        
        try:
            # Create aggregated sentiment entries
            aggregated_entries = []
            timestamp = datetime.now(timezone.utc)
            
            for token_data in sentiment_data:
                entry = {
                    'token_symbol': token_data['token_symbol'],
                    'time_window': '2h',  # Could be configurable
                    'buy_count': token_data['buys'],
                    'sell_count': token_data['sells'],
                    'buy_percentage': token_data['buy_percentage'],
                    'sell_percentage': token_data['sell_percentage'],
                    'volume_weighted_buy_percentage': token_data['volume_weighted_buy_percentage'],
                    'total_volume_usd': token_data['total_volume'],
                    'sentiment_score': token_data['sentiment_score'],
                    'volume_sentiment_score': token_data['volume_sentiment_score'],
                    'avg_confidence': token_data['avg_confidence'],
                    'avg_whale_score': token_data['avg_whale_score'],
                    'total_transactions': token_data['total_transactions'],
                    'calculated_at': timestamp.isoformat()
                }
                aggregated_entries.append(entry)
            
            # Clear old data for this time window (optional)
            # self.supabase.table('whale_sentiment_aggregated').delete().eq('time_window', '2h').execute()
            
            # Insert new aggregated data
            result = self.supabase.table('whale_sentiment_aggregated').insert(aggregated_entries).execute()
            
            if result.data:
                production_logger.info("Aggregated sentiment stored successfully",
                                     extra={'extra_fields': {
                                         'tokens_stored': len(aggregated_entries),
                                         'timestamp': timestamp.isoformat()
                                     }})
                return True
            else:
                production_logger.error("Failed to store aggregated sentiment")
                return False
                
        except Exception as e:
            production_logger.error("Error storing aggregated sentiment",
                                  extra={'extra_fields': {
                                      'error': str(e),
                                      'stack_trace': traceback.format_exc()
                                  }})
            return False
    
    def print_sentiment_summary(self):
        """Print a console summary of current whale sentiment."""
        try:
            print("\n" + "="*80)
            print("🐋 WHALE SENTIMENT ANALYSIS (Last 2 Hours)")
            print("="*80)
            
            # Get sentiment data
            sentiment_data = self.get_token_sentiment(hours=2)
            
            if not sentiment_data:
                print("No whale transaction data available.")
                return
            
            # Print header
            print(f"{'TOKEN':<10} {'BUYS':>6} {'SELLS':>6} {'BUY%':>7} {'SELL%':>7} {'VOLUME':>12} {'SENTIMENT':>10}")
            print("-" * 80)
            
            # Print top tokens by activity
            for token in sentiment_data[:15]:  # Top 15 most active
                sentiment_indicator = "🟢" if token['buy_percentage'] > 60 else "🔴" if token['sell_percentage'] > 60 else "🟡"
                
                print(f"{token['token_symbol']:<10} "
                      f"{token['buys']:>6} "
                      f"{token['sells']:>6} "
                      f"{token['buy_percentage']:>6.1f}% "
                      f"{token['sell_percentage']:>6.1f}% "
                      f"${token['total_volume']:>10,.0f} "
                      f"{sentiment_indicator} {token['sentiment_score']:>6.1f}")
            
            print("\n🟢 = Bullish (>60% buys) | 🔴 = Bearish (>60% sells) | 🟡 = Neutral")
            
            # Summary stats
            total_transactions = sum(t['total_transactions'] for t in sentiment_data)
            total_volume = sum(t['total_volume'] for t in sentiment_data)
            
            print(f"\nSUMMARY:")
            print(f"Total Tokens: {len(sentiment_data)}")
            print(f"Total Whale Transactions: {total_transactions:,}")
            print(f"Total Volume: ${total_volume:,.0f}")
            print("="*80)
            
        except Exception as e:
            production_logger.error("Error printing sentiment summary",
                                  extra={'extra_fields': {'error': str(e)}})
    
    def run_aggregation_loop(self, interval_seconds: int = 60):
        """
        Run continuous aggregation loop.
        
        Args:
            interval_seconds: How often to update (default: 60 seconds)
        """
        production_logger.info("Starting whale sentiment aggregation loop",
                             extra={'extra_fields': {'interval_seconds': interval_seconds}})
        
        while self.is_running:
            try:
                start_time = time.time()
                
                # Calculate sentiment
                sentiment_data = self.get_token_sentiment(hours=2)
                
                if sentiment_data:
                    # Store in database (optional - for UI persistence)
                    # self.store_aggregated_sentiment(sentiment_data)
                    
                    # Print console summary
                    self.print_sentiment_summary()
                    
                    production_logger.info("Aggregation cycle completed",
                                         extra={'extra_fields': {
                                             'tokens_processed': len(sentiment_data),
                                             'processing_time_ms': round((time.time() - start_time) * 1000, 2)
                                         }})
                else:
                    print("No whale sentiment data to display.")
                
                # Wait for next interval
                time.sleep(interval_seconds)
                
            except Exception as e:
                production_logger.error("Error in aggregation loop",
                                      extra={'extra_fields': {
                                          'error': str(e),
                                          'stack_trace': traceback.format_exc()
                                      }})
                time.sleep(interval_seconds)  # Continue despite errors
    
    def start(self, interval_seconds: int = 60):
        """Start the aggregation service in a background thread."""
        if self.is_running:
            production_logger.warning("Whale sentiment aggregator already running")
            return
        
        self.is_running = True
        self.aggregation_thread = threading.Thread(
            target=self.run_aggregation_loop,
            args=(interval_seconds,),
            daemon=True,
            name="WhaleSentimentAggregator"
        )
        self.aggregation_thread.start()
        
        production_logger.info("Whale sentiment aggregator started",
                             extra={'extra_fields': {'interval_seconds': interval_seconds}})
    
    def stop(self):
        """Stop the aggregation service."""
        self.is_running = False
        if self.aggregation_thread and self.aggregation_thread.is_alive():
            self.aggregation_thread.join(timeout=5)
        
        production_logger.info("Whale sentiment aggregator stopped")

# Global instance
whale_sentiment_aggregator = WhaleSentimentAggregator()

def main():
    """Main function for testing the aggregator."""
    import signal
    
    def signal_handler(signum, frame):
        print("\nShutting down whale sentiment aggregator...")
        whale_sentiment_aggregator.stop()
        exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("🐋 Starting Whale Sentiment Aggregator...")
    print("Press Ctrl+C to stop\n")
    
    # Start aggregation service
    whale_sentiment_aggregator.start(interval_seconds=60)  # Update every minute
    
    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        signal_handler(signal.SIGINT, None)

if __name__ == "__main__":
    main() 
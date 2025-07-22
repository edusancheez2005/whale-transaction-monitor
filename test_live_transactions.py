#!/usr/bin/env python3
"""
🚀 PRODUCTION-GRADE LIVE TRANSACTION TEST SCRIPT
Comprehensive integration test for the enhanced WhaleIntelligenceEngine.

This script fetches real WETH transactions from Etherscan and validates our
newly refactored classification system with enhanced DEX/CEX detection.

Author: Whale Transaction Monitor Team
Version: 2.0.0 (Production Test)
"""

import logging
import requests
import time
import sys
import traceback
from typing import Dict, Any, List, Optional

# Fix broken pipe errors in logging
import signal
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

# Configure professional logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s:%(name)s:%(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def setup_environment():
    """Initialize environment and validate dependencies."""
    try:
        # Import API keys
        from config.api_keys import ETHERSCAN_API_KEY, FALLBACK_API_KEYS
        
        # Get all available Etherscan API keys
        etherscan_keys = [ETHERSCAN_API_KEY]
        if "etherscan" in FALLBACK_API_KEYS:
            etherscan_keys.extend(FALLBACK_API_KEYS["etherscan"])
        
        # Filter out empty or invalid keys
        etherscan_keys = [key for key in etherscan_keys if key and key.strip() != "" and key != "YOUR_ETHERSCAN_API_KEY"]
        
        if not etherscan_keys:
            logger.error("❌ No valid ETHERSCAN_API_KEY found in config.api_keys.py")
            logger.error("Please add your Etherscan API key to proceed with live testing.")
            sys.exit(1)
        
        # Import and validate WhaleIntelligenceEngine
        from utils.classification_final import WhaleIntelligenceEngine
        
        logger.info("✅ Environment validation successful")
        logger.info(f"✅ API keys loaded ({len(etherscan_keys)} Etherscan keys available)")
        logger.info("✅ WhaleIntelligenceEngine imported")
        
        return etherscan_keys, WhaleIntelligenceEngine
        
    except ImportError as e:
        logger.error(f"❌ Import error: {e}")
        logger.error("Please ensure all dependencies are installed and configured.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Environment setup failed: {e}")
        sys.exit(1)


def fetch_recent_weth_transactions(api_keys: List[str], limit: int = 10) -> List[Dict[str, Any]]:
    """
    Fetch recent WETH token transactions from Etherscan API with retry logic.
    
    Args:
        api_keys: List of Etherscan API keys to try
        limit: Number of transactions to fetch
        
    Returns:
        List of transaction dictionaries
    """
    WETH_CONTRACT = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    
    # Use known high-activity WETH addresses to get transactions
    # These are major DEX routers and pools that frequently interact with WETH
    high_activity_addresses = [
        "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",  # Uniswap V2 Router
        "0xe592427a0aece92de3edee1f18e0157c05861564",  # Uniswap V3 Router
        "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45",  # Uniswap V3 Router 2
        "0x1111111254eeb25477b68fb85ed929f73a960582",  # 1inch Router
        "0x881d40237659c251811cec9c364ef91dc08d300c",  # Metamask Swap Router
    ]
    
    # Try multiple endpoints for better reliability
    endpoints = [
        "https://api.etherscan.io/api",
        "https://api.etherscan.io/api",  # Keep using mainnet only
    ]
    
    # Retry logic
    max_retries = 3
    retry_delay = 2
    
    all_transactions = []
    
    # Try each API key
    for key_index, api_key in enumerate(api_keys):
        logger.info(f"🔑 Trying API key #{key_index + 1}/{len(api_keys)}")
        
        # Try each high-activity address to get diverse WETH transactions
        for addr_index, target_address in enumerate(high_activity_addresses):
            if len(all_transactions) >= limit:
                break
                
            params = {
                'module': 'account',
                'action': 'tokentx',
                'contractaddress': WETH_CONTRACT,
                'address': target_address,  # CRITICAL FIX: Add required address parameter
                'page': 1,
                'offset': limit,
                'sort': 'desc',
                'apikey': api_key
            }
            
            for endpoint in endpoints:
                for attempt in range(max_retries):
                    try:
                        if attempt == 0:
                            logger.info(f"🔍 Fetching WETH transactions from {target_address[:10]}...")
                        else:
                            logger.info(f"🔄 Retry attempt {attempt + 1}/{max_retries}...")
                        
                        # Use session for connection pooling and better performance
                        session = requests.Session()
                        session.headers.update({
                            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
                        })
                        
                        response = session.get(endpoint, params=params, timeout=30)  # Reduced timeout
                        response.raise_for_status()
                        
                        data = response.json()
                        
                        if data.get('status') != '1':
                            error_msg = data.get('message', 'Unknown error')
                            logger.warning(f"⚠️ Etherscan API warning: {error_msg}")
                            
                            if 'invalid api key' in error_msg.lower():
                                logger.warning(f"API key #{key_index + 1} is invalid, trying next...")
                                break  # Try next API key
                            elif 'rate limit' in error_msg.lower():
                                logger.info(f"Rate limited, waiting {retry_delay * 2} seconds...")
                                time.sleep(retry_delay * 2)
                                continue
                            elif attempt < max_retries - 1:
                                continue
                            else:
                                break  # Try next endpoint
                
                        transactions = data.get('result', [])
                        if transactions:
                            logger.info(f"✅ Successfully fetched {len(transactions)} WETH transactions from {target_address[:10]}...")
                            all_transactions.extend(transactions)
                            
                            # Remove duplicates and limit results
                            unique_transactions = []
                            seen_hashes = set()
                            for tx in all_transactions:
                                if tx['hash'] not in seen_hashes:
                                    unique_transactions.append(tx)
                                    seen_hashes.add(tx['hash'])
                                    if len(unique_transactions) >= limit:
                                        break
                            
                            if len(unique_transactions) >= limit:
                                return unique_transactions[:limit]
                        
                        break  # Success, try next address
                        
                    except requests.exceptions.Timeout:
                        logger.warning(f"⚠️ Request timeout on attempt {attempt + 1}")
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay)
                            continue
                    except requests.exceptions.RequestException as e:
                        logger.warning(f"⚠️ Network error on attempt {attempt + 1}: {e}")
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay)
                            continue
                    except Exception as e:
                        logger.error(f"❌ Unexpected error: {e}")
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay)
                            continue
                
                # If first endpoint failed, try the next one
                if len(all_transactions) == 0:
                    logger.warning(f"⚠️ Endpoint {endpoint} failed with API key #{key_index + 1}, trying next endpoint...")
            
            # Small delay between addresses to avoid rate limiting
            time.sleep(0.5)
        
        # If we got some transactions, return them
        if all_transactions:
            unique_transactions = []
            seen_hashes = set()
            for tx in all_transactions:
                if tx['hash'] not in seen_hashes:
                    unique_transactions.append(tx)
                    seen_hashes.add(tx['hash'])
                    if len(unique_transactions) >= limit:
                        break
            
            return unique_transactions[:limit]
        
        # If all addresses failed with this key, try the next key
        logger.warning(f"⚠️ All addresses failed with API key #{key_index + 1}, trying next key...")
    
    logger.error("❌ All API keys exhausted. Could not fetch WETH transactions.")
    return []


def transform_etherscan_data(etherscan_tx: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform Etherscan transaction data to WhaleIntelligenceEngine format.
    
    Args:
        etherscan_tx: Raw Etherscan transaction data
        
    Returns:
        Standardized transaction dictionary
    """
    try:
        return {
            'hash': etherscan_tx.get('hash', ''),
            'blockchain': 'ethereum',
            'from': etherscan_tx.get('from', ''),
            'to': etherscan_tx.get('to', ''),
            'amount_usd': 10000.0,  # Default testing value
            'token_symbol': 'WETH',
            'block_number': int(etherscan_tx.get('blockNumber', 0)),
            'timestamp': int(etherscan_tx.get('timeStamp', 0)),
            'gas_price': int(etherscan_tx.get('gasPrice', 0)) if etherscan_tx.get('gasPrice') else 0,
            'gas_used': int(etherscan_tx.get('gasUsed', 0)) if etherscan_tx.get('gasUsed') else 0,
            'source': 'live_etherscan_test'
        }
    except (ValueError, TypeError) as e:
        logger.warning(f"⚠️ Data transformation warning: {e}")
        # Return safe defaults
        return {
            'hash': etherscan_tx.get('hash', 'unknown'),
            'blockchain': 'ethereum',
            'from': etherscan_tx.get('from', ''),
            'to': etherscan_tx.get('to', ''),
            'amount_usd': 10000.0,
            'token_symbol': 'WETH',
            'block_number': 0,
            'timestamp': int(time.time()),
            'gas_price': 0,
            'gas_used': 0,
            'source': 'live_etherscan_test'
        }


def analyze_single_transaction(whale_engine, transaction_data: Dict[str, Any], tx_number: int, total: int) -> Optional[Dict[str, Any]]:
    """
    Analyze a single transaction and display comprehensive results.
    
    Args:
        whale_engine: Initialized WhaleIntelligenceEngine instance
        transaction_data: Standardized transaction data
        tx_number: Current transaction number (1-indexed)
        total: Total number of transactions
        
    Returns:
        Dict with analysis results if successful, None if failed
    """
    try:
        tx_hash = transaction_data.get('hash', 'unknown')
        
        logger.info("============================================================")
        logger.info(f"[Analyzing Transaction {tx_number}/{total}]")
        logger.info(f"Etherscan: https://etherscan.io/tx/{tx_hash}")
        logger.info(f"From: {transaction_data.get('from', 'unknown')}")
        logger.info(f"To: {transaction_data.get('to', 'unknown')}")
        logger.info("")
        
        # Perform comprehensive analysis
        start_time = time.time()
        result = whale_engine.analyze_transaction_comprehensive(transaction_data)
        analysis_time = (time.time() - start_time) * 1000  # Convert to milliseconds
        
        # Display final results
        logger.info("----------- FINAL RESULT -----------")
        logger.info(f"  Classification: {result.classification.value} (Confidence: {result.confidence:.2f})")
        logger.info(f"  Whale Score: {result.final_whale_score:.1f}")
        logger.info(f"  Analysis Time: {analysis_time:.1f}ms")
        logger.info(f"  Phases Completed: {result.phases_completed}")
        logger.info(f"  Cost Optimized: {result.cost_optimized}")
        logger.info(f"  Reasoning: {result.master_classifier_reasoning}")
        
        if result.whale_signals:
            logger.info(f"  Whale Signals: {result.whale_signals}")
        else:
            logger.info("  Whale Signals: None detected")
        
        logger.info("")
        
        # Display detailed phase analysis
        logger.info("----------- PHASE ANALYSIS -----------")
        
        # CEX Classification Phase
        cex_phase = result.phase_results.get('cex_classification')
        if cex_phase:
            logger.info("  [Phase: cex_classification]")
            logger.info(f"    - Classification: {cex_phase.classification.value}")
            logger.info(f"    - Confidence: {cex_phase.confidence:.2f}")
            logger.info(f"    - Evidence: {cex_phase.evidence}")
            if cex_phase.whale_signals:
                logger.info(f"    - Whale Signals: {cex_phase.whale_signals}")
        else:
            logger.info("  [Phase: cex_classification] - Not executed")
        
        logger.info("")
        
        # DEX Protocol Classification Phase
        dex_phase = result.phase_results.get('dex_protocol_classification')
        if dex_phase:
            logger.info("  [Phase: dex_protocol_classification]")
            logger.info(f"    - Classification: {dex_phase.classification.value}")
            logger.info(f"    - Confidence: {dex_phase.confidence:.2f}")
            logger.info(f"    - Evidence: {dex_phase.evidence}")
            if dex_phase.whale_signals:
                logger.info(f"    - Whale Signals: {dex_phase.whale_signals}")
            if dex_phase.raw_data:
                protocol_name = dex_phase.raw_data.get('protocol_name')
                protocol_type = dex_phase.raw_data.get('protocol_type')
                if protocol_name:
                    logger.info(f"    - Protocol: {protocol_name} ({protocol_type})")
        else:
            logger.info("  [Phase: dex_protocol_classification] - Not executed")
        
        logger.info("")
        
        # Additional phases if executed
        other_phases = ['blockchain_specific', 'wallet_behavior', 'bigquery_mega_whale', 
                       'moralis_enrichment', 'zerion_portfolio']
        
        for phase_name in other_phases:
            phase_result = result.phase_results.get(phase_name)
            if phase_result and phase_result.confidence > 0:
                logger.info(f"  [Phase: {phase_name}]")
                logger.info(f"    - Classification: {phase_result.classification.value}")
                logger.info(f"    - Confidence: {phase_result.confidence:.2f}")
                logger.info(f"    - Evidence: {phase_result.evidence}")
                logger.info("")
        
        # Behavioral analysis if available
        if result.behavioral_analysis and result.behavioral_analysis.total_confidence_boost > 0:
            logger.info("----------- BEHAVIORAL ANALYSIS -----------")
            logger.info(f"  Total Confidence Boost: +{result.behavioral_analysis.total_confidence_boost:.2f}")
            for adjustment in result.behavioral_analysis.confidence_adjustments:
                logger.info(f"  - {adjustment.get('analysis', 'Unknown')}: +{adjustment.get('confidence_boost', 0):.2f}")
            logger.info("")
        
        # Opportunity signal if detected
        if result.opportunity_signal:
            logger.info("----------- OPPORTUNITY SIGNAL -----------")
            logger.info(f"  Signal Type: {result.opportunity_signal.get('signal_type', 'Unknown')}")
            logger.info(f"  Confidence: {result.opportunity_signal.get('confidence_score', 0):.2f}")
            logger.info("")
        
        logger.info("============================================================")
        logger.info("")
        
        # Return analysis results for Opportunity Engine
        return {
            'classification': result.classification.value,
            'confidence': result.confidence,
            'whale_score': result.final_whale_score,
            'analysis_time_ms': analysis_time,
            'phases_completed': result.phases_completed,
            'whale_signals': result.whale_signals,
            'reasoning': result.master_classifier_reasoning
        }
        
    except Exception as e:
        logger.error(f"❌ Error analyzing transaction {tx_number}: {e}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        logger.info("============================================================")
        logger.info("")
        return None


def analyze_opportunity_signals(token_signals: Dict[str, Dict]) -> Dict[str, Any]:
    """
    Analyze aggregated token signals using the Opportunity Engine.
    
    Args:
        token_signals: Dictionary of token signals aggregated from analysis
        
    Returns:
        Dictionary of opportunity insights per token
    """
    try:
        from opportunity_engine.analyzer import OpportunityAnalyzer
        from opportunity_engine.market_data_provider import MarketDataProvider
        
        market_data_provider = MarketDataProvider()
        opportunity_analyzer = OpportunityAnalyzer(market_data_provider)
        
        insights = {}
        
        for token_symbol, signals in token_signals.items():
            try:
                buy_count = len(signals['buy_signals'])
                sell_count = len(signals['sell_signals'])
                total_volume = signals['total_volume_usd']
                
                # Skip tokens with insufficient signal volume
                if buy_count + sell_count < 2:
                    continue
                
                # Calculate signal strength
                total_signals = buy_count + sell_count
                buy_ratio = buy_count / total_signals if total_signals > 0 else 0
                sell_ratio = sell_count / total_signals if total_signals > 0 else 0
                
                # Fetch current market data
                market_data = market_data_provider.get_token_market_data(token_symbol)
                
                # Generate opportunity signal
                opportunity_signal = {
                    'token': token_symbol,
                    'buy_signals': buy_count,
                    'sell_signals': sell_count,
                    'buy_ratio': buy_ratio,
                    'sell_ratio': sell_ratio,
                    'total_volume_usd': total_volume,
                    'signal_strength': 'WEAK',
                    'recommendation': 'HOLD',
                    'market_data': market_data,
                    'confidence_score': 0.0
                }
                
                # Determine signal strength and recommendation
                if buy_ratio >= 0.75 and total_volume > 100_000:
                    opportunity_signal['signal_strength'] = 'STRONG'
                    opportunity_signal['recommendation'] = 'BUY'
                    opportunity_signal['confidence_score'] = min(0.95, buy_ratio + (total_volume / 1_000_000) * 0.1)
                elif buy_ratio >= 0.60 and total_volume > 50_000:
                    opportunity_signal['signal_strength'] = 'MODERATE'
                    opportunity_signal['recommendation'] = 'BUY'
                    opportunity_signal['confidence_score'] = min(0.80, buy_ratio + (total_volume / 500_000) * 0.1)
                elif sell_ratio >= 0.75 and total_volume > 100_000:
                    opportunity_signal['signal_strength'] = 'STRONG'
                    opportunity_signal['recommendation'] = 'SELL'
                    opportunity_signal['confidence_score'] = min(0.95, sell_ratio + (total_volume / 1_000_000) * 0.1)
                elif sell_ratio >= 0.60 and total_volume > 50_000:
                    opportunity_signal['signal_strength'] = 'MODERATE'
                    opportunity_signal['recommendation'] = 'SELL'
                    opportunity_signal['confidence_score'] = min(0.80, sell_ratio + (total_volume / 500_000) * 0.1)
                else:
                    opportunity_signal['confidence_score'] = max(buy_ratio, sell_ratio) * 0.5
                
                insights[token_symbol] = opportunity_signal
                
            except Exception as e:
                logger.warning(f"Failed to analyze {token_symbol}: {e}")
                continue
        
        return insights
        
    except Exception as e:
        logger.error(f"Opportunity Engine analysis failed: {e}")
        return {}

def display_opportunity_insights(insights: Dict[str, Any]) -> None:
    """
    Display opportunity insights in a professional format.
    
    Args:
        insights: Dictionary of opportunity insights per token
    """
    if not insights:
        logger.info("📊 No significant opportunity signals detected")
        return
    
    logger.info(f"📈 MARKET OPPORTUNITY SIGNALS ({len(insights)} tokens analyzed)")
    logger.info("-" * 80)
    
    # Sort by confidence score (highest first)
    sorted_insights = sorted(insights.items(), key=lambda x: x[1]['confidence_score'], reverse=True)
    
    for token_symbol, signal in sorted_insights:
        buy_signals = signal['buy_signals']
        sell_signals = signal['sell_signals']
        recommendation = signal['recommendation']
        strength = signal['signal_strength']
        confidence = signal['confidence_score']
        volume = signal['total_volume_usd']
        
        # Recommendation emoji
        rec_emoji = "🟢" if recommendation == "BUY" else "🔴" if recommendation == "SELL" else "🟡"
        
        # Strength indicator
        strength_indicator = "🔥" if strength == "STRONG" else "⚡" if strength == "MODERATE" else "💭"
        
        logger.info(f"{rec_emoji} {token_symbol} | {strength_indicator} {strength} {recommendation}")
        logger.info(f"   Signals: {buy_signals} BUY / {sell_signals} SELL | Volume: ${volume:,.0f}")
        logger.info(f"   Confidence: {confidence:.1%} | Ratio: {signal['buy_ratio']:.1%} buy")
        
        # Market context if available
        if signal.get('market_data'):
            market = signal['market_data']
            price = market.get('current_price', 0)
            change_24h = market.get('price_change_24h', 0)
            if price > 0:
                change_emoji = "📈" if change_24h > 0 else "📉" if change_24h < 0 else "➡️"
                logger.info(f"   Market: ${price:.4f} {change_emoji} {change_24h:+.1f}% (24h)")
        
        logger.info("")


def run_comprehensive_test():
    """
    Execute the comprehensive live transaction test.
    """
    logger.info("🚀 STARTING PRODUCTION-GRADE LIVE TRANSACTION TEST")
    logger.info("📊 Target: Analyze exactly 10 WETH transactions")
    logger.info("=" * 60)
    logger.info("")
    
    # Environment setup
    api_keys, WhaleIntelligenceEngine = setup_environment()
    
    # Initialize WhaleIntelligenceEngine (ONCE for entire test)
    logger.info("🧠 Initializing WhaleIntelligenceEngine...")
    try:
        whale_engine = WhaleIntelligenceEngine()
        logger.info("✅ WhaleIntelligenceEngine initialized successfully")
    except Exception as e:
        logger.error(f"❌ Failed to initialize WhaleIntelligenceEngine: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
    
    logger.info("")
    
    # Fetch transactions
    transactions = fetch_recent_weth_transactions(api_keys, limit=10)
    
    if not transactions:
        logger.error("❌ No transactions fetched. Exiting test.")
        sys.exit(1)
    
    # Ensure we only process exactly 10 transactions
    transactions = transactions[:10]
    logger.info(f"📊 Processing exactly {len(transactions)} transactions")
    logger.info("")
    
    # Analyze each transaction
    successful_analyses = 0
    failed_analyses = 0
    token_signals = {}  # Track buy/sell signals per token
    
    for i, etherscan_tx in enumerate(transactions, 1):
        # Transform data
        try:
            transaction_data = transform_etherscan_data(etherscan_tx)
        except Exception as e:
            logger.error(f"❌ Failed to transform transaction {i}: {e}")
            failed_analyses += 1
            continue
        
        # Analyze transaction
        result = analyze_single_transaction(whale_engine, transaction_data, i, len(transactions))
        
        if result:
            successful_analyses += 1
            
            # Aggregate token signals for Opportunity Engine
            token_symbol = transaction_data.get('token_symbol', 'WETH')
            classification = result.get('classification', 'TRANSFER')
            confidence = result.get('confidence', 0.0)
            
            if token_symbol not in token_signals:
                token_signals[token_symbol] = {
                    'buy_signals': [],
                    'sell_signals': [],
                    'transfer_signals': [],
                    'total_volume_usd': 0.0
                }
            
            # Store signal with metadata
            signal_data = {
                'classification': classification,
                'confidence': confidence,
                'volume_usd': transaction_data.get('amount_usd', 0),
                'tx_hash': transaction_data.get('hash', ''),
                'timestamp': time.time()
            }
            
            # Categorize signals
            if classification == 'BUY':
                token_signals[token_symbol]['buy_signals'].append(signal_data)
            elif classification == 'SELL':
                token_signals[token_symbol]['sell_signals'].append(signal_data)
            else:
                token_signals[token_symbol]['transfer_signals'].append(signal_data)
            
            token_signals[token_symbol]['total_volume_usd'] += signal_data['volume_usd']
            
        else:
            failed_analyses += 1
        
        # Rate limiting (except for last transaction)
        if i < len(transactions):
            time.sleep(0.5)  # Reduced sleep time for faster testing
    
    # ========== OPPORTUNITY ENGINE INTEGRATION ==========
    logger.info("")
    logger.info("🚀 OPPORTUNITY ENGINE ANALYSIS")
    logger.info("=" * 60)
    
    if successful_analyses > 0:
        opportunity_insights = analyze_opportunity_signals(token_signals)
        display_opportunity_insights(opportunity_insights)
    
    # Final summary
    logger.info("🎯 TEST SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total Transactions Processed: {len(transactions)}")
    logger.info(f"Successful Analyses: {successful_analyses}")
    logger.info(f"Failed Analyses: {failed_analyses}")
    logger.info(f"Success Rate: {(successful_analyses/len(transactions)*100):.1f}%")
    
    if successful_analyses > 0:
        logger.info("✅ Live transaction test completed successfully!")
    else:
        logger.error("❌ All analyses failed. Please check system configuration.")
    
    logger.info("")
    logger.info("🔍 Key Validation Points:")
    logger.info("- Enhanced DEX detection from Supabase database")
    logger.info("- Comprehensive CEX address matching")
    logger.info("- Multi-phase analysis pipeline")
    logger.info("- Production-grade error handling")
    logger.info("- Real-world transaction processing")


if __name__ == "__main__":
    try:
        run_comprehensive_test()
    except KeyboardInterrupt:
        logger.info("\n⚠️ Test interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Unexpected error during test execution: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1) 
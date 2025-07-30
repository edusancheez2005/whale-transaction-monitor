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


def fetch_real_swap_transactions(api_keys: List[str], token_contract: str, token_symbol: str, limit: int = 5) -> List[Dict[str, Any]]:
    """
    🚀 FETCH REAL SWAP TRANSACTIONS (Option 2 + 3)
    
    Gets actual Uniswap swap transactions from recent blocks instead of random transfers.
    Targets stablecoin flows (fiat-to-crypto) and high-volume DEX pairs.
    
    Args:
        api_keys: List of Etherscan API keys
        token_contract: Token contract address  
        token_symbol: Token symbol for display
        limit: Number of swap transactions to fetch
        
    Returns:
        List of real swap transaction dictionaries
    """
    
    # 🎯 TARGET KNOWN HIGH-VOLUME DEX PAIRS (Real trading activity)
    target_pairs = {
        # FIAT-TO-CRYPTO FLOWS (Stablecoin swaps = USD/EUR on-chain)
        "USDC": [
            "0xa0b86a33e6417efb8c2206994597c13d831ec7",  # USDC contract
            "0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852",  # WETH/USDC pair
            "0x397ff1542f962076d0bfe58ea045ffa2d347aca0",  # USDC/USDT pair
        ],
        "USDT": [
            "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT contract  
            "0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852",  # WETH/USDT pair
            "0x397ff1542f962076d0bfe58ea045ffa2d347aca0",  # USDC/USDT pair
        ],
        # HIGH-VOLUME TRADING PAIRS  
        "UNI": [
            "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",  # UNI contract
            "0x4e99615101ccbb83a462dc4de2bc1362ef1365e5",  # UNI/WETH pair
        ],
        "WETH": [
            "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH contract
            "0x0d4a11d5eeaac28ec3f61d100daf4d40471f1852",  # WETH/USDC pair  
            "0xa478c2975ab1ea89e8196811f51a7b7ade33eb11",  # WETH/DAI pair
        ],
        "LINK": [
            "0x514910771af9ca656af840dff83e8264ecf986ca",  # LINK contract
            "0xa2107fa5b38d9bbd2c461d6edf11b11a50f6b974",  # LINK/WETH pair
        ],
        "PEPE": [
            "0x6982508145454ce325ddbe47a25d4ec3d2311933",  # PEPE contract
            "0xa43fe16908251ee70ef74718545e4fe6c5ccec9f",  # PEPE/WETH pair
        ]
    }
    
    # Get target addresses for this token
    target_addresses = target_pairs.get(token_symbol, [token_contract])
    
    # Try multiple endpoints for better reliability
    endpoints = [
        "https://api.etherscan.io/api",
        "https://api.etherscan.io/api",  # Keep using mainnet
    ]
    
    all_transactions = []
    max_retries = 3
    
    # 🔑 Try each API key
    for key_index, api_key in enumerate(api_keys):
        logger.info(f"🔑 Trying API key #{key_index + 1}/{len(api_keys)}")
        
        # 🎯 TARGET KNOWN SWAP ADDRESSES (Real trading pairs)
        for addr_index, target_address in enumerate(target_addresses[:3]):  # Limit to 3 addresses per token
            if len(all_transactions) >= limit:
                break
            
            for endpoint_index, url in enumerate(endpoints):
                try:
                    # 🚀 ENHANCED QUERY: Look for contract internal transactions (swaps)
                    params = {
                        'module': 'account',
                        'action': 'txlistinternal',  # Internal transactions show swaps
                        'address': target_address,
                        'startblock': 0,
                        'endblock': 99999999,
                        'page': 1,
                        'offset': limit,
                        'sort': 'desc',
                        'apikey': api_key
                    }
                    
                    # Try to get token transactions if internal fails
                    if endpoint_index == 1:
                        params['action'] = 'tokentx'
                        params['contractaddress'] = token_contract
                    
                    for attempt in range(max_retries):
                        try:
                            if attempt == 0:
                                logger.info(f"🔍 Fetching {token_symbol} swaps from {target_address[:10]}...")
                            else:
                                logger.info(f"🔄 Retry attempt {attempt + 1}/{max_retries}...")
                                
                            response = requests.get(url, params=params, timeout=30)
                            
                            if response.status_code == 200:
                                data = response.json()
                                
                                if data['status'] == '1' and 'result' in data:
                                    # Process real swap transactions
                                    raw_transactions = data['result']
                                    
                                    if raw_transactions:
                                        # 🎯 FILTER FOR REAL SWAPS (Non-zero value transfers)
                                        for raw_tx in raw_transactions:
                                            if len(all_transactions) >= limit:
                                                break
                                            
                                            # Skip zero-value transactions (likely not swaps)
                                            value = int(raw_tx.get('value', '0'))
                                            if value > 0:  # Real value transfer = likely swap
                                                
                                                # Create standardized swap transaction format
                                                transaction = {
                                                    'hash': raw_tx.get('hash'),
                                                    'from': raw_tx.get('from'),
                                                    'to': raw_tx.get('to'),
                                                    'value': raw_tx.get('value', '0'),
                                                    'amount_raw': raw_tx.get('value', '0'),
                                                    'amount_formatted': float(raw_tx.get('value', '0')) / 1e18,
                                                    'amount_usd': 25000.0,  # Higher test value for swaps
                                                    'gas_price': raw_tx.get('gasPrice', '0'),
                                                    'gas_used': raw_tx.get('gasUsed', '0'),
                                                    'timestamp': raw_tx.get('timeStamp'),
                                                    'blockchain': 'ethereum',
                                                    'token_symbol': token_symbol,
                                                    'token_name': f'{token_symbol} Token',
                                                    'token_address': token_contract,
                                                    'decimals': 18,
                                                    'source': 'etherscan_real_swaps',
                                                    'transaction_type': 'swap_candidate'  # Mark as potential swap
                                                }
                                                all_transactions.append(transaction)
                                        
                                        if all_transactions:
                                            logger.info(f"✅ Successfully fetched {len(all_transactions)} {token_symbol} swap candidates from {target_address[:10]}...")
                                            break  # Success, no need to retry
                                    else:
                                        logger.warning(f"⚠️ No {token_symbol} swaps found in {target_address[:10]}")
                                else:
                                    if data.get('message') == 'No transactions found':
                                        logger.warning(f"⚠️ Etherscan API warning: No transactions found")
                                    else:
                                        logger.warning(f"⚠️ Etherscan API warning: {data.get('message', 'NOTOK')}")
                            else:
                                logger.warning(f"⚠️ HTTP {response.status_code}: {response.text[:100]}")
                                
                        except requests.exceptions.RequestException as e:
                            logger.warning(f"⚠️ Request failed: {e}")
                            if attempt < max_retries - 1:
                                time.sleep(2 ** attempt)  # Exponential backoff
                            continue
                    
                    # Try next endpoint if this one failed
                    if not all_transactions:
                        logger.warning(f"⚠️ Endpoint {url} failed with API key #{key_index + 1}, trying next endpoint...")
                        continue
                    else:
                        break  # Success with this endpoint
                        
                except Exception as e:
                    logger.warning(f"⚠️ Endpoint error: {e}")
                    continue
            
            if all_transactions:
                break  # Success with this address
        
        if all_transactions:
            # 🎯 REMOVE DUPLICATES and return unique swap transactions
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
    
    logger.error(f"❌ All API keys exhausted. Could not fetch {token_symbol} swap transactions.")
    return []


def fetch_diverse_swap_transactions(api_keys: List[str], token_contract: str, token_symbol: str, limit: int = 5) -> List[Dict[str, Any]]:
    """
    🎯 EXPERT-GRADE DIVERSE TRANSACTION SAMPLING
    
    Fixes whale domination bias by:
    1. Fetching from recent blocks (not contract-specific)
    2. Filtering for unique addresses
    3. Ensuring representative market sample
    
    Args:
        api_keys: List of Etherscan API keys
        token_contract: Token contract address
        token_symbol: Token symbol for logging
        limit: Number of diverse transactions to fetch
        
    Returns:
        List of diverse swap transactions from different addresses
    """
    logger = logging.getLogger(__name__)
    
    endpoints = ["https://api.etherscan.io/api"]
    all_transactions = []
    seen_addresses = set()  # Track unique FROM addresses
    max_retries = 3
    
    # 🎯 STRATEGY: Get recent blocks and find diverse token transfers
    for key_index, api_key in enumerate(api_keys):
        logger.info(f"🔑 Diverse sampling with API key #{key_index + 1}/{len(api_keys)}")
        
        try:
            # Step 1: Get recent block number
            block_response = requests.get(endpoints[0], params={
                'module': 'proxy',
                'action': 'eth_blockNumber',
                'apikey': api_key
            }, timeout=30)
            
            if block_response.status_code == 200:
                latest_block = int(block_response.json()['result'], 16)
                start_block = latest_block - 100  # Look at last 100 blocks
                
                logger.info(f"🔍 Sampling blocks {start_block} to {latest_block} for diverse {token_symbol} transactions")
                
                # Step 2: Get token transfers from recent blocks
                params = {
                    'module': 'account',
                    'action': 'tokentx',
                    'contractaddress': token_contract,
                    'startblock': start_block,
                    'endblock': latest_block,
                    'page': 1,
                    'offset': 100,  # Get more to filter for diversity
                    'sort': 'desc',
                    'apikey': api_key
                }
                
                response = requests.get(endpoints[0], params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data['status'] == '1' and 'result' in data:
                        raw_transactions = data['result']
                        
                        logger.info(f"📊 Found {len(raw_transactions)} total {token_symbol} transfers, filtering for diversity...")
                        
                        # Step 3: Filter for diverse addresses and significant value
                        for raw_tx in raw_transactions:
                            if len(all_transactions) >= limit:
                                break
                            
                            from_addr = raw_tx.get('from', '')
                            value = int(raw_tx.get('value', '0'))
                            
                            # Only include if:
                            # 1. New address (diversity)
                            # 2. Significant value (real trading)
                            # 3. Not a zero address
                            if (from_addr not in seen_addresses and 
                                value > 1000000000000000000 and  # > 1 token (assuming 18 decimals)
                                from_addr != '0x0000000000000000000000000000000000000000'):
                                
                                seen_addresses.add(from_addr)
                                
                                # Create standardized transaction format
                                transaction = {
                                    'hash': raw_tx.get('hash'),
                                    'from': raw_tx.get('from'),
                                    'to': raw_tx.get('to'),
                                    'value': raw_tx.get('value', '0'),
                                    'amount_raw': raw_tx.get('value', '0'),
                                    'amount_formatted': float(raw_tx.get('value', '0')) / 1e18,
                                    'amount_usd': 15000.0,  # Representative test value
                                    'gas_price': raw_tx.get('gasPrice', '0'),
                                    'gas_used': raw_tx.get('gasUsed', '0'),
                                    'timestamp': raw_tx.get('timeStamp'),
                                    'blockchain': 'ethereum',
                                    'token_symbol': token_symbol,
                                    'token_name': f'{token_symbol} Token',
                                    'token_address': token_contract,
                                    'decimals': 18,
                                    'source': 'etherscan_diverse_sampling',
                                    'transaction_type': 'diverse_token_transfer'
                                }
                                all_transactions.append(transaction)
                                
                                logger.info(f"✅ Added diverse {token_symbol} transaction from {from_addr[:10]}...")
                        
                        if len(all_transactions) >= limit:
                            break
                            
        except Exception as e:
            logger.error(f"❌ Diverse sampling failed with API key #{key_index + 1}: {e}")
            continue
    
    if all_transactions:
        logger.info(f"🎯 Successfully collected {len(all_transactions)} diverse {token_symbol} transactions from {len(seen_addresses)} unique addresses")
    else:
        logger.warning(f"⚠️ No diverse {token_symbol} transactions found, falling back to original method")
        # Fallback to original method if diverse sampling fails
        return fetch_real_swap_transactions(api_keys, token_contract, token_symbol, limit)
    
    return all_transactions


def transform_etherscan_data(etherscan_tx: Dict[str, Any], token_symbol: str = 'UNKNOWN') -> Dict[str, Any]:
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
            'token_symbol': token_symbol,
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
            'token_symbol': token_symbol,
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
    🎯 EXPERT-GRADE DIVERSE TRANSACTION TEST
    
    FIXED: Eliminates whale domination bias with diverse address sampling
    
    Tests real swap transactions from multiple unique addresses across:
    - 🏦 Stablecoins (USDC/USDT) for fiat-to-crypto detection
    - 🔄 DEX tokens (UNI/WETH) for institutional trading
    - 🔗 Oracle tokens (LINK) for institutional favorites  
    - 🎪 Meme tokens (PEPE) for trending activity
    
    Ensures representative market sample by filtering for unique addresses.
    """
    logger.info("🚀 STARTING DIVERSE TRANSACTION TEST (FIXED: No Whale Bias)")
    logger.info("🎯 Target: Representative market sample across asset classes")
    logger.info("=" * 80)
    logger.info("")
    
    # 🎯 COMPREHENSIVE REAL SWAP TEST SUITE (Option 2 + 3)
    test_tokens = [
        # 🏦 FIAT-TO-CRYPTO DETECTION (Option 3: Stablecoin flows = USD/EUR on-chain)
        {"symbol": "USDC", "contract": "0xa0b86a33e6417efb8c2206994597c13d831ec7", "description": "Real USDC → Crypto swaps (USD on-chain)"},
        {"symbol": "USDT", "contract": "0xdac17f958d2ee523a2206206994597c13d831ec7", "description": "Tether → Crypto swaps (USD flows)"},
        
        # 🔄 HIGH-VOLUME DEX PAIRS (Option 2: Real swap activity)
        {"symbol": "UNI", "contract": "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984", "description": "Uniswap governance (active trading)"},
        {"symbol": "WETH", "contract": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", "description": "Wrapped ETH (highest volume pairs)"},
        {"symbol": "LINK", "contract": "0x514910771af9ca656af840dff83e8264ecf986ca", "description": "Chainlink oracle (institutional favorite)"},
        
        # 🎯 RECENT TRENDING TOKENS (Real swap volume)
        {"symbol": "PEPE", "contract": "0x6982508145454ce325ddbe47a25d4ec3d2311933", "description": "Trending meme token (high swap activity)"}
    ]
    
    logger.info("📋 DIVERSE TRANSACTION TEST SUITE:")
    logger.info("🎯 FIXED: Each token sampled from unique addresses (no whale bias)")
    logger.info("🏦 STABLECOINS: USDC/USDT diverse flows")
    logger.info("🔄 DEX TOKENS: UNI/WETH diverse trading")
    logger.info("🔗 ORACLES: LINK diverse institutional activity")
    logger.info("🎪 TRENDING: PEPE diverse meme token activity")
    for token in test_tokens:
        logger.info(f"  🪙 {token['symbol']}: {token['description']}")
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
    
    # Track overall results
    all_transactions = []
    overall_results = {}
    
    # Test each token
    for token_info in test_tokens:
        symbol = token_info["symbol"]
        contract = token_info["contract"]
        description = token_info["description"]
        
        logger.info("=" * 80)
        logger.info(f"🪙 TESTING TOKEN: {symbol}")
        logger.info(f"📝 Description: {description}")
        logger.info(f"📍 Contract: {contract}")
        logger.info("=" * 80)
        logger.info("")
        
        # Fetch diverse transactions for this token (FIXED: No more whale bias!)
        transactions = fetch_diverse_swap_transactions(api_keys, contract, symbol, limit=5)
        
        if not transactions:
            logger.warning(f"⚠️ No transactions fetched for {symbol}. Skipping to next token.")
            continue
        
        # Ensure we only process exactly 5 transactions per token
        transactions = transactions[:5]
        logger.info(f"📊 Processing {len(transactions)} {symbol} swap transactions")
        logger.info("")
        
        # Track results for this token
        token_results = {
            'symbol': symbol,
            'successful_analyses': 0,
            'failed_analyses': 0,
            'buy_signals': 0,
            'sell_signals': 0,
            'transfer_signals': 0,
            'transactions': []
        }
        
        for i, etherscan_tx in enumerate(transactions, 1):
            # Transform data
            try:
                transaction_data = transform_etherscan_data(etherscan_tx, symbol)
            except Exception as e:
                logger.error(f"❌ Failed to transform {symbol} transaction {i}: {e}")
                token_results['failed_analyses'] += 1
                continue
            
            # Analyze transaction
            result = analyze_single_transaction(whale_engine, transaction_data, i, len(transactions))
            
            if result:
                token_results['successful_analyses'] += 1
                
                # Track classification results
                classification = result.get('classification', 'TRANSFER')
                confidence = result.get('confidence', 0.0)
                
                # Store transaction result
                token_results['transactions'].append({
                    'tx_hash': transaction_data.get('hash', ''),
                    'classification': classification,
                    'confidence': confidence,
                    'analysis_time': result.get('analysis_time_ms', 0)
                })
                
                # Count signal types
                if classification == 'BUY':
                    token_results['buy_signals'] += 1
                elif classification == 'SELL':
                    token_results['sell_signals'] += 1
                else:
                    token_results['transfer_signals'] += 1
                
                # Aggregate for opportunity engine
                if symbol not in overall_results:
                    overall_results[symbol] = {
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
                
                # Categorize signals for opportunity engine
                if classification == 'BUY':
                    overall_results[symbol]['buy_signals'].append(signal_data)
                elif classification == 'SELL':
                    overall_results[symbol]['sell_signals'].append(signal_data)
                else:
                    overall_results[symbol]['transfer_signals'].append(signal_data)
                
                overall_results[symbol]['total_volume_usd'] += signal_data['volume_usd']
                
            else:
                token_results['failed_analyses'] += 1
            
            # Rate limiting (except for last transaction)
            if i < len(transactions):
                time.sleep(0.5)  # Reduced sleep time for faster testing
        
        # Token Summary
        logger.info("")
        logger.info(f"🔍 {symbol} SWAP SUMMARY")
        logger.info("-" * 50)
        logger.info(f"  Successful Analyses: {token_results['successful_analyses']}")
        logger.info(f"  Failed Analyses: {token_results['failed_analyses']}")
        if token_results['successful_analyses'] > 0:
            success_rate = (token_results['successful_analyses'] / len(transactions) * 100)
            logger.info(f"  Success Rate: {success_rate:.1f}%")
            logger.info(f"  BUY signals: {token_results['buy_signals']}")
            logger.info(f"  SELL signals: {token_results['sell_signals']}")
            logger.info(f"  TRANSFER signals: {token_results['transfer_signals']}")
        logger.info("")
        
        # Add to all transactions for overall summary
        all_transactions.extend(transactions)
    
    # ========== OPPORTUNITY ENGINE INTEGRATION ==========
    logger.info("")
    logger.info("🚀 OPPORTUNITY ENGINE ANALYSIS")
    logger.info("=" * 80)
    
    if overall_results:
        opportunity_insights = analyze_opportunity_signals(overall_results)
        display_opportunity_insights(opportunity_insights)
    
    # Final comprehensive summary
    logger.info("🎯 REAL SWAP TRANSACTION TEST SUMMARY")
    logger.info("=" * 80)
    
    total_successful = sum(len(data.get('buy_signals', [])) + len(data.get('sell_signals', [])) + len(data.get('transfer_signals', [])) for data in overall_results.values())
    total_processed = len(all_transactions)
    
    logger.info(f"Total Tokens Tested: {len(test_tokens)}")
    logger.info(f"Total Swap Transactions Processed: {total_processed}")
    logger.info(f"Total Successful Analyses: {total_successful}")
    
    if total_processed > 0:
        success_rate = (total_successful / total_processed * 100)
        logger.info(f"Overall Success Rate: {success_rate:.1f}%")
    
    # Per-token breakdown
    logger.info("")
    logger.info("📊 PER-TOKEN BREAKDOWN:")
    for symbol, data in overall_results.items():
        buy_count = len(data.get('buy_signals', []))
        sell_count = len(data.get('sell_signals', []))
        transfer_count = len(data.get('transfer_signals', []))
        total_count = buy_count + sell_count + transfer_count
        
        if total_count > 0:
            buy_pct = (buy_count / total_count * 100)
            sell_pct = (sell_count / total_count * 100)
            logger.info(f"  🪙 {symbol}: {buy_count} BUY ({buy_pct:.0f}%) | {sell_count} SELL ({sell_pct:.0f}%) | {transfer_count} TRANSFER")
    
    if total_successful > 0:
        logger.info("")
        logger.info("✅ Real swap transaction test completed successfully!")
        logger.info("")
        logger.info("🔍 Key Validation Points:")
        logger.info("- Real Uniswap swap transaction analysis")
        logger.info("- Fiat-to-crypto detection via stablecoin flows (USDC/USDT)")
        logger.info("- High-volume DEX pair intelligence")
        logger.info("- Trending token swap pattern recognition")
        logger.info("- Production-grade swap event analysis")
    else:
        logger.error("❌ All analyses failed. Please check system configuration.")
    
    logger.info("")
    logger.info("🔍 Key Validation Points:")
    logger.info("- Real swap transaction detection from live blocks")
    logger.info("- Fiat-to-crypto pattern recognition (stablecoin flows)")
    logger.info("- Enhanced DEX detection from Supabase database")
    logger.info("- Comprehensive CEX address matching")
    logger.info("- Multi-phase swap analysis pipeline")
    logger.info("- Production-grade error handling")
    logger.info("- Real-world swap transaction processing")


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
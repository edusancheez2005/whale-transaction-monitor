import requests
import logging
import json
import time
from typing import Dict, Any, Optional, List
from config.api_keys import HELIUS_API_KEY
from config.settings import STABLECOIN_SYMBOLS
from data.tokens import SOL_TOKENS_TO_MONITOR

logger = logging.getLogger(__name__)

class SolanaParser:
    """
    Solana transaction parser using Helius API.
    Analyzes Solana transactions to detect DEX swaps with Helius's parsed data.
    """
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or HELIUS_API_KEY
        self.base_url = "https://api.helius.xyz"
        
        # Create reverse mapping for Solana tokens
        self.reverse_token_map = {v['mint'].lower(): k for k, v in SOL_TOKENS_TO_MONITOR.items()}
        self.solana_stablecoins = {
            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'.lower(): 'USDC',  # Native USDC
            'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'.lower(): 'USDT',  # Native USDT
        }
        
        # Solana DeFi program IDs for protocol identification
        self.defi_programs = {
            'RAYDIUM_AMM': '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
            'ORCA_WHIRLPOOLS': 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
            'JUPITER_AGGREGATOR': 'JUP2jxvXaqu7NQY1GmNF4m1kzoPie5oK5N5F8jPHlwY',
            'SERUM_DEX': '9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM',
            'MARINADE_FINANCE': '8szGkuLTAux9XMgZ2vtY39jVSowEcpBfFfD8hXSEqdGC',
            'SABER_SWAP': 'SSwpkEEWHvPRx4kEurpjosyBXLkJSQAuNpSaePomh4T',
            'METEORA': 'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB'
        }
        
        # Known stablecoin mints on Solana
        self.stablecoin_mints = {
            'USDC': 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
            'USDT': 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
            'USDH': 'USDH1SM1ojwWUga67PGrgFWUHibbjqMvuMaDkRJTgkX'
        }
        
        # Transaction type indicators
        self.transaction_indicators = {
            'SWAP': ['swap', 'exchange', 'trade'],
            'LIQUIDITY': ['addLiquidity', 'removeLiquidity', 'mint', 'burn'],
            'LENDING': ['deposit', 'withdraw', 'borrow', 'repay'],
            'STAKING': ['stake', 'unstake', 'delegate', 'undelegate'],
            'NFT': ['mintNft', 'transferNft', 'listNft', 'buyNft']
        }
        
        logger.info(f"Initialized Solana parser with {len(self.reverse_token_map)} tracked tokens")

    def analyze_swap(self, tx_hash: str) -> Optional[Dict[str, Any]]:
        """
        Analyzes a Solana transaction for DEX swap activity using Helius API.
        
        METHODOLOGY:
        1. Query Helius /transactions endpoint with transaction signature
        2. Look for parsed 'SWAP' events in the response
        3. Analyze tokenTransfers to determine direction
        4. Apply stablecoin flow logic for BUY/SELL classification
        
        Helius provides:
        - Parsed transaction events (much easier than raw logs)
        - Token transfer details with symbols and amounts
        - DEX source identification (Jupiter, Raydium, etc.)
        """
        if not self.api_key:
            logger.warning("Helius API key not configured, Solana parsing disabled")
            return None
        
        try:
            # Helius expects POST request with transaction signatures
            payload = {
                "transactions": [tx_hash]
            }
            
            response = requests.post(
                self.base_url,
                json=payload,
                params={"api-key": self.api_key}
            )
            response.raise_for_status()
            
            transactions = response.json()
            if not transactions or len(transactions) == 0:
                logger.debug(f"No transaction data returned for {tx_hash}")
                return None
                
            parsed_tx = transactions[0]
            
            if not parsed_tx or 'events' not in parsed_tx:
                logger.debug(f"No events found in transaction {tx_hash}")
                return None
            
            # Analyze the parsed transaction
            return self._analyze_helius_transaction(parsed_tx, tx_hash)
            
        except Exception as e:
            logger.error(f"Helius API error for {tx_hash}: {e}")
            return None

    def _analyze_helius_transaction(self, parsed_tx: Dict[str, Any], tx_hash: str) -> Optional[Dict[str, Any]]:
        """
        Analyzes Helius parsed transaction data to detect and classify swaps.
        
        HELIUS ADVANTAGE:
        - Pre-parsed events with clear 'SWAP' type identification
        - tokenTransfers array with detailed flow information
        - DEX source attribution (Jupiter, Raydium, Orca, etc.)
        """
        try:
            fee_payer = parsed_tx.get('feePayer', '').lower()
            
            # Look for SWAP events in the parsed transaction
            for event in parsed_tx.get('events', []):
                if event.get('type') == 'SWAP':
                    logger.info(f"✅ Confirmed Solana DEX swap found in {tx_hash}")
                    
                    # Analyze token transfers in this swap event
                    swap_analysis = self._analyze_solana_token_transfers(
                        event, fee_payer, tx_hash
                    )
                    
                    if swap_analysis:
                        return swap_analysis
                    else:
                        # Fallback: confirmed swap but couldn't determine direction
                        dex_source = event.get('source', 'Unknown DEX')
                        return {
                            'direction': 'TRANSFER',
                            'evidence': f"Confirmed swap on Solana DEX: {dex_source} (direction unclear)",
                            'confidence': 0.85,
                            'dex_name': dex_source,
                            'swap_details': {'method': 'helius_fallback_detection'}
                        }
            
            # Check for other relevant events (like token transfers that might indicate swaps)
            return self._analyze_general_token_transfers(parsed_tx, fee_payer, tx_hash)
            
        except Exception as e:
            logger.error(f"Error analyzing Helius transaction data for {tx_hash}: {e}")
            return None

    def _analyze_solana_token_transfers(self, swap_event: Dict[str, Any], fee_payer: str, tx_hash: str) -> Optional[Dict[str, Any]]:
        """
        Analyzes token transfers within a SWAP event to determine direction.
        
        SOLANA DEX PATTERNS:
        - tokenTransfers array contains all token movements
        - Each transfer has: mint, fromUserAccount, toUserAccount, tokenAmount
        - Fee payer is typically the transaction initiator
        """
        try:
            dex_source = swap_event.get('source', 'Unknown DEX')
            token_transfers = swap_event.get('tokenTransfers', [])
            
            if not token_transfers:
                return None
                
            # Track what the fee payer (initiator) sent vs received
            initiator_sent = []
            initiator_received = []
            
            for transfer in token_transfers:
                mint_address = transfer.get('mint', '').lower()
                from_account = transfer.get('fromUserAccount', '').lower()
                to_account = transfer.get('toUserAccount', '').lower()
                
                # Get token symbol
                token_symbol = self._get_solana_token_symbol(mint_address)
                
                # Track initiator's token flows
                if from_account == fee_payer:
                    initiator_sent.append({
                        'symbol': token_symbol,
                        'mint': mint_address,
                        'amount': transfer.get('tokenAmount', 0)
                    })
                    
                if to_account == fee_payer:
                    initiator_received.append({
                        'symbol': token_symbol,
                        'mint': mint_address,
                        'amount': transfer.get('tokenAmount', 0)
                    })
            
            # Apply stablecoin flow analysis
            return self._classify_solana_swap_direction(
                initiator_sent, initiator_received, dex_source, tx_hash
            )
            
        except Exception as e:
            logger.error(f"Error analyzing Solana token transfers for {tx_hash}: {e}")
            return None

    def _classify_solana_swap_direction(self, sent: List[Dict], received: List[Dict], dex_source: str, tx_hash: str) -> Optional[Dict[str, Any]]:
        """
        Classifies Solana swap direction using stablecoin flow analysis.
        
        Same logic as EVM but adapted for Solana tokens and DEXs.
        """
        try:
            sent_symbols = [t['symbol'] for t in sent if t['symbol'] != 'UNKNOWN']
            received_symbols = [t['symbol'] for t in received if t['symbol'] != 'UNKNOWN']
            
            # Categorize tokens (including Solana-specific stablecoins)
            sent_stables = [s for s in sent_symbols if self._is_solana_stablecoin(s)]
            received_stables = [s for s in received_symbols if self._is_solana_stablecoin(s)]
            sent_volatiles = [v for v in sent_symbols if not self._is_solana_stablecoin(v)]
            received_volatiles = [v for v in received_symbols if not self._is_solana_stablecoin(v)]
            
            logger.debug(f"Solana token flow analysis for {tx_hash}:")
            logger.debug(f"  Sent stables: {sent_stables}")
            logger.debug(f"  Received volatiles: {received_volatiles}")
            logger.debug(f"  Sent volatiles: {sent_volatiles}")
            logger.debug(f"  Received stables: {received_stables}")
            
            # PRIMARY PATTERN: Stable → Volatile = BUY
            if sent_stables and received_volatiles:
                stable_token = sent_stables[0]
                volatile_token = received_volatiles[0]
                return {
                    'direction': 'BUY',
                    'evidence': f"Solana DEX Swap on {dex_source}: {stable_token} → {volatile_token} (Stable→Volatile = BUY)",
                    'confidence': 0.95,
                    'dex_name': dex_source,
                    'swap_details': {
                        'from_token': stable_token,
                        'to_token': volatile_token,
                        'method': 'solana_stablecoin_flow_analysis'
                    }
                }
            
            # SECONDARY PATTERN: Volatile → Stable = SELL
            elif sent_volatiles and received_stables:
                volatile_token = sent_volatiles[0]
                stable_token = received_stables[0]
                return {
                    'direction': 'SELL',
                    'evidence': f"Solana DEX Swap on {dex_source}: {volatile_token} → {stable_token} (Volatile→Stable = SELL)",
                    'confidence': 0.95,
                    'dex_name': dex_source,
                    'swap_details': {
                        'from_token': volatile_token,
                        'to_token': stable_token,
                        'method': 'solana_stablecoin_flow_analysis'
                    }
                }
            
            # SOL-specific patterns
            elif 'SOL' in sent_symbols and received_stables:
                stable_token = received_stables[0]
                return {
                    'direction': 'SELL',
                    'evidence': f"Solana DEX Swap on {dex_source}: SOL → {stable_token} (SOL→Stable = SELL)",
                    'confidence': 0.95,
                    'dex_name': dex_source,
                    'swap_details': {
                        'from_token': 'SOL',
                        'to_token': stable_token,
                        'method': 'sol_to_stable_analysis'
                    }
                }
            
            elif sent_stables and 'SOL' in received_symbols:
                stable_token = sent_stables[0]
                return {
                    'direction': 'BUY',
                    'evidence': f"Solana DEX Swap on {dex_source}: {stable_token} → SOL (Stable→SOL = BUY)",
                    'confidence': 0.95,
                    'dex_name': dex_source,
                    'swap_details': {
                        'from_token': stable_token,
                        'to_token': 'SOL',
                        'method': 'stable_to_sol_analysis'
                    }
                }
            
            else:
                # Complex or unknown pattern
                return {
                    'direction': 'TRANSFER',
                    'evidence': f"Solana DEX Swap on {dex_source}: Complex token flow pattern",
                    'confidence': 0.75,
                    'dex_name': dex_source,
                    'swap_details': {
                        'method': 'solana_complex_pattern',
                        'sent_count': len(sent_symbols),
                        'received_count': len(received_symbols)
                    }
                }
                
        except Exception as e:
            logger.error(f"Error classifying Solana swap direction for {tx_hash}: {e}")
            return None

    def _analyze_general_token_transfers(self, parsed_tx: Dict[str, Any], fee_payer: str, tx_hash: str) -> Optional[Dict[str, Any]]:
        """
        Fallback analysis for transactions without explicit SWAP events.
        Looks for patterns in general token transfers that might indicate trading.
        """
        try:
            # This is a simplified fallback - in a full implementation,
            # we would analyze other event types and token transfer patterns
            logger.debug(f"No explicit swap events found in {tx_hash}, checking general transfers")
            return None
            
        except Exception as e:
            logger.error(f"Error in general transfer analysis for {tx_hash}: {e}")
            return None

    def _get_solana_token_symbol(self, mint_address: str) -> str:
        """Get token symbol from mint address."""
        mint_lower = mint_address.lower()
        
        # Check our tracked tokens first
        if mint_lower in self.reverse_token_map:
            return self.reverse_token_map[mint_lower]
        
        # Check stablecoins
        if mint_lower in self.solana_stablecoins:
            return self.solana_stablecoins[mint_lower]
        
        # Handle native SOL
        if mint_lower == 'So11111111111111111111111111111111111111112'.lower():
            return 'SOL'
        
        return 'UNKNOWN'

    def _is_solana_stablecoin(self, symbol: str) -> bool:
        """Check if a token symbol is a stablecoin on Solana."""
        symbol_upper = symbol.upper()
        
        # Standard stablecoins
        if symbol_upper in STABLECOIN_SYMBOLS:
            return True
        
        # Solana-specific stablecoin patterns
        solana_stables = {'USDC', 'USDT', 'USDH', 'UXD', 'PAI'}
        return symbol_upper in solana_stables

    def analyze_transaction_advanced(self, signature: str) -> Dict:
        """
        ADVANCED SOLANA TRANSACTION ANALYSIS using Helius Enhanced Transactions API
        """
        try:
            # 🔗 CORRECT API SYNTAX - Helius Enhanced Transactions
            url = f"{self.base_url}/v0/transactions?api-key={self.api_key}"
            payload = {
                "transactions": [signature]
            }
            
            response = requests.post(url, json=payload, timeout=15)
            
            if response.status_code != 200:
                return {'error': f'API error: {response.status_code}', 'confidence_score': 0}
            
            data = response.json()
            if not data or len(data) == 0:
                return {'error': 'No transaction data found', 'confidence_score': 0}
            
            enhanced_data = data[0]
            
            # Initialize analysis result
            analysis_result = {
                'transaction_category': 'UNKNOWN',
                'confidence_score': 0.0,
                'protocol_interactions': [],
                'token_flow_analysis': {},
                'program_analysis': {},
                'jupiter_route_info': {},
                'nft_activity': {},
                'defi_operations': []
            }
            
            # ANALYZE: Helius parsed events
            if 'events' in enhanced_data:
                events = enhanced_data['events']
                
                if 'swap' in events:
                    swap_analysis = self._analyze_jupiter_swap(events['swap'])
                    analysis_result.update(swap_analysis)
                    analysis_result['transaction_category'] = 'SOLANA_JUPITER_SWAP'
                    analysis_result['confidence_score'] = 0.93
                    
                elif 'nft' in events:
                    nft_analysis = self._analyze_nft_transaction(events['nft'])
                    analysis_result.update(nft_analysis)
                    analysis_result['transaction_category'] = 'NFT_TRANSACTION'
                    analysis_result['confidence_score'] = 0.90
                    
                elif 'compressed' in events:
                    compressed_analysis = self._analyze_compressed_nft(events['compressed'])
                    analysis_result.update(compressed_analysis)
                    analysis_result['transaction_category'] = 'COMPRESSED_NFT'
                    analysis_result['confidence_score'] = 0.88
            
            # ADD: Advanced program interaction analysis
            if 'instructions' in enhanced_data:
                program_analysis = self._analyze_program_interactions(enhanced_data['instructions'])
                analysis_result['program_analysis'] = program_analysis
                
                # Update category if no events detected but programs identified
                if analysis_result['transaction_category'] == 'UNKNOWN' and program_analysis.get('primary_protocol'):
                    analysis_result['transaction_category'] = f"SOLANA_{program_analysis['primary_protocol']}"
                    analysis_result['confidence_score'] = 0.75
            
            # ADD: Solana DeFi protocol detection
            defi_operations = self.detect_solana_defi_protocols(enhanced_data.get('instructions', []))
            analysis_result['defi_operations'] = defi_operations
            
            # ADD: Jupiter aggregator route analysis
            if analysis_result['transaction_category'] == 'SOLANA_JUPITER_SWAP':
                jupiter_route = self.analyze_jupiter_route(enhanced_data)
                analysis_result['jupiter_route_info'] = jupiter_route
            
            # Enhance token flow analysis
            if 'tokenTransfers' in enhanced_data:
                token_flow = self._analyze_solana_token_flow(enhanced_data['tokenTransfers'])
                analysis_result['token_flow_analysis'] = token_flow
            
            return analysis_result
            
        except Exception as e:
            print(f"Error in advanced Solana transaction analysis: {e}")
            return {'error': str(e), 'confidence_score': 0}

    def _analyze_jupiter_swap(self, swap_data: Dict) -> Dict:
        """Analyze Jupiter swap events with enhanced details"""
        swap_analysis = {
            'swap_type': 'UNKNOWN',
            'input_token': None,
            'output_token': None,
            'route_info': {},
            'slippage_analysis': {}
        }
        
        try:
            # Analyze token inputs and outputs
            token_inputs = swap_data.get('tokenInputs', [])
            token_outputs = swap_data.get('tokenOutputs', [])
            
            if token_inputs and token_outputs:
                input_token = token_inputs[0].get('mint')
                output_token = token_outputs[0].get('mint')
                
                swap_analysis['input_token'] = input_token
                swap_analysis['output_token'] = output_token
                
                # Determine swap type based on stablecoin involvement
                input_is_stable = input_token in self.stablecoin_mints.values()
                output_is_stable = output_token in self.stablecoin_mints.values()
                
                if input_is_stable and not output_is_stable:
                    swap_analysis['swap_type'] = 'BUY'
                elif not input_is_stable and output_is_stable:
                    swap_analysis['swap_type'] = 'SELL'
                elif input_is_stable and output_is_stable:
                    swap_analysis['swap_type'] = 'ARBITRAGE'
                else:
                    swap_analysis['swap_type'] = 'VOLATILE_SWAP'
            
            # Analyze inner swaps for route complexity
            inner_swaps = swap_data.get('innerSwaps', [])
            if inner_swaps:
                swap_analysis['route_info'] = {
                    'is_multi_hop': True,
                    'hop_count': len(inner_swaps),
                    'route_complexity': 'HIGH' if len(inner_swaps) > 2 else 'MEDIUM'
                }
            
        except Exception as e:
            print(f"Error analyzing Jupiter swap: {e}")
        
        return swap_analysis

    def _analyze_nft_transaction(self, nft_data: Dict) -> Dict:
        """Analyze NFT transaction events"""
        nft_analysis = {
            'nft_type': nft_data.get('type', 'UNKNOWN'),
            'marketplace': nft_data.get('source', 'UNKNOWN'),
            'nft_details': {},
            'price_analysis': {}
        }
        
        try:
            # Extract NFT details
            nfts = nft_data.get('nfts', [])
            if nfts:
                nft_analysis['nft_details'] = {
                    'mint': nfts[0].get('mint'),
                    'token_standard': nfts[0].get('tokenStandard'),
                    'collection': nfts[0].get('collection')
                }
            
            # Price analysis
            amount = nft_data.get('amount')
            if amount:
                nft_analysis['price_analysis'] = {
                    'price_lamports': amount,
                    'price_sol': amount / 1_000_000_000,  # Convert to SOL
                    'marketplace_fee': nft_data.get('fee', 0)
                }
            
        except Exception as e:
            print(f"Error analyzing NFT transaction: {e}")
        
        return nft_analysis

    def _analyze_compressed_nft(self, compressed_data: Dict) -> Dict:
        """Analyze compressed NFT events"""
        compressed_analysis = {
            'operation_type': compressed_data.get('type', 'UNKNOWN'),
            'tree_info': {},
            'compression_details': {}
        }
        
        try:
            compressed_analysis['tree_info'] = {
                'tree_id': compressed_data.get('treeId'),
                'asset_id': compressed_data.get('assetId'),
                'leaf_index': compressed_data.get('leafIndex')
            }
            
            compressed_analysis['compression_details'] = {
                'new_owner': compressed_data.get('newLeafOwner'),
                'old_owner': compressed_data.get('oldLeafOwner'),
                'instruction_index': compressed_data.get('instructionIndex')
            }
            
        except Exception as e:
            print(f"Error analyzing compressed NFT: {e}")
        
        return compressed_analysis

    def _analyze_program_interactions(self, instructions: List[Dict]) -> Dict:
        """Analyze program interactions to identify protocols"""
        program_analysis = {
            'programs_involved': [],
            'primary_protocol': None,
            'interaction_complexity': 'LOW'
        }
        
        try:
            program_ids = set()
            protocol_interactions = []
            
            for instruction in instructions:
                program_id = instruction.get('programId')
                if program_id:
                    program_ids.add(program_id)
                    
                    # Identify known DeFi protocols
                    protocol = self._identify_protocol_from_program_id(program_id)
                    if protocol:
                        protocol_interactions.append(protocol)
            
            program_analysis['programs_involved'] = list(program_ids)
            
            if protocol_interactions:
                # Most frequent protocol becomes primary
                primary_protocol = max(set(protocol_interactions), key=protocol_interactions.count)
                program_analysis['primary_protocol'] = primary_protocol
            
            # Determine complexity
            if len(program_ids) > 5:
                program_analysis['interaction_complexity'] = 'HIGH'
            elif len(program_ids) > 2:
                program_analysis['interaction_complexity'] = 'MEDIUM'
            
        except Exception as e:
            print(f"Error analyzing program interactions: {e}")
        
        return program_analysis

    def _identify_protocol_from_program_id(self, program_id: str) -> Optional[str]:
        """Identify DeFi protocol from program ID"""
        for protocol, pid in self.defi_programs.items():
            if program_id == pid:
                return protocol
        return None

    def _analyze_solana_token_flow(self, token_transfers: List[Dict]) -> Dict:
        """Analyze token transfer flow for Solana transactions"""
        flow_analysis = {
            'transfer_count': len(token_transfers),
            'stablecoin_involved': False,
            'flow_direction': 'UNKNOWN',
            'token_diversity': 0
        }
        
        try:
            unique_mints = set()
            stablecoin_transfers = 0
            
            for transfer in token_transfers:
                mint = transfer.get('mint')
                if mint:
                    unique_mints.add(mint)
                    
                    if mint in self.stablecoin_mints.values():
                        stablecoin_transfers += 1
            
            flow_analysis['token_diversity'] = len(unique_mints)
            flow_analysis['stablecoin_involved'] = stablecoin_transfers > 0
            
            # Determine flow direction
            if stablecoin_transfers > 0 and len(unique_mints) > 1:
                if stablecoin_transfers >= len(token_transfers) / 2:
                    flow_analysis['flow_direction'] = 'STABLE_DOMINANT'
                else:
                    flow_analysis['flow_direction'] = 'VOLATILE_DOMINANT'
            
        except Exception as e:
            print(f"Error analyzing token flow: {e}")
        
        return flow_analysis

    # ADD NEW METHOD: Jupiter Route Analysis  
    def analyze_jupiter_route(self, enhanced_data: Dict) -> Dict:
        """
        DECODE Jupiter aggregator swap routes
        - Multi-hop swap detection
        - Route optimization analysis
        - Slippage and MEV impact
        """
        route_analysis = {
            'route_type': 'DIRECT',
            'hop_details': [],
            'optimization_score': 0,
            'slippage_analysis': {},
            'mev_protection': False
        }
        
        try:
            # Analyze swap events for route complexity
            events = enhanced_data.get('events', {})
            swap_data = events.get('swap', {})
            
            inner_swaps = swap_data.get('innerSwaps', [])
            
            if inner_swaps:
                route_analysis['route_type'] = 'MULTI_HOP'
                
                for i, inner_swap in enumerate(inner_swaps):
                    program_info = inner_swap.get('programInfo', {})
                    hop_detail = {
                        'hop_number': i + 1,
                        'dex': program_info.get('source', 'UNKNOWN'),
                        'program_name': program_info.get('programName', 'UNKNOWN'),
                        'instruction': program_info.get('instructionName', 'UNKNOWN')
                    }
                    route_analysis['hop_details'].append(hop_detail)
                
                # Calculate optimization score based on route efficiency
                unique_dexes = len(set(hop['dex'] for hop in route_analysis['hop_details']))
                route_analysis['optimization_score'] = min(100, (unique_dexes * 25) + (len(inner_swaps) * 10))
            
            # Analyze slippage protection
            token_fees = swap_data.get('tokenFees', [])
            if token_fees:
                route_analysis['slippage_analysis'] = {
                    'has_fees': True,
                    'fee_count': len(token_fees),
                    'protection_level': 'HIGH' if len(token_fees) > 2 else 'MEDIUM'
                }
            
            # Check for MEV protection indicators
            if len(inner_swaps) > 1 and route_analysis['optimization_score'] > 50:
                route_analysis['mev_protection'] = True
            
        except Exception as e:
            print(f"Error analyzing Jupiter route: {e}")
        
        return route_analysis

    # ADD NEW METHOD: Solana DeFi Protocol Detection
    def detect_solana_defi_protocols(self, instructions: List[Dict]) -> List[Dict]:
        """
        IDENTIFY Solana DeFi protocol interactions
        - Raydium AMM operations
        - Orca Whirlpool swaps  
        - Serum DEX trading
        - Marinade liquid staking
        """
        defi_operations = []
        
        try:
            for instruction in instructions:
                program_id = instruction.get('programId')
                accounts = instruction.get('accounts', [])
                data = instruction.get('data', '')
                
                # Identify protocol and operation type
                protocol = self._identify_protocol_from_program_id(program_id)
                
                if protocol:
                    operation = {
                        'protocol': protocol,
                        'program_id': program_id,
                        'operation_type': self._determine_operation_type(protocol, data),
                        'account_count': len(accounts),
                        'complexity': 'HIGH' if len(accounts) > 10 else 'MEDIUM' if len(accounts) > 5 else 'LOW'
                    }
                    
                    # Add protocol-specific analysis
                    if protocol == 'RAYDIUM_AMM':
                        operation['amm_analysis'] = self._analyze_raydium_operation(instruction)
                    elif protocol == 'ORCA_WHIRLPOOLS':
                        operation['whirlpool_analysis'] = self._analyze_orca_operation(instruction)
                    elif protocol == 'MARINADE_FINANCE':
                        operation['staking_analysis'] = self._analyze_marinade_operation(instruction)
                    
                    defi_operations.append(operation)
            
        except Exception as e:
            print(f"Error detecting DeFi protocols: {e}")
        
        return defi_operations

    def _determine_operation_type(self, protocol: str, instruction_data: str) -> str:
        """Determine the type of operation based on protocol and instruction data"""
        # Simplified operation type detection
        # In a real implementation, you'd decode the instruction data properly
        
        operation_patterns = {
            'RAYDIUM_AMM': {
                'swap': ['swap', 'exchange'],
                'liquidity': ['addLiquidity', 'removeLiquidity']
            },
            'ORCA_WHIRLPOOLS': {
                'swap': ['whirlpoolSwap'],
                'liquidity': ['increaseLiquidity', 'decreaseLiquidity']
            },
            'MARINADE_FINANCE': {
                'stake': ['deposit', 'liquidStake'],
                'unstake': ['withdraw', 'delayedUnstake']
            }
        }
        
        protocol_patterns = operation_patterns.get(protocol, {})
        
        # Simple pattern matching (in practice, you'd decode instruction data)
        for op_type, patterns in protocol_patterns.items():
            for pattern in patterns:
                if pattern.lower() in instruction_data.lower():
                    return op_type.upper()
        
        return 'UNKNOWN'

    def _analyze_raydium_operation(self, instruction: Dict) -> Dict:
        """Analyze Raydium AMM-specific operations"""
        return {
            'pool_interaction': True,
            'estimated_type': 'AMM_OPERATION',
            'account_complexity': len(instruction.get('accounts', []))
        }

    def _analyze_orca_operation(self, instruction: Dict) -> Dict:
        """Analyze Orca Whirlpool-specific operations"""
        return {
            'whirlpool_interaction': True,
            'estimated_type': 'CONCENTRATED_LIQUIDITY',
            'precision_level': 'HIGH'
        }

    def _analyze_marinade_operation(self, instruction: Dict) -> Dict:
        """Analyze Marinade liquid staking operations"""
        return {
            'staking_operation': True,
            'liquid_staking': True,
            'validator_interaction': len(instruction.get('accounts', [])) > 8
        }

    def get_transaction_history(self, address: str, limit: int = 100, 
                              transaction_type: str = None) -> List[Dict]:
        """
        Get transaction history for an address using Helius Enhanced Transactions API
        """
        try:
            url = f"{self.base_url}/v0/addresses/{address}/transactions"
            params = {
                'api-key': self.api_key,
                'limit': min(limit, 100)  # API limit
            }
            
            if transaction_type:
                params['type'] = transaction_type
            
            response = requests.get(url, params=params, timeout=15)
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error fetching transaction history: {response.status_code}")
                return []
                
        except Exception as e:
            print(f"Error in get_transaction_history: {e}")
            return []

    def parse_transaction_by_signature(self, signature: str) -> Dict:
        """
        Parse a single transaction by signature with enhanced analysis
        """
        return self.analyze_transaction_advanced(signature)

# Global instance
solana_parser = SolanaParser() 
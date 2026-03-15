"""
Solana API poller using Alchemy Solana RPC exclusively.

Polls recent large SPL token transfers for top Solana tokens every 60 seconds.
Uses Alchemy getSignaturesForAddress on token mint accounts, then
getTransaction (jsonParsed) to extract transfer details.

No Solscan or Helius dependencies.
"""

import time
import logging

from config.settings import (
    solana_last_processed_signature,
    GLOBAL_USD_THRESHOLD,
    shutdown_flag,
)
from data.tokens import SOL_TOKENS_TO_MONITOR, TOKEN_PRICES
from utils.base_helpers import safe_print
from utils.alchemy_rpc import get_alchemy_rpc, _rpc_call

logger = logging.getLogger(__name__)

# Known Solana exchange and DEX addresses for classification
# Sources: Solscan labeled, SolanaFM labeled, CoinCarp tracker
SOLANA_CEX_ADDRESSES = {
    # Binance — Solscan labeled
    '5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhUvuAi9',  # Binance 2 (hot wallet)
    '9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM',  # Binance (largest Bonk holder)
    '53unSgGWqEWANcPYRF35B2Bgf8BkszUtcccKiXwGGLyr',  # Binance.us Hot Wallet
    'DRpbCBMxVnDK7maPM5tGv6MvB3v1sRMC86PZ8okm21hy',  # Binance staking
    # Coinbase — Solscan labeled
    'H8sMJSCQxfKiFTCfDR3DUMLPwcRbM61LGFJ8N4dK3WjS',  # Coinbase
    'GJRs4FwHtemZ5ZE9x3FNvJ8TMwitKTh21yxdRPqn7npE',  # Coinbase Hot Wallet 2
    'D89hHJT5Aqyx1trP6EnGY9jJUB3whgnq3aUvvCqedvzf',  # Coinbase Hot Wallet 3
    '2AQdpHJ2JpcEgPiATUXjQxA8QmafFegfQwSLWSprPicm',  # Coinbase
    # OKX — Solscan/SolanaFM labeled
    '5VCwKtCXgCJ6kit5FybXjvriW3xELsFDhYrPSqtJNmcD',  # OKX main wallet
    'C68a6RCGLiPskbPYtAcsCjhG8tfTWYcoB4JjCrXFdqyo',  # OKX Hot Wallet ($107M+)
    'is6MTRHEgyFLNTfYcuV4QBWLjrZBfmhVNYR6ccgr8KV',   # OKX Hot Wallet (secondary)
    # Bybit — Solscan/SolanaFM labeled
    'AC5RDfQFmDS1deWZos921JfqscXdByf8BKHs5ACWjtW2',  # Bybit Hot Wallet
    'FFaPfxY3BJ6Ph7S2UZTxePixVo5UsmCQR83qkUxn6ttn',  # Bybit (SolanaFM)
    # Kraken
    'FWznbcNXWQuHTawe9RxvQ2LdCENssh12dsznf4RiouN5',  # Kraken
    # HTX — Solscan labeled
    'BY4StcU9Y2BpgH8quZzorg31EGE4L1rjomN8FNsCBEcx',  # HTX Hot Wallet
    # Crypto.com — Solscan labeled
    'AobVSwdW9BbpMdJvTqeCN4hPAmh4rHm7vwLnQ5ATSyrS',  # Crypto.com Hot Wallet 2
    # Gate.io — CoinCarp tracker
    'ASTyfSima4LLAdDgoFGkgqoKowG1LZFDr9fAQrg7iaJZ',  # Gate.io
    'u6PJ8DtQuPFnfmwHbGFULQ4u4EgjDiyYKjVEsynXq2w',   # Gate.io
    # KuCoin — CoinCarp tracker
    'BmFdpraQhkiDQE6SnfG5PkRQ6dQkwKQaFx5iEq5nLFpK',  # KuCoin
    # Bitget — CoinCarp tracker
    'CL8Mmkf45ic5MczN7SqpPGBuAq7dmhUVwNaFk4dVBv7j',  # Bitget
    # Market makers
    '44P5Ct5JkPz76Rs2K6juC65zXMpFRDrHatxcASJ4Dyra',  # Wintermute
}
SOLANA_DEX_ADDRESSES = {
    'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4',   # Jupiter V6
    'JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB',   # Jupiter V4
    'jupoNjAxXgZ4rjzxzPMP4oxduvQsQtZzyknqvzYNrNu',   # Jupiter Limit Order
    'DCA265Vj8a9CEuX1eb1LWRnDT7uK6q1xMipnNyatn23M',  # Jupiter DCA
    '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',  # Raydium AMM V4
    'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK',  # Raydium CLMM
    'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C',  # Raydium CPMM
    'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',   # Orca Whirlpool
    '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP',  # Orca V2
    'srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX',   # OpenBook V1
    'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo',   # Meteora DLMM
    'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB',  # Meteora Pools
    'PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY',   # Phoenix DEX
    'dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH',   # Drift V2
    'MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD',   # Marinade Staking
}


def _classify_solana_transfer(from_addr, to_addr):
    """Classify a Solana transfer using known exchange and DEX addresses."""
    from_is_cex = from_addr in SOLANA_CEX_ADDRESSES
    to_is_cex = to_addr in SOLANA_CEX_ADDRESSES
    from_is_dex = from_addr in SOLANA_DEX_ADDRESSES
    to_is_dex = to_addr in SOLANA_DEX_ADDRESSES

    if from_is_cex and not to_is_cex:
        return 'BUY'    # Withdrawal from exchange
    elif to_is_cex and not from_is_cex:
        return 'SELL'   # Deposit to exchange
    elif from_is_dex or to_is_dex:
        return 'BUY' if from_is_dex else 'SELL'
    else:
        return 'TRANSFER'

# All Solana tokens to track — includes stablecoins because they carry real volume on Solana
TOP_SOLANA_TOKENS = [
    "SOL", "BONK", "RAY", "ORCA", "MSOL", "PYTH", "BSOL", "RENDER",
    "SAMO", "DUST", "SRM", "MNGO", "ATLAS", "MEAN", "SHDW", "COPE",
]

# SPL Token Program - used as fallback
SPL_TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

# Track last seen signature per token
_last_seen_sig = {}
_global_last_sig = None


def _active_tokens():
    """Return the subset of SOL_TOKENS_TO_MONITOR limited to TOP_SOLANA_TOKENS."""
    return {k: v for k, v in SOL_TOKENS_TO_MONITOR.items() if k in TOP_SOLANA_TOKENS}


def _mint_to_symbol():
    """Build a lookup: mint address -> (symbol, decimals) for ALL monitored Solana tokens."""
    return {
        info["mint"]: (sym, info["decimals"])
        for sym, info in SOL_TOKENS_TO_MONITOR.items()
    }


def _alchemy_get_signatures(mint_addr, limit=20, before=None):
    """Fetch recent signatures for a Solana mint address via Alchemy RPC."""
    rpc_url = get_alchemy_rpc('solana')
    if not rpc_url:
        logger.warning("Alchemy Solana RPC URL not configured")
        return []

    params = {"limit": limit, "commitment": "confirmed"}
    if before:
        params["before"] = before

    result = _rpc_call(rpc_url, 'getSignaturesForAddress', [mint_addr, params], cu_cost=40, timeout=15)
    if result and isinstance(result, list):
        return result
    return []


def _alchemy_get_transaction(signature):
    """Fetch a parsed Solana transaction via Alchemy RPC."""
    rpc_url = get_alchemy_rpc('solana')
    if not rpc_url:
        return None

    result = _rpc_call(
        rpc_url, 'getTransaction',
        [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0, "commitment": "confirmed"}],
        cu_cost=40, timeout=15
    )
    return result


def _extract_spl_transfers(parsed_tx, mint_addr):
    """Extract SPL token transfer instructions from a parsed transaction."""
    transfers = []
    if not parsed_tx:
        return transfers

    meta = parsed_tx.get("meta")
    if not meta or meta.get("err") is not None:
        return transfers

    # Check inner instructions + top-level instructions for SPL transfers
    all_instructions = []
    msg = parsed_tx.get("transaction", {}).get("message", {})
    all_instructions.extend(msg.get("instructions", []))

    for inner in (meta.get("innerInstructions") or []):
        all_instructions.extend(inner.get("instructions", []))

    for ix in all_instructions:
        parsed = ix.get("parsed")
        if not parsed:
            continue
        ix_type = parsed.get("type", "")
        info = parsed.get("info", {})

        if ix_type in ("transfer", "transferChecked"):
            ix_mint = info.get("mint", "")
            if ix_mint and ix_mint != mint_addr:
                continue

            token_amount = 0
            if "tokenAmount" in info:
                token_amount = float(info["tokenAmount"].get("uiAmount") or 0)
            elif "amount" in info:
                token_amount = float(info["amount"])

            if token_amount <= 0:
                continue

            transfers.append({
                "from": info.get("source", info.get("authority", "")),
                "to": info.get("destination", ""),
                "amount": token_amount,
                "mint": ix_mint or mint_addr,
            })

    return transfers


def initialize_baseline():
    """Initialize last-seen slot for Solana block scanning."""
    global _global_last_sig
    rpc_url = get_alchemy_rpc('solana')
    if rpc_url:
        slot = _rpc_call(rpc_url, 'getSlot', [{"commitment": "confirmed"}], cu_cost=10)
        if slot:
            _global_last_sig = slot
            safe_print(f"  Solana baseline slot: {slot}")
        else:
            _global_last_sig = None
            safe_print(f"  Solana: could not get current slot")


def fetch_solana_token_transfers():
    """
    Poll Solana by scanning recent CONFIRMED blocks via getBlock.
    Extracts ALL SPL token balance changes and matches against monitored mints.
    This catches every token including SOL, JTO, WIF that per-mint queries miss.
    """
    global _global_last_sig
    results = []
    mint_lookup = _mint_to_symbol()
    
    rpc_url = get_alchemy_rpc('solana')
    if not rpc_url:
        return results
    
    # Get current confirmed slot
    current_slot = _rpc_call(rpc_url, 'getSlot', [{"commitment": "confirmed"}], cu_cost=10)
    if not current_slot:
        return results
    
    if _global_last_sig is None:
        _global_last_sig = current_slot
        return results
    
    # Calculate which target slots to scan (offset by 50 for availability)
    # We track the last TARGET slot we actually scanned, not the current slot
    last_target = _global_last_sig - 50
    current_target = current_slot - 50
    
    if current_target <= last_target:
        return results
    
    # Scan up to 150 slots per cycle (~60 sec of Solana data at 2.5 slots/sec)
    # With 10K CU/s Alchemy budget, 150 getBlock calls at 40 CU = 6000 CU = well within budget
    start_target = last_target + 1
    end_target = min(current_target, start_target + 150)
    
    for slot in range(start_target, end_target + 1):
        if shutdown_flag.is_set():
            break
        
        target_slot = slot  # Already offset
        
        block_data = _rpc_call(rpc_url, 'getBlock', [target_slot, {
            "encoding": "jsonParsed",
            "maxSupportedTransactionVersion": 0,
            "transactionDetails": "full",
            "rewards": False,
        }], cu_cost=40, timeout=15)
        
        if not block_data:
            continue
        
        txs = block_data.get('transactions', [])
        block_time = block_data.get('blockTime', int(time.time()))
        
        for tx_data in txs:
            meta = tx_data.get('meta', {})
            if not meta or meta.get('err'):
                continue
            
            pre_bals = {b.get('accountIndex'): b for b in (meta.get('preTokenBalances') or [])}
            post_bals = {b.get('accountIndex'): b for b in (meta.get('postTokenBalances') or [])}
            
            if not pre_bals and not post_bals:
                continue
            
            # Get tx signature
            tx_sig = ''
            tx_obj = tx_data.get('transaction', {})
            sigs = tx_obj.get('signatures', [])
            if sigs:
                tx_sig = sigs[0]
            
            # Check each token balance change
            for idx in set(list(pre_bals.keys()) + list(post_bals.keys())):
                pre = pre_bals.get(idx, {})
                post = post_bals.get(idx, {})
                mint = post.get('mint') or pre.get('mint', '')
                
                if mint not in mint_lookup:
                    continue
                
                symbol, decimals = mint_lookup[mint]
                pre_amt = float((pre.get('uiTokenAmount') or {}).get('uiAmount') or 0)
                post_amt = float((post.get('uiTokenAmount') or {}).get('uiAmount') or 0)
                diff = post_amt - pre_amt
                
                if abs(diff) < 0.001:
                    continue
                
                owner = post.get('owner') or pre.get('owner', '')
                
                results.append({
                    'blockchain': 'solana',
                    'from': owner if diff < 0 else '',
                    'to': owner if diff > 0 else '',
                    'symbol': symbol,
                    'amount': str(abs(diff)),
                    'tx_hash': tx_sig,
                    'timestamp': block_time,
                    'decimals': decimals,
                })
        
        time.sleep(0.02)  # Minimal delay — 10K CU/s budget allows rapid calls
    
    # Track where we actually scanned up to (add back the 50 offset)
    _global_last_sig = end_target + 50
    return results


def print_new_solana_transfers():
    """
    Continuously polls and prints new Solana token transfers via Alchemy RPC.
    Runs in its own thread with a polling interval.
    """
    from config.settings import shutdown_flag as _shutdown_flag

    safe_print("Solana Alchemy block-scanner started (60s interval, scanning all monitored tokens)")

    if not _last_seen_sig:
        safe_print("   Initializing Solana token baselines via Alchemy...")
        initialize_baseline()

    poll_interval = 15  # 15s cycles to keep up with Solana's speed
    backoff_multiplier = 1

    while not _shutdown_flag.is_set():
        try:
            transfers = fetch_solana_token_transfers()
            backoff_multiplier = 1

            for event in transfers:
                try:
                    symbol = event["symbol"]
                    decimals = event["decimals"]
                    raw_amount = event["amount"]

                    try:
                        amount_val = float(raw_amount)
                    except (ValueError, TypeError):
                        amount_val = 0

                    if amount_val > 10 ** decimals:
                        token_amount = amount_val / (10 ** decimals)
                    else:
                        token_amount = amount_val

                    price = TOKEN_PRICES.get(symbol, 0)
                    estimated_usd = token_amount * price

                    solana_threshold = 500  # $500 for Solana volatile tokens
                    if estimated_usd < solana_threshold:
                        continue

                    from_addr = event["from"]
                    to_addr = event["to"]
                    tx_hash = event["tx_hash"]

                    # Solana-specific classification
                    classification = _classify_solana_transfer(from_addr, to_addr)

                    from utils.dedup import handle_event
                    event.update({
                        'classification': classification,
                        'usd_value': estimated_usd,
                        'source': 'solana_alchemy'
                    })

                    handle_event(event)

                    from config.settings import solana_api_buy_counts, solana_api_sell_counts
                    if 'BUY' in classification:
                        solana_api_buy_counts[symbol] += 1
                    elif 'SELL' in classification:
                        solana_api_sell_counts[symbol] += 1

                    safe_print(f"\n[SOLANA - {symbol} | ${estimated_usd:,.2f} USD] Tx {tx_hash[:24]}...")
                    safe_print(f"  Amount: {token_amount:,.6f} {symbol} (~${estimated_usd:,.2f} USD)")
                    safe_print(f"  Classification: {classification}")

                except Exception as e:
                    safe_print(f"Error processing Solana transfer: {str(e)}")
                    continue

        except Exception as e:
            error_str = str(e)
            if "429" in error_str or "rate" in error_str.lower():
                backoff_multiplier = min(backoff_multiplier * 2, 8)
                safe_print(f"[Solana] Rate limited, backing off {poll_interval * backoff_multiplier}s")
            else:
                safe_print(f"Solana Alchemy polling error: {e}")

        _shutdown_flag.wait(timeout=poll_interval * backoff_multiplier)


def test_helius_connection():
    """Test Alchemy Solana RPC connection (function name kept for compatibility)."""
    try:
        safe_print("Testing Alchemy Solana RPC connection...")
        rpc_url = get_alchemy_rpc('solana')
        if not rpc_url:
            safe_print("Alchemy Solana RPC URL not configured")
            return False
        result = _rpc_call(rpc_url, 'getHealth', [], cu_cost=10, timeout=10)
        if result == "ok":
            safe_print("Alchemy Solana RPC connection successful")
            return True
        else:
            safe_print(f"Alchemy Solana RPC health check: {result}")
            return result is not None
    except Exception as e:
        safe_print(f"Error connecting to Alchemy Solana: {e}")
        return False

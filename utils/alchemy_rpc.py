"""
Alchemy RPC client with rate limiting and multi-chain support.

Provides receipt fetching for $50k+ transactions and block-level polling
across Ethereum, Polygon, Solana, Bitcoin, and Tron — with automatic
fallback, CU budget tracking, and hourly usage logging.
"""

import time
import logging
import threading
import requests
from typing import Optional, Dict, Any, List

from config.api_keys import (
    ALCHEMY_API_KEY,
    ALCHEMY_ETHEREUM_RPC,
    ALCHEMY_POLYGON_RPC,
    ALCHEMY_SOLANA_RPC,
    ALCHEMY_BITCOIN_RPC,
    ALCHEMY_TRON_RPC,
    HELIUS_RPC_URL,
)

logger = logging.getLogger(__name__)

RECEIPT_USD_THRESHOLD = 50_000


class AlchemyRateLimiter:
    """Thread-safe rate limiter for Alchemy free tier (500 CU/s, 20M CU/month budget).

    Also enforces a concurrency limit to avoid Alchemy's "exceeded concurrent
    requests capacity" error on free-tier accounts.
    """

    def __init__(self, max_rps: int = 20, monthly_cu_budget: int = 20_000_000,
                 max_concurrent: int = 3):
        self.max_rps = max_rps
        self.monthly_budget = monthly_cu_budget
        self.cu_used = 0
        self._lock = threading.Lock()
        self._window_start = time.time()
        self._requests_in_window = 0
        self._last_log_time = time.time()
        self._LOG_INTERVAL = 3600  # log CU usage once per hour
        self._semaphore = threading.Semaphore(max_concurrent)

    def can_request(self, cu_cost: int = 25) -> bool:
        with self._lock:
            if self.cu_used + cu_cost > self.monthly_budget:
                return False
            now = time.time()
            if now - self._window_start >= 1.0:
                self._window_start = now
                self._requests_in_window = 0
            return self._requests_in_window < self.max_rps

    def record_request(self, cu_cost: int = 25):
        with self._lock:
            self.cu_used += cu_cost
            self._requests_in_window += 1
            now = time.time()
            if now - self._last_log_time >= self._LOG_INTERVAL:
                self._last_log_time = now
                pct = round(self.cu_used / self.monthly_budget * 100, 2)
                remaining = self.monthly_budget - self.cu_used
                logger.info(
                    f"Alchemy CU usage: {self.cu_used:,} / {self.monthly_budget:,} "
                    f"({pct}%) — {remaining:,} CU remaining"
                )

    def wait_if_needed(self, cu_cost: int = 25):
        """Block until a request slot is available."""
        while not self.can_request(cu_cost):
            time.sleep(0.05)
        self.record_request(cu_cost)


_rate_limiter = AlchemyRateLimiter()

_CHAIN_RPC_MAP = {
    'ethereum': ALCHEMY_ETHEREUM_RPC,
    'polygon': ALCHEMY_POLYGON_RPC,
    'solana': ALCHEMY_SOLANA_RPC,
    'bitcoin': ALCHEMY_BITCOIN_RPC,
}

_CHAIN_FALLBACK_MAP = {
    'solana': HELIUS_RPC_URL,
}


def get_rate_limiter():
    """Return the shared singleton rate limiter instance."""
    return _rate_limiter


def get_alchemy_rpc(blockchain: str) -> Optional[str]:
    return _CHAIN_RPC_MAP.get(blockchain)


def _rpc_call(rpc_url: str, method: str, params: list, timeout: int = 10, cu_cost: int = 25) -> Optional[Dict]:
    """Execute a JSON-RPC call with rate limiting and concurrency control."""
    _rate_limiter.wait_if_needed(cu_cost)
    with _rate_limiter._semaphore:
        try:
            resp = requests.post(rpc_url, json={
                'jsonrpc': '2.0',
                'method': method,
                'params': params,
                'id': 1,
            }, timeout=timeout)
            if resp.status_code == 429:
                return None
            data = resp.json()
            if 'error' in data:
                logger.warning(f"Alchemy RPC error ({method}): {data['error']}")
                return None
            return data.get('result')
        except Exception as e:
            logger.warning(f"Alchemy RPC call failed ({method}): {e}")
            return None


def _http_call(url: str, payload: Optional[Dict] = None, timeout: int = 10, cu_cost: int = 20) -> Optional[Dict]:
    """Execute an HTTP REST call (for Tron HTTP endpoints) with rate limiting."""
    _rate_limiter.wait_if_needed(cu_cost)
    try:
        body = payload if payload is not None else {}
        resp = requests.post(url, json=body, timeout=timeout)
        if resp.status_code == 429:
            return None
        return resp.json()
    except Exception as e:
        logger.warning(f"Alchemy HTTP call failed ({url.split('/')[-1]}): {e}")
        return None


# ---------------------------------------------------------------------------
# EVM helpers (Ethereum / Polygon)
# ---------------------------------------------------------------------------

def fetch_evm_receipt(tx_hash: str, blockchain: str = 'ethereum') -> Optional[Dict]:
    """Fetch full EVM transaction receipt from Alchemy (Ethereum/Polygon)."""
    rpc_url = get_alchemy_rpc(blockchain)
    if not rpc_url:
        return None
    result = _rpc_call(rpc_url, 'eth_getTransactionReceipt', [tx_hash], cu_cost=20)
    if result:
        logger.debug(f"Fetched receipt for {tx_hash[:16]}... ({blockchain})")
    return result


def fetch_evm_transaction(tx_hash: str, blockchain: str = 'ethereum') -> Optional[Dict]:
    """Fetch EVM transaction details (value, input, gas)."""
    rpc_url = get_alchemy_rpc(blockchain)
    if not rpc_url:
        return None
    return _rpc_call(rpc_url, 'eth_getTransactionByHash', [tx_hash], cu_cost=20)


def fetch_asset_transfers(blockchain: str, from_block: str, to_block: str,
                          contract_addresses: Optional[List[str]] = None,
                          category: Optional[List[str]] = None) -> Optional[List[Dict]]:
    """Call alchemy_getAssetTransfers for token transfer discovery (120 CU)."""
    rpc_url = get_alchemy_rpc(blockchain)
    if not rpc_url:
        return None
    params: Dict[str, Any] = {
        "fromBlock": from_block,
        "toBlock": to_block,
        "category": category or ["erc20"],
        "withMetadata": True,
        "excludeZeroValue": True,
        "maxCount": "0x3e8",
        "order": "desc",
    }
    if contract_addresses:
        params["contractAddresses"] = contract_addresses
    result = _rpc_call(rpc_url, 'alchemy_getAssetTransfers', [params], cu_cost=120, timeout=15)
    if result and 'transfers' in result:
        return result['transfers']
    return None


# ---------------------------------------------------------------------------
# Solana helpers
# ---------------------------------------------------------------------------

def fetch_solana_transaction(signature: str) -> Optional[Dict]:
    """Fetch parsed Solana transaction. Tries Alchemy first, then Helius."""
    rpc_url = get_alchemy_rpc('solana')
    if rpc_url:
        result = _rpc_call(rpc_url, 'getTransaction', [
            signature,
            {'encoding': 'jsonParsed', 'maxSupportedTransactionVersion': 0, 'commitment': 'confirmed'}
        ], cu_cost=40)
        if result:
            return result

    fallback = _CHAIN_FALLBACK_MAP.get('solana')
    if fallback:
        logger.info(f"Alchemy Solana failed, trying Helius for {signature[:16]}...")
        return _rpc_call(fallback, 'getTransaction', [
            signature,
            {'encoding': 'jsonParsed', 'maxSupportedTransactionVersion': 0, 'commitment': 'confirmed'}
        ], cu_cost=0)
    return None


def fetch_solana_signatures(mint_address: str, limit: int = 100, before: Optional[str] = None) -> Optional[List]:
    """Fetch recent signatures for a Solana address (40 CU)."""
    rpc_url = get_alchemy_rpc('solana')
    if not rpc_url:
        rpc_url = _CHAIN_FALLBACK_MAP.get('solana')
    if not rpc_url:
        return None
    params: Dict[str, Any] = {"limit": limit}
    if before:
        params["before"] = before
    cu = 40 if rpc_url == get_alchemy_rpc('solana') else 0
    return _rpc_call(rpc_url, 'getSignaturesForAddress', [mint_address, params], cu_cost=cu)


# ---------------------------------------------------------------------------
# Bitcoin helpers
# ---------------------------------------------------------------------------

def fetch_bitcoin_blockcount() -> Optional[int]:
    """Get current Bitcoin block height (10 CU)."""
    rpc_url = get_alchemy_rpc('bitcoin')
    if not rpc_url:
        return None
    return _rpc_call(rpc_url, 'getblockcount', [], cu_cost=10)


def fetch_bitcoin_blockhash(height: int) -> Optional[str]:
    """Get block hash for a given height (10 CU)."""
    rpc_url = get_alchemy_rpc('bitcoin')
    if not rpc_url:
        return None
    return _rpc_call(rpc_url, 'getblockhash', [height], cu_cost=10)


def fetch_bitcoin_block(blockhash: str, verbosity: int = 2) -> Optional[Dict]:
    """Fetch full Bitcoin block with transactions (10 CU). verbosity=2 includes decoded txs."""
    rpc_url = get_alchemy_rpc('bitcoin')
    if not rpc_url:
        return None
    return _rpc_call(rpc_url, 'getblock', [blockhash, verbosity], cu_cost=10, timeout=30)


def fetch_bitcoin_transaction(tx_hash: str) -> Optional[Dict]:
    """Fetch a Bitcoin transaction by hash (10 CU)."""
    rpc_url = get_alchemy_rpc('bitcoin')
    if not rpc_url:
        return None
    return _rpc_call(rpc_url, 'getrawtransaction', [tx_hash, True], cu_cost=10)


# ---------------------------------------------------------------------------
# Tron helpers
# ---------------------------------------------------------------------------

def fetch_tron_now_block() -> Optional[Dict]:
    """Get the latest Tron block via /wallet/getnowblock (20 CU)."""
    rpc_url = get_alchemy_rpc('tron')
    if not rpc_url:
        return None
    url = f"{rpc_url}/wallet/getnowblock"
    return _http_call(url, cu_cost=20)


def fetch_tron_block_txinfo(block_num: int) -> Optional[List]:
    """Get transaction info for all txs in a Tron block (20 CU)."""
    rpc_url = get_alchemy_rpc('tron')
    if not rpc_url:
        return None
    url = f"{rpc_url}/wallet/gettransactioninfobyblocknum"
    return _http_call(url, payload={"num": block_num}, cu_cost=20)


def fetch_tron_transaction(tx_id: str) -> Optional[Dict]:
    """Fetch a single Tron transaction by ID (20 CU)."""
    rpc_url = get_alchemy_rpc('tron')
    if not rpc_url:
        return None
    url = f"{rpc_url}/wallet/gettransactionbyid"
    return _http_call(url, payload={"value": tx_id}, cu_cost=20)


# ---------------------------------------------------------------------------
# Generic receipt helper
# ---------------------------------------------------------------------------

def fetch_receipt_if_needed(tx_hash: str, usd_value: float, blockchain: str) -> Optional[Dict]:
    """
    Fetch full transaction receipt/data for high-value transactions.
    Returns None for transactions below the $50k threshold or on failure.
    """
    if usd_value < RECEIPT_USD_THRESHOLD:
        return None

    if blockchain in ('ethereum', 'polygon'):
        return fetch_evm_receipt(tx_hash, blockchain)
    elif blockchain == 'solana':
        return fetch_solana_transaction(tx_hash)
    elif blockchain == 'bitcoin':
        return fetch_bitcoin_transaction(tx_hash)
    return None


# ---------------------------------------------------------------------------
# Solana token flow extraction
# ---------------------------------------------------------------------------

def extract_solana_token_flow(parsed_tx: Dict) -> Optional[Dict[str, Any]]:
    """
    Extract token flow from a parsed Solana transaction.
    Returns {'direction': 'BUY'/'SELL'/'TRANSFER', 'confidence': float, 'evidence': [...]}
    """
    if not parsed_tx:
        return None

    try:
        meta = parsed_tx.get('meta', {})
        if not meta or meta.get('err') is not None:
            return None

        pre_balances = {b['accountIndex']: b for b in (meta.get('preTokenBalances') or [])}
        post_balances = {b['accountIndex']: b for b in (meta.get('postTokenBalances') or [])}

        sol_stablecoins = {
            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': 'USDC',
            'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB': 'USDT',
        }

        tokens_in = []
        tokens_out = []

        for idx in set(list(pre_balances.keys()) + list(post_balances.keys())):
            pre = pre_balances.get(idx, {})
            post = post_balances.get(idx, {})
            pre_amt = float(pre.get('uiTokenAmount', {}).get('uiAmount') or 0)
            post_amt = float(post.get('uiTokenAmount', {}).get('uiAmount') or 0)
            mint = post.get('mint') or pre.get('mint', '')
            owner = post.get('owner') or pre.get('owner', '')
            diff = post_amt - pre_amt

            if abs(diff) < 0.001:
                continue

            token_name = sol_stablecoins.get(mint, mint[:8] if mint else 'UNKNOWN')
            entry = {'token': token_name, 'mint': mint, 'amount': abs(diff), 'owner': owner}

            if diff > 0:
                tokens_in.append(entry)
            else:
                tokens_out.append(entry)

        if not tokens_in and not tokens_out:
            return None

        stables_out = [t for t in tokens_out if t['token'] in ('USDC', 'USDT')]
        stables_in = [t for t in tokens_in if t['token'] in ('USDC', 'USDT')]
        volatile_out = [t for t in tokens_out if t['token'] not in ('USDC', 'USDT')]
        volatile_in = [t for t in tokens_in if t['token'] not in ('USDC', 'USDT')]

        if stables_out and volatile_in:
            return {'direction': 'BUY', 'confidence': 0.90,
                    'evidence': [f"Solana: spent {stables_out[0]['token']}, received {volatile_in[0]['token']}"]}
        elif volatile_out and stables_in:
            return {'direction': 'SELL', 'confidence': 0.90,
                    'evidence': [f"Solana: spent {volatile_out[0]['token']}, received {stables_in[0]['token']}"]}
        elif tokens_out and tokens_in:
            return {'direction': 'TRANSFER', 'confidence': 0.70,
                    'evidence': [f"Solana: token swap without clear stablecoin direction"]}

        return None
    except Exception as e:
        logger.warning(f"Solana token flow extraction failed: {e}")
        return None


def get_rate_limiter_stats() -> Dict[str, Any]:
    return {
        'cu_used': _rate_limiter.cu_used,
        'cu_budget': _rate_limiter.monthly_budget,
        'cu_remaining': _rate_limiter.monthly_budget - _rate_limiter.cu_used,
        'utilization_pct': round(_rate_limiter.cu_used / _rate_limiter.monthly_budget * 100, 2),
    }

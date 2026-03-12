"""
Ethereum ERC-20 whale transaction monitor.

Primary: Alchemy alchemy_getAssetTransfers (single call covers all tokens).
Fallback: Etherscan tokentx per contract (if Alchemy fails).

Polls every 60 seconds.
"""

import time
import logging
import requests
import random
from typing import Dict, List, Optional

from config.api_keys import ETHERSCAN_API_KEY, ETHERSCAN_API_KEYS
from config.settings import (
    GLOBAL_USD_THRESHOLD,
    last_processed_block,
    etherscan_buy_counts,
    etherscan_sell_counts,
    print_lock,
    shutdown_flag,
)
from data.tokens import TOKENS_TO_MONITOR, TOKEN_PRICES
from utils.classification_final import WhaleIntelligenceEngine, comprehensive_stablecoin_analysis
from utils.base_helpers import safe_print, log_error
from utils.summary import record_transfer
from utils.summary import has_been_classified, mark_as_classified
from data.market_makers import MARKET_MAKER_ADDRESSES, FILTER_SETTINGS
from utils.dedup import deduplicator, get_dedup_stats, deduped_transactions, handle_event

logger = logging.getLogger(__name__)

# Global variable for batch timing
last_batch_storage_time = time.time()

POLL_INTERVAL = 60

def _is_whale_relevant_transaction(from_addr: str, to_addr: str, token_symbol: str) -> bool:
    """
    PROFESSIONAL WHALE FILTERING: Only process transactions relevant to whale monitoring.

    Filters out random wallet-to-wallet transfers and focuses on:
    - DEX router interactions (Uniswap, SushiSwap, 1inch, etc.)
    - CEX deposits/withdrawals (Binance, Coinbase, etc.)
    - Major DeFi protocol interactions (Aave, Compound, etc.)
    - Bridge transactions (cross-chain activity)
    """
    from_addr = from_addr.lower()
    to_addr = to_addr.lower()

    dex_routers = {
        '0x7a250d5630b4cf539739df2c5dacb4c659f2488d',  # Uniswap V2 Router
        '0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45',  # Uniswap V3 Router
        '0xe592427a0aece92de3edee1f18e0157c05861564',  # Uniswap V3 Router 2
        '0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad',  # Uniswap Universal Router
        '0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f',  # SushiSwap Router
        '0x1111111254fb6c44bac0bed2854e76f90643097d',  # 1inch Router V4
        '0xdef1c0ded9bec7f1a1670819833240f027b25eff',  # 0x Protocol Exchange
        '0x99a58482ba3d06e0e1e9444c8b7a8c7649e8c9c1',  # Curve Router
        '0xba12222222228d8ba445958a75a0704d566bf2c8',  # Balancer V2 Vault
        '0x9008d19f58aabd9ed0d60971565aa8510560ab41',  # CoW Protocol Settlement
    }

    cex_addresses = {
        '0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be',  # Binance Hot Wallet
        '0xd551234ae421e3bcba99a0da6d736074f22192ff',  # Binance Hot Wallet 2
        '0x28c6c06298d514db089934071355e5743bf21d60',  # Binance Hot Wallet 14
        '0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43',  # Coinbase Hot Wallet
        '0x71660c4005ba85c37ccec55d0c4493e66fe775d3',  # Coinbase Hot Wallet 2
        '0x2910543af39aba0cd09dbb2d50200b3e800a63d2',  # Kraken Hot Wallet
        '0x6cc5f688a315f3dc28a7781717a9a798a59fda7b',  # OKEx Hot Wallet
        '0xdc76cd25977e0a5ae17155770273ad58648900d3',  # Huobi Hot Wallet
        '0xf89d7b9c864f589bbf53a82105107622b35eaa40',  # Bybit Hot Wallet
        '0x1522900b6dafac587d499a862861c0869be6e428',  # KuCoin Hot Wallet
    }

    defi_protocols = {
        '0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9',  # Aave Lending Pool
        '0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2',  # Aave Pool V3
        '0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b',  # Compound cDAI
        '0x39aa39c021dfbae8fac545936693ac917d5e7563',  # Compound cUSDC
        '0xae7ab96520de3a18e5e111b5eaab95820216e558',  # Lido stETH
        '0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7',  # Curve 3Pool
    }

    bridge_contracts = {
        '0x40ec5b33f54e0e8a33a975908c5ba1c14e5bbbdf',  # Polygon Bridge
        '0x8315177ab297ba92a06054ce80a67ed4dbd7ed3a',  # Arbitrum Bridge
        '0x99c9fc46f92e8a1c0dec1b1747d010903e884be1',  # Optimism Gateway
        '0x6b7a87899490ece95443e979ca9485cbe7e71522',  # Multichain Router
    }

    whale_relevant_addresses = dex_routers | cex_addresses | defi_protocols | bridge_contracts
    is_relevant = from_addr in whale_relevant_addresses or to_addr in whale_relevant_addresses

    major_tokens = {'ETH', 'WETH', 'USDC', 'USDT', 'DAI'}
    if token_symbol in major_tokens:
        return is_relevant
    else:
        return True


# ---------------------------------------------------------------------------
# Alchemy primary: alchemy_getAssetTransfers
# ---------------------------------------------------------------------------

def _alchemy_get_block_number() -> Optional[int]:
    """Get latest Ethereum block number via Alchemy."""
    try:
        from utils.alchemy_rpc import fetch_eth_block_number
        return fetch_eth_block_number('ethereum')
    except Exception as e:
        logger.warning(f"Alchemy Ethereum block number error: {e}")
    return None


def _alchemy_fetch_transfers(from_hex: str, to_hex: str, contract_addresses: list) -> Optional[list]:
    """Fetch ERC-20 transfers via Alchemy alchemy_getAssetTransfers (single call for all tokens)."""
    try:
        from utils.alchemy_rpc import fetch_asset_transfers
        transfers = fetch_asset_transfers(
            blockchain='ethereum',
            from_block=from_hex,
            to_block=to_hex,
            contract_addresses=contract_addresses,
            category=["erc20"],
        )
        return transfers
    except Exception as e:
        logger.warning(f"Alchemy Ethereum transfers error: {e}")
        return None


def _process_alchemy_transfer(tx: dict, contract_map: dict) -> Optional[dict]:
    """Process an Alchemy alchemy_getAssetTransfers result into an event."""
    raw_contract = (tx.get("rawContract") or {})
    contract_addr = raw_contract.get("address", "").lower()
    symbol_info = contract_map.get(contract_addr)
    if not symbol_info:
        return None

    symbol, decimals = symbol_info
    price = TOKEN_PRICES.get(symbol, 0)
    if price == 0:
        return None

    value = tx.get("value")
    if value is None:
        raw_hex = raw_contract.get("value", "0x0")
        try:
            raw_int = int(raw_hex, 16)
            value = raw_int / (10 ** decimals)
        except (ValueError, TypeError):
            return None

    token_amount = float(value)
    usd_value = token_amount * price
    if usd_value < GLOBAL_USD_THRESHOLD:
        return None

    from_addr = tx.get("from", "")
    to_addr = tx.get("to", "")

    # Apply whale relevance filter
    if not _is_whale_relevant_transaction(from_addr, to_addr, symbol):
        return None

    # Extract timestamp from metadata if available
    metadata = tx.get("metadata", {})
    block_timestamp = metadata.get("blockTimestamp", "")
    timestamp = time.time()
    if block_timestamp:
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(block_timestamp.replace("Z", "+00:00"))
            timestamp = dt.timestamp()
        except Exception:
            pass

    return {
        "blockchain": "ethereum",
        "tx_hash": tx.get("hash", ""),
        "from": from_addr,
        "to": to_addr,
        "symbol": symbol,
        "amount": token_amount,
        "estimated_usd": usd_value,
        "usd_value": usd_value,
        "timestamp": timestamp,
        "source": "ethereum_alchemy",
        "block_num": tx.get("blockNum", ""),
    }


# ---------------------------------------------------------------------------
# Etherscan fallback (per-token)
# ---------------------------------------------------------------------------

def _etherscan_get_block_number() -> Optional[int]:
    """Get latest Ethereum block number via Etherscan (fallback)."""
    try:
        api_key = random.choice(ETHERSCAN_API_KEYS)
        resp = requests.get("https://api.etherscan.io/v2/api", params={
            "chainid": 1,
            "module": "proxy",
            "action": "eth_blockNumber",
            "apikey": api_key,
        }, timeout=10)
        data = resp.json()
        result = data.get("result")
        if result:
            return int(result, 16)
    except Exception as e:
        logger.warning(f"Etherscan block number error: {e}")
    return None


def _get_eth_block_number() -> Optional[int]:
    """Get block number — try Alchemy first, Etherscan fallback."""
    block = _alchemy_get_block_number()
    if block:
        return block
    return _etherscan_get_block_number()


def fetch_erc20_transfers(contract_address, sort="desc", start_block: int = 0, end_block: int = 99999999, page: int | None = None, offset: int | None = None):
    """Etherscan fallback: fetch ERC-20 transfers for a single contract."""
    url = "https://api.etherscan.io/v2/api"
    api_key = random.choice(ETHERSCAN_API_KEYS)

    params = {
        "chainid": 1,
        "module": "account",
        "action": "tokentx",
        "contractaddress": contract_address,
        "startblock": start_block,
        "endblock": end_block,
        "sort": sort,
        "apikey": api_key
    }
    if page is not None:
        params["page"] = page
    if offset is not None:
        params["offset"] = offset

    max_attempts = 4
    backoff = 1.5
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()

            status = data.get("status")
            message = data.get("message", "")
            result = data.get("result", [])

            if status == "0" and message == "No transactions found":
                return []

            if status == "1" and isinstance(result, list):
                return result

            params["apikey"] = random.choice(ETHERSCAN_API_KEYS)
            time.sleep(backoff * attempt)
            continue

        except requests.RequestException as e:
            last_error = e
            params["apikey"] = random.choice(ETHERSCAN_API_KEYS)
            time.sleep(backoff * attempt)
            continue
        except Exception as e:
            log_error(f"Etherscan fetch error: {e}")
            return []

    if last_error:
        log_error(f"Etherscan fetch failed after retries: {last_error}")
    return []


def _etherscan_fallback_poll(start_block: int, end_block: int):
    """Fallback: poll Etherscan per-token when Alchemy is unavailable."""
    processed = 0
    for symbol, info in TOKENS_TO_MONITOR.items():
        contract = info["contract"]
        decimals = info["decimals"]
        price = TOKEN_PRICES.get(symbol, 0)
        if price == 0:
            continue

        transfers = fetch_erc20_transfers(contract, sort="desc", start_block=start_block, end_block=end_block)
        if not transfers:
            continue

        for tx in transfers:
            try:
                raw_value = int(tx["value"])
                token_amount = raw_value / (10 ** decimals)
                estimated_usd = token_amount * price

                if estimated_usd < GLOBAL_USD_THRESHOLD:
                    continue

                from_addr = tx["from"]
                to_addr = tx["to"]
                tx_hash = tx["hash"]

                if not _is_whale_relevant_transaction(from_addr, to_addr, symbol):
                    continue

                event = {
                    "blockchain": "ethereum",
                    "tx_hash": tx_hash,
                    "from": from_addr,
                    "to": to_addr,
                    "symbol": symbol,
                    "amount": token_amount,
                    "estimated_usd": estimated_usd,
                    "usd_value": estimated_usd,
                    "block_number": int(tx["blockNumber"]),
                    "timestamp": int(tx.get("timeStamp", "0")),
                    "source": "etherscan_fallback",
                }

                _classify_and_store(event)
                processed += 1
            except Exception as e:
                log_error(f"Error processing {symbol} transfer (Etherscan fallback): {e}")
                continue

        time.sleep(0.25)  # Rate limit

    return processed


# ---------------------------------------------------------------------------
# Shared classify + store pipeline
# ---------------------------------------------------------------------------

def _classify_and_store(event: dict):
    """Classify an Ethereum event and route through the dedup/storage pipeline."""
    tx_hash = event.get("tx_hash", "")
    symbol = event.get("symbol", "")
    usd_value = event.get("estimated_usd") or event.get("usd_value", 0)

    # Fetch full receipt from Alchemy for $50k+ transactions
    if usd_value >= 50_000:
        try:
            from utils.alchemy_rpc import fetch_evm_receipt
            receipt = fetch_evm_receipt(tx_hash, 'ethereum')
            if receipt:
                event['receipt'] = receipt
        except Exception:
            pass

    # Classify via WhaleIntelligenceEngine
    try:
        from utils.classification_final import process_and_enrich_transaction
        enriched = process_and_enrich_transaction(event)
    except Exception:
        enriched = None

    classification = 'TRANSFER'
    confidence = 0.0
    if enriched:
        if isinstance(enriched, dict):
            classification = enriched.get('classification', 'TRANSFER')
            confidence = enriched.get('confidence', 0.0)
        elif hasattr(enriched, 'classification'):
            classification = enriched.classification.value if hasattr(enriched.classification, 'value') else str(enriched.classification)
            confidence = getattr(enriched, 'confidence', 0.0)

    event['classification'] = classification.upper()
    event['usd_value'] = usd_value
    handle_event(event)

    if enriched:
        # Update counters
        if classification.upper() in ("BUY", "MODERATE_BUY", "BUY_MODERATE"):
            etherscan_buy_counts[symbol] += 1
        elif classification.upper() in ("SELL", "MODERATE_SELL", "SELL_MODERATE"):
            etherscan_sell_counts[symbol] += 1

        whale_indicator = " 🐋" if isinstance(enriched, dict) and enriched.get('is_whale_transaction') else ""
        current_time = time.strftime('%Y-%m-%d %H:%M:%S')
        safe_print(f"\n[ETH - {symbol} | ${usd_value:,.2f} USD] Block {event.get('block_num', event.get('block_number', '?'))}{whale_indicator}")
        safe_print(f"  Time: {current_time}")
        safe_print(f"  From: {event.get('from', '')}")
        safe_print(f"  To:   {event.get('to', '')}")
        safe_print(f"  Amount: {event.get('amount', 0):,.2f} {symbol} (~${usd_value:,.2f} USD)")
        safe_print(f"  Classification: {classification.upper()} (confidence: {confidence:.2f})")

        if isinstance(enriched, dict) and enriched.get('whale_classification'):
            safe_print(f"  Whale Analysis: {enriched['whale_classification']}")

        record_transfer(symbol, event.get('amount', 0), event.get('from', ''), event.get('to', ''), tx_hash)

    # Persist to Supabase
    if enriched:
        try:
            from utils.supabase_writer import store_transaction
            classification_data = {
                'classification': classification.upper() if classification else 'TRANSFER',
                'confidence': confidence,
                'whale_score': enriched.get('whale_score', 0.0) if isinstance(enriched, dict) else getattr(enriched, 'final_whale_score', 0.0),
                'reasoning': enriched.get('reasoning', '') if isinstance(enriched, dict) else getattr(enriched, 'master_classifier_reasoning', ''),
            }
            store_transaction(event, classification_data)
        except Exception as e:
            safe_print(f"  Supabase write error: {e}")


# ---------------------------------------------------------------------------
# Main polling loop
# ---------------------------------------------------------------------------

def print_new_erc20_transfers():
    """
    Continuously polls for large Ethereum ERC-20 transfers.
    Primary: Alchemy (single call). Fallback: Etherscan (per-token).
    """
    safe_print("✅ Ethereum Alchemy monitor started (60s interval)")

    contract_addresses = [
        info["contract"] for info in TOKENS_TO_MONITOR.values()
    ]
    contract_map = {
        info["contract"].lower(): (symbol, info["decimals"])
        for symbol, info in TOKENS_TO_MONITOR.items()
    }

    current_block = _get_eth_block_number()
    if current_block:
        safe_print(f"   Ethereum tip: block {current_block}")
        last_block = current_block
    else:
        safe_print("⚠️  Ethereum: could not fetch initial block, will retry")
        last_block = None

    seen_hashes = set()

    while not shutdown_flag.is_set():
        try:
            tip = _get_eth_block_number()
            if tip is None:
                safe_print("⚠️  Ethereum: block fetch failed, retrying...")
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            if last_block is None:
                last_block = tip
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            if tip <= last_block:
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            from_hex = hex(last_block + 1)
            to_hex = hex(tip)
            blocks_scanned = tip - last_block

            # Try Alchemy first (single call for all tokens)
            transfers = _alchemy_fetch_transfers(from_hex, to_hex, contract_addresses)

            if transfers is not None:
                processed = 0
                for tx in transfers:
                    event = _process_alchemy_transfer(tx, contract_map)
                    if event and event['tx_hash'] not in seen_hashes:
                        seen_hashes.add(event['tx_hash'])
                        _classify_and_store(event)
                        processed += 1
                if processed > 0:
                    safe_print(f"  Ethereum: {processed} whale tx in {blocks_scanned} blocks (Alchemy)")
            else:
                # Fallback: Etherscan per-token
                safe_print(f"  Ethereum: Alchemy unavailable, using Etherscan fallback")
                processed = _etherscan_fallback_poll(last_block + 1, tip)
                if processed > 0:
                    safe_print(f"  Ethereum: {processed} whale tx in {blocks_scanned} blocks (Etherscan fallback)")

            last_block = tip

            # Trim seen_hashes to prevent unbounded growth
            if len(seen_hashes) > 10000:
                seen_hashes.clear()

        except Exception as e:
            logger.warning(f"Ethereum poll error: {e}")

        shutdown_flag.wait(timeout=POLL_INTERVAL)


def test_etherscan_connection():
    """Test Alchemy + Etherscan API connection."""
    # Test Alchemy first
    block = _alchemy_get_block_number()
    if block:
        safe_print(f"✅ Ethereum Alchemy connection OK (block {block})")
        return True

    # Fallback to Etherscan test
    try:
        api_key = ETHERSCAN_API_KEYS[0]
        url = "https://api.etherscan.io/v2/api"
        params = {
            "chainid": 1,
            "module": "stats",
            "action": "ethsupply",
            "apikey": api_key
        }
        r = requests.get(url, params=params, timeout=20)
        data = r.json()
        if data.get("status") == "1":
            safe_print("✅ Etherscan API connection successful (Alchemy unavailable)")
            return True
        else:
            safe_print(f"❌ Etherscan API error: {data.get('message', 'No message')}")
            return False
    except Exception as e:
        safe_print(f"❌ Error connecting to Ethereum APIs: {e}")
        return False

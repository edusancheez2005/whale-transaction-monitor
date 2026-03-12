"""
Polygon PoS whale transaction monitor.

Primary: Alchemy alchemy_getAssetTransfers (single call covers all tokens).
Fallback: PolygonScan tokentx per contract (if Alchemy fails).

Polls every 60 seconds.
"""

import time
import logging
import requests

from config.api_keys import POLYGONSCAN_API_KEY
from config.settings import (
    polygon_last_processed_block,
    GLOBAL_USD_THRESHOLD,
    shutdown_flag,
)
from data.tokens import POLYGON_TOKENS_TO_MONITOR, TOKEN_PRICES
from utils.base_helpers import safe_print, log_error

logger = logging.getLogger(__name__)

POLL_INTERVAL = 60
POLYGONSCAN_API_BASE = "https://api.polygonscan.com/api"


def _alchemy_get_block_number() -> int | None:
    """Get latest Polygon block number via Alchemy."""
    try:
        from utils.alchemy_rpc import get_alchemy_rpc, _rpc_call
        rpc_url = get_alchemy_rpc('polygon')
        if not rpc_url:
            return None
        result = _rpc_call(rpc_url, 'eth_blockNumber', [], cu_cost=10)
        if result:
            return int(result, 16)
    except Exception as e:
        logger.warning(f"Alchemy Polygon block number error: {e}")
    return None


def _polygonscan_get_block_number() -> int | None:
    """Get latest Polygon block number via PolygonScan (fallback)."""
    try:
        resp = requests.get(POLYGONSCAN_API_BASE, params={
            "module": "proxy",
            "action": "eth_blockNumber",
            "apikey": POLYGONSCAN_API_KEY,
        }, timeout=10)
        data = resp.json()
        result = data.get("result")
        if result:
            return int(result, 16)
    except Exception as e:
        logger.warning(f"PolygonScan block number error: {e}")
    return None


def _get_polygon_block_number() -> int | None:
    """Get block number — try Alchemy first, PolygonScan fallback."""
    block = _alchemy_get_block_number()
    if block:
        return block
    return _polygonscan_get_block_number()


def _alchemy_fetch_transfers(from_hex: str, to_hex: str, contract_addresses: list) -> list | None:
    """Fetch ERC-20 transfers via Alchemy alchemy_getAssetTransfers (single call for all tokens)."""
    try:
        from utils.alchemy_rpc import fetch_asset_transfers
        transfers = fetch_asset_transfers(
            blockchain='polygon',
            from_block=from_hex,
            to_block=to_hex,
            contract_addresses=contract_addresses,
            category=["erc20"],
        )
        return transfers
    except Exception as e:
        logger.warning(f"Alchemy Polygon transfers error: {e}")
        return None


def _polygonscan_fetch_transfers(contract_address: str, start_block: int, end_block: int) -> list:
    """Fetch ERC-20 token transfers from PolygonScan (per-token fallback)."""
    try:
        resp = requests.get(POLYGONSCAN_API_BASE, params={
            "module": "account",
            "action": "tokentx",
            "contractaddress": contract_address,
            "startblock": start_block,
            "endblock": end_block,
            "page": 1,
            "offset": 100,
            "sort": "desc",
            "apikey": POLYGONSCAN_API_KEY,
        }, timeout=15)
        data = resp.json()
        if data.get("status") == "1" and data.get("result"):
            return data["result"]
    except Exception as e:
        logger.warning(f"PolygonScan token transfer error: {e}")
    return []


def _process_alchemy_transfer(tx: dict, contract_map: dict) -> dict | None:
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

    usd_value = float(value) * price
    polygon_threshold = 5_000  # $5K threshold for Polygon ERC-20
    if usd_value < polygon_threshold:
        return None

    return {
        "blockchain": "polygon",
        "tx_hash": tx.get("hash", ""),
        "from": tx.get("from", ""),
        "to": tx.get("to", ""),
        "symbol": symbol,
        "amount": float(value),
        "usd_value": usd_value,
        "timestamp": time.time(),
        "source": "polygon_alchemy",
        "block_num": tx.get("blockNum", ""),
    }


def _process_polygonscan_transfer(tx: dict, symbol: str, decimals: int, price: float) -> dict | None:
    """Process a PolygonScan tokentx result into an event."""
    try:
        raw_value = int(tx.get("value", "0"))
        token_amount = raw_value / (10 ** decimals)
        usd_value = token_amount * price

        polygon_threshold = 1_000  # Lower threshold for Polygon
        if usd_value < polygon_threshold:
            return None

        return {
            "blockchain": "polygon",
            "tx_hash": tx.get("hash", ""),
            "from": tx.get("from", ""),
            "to": tx.get("to", ""),
            "symbol": symbol,
            "amount": token_amount,
            "usd_value": usd_value,
            "timestamp": int(tx.get("timeStamp", time.time())),
            "source": "polygonscan",
            "block_num": tx.get("blockNumber", ""),
        }
    except (ValueError, TypeError):
        return None


def _classify_and_store(event: dict):
    """Classify a Polygon event with chain-specific logic and store via dedup pipeline."""
    from utils.dedup import handle_event

    # Polygon-specific DEX/CEX address matching
    POLYGON_DEX_ADDRESSES = {
        '0xa5e0829caced82f9edc736e8167366c1e5104d41',  # QuickSwap Router
        '0xe592427a0aece92de3edee1f18e0157c05861564',  # Uniswap V3 Router
        '0x1b02da8cb0d097eb8d57a175b88c7d8b47997506',  # SushiSwap Router
        '0xdef1c0ded9bec7f1a1670819833240f027b25eff',  # 0x Exchange Proxy
        '0x3a1d87f206d12415f5b0a33e786967680aab4f6d',  # Balancer Vault (Polygon)
        '0xba12222222228d8ba445958a75a0704d566bf2c8',  # Balancer V2 Vault
        '0x1111111254fb6c44bac0bed2854e76f90643097d',  # 1inch V4 Router
        '0x1111111254eeb25477b68fb85ed929f73a960582',  # 1inch V5 Router
        '0xdef171fe48cf0115b1d80b88dc8eab59176fee57',  # Paraswap V5
        '0x6131b5fae19ea4f9d964eac0408e4408b66337b5',  # Kyber Network Router
        '0xf5b509bb0909a69b1c207e495f687a596c168e12',  # QuickSwap V3
    }
    POLYGON_CEX_ADDRESSES = {
        '0x28c6c06298d514db089934071355e5743bf21d60',  # Binance Hot Wallet
        '0x21a31ee1afc51d94c2efccaa2092ad1028285549',  # Binance 2
        '0x1ab4973a48dc892cd9971ece8e01dcc7688f8f23',  # Binance 3
        '0xe7804c37c13166ff0b37f5ae0bb07a3aebb6e245',  # Binance 48 (from PolygonScan)
        '0xf977814e90da44bfa03b6295a0616a897441acec',  # Binance 8
        '0x72a53cdbbcc1b9efa39c834a540550e23463aacb',  # Crypto.com
        '0x6cc5f688a315f3dc28a7781717a9a798a59fda7b',  # OKX
        '0x4c569c1e53db4636e0f700ba68bc2efb5882c2e0',  # Upbit (verified PolygonScan - 429M POL)\n        '0x2933782b20aded1b2e989c4dc54ee6d7242f1b57',  # KuCoin (verified PolygonScan)
        '0x3727cfcb9aa21b8f12e6c2c59a68e494484c4bb8',  # Bitbank 2 (verified PolygonScan)
        '0x76ec5a0d62678fbd3',                          # BtcTurk 13
        '0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43',  # Coinbase
        '0x71660c4005ba85c37ccec55d0c4493e66fe775d3',  # Coinbase 2
        '0x2910543af39aba0cd09dbb2d50200b3e800a63d2',  # Kraken
        '0xf89d7b9c864f589bbf53a82105107622b35eaa40',  # Bybit
    }
    POLYGON_DEFI = {
        '0x794a61358d6845594f94dc1db02a252b5b4814ad',  # Aave V3 Pool
        '0x8dff5e27ea6b7ac08ebfdf9eb090f32ee9a30fcf',  # Aave V2 Pool
        '0x1a13f4ca1d028320a707d99520abfefca3998b7f',  # Aave V2 amUSDC
    }

    from_addr = event.get('from', '').lower()
    to_addr = event.get('to', '').lower()
    classification = 'TRANSFER'

    # Chain-specific classification
    from_is_cex = from_addr in POLYGON_CEX_ADDRESSES
    to_is_cex = to_addr in POLYGON_CEX_ADDRESSES
    from_is_dex = from_addr in POLYGON_DEX_ADDRESSES
    to_is_dex = to_addr in POLYGON_DEX_ADDRESSES
    from_is_defi = from_addr in POLYGON_DEFI
    to_is_defi = to_addr in POLYGON_DEFI

    if from_is_cex and not to_is_cex:
        classification = 'BUY'    # Withdrawal from exchange
    elif to_is_cex and not from_is_cex:
        classification = 'SELL'   # Deposit to exchange
    elif from_is_dex or to_is_dex:
        classification = 'BUY' if from_is_dex else 'SELL'
    elif from_is_defi or to_is_defi:
        classification = 'TRANSFER'  # DeFi interaction
    else:
        # Fall back to WhaleIntelligenceEngine for unknown addresses
        try:
            from utils.classification_final import process_and_enrich_transaction
            enriched = process_and_enrich_transaction(event)
            if enriched and isinstance(enriched, dict):
                classification = enriched.get('classification', 'TRANSFER').upper()
        except Exception:
            pass

    event['classification'] = classification
    handle_event(event)

    from config.settings import polygon_buy_counts, polygon_sell_counts
    if 'BUY' in classification:
        polygon_buy_counts[event['symbol']] += 1
    elif 'SELL' in classification:
        polygon_sell_counts[event['symbol']] += 1

    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    safe_print(
        f"\n[POLYGON - {event['symbol']} | ${event['usd_value']:,.0f} USD] "
        f"Block {event.get('block_num', '?')}"
    )
    safe_print(f"  Time : {current_time}")
    safe_print(f"  TX   : {event['tx_hash'][:24]}...")
    safe_print(f"  Classification: {classification}")


def print_new_polygon_transfers():
    """
    Continuously polls for large Polygon ERC-20 transfers.
    Primary: Alchemy (single call). Fallback: PolygonScan (per-token).
    """
    safe_print("✅ Polygon monitor started (60s interval)")

    contract_addresses = [
        info["contract"] for info in POLYGON_TOKENS_TO_MONITOR.values()
    ]
    contract_map = {
        info["contract"].lower(): (symbol, info["decimals"])
        for symbol, info in POLYGON_TOKENS_TO_MONITOR.items()
    }

    current_block = _get_polygon_block_number()
    if current_block:
        safe_print(f"   Polygon tip: block {current_block}")
        last_block = current_block
    else:
        safe_print("⚠️  Polygon: could not fetch initial block, will retry")
        last_block = None

    seen_hashes = set()

    while not shutdown_flag.is_set():
        try:
            tip = _get_polygon_block_number()
            if tip is None:
                safe_print("Polygon: block fetch failed, retrying...")
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            if last_block is None:
                last_block = tip
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            if tip <= last_block:
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            # Polygon produces ~30 blocks per 60s poll interval (2s block time)
            # Cap to avoid scanning too many blocks at once
            scan_from = max(last_block + 1, tip - 60)
            from_hex = hex(scan_from)
            to_hex = hex(tip)
            blocks_scanned = tip - scan_from + 1

            safe_print(f"  Polygon: scanning blocks {scan_from}-{tip} ({blocks_scanned} blocks)")

            # Try Alchemy first (single call for all tokens)
            transfers = _alchemy_fetch_transfers(from_hex, to_hex, contract_addresses)

            if transfers is not None:
                # Process Alchemy results
                processed = 0
                for tx in transfers:
                    event = _process_alchemy_transfer(tx, contract_map)
                    if event and event['tx_hash'] not in seen_hashes:
                        seen_hashes.add(event['tx_hash'])
                        _classify_and_store(event)
                        processed += 1
                if processed > 0:
                    safe_print(f"  Polygon: {processed} ERC-20 whale tx in {blocks_scanned} blocks")

            # Also scan native MATIC transfers (external category)
            try:
                from utils.alchemy_rpc import fetch_asset_transfers as _fetch_at
                matic_transfers = _fetch_at('polygon', from_hex, to_hex, category=['external'])
                if matic_transfers:
                    matic_price = TOKEN_PRICES.get('MATIC', TOKEN_PRICES.get('WMATIC', 1.0))
                    matic_processed = 0
                    for tx in matic_transfers:
                        val = float(tx.get('value', 0) or 0)
                        usd_value = val * matic_price
                        if usd_value < 5_000:  # $5K threshold for native MATIC
                            continue
                        tx_hash = tx.get('hash', '')
                        if tx_hash in seen_hashes:
                            continue
                        seen_hashes.add(tx_hash)
                        event = {
                            "blockchain": "polygon",
                            "tx_hash": tx_hash,
                            "from": tx.get("from", ""),
                            "to": tx.get("to", ""),
                            "symbol": "MATIC",
                            "amount": val,
                            "usd_value": usd_value,
                            "timestamp": time.time(),
                            "source": "polygon_alchemy_native",
                            "block_num": tx.get("blockNum", ""),
                        }
                        _classify_and_store(event)
                        matic_processed += 1
                    if matic_processed > 0:
                        safe_print(f"  Polygon: {matic_processed} native MATIC whale tx in {blocks_scanned} blocks")
            except Exception as e:
                logger.warning(f"Polygon native MATIC scan error: {e}")

            if transfers is None:
                # Fallback: PolygonScan per-token
                safe_print(f"  Polygon: Alchemy unavailable, using PolygonScan fallback")
                processed = 0
                for symbol, info in POLYGON_TOKENS_TO_MONITOR.items():
                    price = TOKEN_PRICES.get(symbol, 0)
                    if price == 0:
                        continue
                    raw_transfers = _polygonscan_fetch_transfers(
                        info["contract"], last_block + 1, tip
                    )
                    time.sleep(0.25)  # Rate limit: 5 calls/sec free tier

                    for tx in raw_transfers:
                        tx_hash = tx.get("hash", "")
                        if tx_hash in seen_hashes:
                            continue
                        event = _process_polygonscan_transfer(tx, symbol, info["decimals"], price)
                        if event:
                            seen_hashes.add(tx_hash)
                            _classify_and_store(event)
                            processed += 1

                if processed > 0:
                    safe_print(f"  Polygon: {processed} whale tx in {blocks_scanned} blocks (PolygonScan)")

            last_block = tip

            # Trim seen_hashes to prevent unbounded growth
            if len(seen_hashes) > 10000:
                seen_hashes.clear()

        except Exception as e:
            logger.warning(f"Polygon poll error: {e}")

        shutdown_flag.wait(timeout=POLL_INTERVAL)


def test_polygonscan_connection():
    """Quick check that Polygon APIs are reachable."""
    block = _get_polygon_block_number()
    if block:
        safe_print(f"✅ Polygon connection OK (block {block})")
        return True
    safe_print("⚠️  Polygon connection failed")
    return False

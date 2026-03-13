"""
Bitcoin whale transaction monitor using Alchemy Bitcoin RPC.

Polls for new blocks every 30 seconds.  When a new block arrives the full
block (verbosity=2) is fetched and every transaction output is checked
against the USD threshold.  Qualifying transfers are printed and routed
through the classification / dedup pipeline.

CU budget: ~700 CU/hour (extremely efficient — Bitcoin produces ~6 blocks/hr).
"""

import time
import logging
from typing import Optional

from config.settings import shutdown_flag, GLOBAL_USD_THRESHOLD
from data.tokens import TOKEN_PRICES
from utils.base_helpers import safe_print
from utils.alchemy_rpc import (
    fetch_bitcoin_blockcount,
    fetch_bitcoin_blockhash,
    fetch_bitcoin_block,
)

logger = logging.getLogger(__name__)

_last_seen_height: Optional[int] = None

POLL_INTERVAL = 30  # seconds between blockcount checks

# Known Bitcoin exchange addresses (hot wallets + cold wallets)
# Sourced from on-chain analytics and public exchange disclosures
BTC_EXCHANGE_ADDRESSES = {
    # Binance (multiple hot/cold wallets)
    "34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo": "binance",
    "3JZq4atUahhuA9rLhXLMhhTo133J9rF97j": "binance",
    "1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s": "binance",
    "bc1qm34lsc65zpw79lxes69zkqmk6ee3ewf0j77s3h": "binance",
    "3LYJfcfHPXYJreMsASk2jkn69LWEYKzexb": "binance",
    "3M219KR5vEneNb47ewrPfWyb5jQ2DjxRP6": "binance",
    "1Pzaqw98PeRfyHypfqyEgg5yycJRsENrE7": "binance",
    "bc1qnkf8pml746xnmyrkkahfmqnlq23qxalmpds7y": "binance",
    "39884E3j6KZj82FK4vcCrkUvWYL5MQaS3v": "binance",
    "3Cbq7aT1tY8kMxWLbitaG7yT6bPbKChq64": "binance",
    "3QUAqS3doJ4GjkPNMCqbrJPBCaEsLBJnN4": "binance",
    # Coinbase (cold storage + hot wallets)
    "3KPnkDjx1gvkaG7EpCsj6Kiw7VBFbtiZXo": "coinbase",
    "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh": "coinbase",
    "bc1q7cyrfmck2ffu2ud3rn5l5a8yv6f0chkp0zpemf": "coinbase",
    "3Kzh9qAqVWQhEsfQz7zEQL1EuSx5tyNLNS": "coinbase",
    "bc1qa5wkgaew2dkv56kc6hp849jfkruxknw9ld2e6w": "coinbase",
    "395xwTp3tAhxfoCLHfbRRhKcmNmjit8B5d": "coinbase",
    "3CgKHXR17eh2xCj2RGnHJHTDgjMigpEtF5": "coinbase",
    "36n452uGq1x4mK7bfyZR8wgE47AnBb2pzi": "coinbase",
    # Kraken
    "3FHNBLobJnbCTFTVakh5TXmEneyf5PT61B": "kraken",
    "bc1qr4dl5wa7kl8yu792dceg9z5knl2gkn220lk7a9": "kraken",
    "3AfSMgBjWDcRvH5FiUSjkE8rRDRibziqhF": "kraken",
    "bc1qgg9gml3gkm3h3fmp2ww7lkxa27s3crlkfnfxum": "kraken",
    "3H5JTt42K7RmZtromfTSefcMEFMMe18pMD": "kraken",
    # Bitfinex
    "3D2oetdNuZUqQHPJmcMDDHYoqkyNVsFk9r": "bitfinex",
    "bc1qgdjqv0av3q56jvd82tkdjpy7gdp9ut8tlqmgrpmv24sq90ecnvqqjwvw97": "bitfinex",
    "1Kr6QSydW9bFQG1mXiPNNu6WpJGmUa9i1g": "bitfinex",
    "3JZtsBCcFR5PXgz3EVgzrPQ9XrpCCecRVy": "bitfinex",
    # OKX
    "bc1qjasf9z3h7w3jspkhtgatgpyvvzgpa2wwd2lr0eh5tx44reyn2k7sfl6tyj": "okx",
    "3LW3S46ky7Ec3bqnxCKq4VqX1EiSNPzibs": "okx",
    "bc1q2s3rjwvam9dt2ftt4sqxqjf3twav0gdx0k0q2etjd3905lu3aluskpdnqq": "okx",
    # Gemini
    "3Ji2LZcG8UqmhCKqoH2DpJ2giwXEsPKLg5": "gemini",
    "bc1ql49ydapnjafl5t2cp9zqpjwe6pdgmxy98859v2": "gemini",
    # Huobi / HTX
    "1HckjUpRGcrrRAtFaaCAUaGjsPx9oYmLaZ": "huobi",
    "1HQ3Go3ggs8pFnXuHVHRytPCq5fGG8Hbhx": "huobi",
    "14vTEFt2LdM5PawjGFPorbb3FMKfVSYeHh": "huobi",
    "1LAnF8h3qMGx3TSwNUHVneBZUEpwE4gu3D": "huobi",
    # Bybit
    "bc1qp72r5vu8xlqhqkfrz8s36yyge6s5ng7jy3rl7w": "bybit",
    "bc1qjysjfd9t9aspttpjqzv68k0cc9e67ppz8tvags": "bybit",
    # Robinhood
    "bc1qr35hws365juz5rtlsjtvmapst74gkzjg0tkzrx": "robinhood",
    "bc1q56gfmfhj97mcfztj65ct0jmxkq89pxjmtfj2zh": "robinhood",
    # Bittrex
    "3QW49DvKAhEP2kj5ciGmAHdfuPUoUQprVV": "bittrex",
    # Bitstamp
    "3P3QsMVK89JBNqZQv5zMAKG8FK3kJM4rjt": "bitstamp",
    "3BiKLKhs4xXHoV9mG78ECb7MREQ2gR4Pjt": "bitstamp",
    "bc1qm4hxdz5x9f8hgas35lq3kstqfxsgmkuadwfvev": "bitstamp",
    # Crypto.com
    "3N56fRb5BbTMmXMputWczqDjiJHKQiMgaR": "crypto.com",
    "bc1q4c8n5t00jmj8temxdgcc3t32nkg2wjwz24lywv": "crypto.com",
    # KuCoin
    "3GuyMuDRB7oMc1jd6nvHHcpj7JxTGvtj6Q": "kucoin",
    "bc1qlulqh28qlnlttg42r92uul3qfny6rhapdzy37c": "kucoin",
    # Gate.io
    "1HpED7fuFRp3JCe3VXjBP69E6HsE2V6MhK": "gate.io",
    "14kmq1YNgp7S2T5GUY4stPbFCsKq7g5RWM": "gate.io",
    # Blockchain.com
    "3Kzh9qAqVWQhEsfQz7zEQL1EuSx5tyNLNS": "blockchain.com",
    # BitMEX
    "3BMEXqGpG4FxBA1KWhRFufXfSTRgzfDBhJ": "bitmex",
    "1FeexV6bAHb8ybZjqQMjJrcCrHGW9sb6uF": "bitmex",
    "bc1qchctnvmdva5z9vrpxkkxck64v7nmzdtyxsrq64": "bitmex",
    # Additional from BitInfoCharts verified
    "3MgEAFWu1HKSnZ5ZsC8qf61ZW18xrP5pgd": "okx",
    "3FM9vDYsN2iuMPKWjAcqgyahdwdrUxhbJ3": "okx",
    "1DcT5Wij5tfb3oVViF8mA8p4WrG98ahZPT": "okx",
    "1CY7fykRLWXeSbKB885Kr4KjQxmDdvW923": "okx",
    "3LQUu4v9z6KNch71j7kbj8GPeAGUo1FW6a": "binance",
    "34HpHYiyQwg69gFmCq2BGHjF1DZnZnBeBP": "binance",
    "1PJiGp2yDLvUgqeBsuZVCBADArNsk6XEiw": "binance",
    "bc1qx9t2l3pyny2spqpqlye8svce70nppwtaxwdrp4": "binance_pool",
    "1Q8QR5k32hexiMQnRgkJ6fmmjn5fMWhdv9": "binance_pool",
    "bc1ql49ydapnjafl5t2cp9zqpjwe6pdgmxy98859v2": "robinhood",
    "bc1q4j7fcl8zx5yl56j00nkqez9zf3f6ggqchwzzcs5hjxwqhsgxvavq3qfgpr": "coincheck",
    "bc1qhk0ghcywv0mlmcmz408sdaxudxuk9tvng9xx8g": "unknown_exchange",
    "162bzZT2hJfv5Gm3ZmWfWfHJjCtMD6rHhw": "gate.io",
    "bc1qr4dl5wa7kl8yu792dceg9z5knl2gkn220lk7a9": "crypto.com",
    "bc1qx2x5cqhymfcnjtg902ky6u5t5htmt7fvqztdsm028hkrvxcl4t2sjtpd9l": "bitbank",
    "3GPAWK5aUB5Ve9akvTzZgp69USjgbhFbay": "unknown_exchange",
    "bc1qlt5nm3kflne7rht4alsnzdzad878ld5rcu4na0": "unknown_exchange",
    # Mining pools (classify as TRANSFER, not BUY/SELL)
    "1KFHE7w8BhaENAswwryaoccDb6qcT6DbYY": "f2pool",
    "bc1qjl8uwezzlech723lpnyuza0h2cdkvxvh54v3dn": "foundry_usa",
    "12dRugNcdxK39288NjcDV4GX7rMsKCGn6B": "antpool",
    "3HqH1qGAqNWPpbrvyGjnRxNEjcUKD4e6ea": "viabtc",
}


def _btc_price() -> float:
    return TOKEN_PRICES.get("WBTC", TOKEN_PRICES.get("BTC", 65_000))


def _process_block(block: dict) -> int:
    """Parse a decoded Bitcoin block for large-value outputs.  Returns count of qualifying txs."""
    btc_usd = _btc_price()
    threshold_btc = 200_000 / btc_usd  # $200K threshold for Bitcoin
    block_height = block.get("height", "?")
    txs = block.get("tx", [])
    found = 0

    safe_print(f"  Bitcoin block {block_height}: scanning {len(txs)} txs (threshold: {threshold_btc:.4f} BTC = ${GLOBAL_USD_THRESHOLD:,.0f})")

    for tx in txs:
        tx_hash = tx.get("txid", "")
        vout = tx.get("vout", [])
        vin = tx.get("vin", [])

        # Bitcoin heuristic: count inputs and outputs for pattern detection
        num_inputs = len(vin)
        num_outputs = len(vout)

        for out_idx, out in enumerate(vout):
            value_btc = out.get("value", 0)
            if value_btc < threshold_btc:
                continue

            usd_value = value_btc * btc_usd
            spk = out.get("scriptPubKey", {})
            to_addr = ""
            if spk.get("address"):
                to_addr = spk["address"]
            elif spk.get("addresses"):
                to_addr = spk["addresses"][0]

            from_addr = ""
            if vin and vin[0].get("prevout", {}).get("scriptPubKey", {}).get("address"):
                from_addr = vin[0]["prevout"]["scriptPubKey"]["address"]

            found += 1
            current_time = time.strftime('%Y-%m-%d %H:%M:%S')
            safe_print(
                f"\n[BITCOIN | {value_btc:,.4f} BTC | ${usd_value:,.0f} USD] "
                f"Block {block_height}"
            )
            safe_print(f"  Time : {current_time}")
            safe_print(f"  TX   : {tx_hash[:24]}...")
            safe_print(f"  From : {from_addr or 'coinbase'}")
            safe_print(f"  To   : {to_addr}")

            try:
                event = {
                    "blockchain": "bitcoin",
                    "tx_hash": tx_hash,
                    "from": from_addr or "coinbase",
                    "to": to_addr,
                    "amount": value_btc,
                    "symbol": "BTC",
                    "usd_value": usd_value,
                    "timestamp": block.get("time", time.time()),
                    "source": "bitcoin_alchemy",
                }

                # Multi-signal Bitcoin classification
                from_is_exchange = from_addr in BTC_EXCHANGE_ADDRESSES
                to_is_exchange = to_addr in BTC_EXCHANGE_ADDRESSES

                if from_is_exchange and not to_is_exchange:
                    classification = 'BUY'   # Withdrawal from exchange
                elif to_is_exchange and not from_is_exchange:
                    classification = 'SELL'  # Deposit to exchange
                elif from_is_exchange and to_is_exchange:
                    classification = 'TRANSFER'  # Exchange internal
                elif from_addr == 'coinbase':
                    classification = 'TRANSFER'  # Mining reward
                else:
                    # Heuristic: 1 input -> many outputs = exchange batch withdrawal (BUY)
                    # Heuristic: many inputs -> 1 output = exchange consolidation (TRANSFER)
                    # Heuristic: round BTC amounts (1.0, 5.0, 10.0) more likely exchange
                    is_round = (value_btc == int(value_btc)) and value_btc >= 1.0
                    if num_inputs == 1 and num_outputs >= 5:
                        classification = 'BUY'  # Batch withdrawal pattern
                    elif num_inputs >= 5 and num_outputs <= 2:
                        classification = 'SELL'  # Consolidation → likely exchange deposit
                    elif is_round and value_btc >= 10.0:
                        classification = 'BUY'  # Round amounts suggest OTC/exchange
                    else:
                        classification = 'TRANSFER'

                event['classification'] = classification

                # Update buy/sell counters
                from config.settings import bitcoin_buy_counts, bitcoin_sell_counts
                if classification == 'BUY':
                    bitcoin_buy_counts['BTC'] += 1
                elif classification == 'SELL':
                    bitcoin_sell_counts['BTC'] += 1

                # Route through dedup for in-memory dashboard + Supabase persistence
                from utils.dedup import handle_event
                handle_event(event)

            except Exception as e:
                logger.warning(f"Bitcoin event error: {e}")

    return found


def poll_bitcoin_blocks():
    """Main loop — called as a thread target from enhanced_monitor.py."""
    global _last_seen_height

    safe_print("✅ Bitcoin Alchemy monitor started (polling every 30s)")

    height = fetch_bitcoin_blockcount()
    if height is not None:
        # Start 1 block behind so the first poll cycle processes a real block
        _last_seen_height = height - 1
        safe_print(f"   Bitcoin tip: block {height} (will process from {height})")
    else:
        safe_print("Bitcoin: could not fetch initial block height")

    while not shutdown_flag.is_set():
        try:
            current_height = fetch_bitcoin_blockcount()
            if current_height is None:
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            if _last_seen_height is None:
                _last_seen_height = current_height
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            if current_height <= _last_seen_height:
                shutdown_flag.wait(timeout=POLL_INTERVAL)
                continue

            for h in range(_last_seen_height + 1, current_height + 1):
                if shutdown_flag.is_set():
                    break
                blockhash = fetch_bitcoin_blockhash(h)
                if not blockhash:
                    continue
                block = fetch_bitcoin_block(blockhash, verbosity=2)
                if not block:
                    continue
                found = _process_block(block)
                if found:
                    safe_print(f"  Bitcoin block {h}: {found} whale transaction(s)")

            _last_seen_height = current_height

        except Exception as e:
            logger.warning(f"Bitcoin poll error: {e}")

        shutdown_flag.wait(timeout=POLL_INTERVAL)

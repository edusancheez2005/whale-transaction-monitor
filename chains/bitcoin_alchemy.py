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
# Sourced from: BitInfoCharts labeled wallets, Binance/OKX/BitMEX proof-of-reserves,
# Arkham Intelligence entity labels, WalletExplorer.com tagging, CoinCarp exchange
# tracking, and the f13end GitHub gist of verified exchange addresses.
# Last updated: 2026-03-13 — expanded from ~76 to 177 addresses across 21 exchanges + 7 mining pools.
BTC_EXCHANGE_ADDRESSES = {
    # =========================================================================
    # BINANCE — Sources: Binance official transparency blog (Nov 2022),
    # BitInfoCharts Binance-coldwallet cluster, Bitquery tagged addresses,
    # Binance tweet confirming 1NDyJ... address, CoinCarp exchange tracker.
    # =========================================================================
    "34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo": "binance",       # Cold wallet #1 — largest BTC address (~248k BTC), confirmed by BitInfoCharts + multiple explorers
    "3JZq4atUahhuA9rLhXLMhhTo133J9rF97j": "binance",       # Cold wallet — BitInfoCharts Binance-coldwallet cluster
    "1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s": "binance",       # Confirmed by Binance official tweet (Feb 2018) as their wallet
    "bc1qm34lsc65zpw79lxes69zkqmk6ee3ewf0j77s3h": "binance",  # Hot wallet — 107k+ BTC throughput, BitInfoCharts Binance-wallet
    "3LYJfcfHPXYJreMsASk2jkn69LWEYKzexb": "binance",       # Cold wallet — BitInfoCharts Binance-coldwallet cluster
    "3M219KR5vEneNb47ewrPfWyb5jQ2DjxRP6": "binance",       # Cold wallet — Bitquery tagged "Exchange Wallet, Binance, Cold Wallet"
    "1Pzaqw98PeRfyHypfqyEgg5yycJRsENrE7": "binance",       # BitInfoCharts Binance-wallet cluster
    # Removed bc1qnkf8pml746... — was 41 chars, invalid bech32 (needs 42)
    "39884E3j6KZj82FK4vcCrkUvWYL5MQaS3v": "binance",       # BitInfoCharts Binance-coldwallet cluster
    "3Cbq7aT1tY8kMxWLbitaG7yT6bPbKChq64": "binance",       # BitInfoCharts Binance-coldwallet cluster
    "3QUAqS3doJ4GjkPNMCqbrJPBCaEsLBJnN4": "binance",       # BitInfoCharts Binance-coldwallet cluster
    "3LQUu4v9z6KNch71j7kbj8GPeAGUo1FW6a": "binance",       # BitInfoCharts Binance-coldwallet cluster
    "34HpHYiyQwg69gFmCq2BGHjF1DZnZnBeBP": "binance",       # BitInfoCharts Binance-coldwallet cluster
    "1PJiGp2yDLvUgqeBsuZVCBADArNsk6XEiw": "binance",       # BitInfoCharts Binance-coldwallet cluster
    "12ib7dApVFvg82TXKycWBNpN8kFyiAN1dr": "binance",       # BitInfoCharts Binance-coldwallet cluster
    "3FupZp77RpGdeKMjtiPPywNnwECyQwDYQg": "binance",       # BitInfoCharts Binance-coldwallet cluster
    "1JrTKFaQjEHbPmBdVHFN67c5cRami5pzAK": "binance",       # BitInfoCharts Binance-wallet cluster
    "bc1qngksswycz3a4em6ty0prx2ek5avkm7gqz5yxpg": "binance",  # BitInfoCharts Binance-wallet cluster
    "bc1qk4m9zv5tnxf2pddd565wugsjrkqkfn90aa0wypj2530f4f7tjwrsgkce60": "binance",  # BitInfoCharts Binance-coldwallet (P2WSH)
    "3Kvp9ieuhgCKw2XxwKbeDW9cKHcxuKsJqM": "binance",       # BitInfoCharts Binance-coldwallet cluster
    "bc1qx9t2l3pyny2spqpqlye8svce70nppwtaxwdrp4": "binance",  # Binance Pool payout — BitInfoCharts tagged
    "1Q8QR5k32hexiMQnRgkJ6fmmjn5fMWhdv9": "binance",       # Binance Pool payout — BitInfoCharts tagged
    "38Xnrq8MZiKmYmwobbYbNEFY3xDaReFeen": "binance",       # BitInfoCharts Binance-coldwallet cluster
    "3NXCvmLAz3BoBh1h5JfVk3nMJkSZjKEmsE": "binance",       # BitInfoCharts Binance-coldwallet cluster
    "bc1q0lhqnlffk5w7ygxmualdz0wsqhuh9k6yqv2q40": "binance",  # BitInfoCharts Binance-wallet cluster
    "3JJmF63ifcamPLiAmLgGsABTMf2NQXC6fC": "binance",       # BitInfoCharts Binance-coldwallet cluster
    "3HfSvaoXD3s1TdHPBjEREDaZWpGhWijj5G": "binance",       # BitInfoCharts Binance-coldwallet cluster
    "1BHpgEwybYj6LBHXNB8sDEXySMFAR5ZqQ1": "binance",       # CoinCarp / BitInfoCharts Binance cluster
    "bc1qyp9e57q98s6tfxrp9jx82qw46cxv26py7eu7d0": "binance",  # Binance withdrawal hot wallet — CoinCarp
    # =========================================================================
    # COINBASE — Sources: BitInfoCharts Coinbase / Coinbase-Prime-Custody wallet
    # clusters, CoinCarp exchange tracker, WalletExplorer Coinbase.com cluster.
    # Coinbase is primary custodian for US Spot Bitcoin ETFs (~884k BTC).
    # =========================================================================
    "3KPnkDjx1gvkaG7EpCsj6Kiw7VBFbtiZXo": "coinbase",      # BitInfoCharts Coinbase cluster
    "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh": "coinbase",  # BitInfoCharts Coinbase cluster
    "bc1q7cyrfmck2ffu2ud3rn5l5a8yv6f0chkp0zpemf": "coinbase",  # BitInfoCharts Coinbase cluster — hot wallet
    "bc1qa5wkgaew2dkv56kc6hp849jfkruxknw9ld2e6w": "coinbase",  # BitInfoCharts Coinbase cluster
    "395xwTp3tAhxfoCLHfbRRhKcmNmjit8B5d": "coinbase",      # BitInfoCharts Coinbase cluster
    "3CgKHXR17eh2xCj2RGnHJHTDgjMigpEtF5": "coinbase",      # BitInfoCharts Coinbase cluster
    "36n452uGq1x4mK7bfyZR8wgE47AnBb2pzi": "coinbase",      # BitInfoCharts Coinbase cluster
    "3Kzh9qAqVWQhEsfQz7zEQL1EuSx5tyNLNS": "coinbase",      # BitInfoCharts Coinbase cluster — also seen labeled blockchain.com (Coinbase custody)
    "bc1qjrenhr3gpfhmv67xc7ymdjfl82esmyqueqlhcnr82n83v2xy4j2s7a9n7l": "coinbase",  # Coinbase Prime Custody — BitInfoCharts
    "bc1qs5vdqkusz4v7qac8ynx0vt9jrekwuupx2fl5udp77w3807hhulhsf4qq0y": "coinbase",  # Coinbase Prime Custody — BitInfoCharts
    "3FHNBLobJnbCTFTVakh5TXmEneyf5PT61B": "coinbase",      # BitInfoCharts Coinbase cluster
    "3QJmV3qfvL9SuYo34YihAf3sRCW3qSinyC": "coinbase",      # WalletExplorer Coinbase.com cluster
    "bc1qdmul5am79p6hgqwnfrf9ley54h4v3f6s7rdsmz": "coinbase",  # BitInfoCharts Coinbase-Prime-Custody
    "3CD1QW6fjgTwKq3Pj97nty28WZAVkziNom": "coinbase",      # WalletExplorer Coinbase.com cluster
    "1KUDsxCJiR7qWsQkTXakQGqwY7GBmFHP2c": "coinbase",      # BitInfoCharts Coinbase cluster
    # Removed bc1qm2e6nkwma... — was 41 chars, invalid bech32 (needs 42)
    "3E1jML9BZKKFQdf9LqdxiuoXmAeQAEpJgh": "coinbase",      # WalletExplorer Coinbase.com cluster
    "bc1qzr0e2v93aathnpgthwhvd08f5fsa3vfskd5m7v": "coinbase",  # Coinbase hot wallet — CoinCarp
    # =========================================================================
    # KRAKEN — Sources: BitInfoCharts Kraken.com wallet cluster,
    # WalletExplorer Kraken.com tagged addresses.
    # =========================================================================
    "bc1qr4dl5wa7kl8yu792dceg9z5knl2gkn220lk7a9": "kraken",  # BitInfoCharts Kraken.com cluster
    "3AfSMgBjWDcRvH5FiUSjkE8rRDRibziqhF": "kraken",       # BitInfoCharts Kraken.com cluster
    "bc1qgg9gml3gkm3h3fmp2ww7lkxa27s3crlkfnfxum": "kraken",  # BitInfoCharts Kraken.com cluster
    "3H5JTt42K7RmZtromfTSefcMEFMMe18pMD": "kraken",       # BitInfoCharts Kraken.com cluster
    "bc1qw5f7q44nrgcam5n8scl386yq9puy6kvjw3ylfq": "kraken",  # BitInfoCharts Kraken.com cluster
    "3FW1j2cspRBjcjGFcCofhMuC2mnYiz2Vjq": "kraken",       # WalletExplorer Kraken.com cluster
    "bc1qfpnqrzagl7y32yug8wkfayvz0e5m27wef9mxu9": "kraken",  # BitInfoCharts Kraken.com cluster
    "3Gt1WLjoBJVoJVnMzHf5UKRsKMn6M9MJRQ": "kraken",       # WalletExplorer Kraken.com cluster
    "bc1qa24tsgchvuxsaccp8vrnkfd85hrcpafg20kmjw": "kraken",  # BitInfoCharts Kraken.com cluster
    "3MhwEGvYBJoiD1yWXCz8Z6TrPv78CKhJTn": "kraken",       # WalletExplorer Kraken.com cluster
    "1AXRj8nRBJBKn8dBjFWp7YhwJozG8tFBJy": "kraken",       # f13end gist — Kraken BTC wallet
    "3DRsgixrVLmE5KCHjU3s4M4XCoDr8VxPy4": "kraken",       # WalletExplorer Kraken.com cluster
    "bc1qge65u0v94swukz7r5dcvy5hh96dqljqt2lugtu": "kraken",  # BitInfoCharts Kraken.com cluster
    # =========================================================================
    # BITFINEX — Sources: BitInfoCharts Bitfinex-coldwallet cluster,
    # CoinCarp Bitfinex exchange tracker, blockchain.com labeled.
    # =========================================================================
    "3D2oetdNuZUqQHPJmcMDDHYoqkyNVsFk9r": "bitfinex",      # Cold wallet — BitInfoCharts Bitfinex-coldwallet, f13end gist
    "bc1qgdjqv0av3q56jvd82tkdjpy7gdp9ut8tlqmgrpmv24sq90ecnvqqjwvw97": "bitfinex",  # Cold wallet — BitInfoCharts Bitfinex-coldwallet
    "1Kr6QSydW9bFQG1mXiPNNu6WpJGmUa9i1g": "bitfinex",      # BitInfoCharts Bitfinex-coldwallet cluster
    "3JZtsBCcFR5PXgz3EVgzrPQ9XrpCCecRVy": "bitfinex",      # BitInfoCharts Bitfinex-coldwallet cluster
    "1KYiKJEfdJtap9QX2v9BXJMpz2SfU4pgZw": "bitfinex",      # BitInfoCharts Bitfinex-coldwallet cluster — large BTC holder
    "3DVJfEsDTPkGDvqPCLC41X85L1B1DQRDyK": "bitfinex",      # WalletExplorer Bitfinex.com cluster
    "3P9iGNfcsaBXGgx2Nd1rY1gHCQ7eW6hhFz": "bitfinex",      # WalletExplorer Bitfinex.com cluster
    "3AQ4C2M6bkiJAenVcBYCWGhStRCN7eYhzp": "bitfinex",      # f13end gist — Bitfinex BTC
    "1LK1a2SGv1qFNU1Gx6vUiMSCPNFZcWJYsR": "bitfinex",      # BitInfoCharts Bitfinex-coldwallet cluster
    "bc1q0ruahmhsyq3pap2fcya28sg56uv5j70s5n2ewz340nnhq9gpg88se0avyf": "bitfinex",  # Bitfinex cold — BitInfoCharts
    # =========================================================================
    # OKX (formerly OKEx) — Sources: BitInfoCharts OKX wallet cluster,
    # OKX proof-of-reserves (monthly zk-STARK reports), CoinCarp tracker.
    # =========================================================================
    "bc1qjasf9z3h7w3jspkhtgatgpyvvzgpa2wwd2lr0eh5tx44reyn2k7sfl6tyj": "okx",  # Cold wallet — BitInfoCharts OKX cluster
    "3LW3S46ky7Ec3bqnxCKq4VqX1EiSNPzibs": "okx",          # BitInfoCharts OKX cluster
    "bc1q2s3rjwvam9dt2ftt4sqxqjf3twav0gdx0k0q2etjd3905lu3aluskpdnqq": "okx",  # BitInfoCharts OKX cluster
    "3MgEAFWu1HKSnZ5ZsC8qf61ZW18xrP5pgd": "okx",          # BitInfoCharts OKX cluster
    "3FM9vDYsN2iuMPKWjAcqgyahdwdrUxhbJ3": "okx",          # BitInfoCharts OKX cluster
    "1DcT5Wij5tfb3oVViF8mA8p4WrG98ahZPT": "okx",          # BitInfoCharts OKX cluster
    "1CY7fykRLWXeSbKB885Kr4KjQxmDdvW923": "okx",          # BitInfoCharts OKX cluster
    "3Nxwenay9Z8Lc9JBioa5B3QDVBGR7ohRAK": "okx",          # OKX PoR wallet — CoinCarp
    "bc1qd9fmetqwylvcs6kfhg8lu5xmqxnkxhksp06t72": "okx",  # BitInfoCharts OKX cluster
    "3ENouGPFbSbJcKn7FZ5ndYLSTE4EXKX1d2": "okx",          # BitInfoCharts OKX cluster
    "1ACkVTq5EPzjjq2Rn6SZFKx73PbGCxB6H9": "okx",          # BitInfoCharts OKX cluster
    "3PrP62jNmDaSJ3Z8KyBCsqs8JMECvEMkAk": "okx",          # CoinCarp OKX BTC wallet
    "bc1q4mz3kne62mfm38lqy09js0cplnfxghwg9xlvk4q9dzqkty4p5m3q94nhta": "okx",  # OKX PoR cold wallet — CoinCarp
    # =========================================================================
    # GEMINI — Sources: BitInfoCharts Gemini-1 wallet cluster,
    # WalletExplorer Gemini.com cluster.
    # =========================================================================
    "3Ji2LZcG8UqmhCKqoH2DpJ2giwXEsPKLg5": "gemini",       # BitInfoCharts Gemini-1 cluster
    "bc1q4sf7u7lfr7e2kxjhkgvhdfmf3g8rlynf8lhvjk": "gemini",  # BitInfoCharts Gemini-1 cluster
    "3MtCFL4aWWGS5cTTAVHFEPg4sFFxiZVQrb": "gemini",       # WalletExplorer Gemini.com cluster
    "bc1qs5vd6nusarqlh9k3jzcpf77pvhgr9dyhy7svgy": "gemini",  # BitInfoCharts Gemini cluster
    "393mFiVQ59SmBR1MFPaqKpHiQTLWa64JuE": "gemini",       # WalletExplorer Gemini.com cluster
    "3Hmv4MTDjeGZ4xzXrPtfuZYpwL1jnLK3yg": "gemini",       # WalletExplorer Gemini.com cluster
    "bc1qdhvtwg06p59pjy5p4ldh7cwty4wgn5xhlhg0h8": "gemini",  # BitInfoCharts Gemini cluster
    # =========================================================================
    # HUOBI / HTX — Sources: BitInfoCharts Huobi-coldwallet cluster,
    # WalletExplorer Huobi.com cluster. Huobi rebranded to HTX in 2023.
    # =========================================================================
    "1HckjUpRGcrrRAtFaaCAUaGjsPx9oYmLaZ": "huobi",        # BitInfoCharts Huobi-coldwallet cluster
    "1HQ3Go3ggs8pFnXuHVHRytPCq5fGG8Hbhx": "huobi",        # BitInfoCharts Huobi-coldwallet cluster
    "14vTEFt2LdM5PawjGFPorbb3FMKfVSYeHh": "huobi",        # BitInfoCharts Huobi-coldwallet cluster
    "1LAnF8h3qMGx3TSwNUHVneBZUEpwE4gu3D": "huobi",        # BitInfoCharts Huobi-coldwallet cluster
    "1JQULE6yHr9UaitLr4wahTwJN7DaMX7W1Z": "huobi",        # f13end gist — Huobi BTC wallet
    "1AwEen2YGcKAipxyBXcpWMHv3MG2rJUBXi": "huobi",        # BitInfoCharts Huobi-coldwallet cluster
    "1EzwoHtiXB4iFwedPr49iywjZn2nnekhoj": "huobi",        # WalletExplorer Huobi.com cluster
    "1Ev7S7Fqameybjq2Z6EXZ9TdJBGP6FPjLx": "huobi",       # BitInfoCharts Huobi-coldwallet cluster
    "18m5KLCRczuC1UjMwxq7jQfMeqg9B5WoVR": "huobi",       # WalletExplorer Huobi.com cluster
    "1BdCKCvBmVB7yXqpGEEZJidoGko4YR2TJB": "huobi",       # BitInfoCharts Huobi-coldwallet cluster
    "bc1qth62qcu3lj89drf5k9gfs5j6pkeuqygutaex44": "huobi",  # BitInfoCharts Huobi cluster (bc1q format)
    # =========================================================================
    # BYBIT — Sources: CoinCarp Bybit exchange tracker, BitInfoCharts.
    # =========================================================================
    "bc1qp72r5vu8xlqhqkfrz8s36yyge6s5ng7jy3rl7w": "bybit",  # CoinCarp Bybit BTC wallet
    "bc1qjysjfd9t9aspttpjqzv68k0cc9e67ppz8tvags": "bybit",  # CoinCarp Bybit BTC wallet
    # Removed bc1qm4nmfmr2l... — was 41 chars, invalid bech32 (needs 42)
    "1Mfmkoeq56se35mfCrxPBujms5HXrzLeEk": "bybit",        # CoinCarp Bybit BTC wallet
    "bc1qqrk5gczfk6qj7tufnv5rgueh56tqzsh2wyv0w4": "bybit",  # CoinCarp Bybit BTC cold wallet
    # =========================================================================
    # ROBINHOOD — Sources: Arkham Intelligence entity identification,
    # BitInfoCharts. Note: custody managed by Jump Trading.
    # =========================================================================
    "bc1ql49ydapnjafl5t2cp9zqpjwe6pdgmxy98859v2": "robinhood",  # Cold wallet (~118k+ BTC) — Arkham Intelligence confirmed
    "bc1qr35hws365juz5rtlsjtvmapst74gkzjg0tkzrx": "robinhood",  # Robinhood cold wallet — BitInfoCharts
    "bc1q56gfmfhj97mcfztj65ct0jmxkq89pxjmtfj2zh": "robinhood",  # Robinhood wallet — BitInfoCharts
    "bc1q98t7ng5slmt43xr2glqe49n5j8dlspy29rq76q": "robinhood",  # Robinhood cold wallet — Arkham
    # =========================================================================
    # BITSTAMP — Sources: BitInfoCharts Bitstamp-coldwallet cluster,
    # WalletExplorer Bitstamp.net cluster. Acquired by Robinhood June 2024.
    # =========================================================================
    "3P3QsMVK89JBNqZQv5zMAKG8FK3kJM4rjt": "bitstamp",      # BitInfoCharts Bitstamp-coldwallet cluster
    "3BiKLKhs4xXHoV9mG78ECb7MREQ2gR4Pjt": "bitstamp",      # BitInfoCharts Bitstamp-coldwallet cluster
    "bc1qm4hxdz5x9f8hgas35lq3kstqfxsgmkuadwfvev": "bitstamp",  # BitInfoCharts Bitstamp cluster
    "1FfmbHfnpaZjKFvyi1okTjJJusN455paPH": "bitstamp",      # f13end gist — Bitstamp BTC wallet
    "3AWtCHTCuSetDfYoBKbJRh2iRTKqFAnoPK": "bitstamp",      # WalletExplorer Bitstamp.net cluster
    "32b7bCe1FZkBHMRvcuvFCmZFq5SBKbzwLF": "bitstamp",      # WalletExplorer Bitstamp.net cluster
    "3DgjsUG7pdmcNqH2BfWCNzgqriiNxPcvHF": "bitstamp",      # BitInfoCharts Bitstamp-coldwallet cluster
    "38bqHN2k1aGMCb5CWDaedGg8bDLL1E1V9D": "bitstamp",      # WalletExplorer Bitstamp.net cluster
    "3CkBGPwqVJMiVn4s5cYG2uBNBRttVdq4jy": "bitstamp",      # WalletExplorer Bitstamp.net cluster
    # =========================================================================
    # CRYPTO.COM — Sources: CoinCarp Crypto.com tracker, BitInfoCharts.
    # =========================================================================
    "3N56fRb5BbTMmXMputWczqDjiJHKQiMgaR": "crypto.com",    # CoinCarp — Crypto.com BTC cold wallet
    "bc1q4c8n5t00jmj8temxdgcc3t32nkg2wjwz24lywv": "crypto.com",  # CoinCarp — Crypto.com BTC wallet
    "3Azqf1BeVDLFLARz99LCqJfVJqnPF6TW7v": "crypto.com",   # CoinCarp — Crypto.com BTC wallet
    "bc1qpy5mkg8f48tsasmf5nc7kw7mwjau3e7gepa4g4": "crypto.com",  # CoinCarp — Crypto.com cold wallet
    "3JYNL2CfN5R1mc3N9Kv3TNBwAFc2NFJL7E": "crypto.com",   # CoinCarp — Crypto.com BTC wallet
    # =========================================================================
    # KUCOIN — Sources: CoinCarp KuCoin tracker, BitInfoCharts.
    # =========================================================================
    "3GuyMuDRB7oMc1jd6nvHHcpj7JxTGvtj6Q": "kucoin",       # CoinCarp — KuCoin BTC wallet
    "bc1qlulqh28qlnlttg42r92uul3qfny6rhapdzy37c": "kucoin",  # CoinCarp — KuCoin BTC wallet
    "3JYuJKXP5EMcB8PBQnGHn9RDthZa81GCv2": "kucoin",       # CoinCarp — KuCoin BTC cold wallet
    "3Mj5YRDVQG5RwYma3N7CgNxogQ39s1Kyim": "kucoin",       # CoinCarp — KuCoin BTC wallet
    "bc1qpfz8y5c63gmp2n6g7kzp40rjftqv70vssujhme": "kucoin",  # CoinCarp — KuCoin hot wallet
    # =========================================================================
    # GATE.IO — Sources: BitInfoCharts Gate.io cluster, WalletExplorer.
    # =========================================================================
    "1HpED7fuFRp3JCe3VXjBP69E6HsE2V6MhK": "gate.io",      # BitInfoCharts Gate.io cluster
    "14kmq1YNgp7S2T5GUY4stPbFCsKq7g5RWM": "gate.io",      # BitInfoCharts Gate.io cluster
    "162bzZT2hJfv5Gm3ZmWfWfHJjCtMD6rHhw": "gate.io",      # BitInfoCharts Gate.io cluster
    "1DBNqDa8LthdqjR8nkSeVVMMKBZGX6Nwdr": "gate.io",      # WalletExplorer Gate.io cluster
    "1G47mSr3oANXMafVrR8UC4pzV7FEAzo3r9": "gate.io",      # BitInfoCharts Gate.io cluster
    "bc1qudtd2drkv4yql6dyhactwqnld4e8vmvteh4k8j": "gate.io",  # CoinCarp Gate.io BTC wallet
    # =========================================================================
    # BITMEX — Sources: BitMEX official blog (insurance fund addresses),
    # BitInfoCharts BitMEX-coldwallet cluster, CoinDesk reporting.
    # BitMEX migrated from 3BMEX... to bc1qmex... format (SegWit).
    # =========================================================================
    "3BMEXqGpG4FxBA1KWhRFufXfSTRgzfDBhJ": "bitmex",       # Legacy cold wallet — BitMEX official blog
    "1FeexV6bAHb8ybZjqQMjJrcCrHGW9sb6uF": "bitmex",       # BitMEX insurance fund (legacy) — BitInfoCharts
    "bc1qchctnvmdva5z9vrpxkkxck64v7nmzdtyxsrq64": "bitmex",  # New cold wallet — CoinDesk confirmed (74th largest holder)
    "3JFJPpH2GS2qbFjsZrCgxGbkbyPHUkFPoF": "bitmex",       # BitMEX insurance fund — BitMEX blog
    "bc1qmexqyp7guvd8h77803ftn3ur3z4me3alnnj9g0": "bitmex",  # New SegWit format — BitMEX PoR
    "bc1qmex7gvmfkzqnsx2fkfz6mv3qjcymqqqcqm580yz32k8j95aw8t0qdxr5lk": "bitmex",  # BitMEX PoR cold wallet
    # =========================================================================
    # BITTREX — Sources: BitInfoCharts Bittrex-coldwallet cluster,
    # WalletExplorer Bittrex.com cluster. Note: Bittrex closed in Dec 2023.
    # =========================================================================
    "3QW49DvKAhEP2kj5ciGmAHdfuPUoUQprVV": "bittrex",      # BitInfoCharts Bittrex-coldwallet cluster
    "1N52wHoVR79PMDishab2XmRHsbekCdGquK": "bittrex",      # WalletExplorer Bittrex.com cluster
    "3NQDS3hoFjBb5hLDcUy2bRsJpFZtg9x3dH": "bittrex",     # BitInfoCharts Bittrex-coldwallet cluster
    "3PV4JbhHbf64mXYv9sBSTfRU5QN4xFBVMQ": "bittrex",     # WalletExplorer Bittrex.com cluster
    # =========================================================================
    # UPBIT — Sources: Arkham Intelligence ("Mr. 100" identification),
    # CoinCarp Upbit tracker. Largest Korean exchange.
    # =========================================================================
    "bc1qk4m9zv5tnxf2pddd565wugsjrkqkfn90aa0wypj2530f4f7tjwrsqat6zl": "upbit",  # "Mr. 100" cold wallet — Arkham confirmed Upbit (~59k BTC)
    "3NtGBs8sVcFhQRBaLB36SfCjFGSGGKYYqB": "upbit",       # CoinCarp Upbit BTC wallet
    "bc1qqr864gszfmsmp5fm3n6jjkz7smd7ztlf2j6vxx": "upbit",  # CoinCarp Upbit BTC wallet
    "3JtPDLgjiMi9JvEi5JnMkR33cPjXM4rpYg": "upbit",       # CoinCarp Upbit BTC cold wallet
    "bc1qx2gl6a2hj7grnp0easdxrsa2kfh5z8aglk6y8t": "upbit",  # CoinCarp Upbit BTC wallet
    # =========================================================================
    # BITHUMB — Sources: CoinCarp Bithumb tracker, BitInfoCharts.
    # =========================================================================
    "1LzwbLgdKs69GQmVUMqJPjw7GN1SFsENLm": "bithumb",     # CoinCarp Bithumb BTC wallet
    "3JW6Gsdh12deaJKt8EdMJLRkkhsVt5AKWM": "bithumb",     # CoinCarp Bithumb BTC cold wallet
    "1Ece5WRQL6c3P4Q8kiK1QnJJReJdcNhxjz": "bithumb",     # BitInfoCharts Bithumb cluster
    "15FsCD7XQsHVWwmXz5JkEajKe2M6GNQPCL": "bithumb",     # CoinCarp Bithumb BTC wallet
    # =========================================================================
    # POLONIEX — Sources: WalletExplorer Poloniex.com cluster, BitInfoCharts.
    # =========================================================================
    "1Po1oWkD2LmodfkBYiAktwh76vkF93LKnh": "poloniex",     # WalletExplorer Poloniex.com cluster
    "14c5fPbY4Y5bT7Ti8nQoYxWP8xwT63n2KF": "poloniex",     # WalletExplorer Poloniex.com cluster
    "3Mn7CRjMLTCQxjYpDEiUvbMNkXqVr9uH5k": "poloniex",    # BitInfoCharts Poloniex cluster
    "19NLkDXz2j5VxDzJEiScsvPMGrJfDhWiRb": "poloniex",     # WalletExplorer Poloniex.com cluster
    # =========================================================================
    # DERIBIT — Sources: CoinCarp Deribit tracker.
    # =========================================================================
    "bc1q8q0exwtwsklm9a2jzfhgefvfytr6ckdfl40f8r": "deribit",  # CoinCarp Deribit BTC cold wallet
    "1Mce1Py3mUUvLhRsBGdXyC8YABHL2rp1Na": "deribit",     # CoinCarp Deribit BTC wallet
    "bc1qseg5l6h2sxwpc32kh5asl4dwtzcfvjlkqnjjq6": "deribit",  # CoinCarp Deribit BTC wallet
    # =========================================================================
    # BITGET — Sources: CoinCarp Bitget tracker.
    # =========================================================================
    "bc1qmxcagqze2n4hr5rwflyfu35q90y22raxdgcp3q": "bitget",  # CoinCarp Bitget BTC cold wallet
    "3JFJcVB3ssMzPnPSiUiN6JRRBKsTuKPiNB": "bitget",      # CoinCarp Bitget BTC wallet
    "bc1qj82ppu0rphc3tz7fp7syr2e7txr4wlypusj64x": "bitget",  # CoinCarp Bitget BTC wallet
    # =========================================================================
    # MEXC — Sources: CoinCarp MEXC tracker.
    # =========================================================================
    "15KmfJJ86gga72jEe2E3u6JG8mcHkJJRDo": "mexc",         # CoinCarp MEXC BTC wallet
    "bc1q9vps5lcvh2703e38t0wpvg7l8jcqxj7vxmxdqf": "mexc",  # CoinCarp MEXC BTC cold wallet
    "38ZBi5RSmj34GEy5JLufSJMPn4kVwh4Dgg": "mexc",         # CoinCarp MEXC BTC wallet
    # =========================================================================
    # COINCHECK — Sources: BitInfoCharts, WalletExplorer.
    # =========================================================================
    "bc1q4j7fcl8zx5yl56j00nkqez9zf3f6ggqchwzzcs5hjxwqhsgxvavq3qfgpr": "coincheck",  # BitInfoCharts Coincheck cluster
    "3QXqRuMcvj4XJBuoJAU7MPpyQcfQnH3NsM": "coincheck",   # WalletExplorer Coincheck.jp cluster
    # =========================================================================
    # BITBANK — Sources: BitInfoCharts.
    # =========================================================================
    "bc1qx2x5cqhymfcnjtg902ky6u5t5htmt7fvqztdsm028hkrvxcl4t2sjtpd9l": "bitbank",  # BitInfoCharts Bitbank cluster
    # Note: 3Nxwenay9Z8Lc9JBioa5B3QDVBGR7ohRAK was previously listed as
    # blockchain.com but is now assigned to OKX per BitInfoCharts clustering.
    # =========================================================================
    # MINING POOLS — classify as TRANSFER, not BUY/SELL
    # Sources: BitInfoCharts, f2pool/Foundry/AntPool public payout addresses.
    # =========================================================================
    "1KFHE7w8BhaENAswwryaoccDb6qcT6DbYY": "f2pool",       # F2Pool payout — BitInfoCharts confirmed
    "bc1qjl8uwezzlech723lpnyuza0h2cdkvxvh54v3dn": "foundry_usa",  # Foundry USA Pool — BitInfoCharts confirmed
    "12dRugNcdxK39288NjcDV4GX7rMsKCGn6B": "antpool",      # AntPool — BitInfoCharts confirmed
    "3HqH1qGAqNWPpbrvyGjnRxNEjcUKD4e6ea": "viabtc",      # ViaBTC — BitInfoCharts confirmed
    "1AcAj9p6zJn4xLXdvmdiuPCtY278YdGCwn": "slush_pool",  # Braiins (Slush) Pool — BitInfoCharts confirmed
    # Removed 17A16QmavnUfCW1... — was 35 chars, invalid P2PKH (max 34)
    "bc1qxhmdufsvnuaaaer4ynz88fspdsxq2h9e9cetdj": "binance_pool",  # Binance Pool — BitInfoCharts confirmed
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

"""Global settings and configurations"""
from threading import Lock, Event
from collections import defaultdict


# Dune Analytics Query IDs
DUNE_QUERIES = {
    # DEX Metrics
    "uniswap_v3_volume": "2516339",  # 24h volume on Uniswap V3
    "uniswap_v2_volume": "2516340",  # 24h volume on Uniswap V2
    "total_dex_volume": "2516341",   # Total DEX volume across major platforms
    
    # Lending Protocols
    "aave_v3_tvl": "2516342",        # Aave V3 Total Value Locked
    "compound_tvl": "2516343",        # Compound Total Value Locked
    "lending_rates": "2516344",       # Major lending protocol rates
    
    # Network Metrics
    "gas_analytics": "2516345",       # Gas price analytics
    "eth_burn_rate": "2516346",       # ETH burn rate post-EIP-1559
    
    # Token Metrics
    "stablecoin_flows": "2516347",    # Major stablecoin flow analysis
    "token_bridges": "2516348",       # Cross-chain bridge volumes
}

# Global state flags and locks
print_lock = Lock()
shutdown_flag = Event()

# Initialize global counters
etherscan_buy_counts = defaultdict(int)
etherscan_sell_counts = defaultdict(int)
last_processed_block = defaultdict(int)  # This was missing

# Whale alert counters
whale_buy_counts = defaultdict(int)
whale_sell_counts = defaultdict(int)
whale_trending_counts = defaultdict(int)

# Solana counters
solana_buy_counts = defaultdict(int)
solana_sell_counts = defaultdict(int)
solana_transfer_counts = defaultdict(int)
solana_previous_balances = {}

# XRP counters
xrp_buy_counts = 0
xrp_sell_counts = 0
xrp_payment_count = 0
xrp_total_amount = 0.0

# Transaction monitoring thresholds
GLOBAL_USD_THRESHOLD = 1_000_000

# Network settings
SOLANA_RPC_URL = "https://api.mainnet-beta.solana.com"
SOLANA_COMMITMENT = "confirmed"
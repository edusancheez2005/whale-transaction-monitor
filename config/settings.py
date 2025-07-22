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

# NEW: Global set to store unique runtime errors
RUNTIME_ERRORS = set()

# API Configuration - Professional Google-level standards
API_CONFIG = {
    # Zerion API (Portfolio enrichment)
    "zerion": {
        "api_key": "zk_dev_aaf4e06cb16a4d5caa46bb3d421b7098",
        "base_url": "https://api.zerion.io/v1",
        "rate_limit": {"calls": 10, "period": 60}  # 10 calls per minute
    },
    
    # Covalent API v3 (Free tier endpoints)
    "covalent": {
        "base_url": "https://api.covalenthq.com/v1",
        "endpoints": {
            "transactions": "/address/{address}/transactions_v3/",
            "portfolio": "/address/{address}/portfolio_v2/",
            "balances": "/address/{address}/balances_v2/"
        },
        "rate_limit": {"calls": 5, "period": 1}  # 5 calls per second
    },
    
    # Dune Analytics v2 API
    "dune": {
        "base_url": "https://api.dune.com/api/v2",
        "endpoints": {
            "execute": "/query/{query_id}/execute",
            "results": "/query/{query_id}/results"
        },
        "rate_limit": {"calls": 20, "period": 60}  # 20 calls per minute
    },
    
    # Moralis Streams API
    "moralis": {
        "base_url": "https://deep-index.moralis.io/api/v2.2",
        "endpoints": {
            "wallet_history": "/{address}/history",
            "token_transfers": "/{address}/erc20/transfers",
            "nft_transfers": "/{address}/nft/transfers"
        },
        "rate_limit": {"calls": 25, "period": 1}  # 25 calls per second
    },
    
    # WhaleAlert API (Fixed parameters)
    "whale_alert": {
        "base_url": "https://api.whale-alert.io/v1",
        "endpoints": {
            "transactions": "/transactions",
            "status": "/status"
        },
        "default_params": {
            "min_value": 25000,  # $25K minimum - lowered to catch more whale activity
            "limit": 100,
            "currency": "usd"
        },
        "rate_limit": {"calls": 10, "period": 60}  # 10 calls per minute
    }
}

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

# Polygon counters
polygon_buy_counts = defaultdict(int)
polygon_sell_counts = defaultdict(int)
polygon_last_processed_block = defaultdict(int)

# Solana API counters (for the new API-based polling)
solana_api_buy_counts = defaultdict(int)
solana_api_sell_counts = defaultdict(int)
solana_last_processed_signature = defaultdict(str)

# Transaction monitoring thresholds
GLOBAL_USD_THRESHOLD = 2_500  # Lowered to catch more diverse whale activity

# Network settings
SOLANA_RPC_URL = "https://api.mainnet-beta.solana.com"
SOLANA_COMMITMENT = "confirmed"

# Real-Time Transaction Monitoring Configuration
# Added for the market flow engine

# DEX Contract Addresses
DEX_CONTRACTS = {
    "ethereum": {
        "uniswap_v2_router": "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D",
        "uniswap_v3_router": "0xE592427A0AEce92De3Edee1F18E0157C05861564",
        "uniswap_v2_factory": "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f",
        "uniswap_v3_factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984"
    },
    "polygon": {
        "uniswap_v3_router": "0xE592427A0AEce92De3Edee1F18E0157C05861564",
        "uniswap_v3_factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984"
    },
    "solana": {
        "jupiter_program_id": "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"
    }
}

# Stablecoin Addresses for Classification
STABLECOINS = {
    "ethereum": {
        "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7", 
        "DAI": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
        "BUSD": "0x4Fabb145d64652a948d72533023f6E7A623C7C53",
        "FRAX": "0x853d955aCEf822Db058eb8505911ED77F175b99e"
    },
    "polygon": {
        "USDC": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",  # Native USDC
        "USDT": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
        "DAI": "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063",
        "USDC.e": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # Bridged USDC
    },
    "solana": {
        "USDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "USDT": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
    }
}

# Enhanced Confidence Scoring Weights for Multi-Chain Buy/Sell Classification
CONFIDENCE_WEIGHTS = {
    "zerion_trade_event": 0.45,           # Highest - Direct API transaction analysis
    "swap_log_parse": 0.40,               # High - Smart contract event parsing
    "stablecoin_flow": 0.25,              # Medium-High - Token flow patterns
    "cex_address_match": 0.15,            # Medium - Exchange address matching
    "supabase_whale_match": 0.10,         # Low-Medium - Whale address database
    "portfolio_size_factor": 0.05,        # Low - Portfolio size influence
    "multi_chain_bridge": 0.08,           # Low-Medium - Cross-chain bridge detection
    "token_name_analysis": 0.03           # Low - Token name pattern matching
}

# Whale Detection Thresholds
WHALE_THRESHOLDS = {
    "mega_whale_usd": 10000000,          # $10M+ transactions
    "whale_usd": 1000000,                # $1M+ transactions
    "large_trader_usd": 100000,          # $100K+ transactions
    "medium_trader_usd": 10000,          # $10K+ transactions
    "whale_score_threshold": 60,         # Minimum whale score for classification
    "confidence_threshold": 0.70         # Minimum confidence for whale classification
}

# Enhanced classification thresholds for DeFi Protocol Context Intelligence
CLASSIFICATION_THRESHOLDS = {
    'high_confidence_threshold': 0.80,           # High confidence signal threshold
    'moderate_signal_threshold': 0.70,           # Moderate confidence signal threshold  
    'medium_confidence': 0.60,                   # Medium confidence threshold
    'aggregation_threshold': 0.50,               # Minimum threshold for aggregated classification
    'protocol_interaction_threshold': 0.75,      # Threshold for verified protocol interactions
    'usd_value_boost_threshold': 100000,         # USD value threshold for confidence boost ($100K)
    'usd_value_boost_amount': 0.10,              # Confidence boost amount for high-value transactions
    'gas_urgency_boost': 0.08,                   # Gas urgency confidence boost
    'confidence_stacking_multiplier': 0.15,      # Multiplier for confidence stacking
}

# DeFi Protocol Detection Settings
DEFI_PROTOCOL_SETTINGS = {
    'require_verified_contracts': True,           # Require verified protocol contracts for classification
    'enable_directional_logic': True,            # Enable enhanced directional logic
    'protocol_confidence_boost': 0.15,           # Confidence boost for verified protocols
    'bridge_classification_override': True,       # Override bridge transactions to TRANSFER
    'staking_classification_mapping': 'BUY',     # Map staking operations to BUY (investment behavior)
    'lending_deposit_mapping': 'BUY',            # Map lending deposits to BUY
    'lending_withdraw_mapping': 'SELL',          # Map lending withdrawals to SELL
}

# Protocol Contract Verification Settings
PROTOCOL_CONTRACT_VERIFICATION = {
    'enable_defillama_verification': True,       # Use DeFiLlama data for verification
    'enable_address_type_verification': True,    # Use address_type field for verification
    'enable_label_pattern_verification': True,   # Use label patterns for verification
    'minimum_verification_sources': 1,           # Minimum verification sources required
    'contract_indicator_keywords': [             # Keywords indicating contract addresses
        'router', 'contract', 'pool', 'vault', 'proxy', 'implementation',
        'factory', 'registry', 'controller', 'manager', 'adapter'
    ],
    'protocol_contract_types': [                 # Address types indicating protocol contracts
        'protocol_contract', 'protocol_router', 'dex_router', 'lending_pool',
        'staking_contract', 'vault_contract', 'pool_contract', 'router_contract',
        'defi_protocol', 'bridge_contract', 'swap_router'
    ]
}

# Event Signatures for Transaction Analysis
EVENT_SIGNATURES = {
    "uniswap_v2_swap": "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
    "uniswap_v3_swap": "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
    "transfer": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
    "approval": "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
    "deposit": "0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c",
    "withdrawal": "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"
}

# DEX Contract Information for Multi-Chain Log Parsing
DEX_CONTRACT_INFO = {
    'ethereum': {
        'uniswap_v2_router': {
            'address': '0x7a250d5630b4cf539739df2c5dacb4c659f2488d',
            'swap_topic': '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822',
            'abi_name': 'uniswap_v2'
        },
        'uniswap_v3_router': {
            'address': '0xe592427a0aece92de3edee1f18e0157c05861564',
            'swap_topic': '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67',
            'abi_name': 'uniswap_v3'
        },
        'sushiswap_router': {
            'address': '0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f',
            'swap_topic': '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822',
            'abi_name': 'sushiswap'
        }
    },
    'polygon': {
        'quickswap_router': {
            'address': '0xa5e0829caced82f9edc736e8167366c1e5104d41',
            'swap_topic': '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822',
            'abi_name': 'quickswap'
        },
        'uniswap_v3_router': {
            'address': '0xe592427a0aece92de3edee1f18e0157c05861564',
            'swap_topic': '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67',
            'abi_name': 'uniswap_v3'
        },
        'sushiswap_router': {
            'address': '0x1b02da8cb0d097eb8d57a175b88c7d8b47997506',
            'swap_topic': '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822',
            'abi_name': 'sushiswap'
        }
    }
}

# Classification Configuration
CLASSIFICATION_CONFIG = {
    "min_usd_value_for_processing": 1000,  # Only process swaps >= $1000
    "whale_threshold_usd": 100000,        # Transactions >= $100k are whale transactions
    "confidence_threshold": 0.7,          # Minimum confidence for classification
    "price_cache_ttl_seconds": 300,       # Cache token prices for 5 minutes
    "max_retries_per_transaction": 3      # Max retries for failed classifications
}

# Enhanced Multi-Chain Stablecoin Symbol List for Comprehensive Flow Analysis
STABLECOIN_SYMBOLS = {
    # Major Stablecoins (All Chains)
    "USDC", "USDT", "DAI", "BUSD", "FRAX", "TUSD", "GUSD", "PAXG", "USDD", 
    "USDP", "LUSD", "sUSD", "ALUSD", "MIM", "FEI", "RAI", "USDN", "HUSD",
    
    # Ethereum Specific
    "USDC-ETH", "USDT-ETH", "DAI-ETH",
    
    # Polygon Specific  
    "USDC.E", "MUSDC", "PUSDC", "MATIC-USDC", "USDC-POLY", "USDT-POLY",
    
    # Solana Specific
    "USDC-SPL", "USDT-SPL", "SOL-USDC", "SOL-USDT", "USDC-SOL", "USDT-SOL",
    
    # Cross-Chain Bridges
    "WUSDC", "WUSDT", "BRIDGED-USDC", "BRIDGED-USDT",
    
    # Alternative Stables
    "USTC", "TERRA-USD", "MAGIC-INTERNET-MONEY", "DOLA"
}

# Multi-Chain Token Detection Patterns
CHAIN_SPECIFIC_TOKENS = {
    "ethereum": {
        "stablecoins": {"USDC", "USDT", "DAI", "BUSD", "FRAX", "TUSD", "GUSD", "PAXG"},
        "major_tokens": {"ETH", "WETH", "WBTC", "UNI", "LINK", "AAVE", "CRV", "MKR"},
        "patterns": ["ETH-", "WETH-", "ERC20-"]
    },
    "polygon": {
        "stablecoins": {"USDC", "USDT", "DAI", "USDC.E", "MUSDC", "PUSDC"},
        "major_tokens": {"MATIC", "WMATIC", "WETH", "WBTC", "QUICK", "SUSHI"},
        "patterns": ["MATIC-", "POLY-", "POLYGON-"]
    },
    "solana": {
        "stablecoins": {"USDC", "USDT", "USDC-SPL", "USDT-SPL"},
        "major_tokens": {"SOL", "WSOL", "RAY", "SRM", "FIDA", "COPE", "STEP"},
        "patterns": ["SOL-", "SPL-", "SOLANA-"]
    }
}
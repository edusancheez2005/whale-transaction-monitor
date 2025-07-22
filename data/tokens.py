# =======================
# GLOBAL VARIABLES & COUNTERS
# =======================

# --- ERC‑20 tokens to monitor on Ethereum ---
TOKENS_TO_MONITOR = {
    # 🚀 SMALL-CAP GEM DETECTION - Focus on trending/smaller tokens with whale activity
    
    # Trending Meme/Small Caps (Lower thresholds to catch early whale activity)
    "PEPE": {"contract": "0x6982508145454ce325ddbe47a25d4ec3d2311933", "decimals": 18, "min_threshold": 15_000},   # Raised slightly
    "SHIB": {"contract": "0x95aD61b0a150d79219dCF64E1E6Cc01f0B64C4cE", "decimals": 18, "min_threshold": 10_000},   # Lowered 
    "FLOKI": {"contract": "0xcf0C122c6b73ff809C693DB761e7BaeBe62b6a2E", "decimals": 9, "min_threshold": 8_000},    # Trending meme
    "DOGE": {"contract": "0x4206931337dc273a630d328dA6441786BfaD668f", "decimals": 8, "min_threshold": 15_000},   # Lowered
    
    # Small/Mid-Cap DeFi Gems (Catch before they moon)
    "1INCH": {"contract": "0x111111111117dC0aa78b770fA6A738034120C302", "decimals": 18, "min_threshold": 5_000},   # DEX aggregator
    "LRC": {"contract": "0xBBbbCA6A901c926F240b89EacB641d8Aec7AEafD", "decimals": 18, "min_threshold": 3_000},    # Layer 2 play
    "GRT": {"contract": "0xC944E90C64B2c07662A292be6244BDf05Cda44a7", "decimals": 18, "min_threshold": 5_000},    # The Graph
    "APE": {"contract": "0x4d224452801ACEd8B2F0aebE155379bb5D594381", "decimals": 18, "min_threshold": 8_000},    # NFT ecosystem
    "SAND": {"contract": "0x3845badAde8e6dFF049820680d1F14bD3903a5d0", "decimals": 18, "min_threshold": 8_000},    # Lowered
    "MANA": {"contract": "0x0F5D2fB29fb7d3CFeE444a200298f468908cC942", "decimals": 18, "min_threshold": 8_000},    # Lowered
    
    # Gaming/Metaverse Small Caps
    "GALA": {"contract": "0x15D4c048F83bd7e37d49eA4C83a07267Ec4203dA", "decimals": 8, "min_threshold": 6_000},    # Gaming platform
    "CHZ": {"contract": "0x3506424F91fD33084466F402d5D97f05F8e3b4AF", "decimals": 18, "min_threshold": 6_000},     # Lowered
    "ENJ": {"contract": "0xF629cBd94d3791C9250152BD8dfBDF380E2a3B9c", "decimals": 18, "min_threshold": 7_000},    # Enjin ecosystem
    
    # Layer 2 & Scaling (Early movers)
    "MATIC": {"contract": "0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0", "decimals": 18, "min_threshold": 8_000},    # Lowered
    "OP": {"contract": "0x4200000000000000000000000000000000000042", "decimals": 18, "min_threshold": 6_000},     # Lowered 
    "ARB": {"contract": "0x912CE59144191C1204E64559FE8253a0e49E6548", "decimals": 18, "min_threshold": 8_000},    # Lowered
    "LDO": {"contract": "0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32", "decimals": 18, "min_threshold": 6_000},     # Lowered
    
    # AI/Data Tokens (Hot sector)
    "FET": {"contract": "0xaea46A60368A7bD060eec7DF8CBa43b7EF41Ad85", "decimals": 18, "min_threshold": 6_000},    # Fetch.ai
    "OCEAN": {"contract": "0x967da4048cD07aB37855c090aAF366e4ce1b9F48", "decimals": 18, "min_threshold": 5_000},  # Ocean Protocol
    "AGIX": {"contract": "0x5B7533812759B45C2B44C19e320ba2cD2681b542", "decimals": 8, "min_threshold": 7_000},    # SingularityNET
    
    # Small DeFi Protocols (Explosive potential)
    "CRV": {"contract": "0xD533a949740bb3306d119CC777fa900bA034cd52", "decimals": 18, "min_threshold": 5_000},    # Curve
    "BAL": {"contract": "0xba100000625a3754423978a60c9317c58a424e3D", "decimals": 18, "min_threshold": 6_000},    # Balancer
    "SUSHI": {"contract": "0x6B3595068778DD592e39A122f4f5a5cF09C90fE2", "decimals": 18, "min_threshold": 4_000},  # SushiSwap
    "CVX": {"contract": "0x4e3FBD56CD56c3e72c1403e103b45Db9da5B9D2B", "decimals": 18, "min_threshold": 6_000},    # Convex
    
    # Major tokens but LOWER thresholds for more activity  
    "WETH": {"contract": "0xC02aaa39b223FE8D0A0e5C4F27ead9083C756Cc2", "decimals": 18, "min_threshold": 15_000},   # Lowered from 50K
    "LINK": {"contract": "0x514910771af9ca656af840dff83e8264ecf986ca", "decimals": 18, "min_threshold": 10_000},   # Lowered from 25K
    "UNI": {"contract": "0x1F9840a85d5aF5bf1D1762F925BDADdC4201F984", "decimals": 18, "min_threshold": 8_000},     # Lowered from 20K
    
    # REMOVED: USDC, USDT, DAI (no stablecoins - we want trending coins only)
}

# --- Polygon tokens to monitor ---
POLYGON_TOKENS_TO_MONITOR = {
    # Major DeFi tokens on Polygon
    "WMATIC": {"contract": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270", "decimals": 18, "min_threshold": 3_000},
    "WETH": {"contract": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619", "decimals": 18, "min_threshold": 5_000},
    "USDC": {"contract": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359", "decimals": 6, "min_threshold": 10_000},
    "USDC.e": {"contract": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174", "decimals": 6, "min_threshold": 10_000},
    "USDT": {"contract": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F", "decimals": 6, "min_threshold": 10_000},
    "DAI": {"contract": "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063", "decimals": 18, "min_threshold": 10_000},
    "AAVE": {"contract": "0xD6DF932A45C0f255f85145f286eA0b292B21C90B", "decimals": 18, "min_threshold": 1_000},
    "UNI": {"contract": "0xb33EaAd8d922B1083446DC23f610c2567fB5180f", "decimals": 18, "min_threshold": 1_000},
    "LINK": {"contract": "0x53E0bca35eC356BD5ddDFebbD1Fc0fD03FaBad39", "decimals": 18, "min_threshold": 3_000},
    "CRV": {"contract": "0x172370d5Cd63279eFa6d502DAB29171933a610AF", "decimals": 18, "min_threshold": 1_000},
    "SUSHI": {"contract": "0x0b3F868E0BE5597D5DB7fEB59E1CADBb0fdDa50a", "decimals": 18, "min_threshold": 1_000},
    "BAL": {"contract": "0x9a71012B13CA4d3D0Cdc72A177DF3ef03b0E76A3", "decimals": 18, "min_threshold": 1_000},
    "COMP": {"contract": "0x8505b9d2254A7Ae468c0E9dd10Ccea3A837aef5c", "decimals": 18, "min_threshold": 1_000},
    "SNX": {"contract": "0x50B728D8D964fd00C2d0AAD81718b71311feF68a", "decimals": 18, "min_threshold": 1_000},
    "1INCH": {"contract": "0x9c2C5fd7b07E95EE044DDeba0E97a665F142394f", "decimals": 18, "min_threshold": 500},
    "GHST": {"contract": "0x385Eeac5cB85A38A9a07A70c73e0a3271CfB54A7", "decimals": 18, "min_threshold": 500},
    "QUICK": {"contract": "0xB5C064F955D8e7F38fE0460C556a72987494eE17", "decimals": 18, "min_threshold": 500},
    "DQUICK": {"contract": "0x958d208Cdf087843e9AD98d23823d32E17d723A1", "decimals": 18, "min_threshold": 500},
}

SOL_TOKENS_TO_MONITOR = {
    # Major Solana Tokens
    "SOL": {"mint": "So11111111111111111111111111111111111111112", "decimals": 9, "min_threshold": 5_000_000},
    "BONK": {"mint": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263", "decimals": 5, "min_threshold": 1_500},
    "RAY": {"mint": "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R", "decimals": 6, "min_threshold": 2_500},
    "SAMO": {"mint": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU", "decimals": 9, "min_threshold": 1_500},
    "DUST": {"mint": "DUSTawucrTsGU8hcqRdHDCbuYhCPADMLM2VcCb8VnFnQ", "decimals": 9, "min_threshold": 2_500},
    "ORCA": {"mint": "orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE", "decimals": 6, "min_threshold": 1_500},
    "MSOL": {"mint": "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So", "decimals": 9, "min_threshold": 2_500},
    "SRM": {"mint": "SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt", "decimals": 6, "min_threshold": 2_500},
    "MNGO": {"mint": "MangoCzJ36AjZyKwVj3VnYU4GTonjfVEnJmvvWaxLac", "decimals": 6, "min_threshold": 1_000},
    "ATLAS": {"mint": "ATLASXmbPQxBUYbxPsV97usA3fPQYEqzQBUHgiFCUsXx", "decimals": 8, "min_threshold": 1_000},
    
    # Additional Solana Tokens
    "JTO": {"mint": "jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9wYA", "decimals": 9, "min_threshold": 1_500},
    "PYTH": {"mint": "HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3", "decimals": 6, "min_threshold": 1_500},
    "BSOL": {"mint": "bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1", "decimals": 9, "min_threshold": 2_500},
    "WIF": {"mint": "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLZYQJV5sCvpr", "decimals": 6, "min_threshold": 1_000},
    "RENDER": {"mint": "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R", "decimals": 8, "min_threshold": 1_500},
    "MEAN": {"mint": "MEANeD3XDdUmNMsRGjASkSWdC8prLYsoRJ61pPeHctD", "decimals": 6, "min_threshold": 1_000},
    "UXDY": {"mint": "UXD8m9cvwk4RcSxnX2HZ9VudQCEeDH6mQRm2YaTFstq", "decimals": 6, "min_threshold": 2_000},
    "USDR": {"mint": "USDrbBQwQbQ2oWHUPfA8QBHcyVxKUq1xHyXsSLKdUq2", "decimals": 6, "min_threshold": 5_000},
    "SHDW": {"mint": "SHDWyBxihqiCj6YekG2GUr7wqKLeLAMK1gHZck9pL6y", "decimals": 9, "min_threshold": 1_500},
    "COPE": {"mint": "8HGyAAB1yoM1ttS7pXjHMa3dukTFGQggnFFH3hJZgzQh", "decimals": 6, "min_threshold": 1_000}
}

# --- TOKEN PRICES (USD) for ERC‑20 tokens ---
TOKEN_PRICES = {
    # Original tokens
    "WETH": 1600,
    "LINK": 7,
    "UNI": 5,
    "AAVE": 75,
    "COMP": 60,
    "SNX": 2.5,
    "MKR": 1500,
    "YFI": 25000,
    "SUSHI": 2.5,
    "CRV": 2.5,
    "BAL": 10,
    "BNT": 2,
    "REN": 0.3,
    "OMG": 1.5,
    "ZRX": 0.4,
    "BAT": 0.6,
    "GRT": 0.5,
    "LRC": 0.3,
    "1INCH": 2.5,
    "MATIC": 1,
    
    # New Ethereum tokens
    "PEPE": 0.0000008,
    "SHIB": 0.00002,
    "APE": 1.5,
    "DYDX": 3.5,
    "OP": 3.0,
    "ARB": 1.2,
    "FRAX": 1.0,
    "LUSD": 1.0,
    "FEI": 1.0,
    "CVX": 4.5,
    "FXS": 7.0,
    "LDO": 2.5,
    "RPL": 30.0,
    "INJ": 35.0,
    
    # Polygon token prices
    "WMATIC": 1.0,
    "GHST": 1.2,
    "QUICK": 0.05,
    "DQUICK": 0.1
}

# Update TOKEN_PRICES with current approximate values
TOKEN_PRICES.update({
    # Original Solana tokens
    "SOL": 150,
    "BONK": 0.00001,
    "RAY": 0.35,
    "SAMO": 0.015,
    "DUST": 0.5,
    "ORCA": 0.45,
    "MSOL": 155,  # Slightly higher than SOL due to staking
    "SRM": 0.1,
    "MNGO": 0.02,
    "ATLAS": 0.01,
    
    # New Solana tokens
    "JTO": 2.0,
    "PYTH": 0.5,
    "BSOL": 160,  # Slightly higher than SOL
    "WIF": 0.15,
    "RENDER": 8.0,
    "MEAN": 0.03,
    "UXDY": 1.0,
    "USDR": 1.0,
    "SHDW": 0.05,
    "COPE": 0.12
})

STABLE_COINS = {"usdt", "usdc", "dai", "tusd", "busd"}

# Alias for backwards compatibility
common_stablecoins = STABLE_COINS# Alias for backwards compatibility
common_stablecoins = STABLE_COINS

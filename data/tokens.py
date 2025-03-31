# =======================
# GLOBAL VARIABLES & COUNTERS
# =======================

# --- ERC‑20 tokens to monitor on Ethereum ---
TOKENS_TO_MONITOR = {
    # Major DeFi & Layer 1/2
    "WETH": {"contract": "0xC02aaa39b223FE8D0A0e5C4F27ead9083C756Cc2", "decimals": 18, "min_threshold": 5_000},
    "LINK": {"contract": "0x514910771af9ca656af840dff83e8264ecf986ca", "decimals": 18, "min_threshold": 5_000},
    "UNI":  {"contract": "0x1F9840a85d5aF5bf1D1762F925BDADdC4201F984", "decimals": 18, "min_threshold": 1_000},
    "AAVE": {"contract": "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9", "decimals": 18, "min_threshold": 1_000},
    "COMP": {"contract": "0xc00e94Cb662C3520282E6f5717214004A7f26888", "decimals": 18, "min_threshold": 1_000},
    "SNX":  {"contract": "0xC011a73ee8576Fb46F5E1c5751cA3B9Fe0Af2a6F", "decimals": 18, "min_threshold": 1_000},
    "MKR":  {"contract": "0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2", "decimals": 18, "min_threshold": 1_500},
    "YFI":  {"contract": "0x0bc529c00C6401aEF6D220BE8C6Ea1667F6Ad93e", "decimals": 18, "min_threshold": 5_000},
    "SUSHI": {"contract": "0x6B3595068778DD592e39A122f4f5a5cF09C90fE2", "decimals": 18, "min_threshold": 1_000},
    "CRV":  {"contract": "0xD533a949740bb3306d119CC777fa900bA034cd52", "decimals": 18, "min_threshold": 1_000},
    "BAL":  {"contract": "0xba100000625a3754423978a60c9317c58a424e3D", "decimals": 18, "min_threshold": 1_000},
    "BNT":  {"contract": "0x1F573D6Fb3F13d689FF844B4cE37794d79A7FF1C", "decimals": 18, "min_threshold": 500},
    "REN":  {"contract": "0x408e41876cCCDC0F92210600ef50372656052a38", "decimals": 18, "min_threshold": 500},
    "OMG":  {"contract": "0xd26114cd6EE289AccF82350c8d8487fedB8A0C07", "decimals": 18, "min_threshold": 500},
    "ZRX":  {"contract": "0xE41d2489571d322189246DaFA5ebDe1F4699F498", "decimals": 18, "min_threshold": 500},
    "BAT":  {"contract": "0x0D8775F648430679A709E98d2b0Cb6250d2887EF", "decimals": 18, "min_threshold": 500},
    "GRT":  {"contract": "0xC944E90C64B2c07662A292be6244BDf05Cda44a7", "decimals": 18, "min_threshold": 500},
    "LRC":  {"contract": "0xBBbbCA6A901c926F240b89EacB641d8Aec7AEafD", "decimals": 18, "min_threshold": 500},
    "1INCH": {"contract": "0x111111111117dC0aa78b770fA6A738034120C302", "decimals": 18, "min_threshold": 500},
    "MATIC": {"contract": "0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0", "decimals": 18, "min_threshold": 1_500},
    
    # Additional Popular Tokens
    "PEPE": {"contract": "0x6982508145454ce325ddbe47a25d4ec3d2311933", "decimals": 18, "min_threshold": 500},
    "SHIB": {"contract": "0x95aD61b0a150d79219dCF64E1E6Cc01f0B64C4cE", "decimals": 18, "min_threshold": 500},
    "APE": {"contract": "0x4d224452801ACEd8B2F0aebE155379bb5D594381", "decimals": 18, "min_threshold": 1_000},
    "DYDX": {"contract": "0x92D6C1e31e14520e676a687F0a93788B716BEff5", "decimals": 18, "min_threshold": 1_000},
    "OP": {"contract": "0x4200000000000000000000000000000000000042", "decimals": 18, "min_threshold": 1_000},
    "ARB": {"contract": "0x912CE59144191C1204E64559FE8253a0e49E6548", "decimals": 18, "min_threshold": 1_000},
    
    # Stablecoins (for non-excluded stablecoin monitoring)
    "FRAX": {"contract": "0x853d955aCEf822Db058eb8505911ED77F175b99e", "decimals": 18, "min_threshold": 10_000},
    "LUSD": {"contract": "0x5f98805A4E8be255a32880FDeC7F6728C6568bA0", "decimals": 18, "min_threshold": 5_000},
    "FEI": {"contract": "0x956F47F50A910163D8BF957Cf5846D573E7f87CA", "decimals": 18, "min_threshold": 5_000},
    
    # DeFi
    "CVX": {"contract": "0x4e3FBD56CD56c3e72c1403e103b45Db9da5B9D2B", "decimals": 18, "min_threshold": 1_000},
    "FXS": {"contract": "0x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0", "decimals": 18, "min_threshold": 1_000},
    "LDO": {"contract": "0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32", "decimals": 18, "min_threshold": 1_000},
    "RPL": {"contract": "0xD33526068D116cE69F19A9ee46F0bd304F21A51f", "decimals": 18, "min_threshold": 1_000},
    "INJ": {"contract": "0xe28b3B32B6c345A34Ff64674606124Dd5Aceca30", "decimals": 18, "min_threshold": 1_000}
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
    "INJ": 35.0
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
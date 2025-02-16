# =======================
# GLOBAL VARIABLES & COUNTERS
# =======================

# --- ERC‑20 tokens to monitor on Ethereum ---
TOKENS_TO_MONITOR = {
    "WETH": {"contract": "0xC02aaa39b223FE8D0A0e5C4F27ead9083C756Cc2", "decimals": 18, "min_threshold": 1_000_000},
    "LINK": {"contract": "0x514910771af9ca656af840dff83e8264ecf986ca", "decimals": 18, "min_threshold": 1_000_000},
    "UNI":  {"contract": "0x1F9840a85d5aF5bf1D1762F925BDADdC4201F984", "decimals": 18, "min_threshold": 1_000_000},
    "AAVE": {"contract": "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9", "decimals": 18, "min_threshold": 1_000_000},
    "COMP": {"contract": "0xc00e94Cb662C3520282E6f5717214004A7f26888", "decimals": 18, "min_threshold": 1_000_000},
    "SNX":  {"contract": "0xC011a73ee8576Fb46F5E1c5751cA3B9Fe0Af2a6F", "decimals": 18, "min_threshold": 1_000_000},
    "MKR":  {"contract": "0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2", "decimals": 18, "min_threshold": 1_000_000},
    "YFI":  {"contract": "0x0bc529c00C6401aEF6D220BE8C6Ea1667F6Ad93e", "decimals": 18, "min_threshold": 1_000_000},
    "SUSHI": {"contract": "0x6B3595068778DD592e39A122f4f5a5cF09C90fE2", "decimals": 18, "min_threshold": 1_000_000},
    "CRV":  {"contract": "0xD533a949740bb3306d119CC777fa900bA034cd52", "decimals": 18, "min_threshold": 1_000_000},
    "BAL":  {"contract": "0xba100000625a3754423978a60c9317c58a424e3D", "decimals": 18, "min_threshold": 1_000_000},
    "BNT":  {"contract": "0x1F573D6Fb3F13d689FF844B4cE37794d79A7FF1C", "decimals": 18, "min_threshold": 1_000_000},
    "REN":  {"contract": "0x408e41876cCCDC0F92210600ef50372656052a38", "decimals": 18, "min_threshold": 1_000_000},
    "OMG":  {"contract": "0xd26114cd6EE289AccF82350c8d8487fedB8A0C07", "decimals": 18, "min_threshold": 1_000_000},
    "ZRX":  {"contract": "0xE41d2489571d322189246DaFA5ebDe1F4699F498", "decimals": 18, "min_threshold": 1_000_000},
    "BAT":  {"contract": "0x0D8775F648430679A709E98d2b0Cb6250d2887EF", "decimals": 18, "min_threshold": 1_000_000},
    "GRT":  {"contract": "0xC944E90C64B2c07662A292be6244BDf05Cda44a7", "decimals": 18, "min_threshold": 1_000_000},
    "LRC":  {"contract": "0xBBbbCA6A901c926F240b89EacB641d8Aec7AEafD", "decimals": 18, "min_threshold": 1_000_000},
    "1INCH": {"contract": "0x111111111117dC0aa78b770fA6A738034120C302", "decimals": 18, "min_threshold": 1_000_000},
    "MATIC": {"contract": "0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0", "decimals": 18, "min_threshold": 1_000_000}
}

# --- TOKEN PRICES (USD) for ERC‑20 tokens ---
TOKEN_PRICES = {
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
    "MATIC": 1
}

SOL_TOKENS_TO_MONITOR = {
    "SOL": {"mint": "So11111111111111111111111111111111111111112", "decimals": 9, "min_threshold": 10_000_000},
    "BONK": {"mint": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263", "decimals": 5, "min_threshold": 1_000_000},
    "RAY": {"mint": "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R", "decimals": 6, "min_threshold": 1_000_000},
    "SAMO": {"mint": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU", "decimals": 9, "min_threshold": 1_000_000},
    "DUST": {"mint": "DUSTawucrTsGU8hcqRdHDCbuYhCPADMLM2VcCb8VnFnQ", "decimals": 9, "min_threshold": 1_000_000},
    "ORCA": {"mint": "orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE", "decimals": 6, "min_threshold": 1_000_000},
    "MSOL": {"mint": "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So", "decimals": 9, "min_threshold": 1_000_000},
    "SRM": {"mint": "SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt", "decimals": 6, "min_threshold": 1_000_000},
    "MNGO": {"mint": "MangoCzJ36AjZyKwVj3VnYU4GTonjfVEnJmvvWaxLac", "decimals": 6, "min_threshold": 1_000_000},
    "ATLAS": {"mint": "ATLASXmbPQxBUYbxPsV97usA3fPQYEqzQBUHgiFCUsXx", "decimals": 8, "min_threshold": 1_000_000}
}

# Update TOKEN_PRICES with current approximate values
TOKEN_PRICES.update({
    "SOL": 150,
    "BONK": 0.00001,
    "RAY": 0.35,
    "SAMO": 0.015,
    "DUST": 0.5,
    "ORCA": 0.45,
    "MSOL": 155,  # Slightly higher than SOL due to staking
    "SRM": 0.1,
    "MNGO": 0.02,
    "ATLAS": 0.01
})

STABLE_COINS = {"usdt", "usdc", "dai", "tusd", "busd"}
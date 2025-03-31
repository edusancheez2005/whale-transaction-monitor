"""Known market maker and high-frequency trading addresses to filter"""

MARKET_MAKER_ADDRESSES = {
    # Solana market makers
    "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1": "binance_mm",
    "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin": "serum_mm",
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "raydium_mm",
    "orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE": "orca_mm",
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc": "orca_whirlpool_mm",
    
    # Ethereum market makers
    "0x56178a0d5f301baf6cf3e1cd53d9863437345bf9": "wintermute",
    "0xf584f8728b874a6a5c7a8d4d387c9aae9172d621": "jump_trading",
    "0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88": "alameda_research",
    "0x3ccdf48c5b8040526815e47322dfd0b524f390d9": "wintermute_2",
    "0x0e5069514a3dd613350bab01b58fd850058e5ca4": "wintermute_3",
    "0x000000006daea1723962647b7e189d311d757fb793": "wintermute_4",
    "0x000002cba8dfb0a86a47a415592835e17fac080a": "wintermute_5",
    "0x21b2be9090d1d319e67a981d42811ba5a4e9b35e": "dv_trading",
    
    # XRP market makers
    "rmj21ybvEi7HeznSkH4srdV7RDQudRaUp": "xrp_mm_1",
    "rKCCdnrYFscTmwW4GvpY42PihhMjn4BN2s": "xrp_mm_2", 
    "rnCAb3meNXyBtVRrox6fwVNwy7877ZdsiF": "xrp_mm_3",
    "rf75gv7kTbN4X19dTh1NLy8Q92Dw1xJb4r": "xrp_mm_4",
    "rDsxqzXUrmNs2jnqvG7pS2g2qA3x43uQyE": "xrp_mm_5",
}

# Configuration for filtering
FILTER_SETTINGS = {
    # Set to True to completely filter out market maker transactions 
    "exclude_market_makers": False,
    
    # Set to True to only show one side of market maker transactions
    "deduplicate_market_makers": True,
    
    # Time window in seconds to consider for detecting wash trading
    "wash_trade_window": 60,
    
    # Minimum time between transactions to not be considered wash trading (seconds)
    "min_transaction_interval": 5
}
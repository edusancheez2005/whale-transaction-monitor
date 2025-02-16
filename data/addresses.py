DEX_ADDRESSES = {
    # Uniswap v2/v3
    "0x7a250d5630b4cf539739df2c5dacb4c659f2488d": "uniswap_v2_router",
    "0xe592427a0aece92de3edee1f18e0157c05861564": "uniswap_v3_router",
    "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45": "uniswap_v3_router_2",
    
    # Sushiswap
    "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f": "sushiswap_router",
    "0x1b02da8cb0d097eb8d57a175b88c7d8b47997506": "sushiswap_router_2",
    
    # Curve
    "0x99a58482bd75cbab83b27ec03ca68ff489b5788f": "curve_pool",
    "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7": "curve_3pool",
    "0xa2b47e3d5c44877cca798226b7b8118f9bfb7a56": "curve_compound",
    
    # Balancer
    "0xba12222222228d8ba445958a75a0704d566bf2c8": "balancer_vault",
    "0x9424b1412450d0f8fc2255faf6046b98213b76bd": "balancer_pool",
    
    # 1inch
    "0x1111111254fb6c44bac0bed2854e76f90643097d": "1inch_v4_router",
    "0x1111111254eeb25477b68fb85ed929f73a960582": "1inch_v5_router",
    
    # Paraswap
    "0xdef171fe48cf0115b1d80b88dc8eab59176fee57": "paraswap_v5",
    "0x216b4b4ba9f3e719726886d34a177484278bfcae": "paraswap_v4",

    # Binance
    "0x28c6c06298d514db089934071355e5743bf21d60": "binance_hot_1",
    "0x21a31ee1afc51d94c2efccaa2092ad1028285549": "binance_hot_2",
    "0x9696f59e4d72e237be84ffd425dcad154bf96976": "binance_hot_3",
    
    # Coinbase
    "0x71660c4005ba85c37ccec55d0c4493e66fe775d3": "coinbase_1",
    "0xa090e606e30bd747d4e6245a1517ebe430f0057e": "coinbase_2",
    "0x71660c4005ba85c37ccec55d0c4493e66fe775d3": "coinbase_hot",
    
    # OKX
    "0x6cc5f688a315f3dc28a7781717a9a798a59fda7b": "okx_1",
    "0x5041ed759dd4afc3a72b8192c143f72f4724081a": "okx_2",
    "0x236f9f97e0e62388479bf9e5ba4889e46b0273c3": "okx_3",
    
    # KuCoin
    "0x2b5634c42055806a59e9107ed44d43c426e58258": "kucoin_1",
    "0xf16e9b0d03470827a95cdfd0cb8a8a3b46969b91": "kucoin_2",
    
    # Crypto.com
    "0x6262998ced04146fa42253a5c0af90ca02dfd2a3": "crypto_com_1",
    "0x46340b20830761efd32832a74d7169b29feb9758": "crypto_com_2"
}

SOLANA_DEX_ADDRESSES = {
    # Raydium
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "raydium_amm",
    "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1": "raydium_pool",
    
    # Serum/OpenBook
    "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin": "serum_dex",
    "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX": "serum_pools",
    
    # Orca
    "orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE": "orca_pools",
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc": "orca_whirlpools"
}


MARKET_MAKER_ADDRESSES = {
    "0x56178a0d5f301baf6cf3e1cd53d9863437345bf9": "wintermute",
    "0xf584f8728b874a6a5c7a8d4d387c9aae9172d621": "jump_trading",
    "0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88": "alameda_research",
    "0x3ccdf48c5b8040526815e47322dfd0b524f390d9": "wintermute_2",
    "0x21b2be9090d1d319e67a981d42811ba5a4e9b35e": "dv_trading",
    "0x000000000000000000000000000000000000dead": "burn_address"
}
known_exchange_addresses = {
    "0x28c6c06298d514db089934071355e5743bf21d60": "binance",
    "0xdfd5293d8e347dfe59e90efd55b2956a1343963d": "binance",
    "0x5a52e96bacdabb82fd05763e25335261b270efcb": "binance",
    "0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be": "binance",
    "0x71660c4005ba85c37ccec55d0c4493e66fe775d3": "coinbase",
    "0x503828976d22510aad0201ac7ec88293211d23da": "coinbase",
    "0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740": "coinbase",
    "0x2faf487a4414fe77e2327f0bf4ae2a264a776ad2": "ftx",
    "0xc098b2a3aa256d2140208c3de6543aaef5cd3a94": "ftx",
    "0x2910543af39aba0cd09dbb2d50200b3e800a63d2": "kraken",
    "0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13": "kraken",
    "0xa83b11093c858c86321fbc4c20fe82cdbd58e09e": "kraken",
    "0x2b5634c42055806a59e9107ed44d43c426e58258": "kucoin",
    "0x689c56aef474df92d44a1b70850f808488f9769c": "kucoin",
    "0x0d0707963952f2fba59dd06f2b425ace40b492fe": "huobi/gate.io",
    "0x1062a747393198f70f71ec65a582423dba7e5ab3": "huobi/gate.io",
    "0x6cc5f688a315f3dc28a7781717a9a798a59fda7b": "okx",
    "0x236f9f97e0e62388479bf9e5ba4889e46b0273c3": "okx",
    "0x7a250d5630b4cf539739df2c5dacb4c659f2488d": "uniswap",
    # Binance
    "0x28c6c06298d514db089934071355e5743bf21d60": "binance",
    "0xdfd5293d8e347dfe59e90efd55b2956a1343963d": "binance",
    "0x5a52e96bacdabb82fd05763e25335261b270efcb": "binance",
    "0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be": "binance",
    "0x85b931a32a0725be14285b66f1a22178c672d69b": "binance",
    
    # Coinbase
    "0x71660c4005ba85c37ccec55d0c4493e66fe775d3": "coinbase",
    "0x503828976d22510aad0201ac7ec88293211d23da": "coinbase",
    "0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740": "coinbase",
    "0xa090e606e30bd747d4e6245a1517ebe430f0057e": "coinbase",
    
    # Crypto.com
    "0x6262998ced04146fa42253a5c0af90ca02dfd2a3": "crypto.com",
    "0x46340b20830761efd32832a74d7169b29feb9758": "crypto.com",
    
    # Kraken
    "0x2910543af39aba0cd09dbb2d50200b3e800a63d2": "kraken",
    "0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13": "kraken",
    "0xa83b11093c858c86321fbc4c20fe82cdbd58e09e": "kraken",
    "0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0": "kraken",
    
    # KuCoin
    "0x2b5634c42055806a59e9107ed44d43c426e58258": "kucoin",
    "0x689c56aef474df92d44a1b70850f808488f9769c": "kucoin",
    "0xf16e9b0d03470827a95cdfd0cb8a8a3b46969b91": "kucoin",
    
    # Huobi/Gate.io
    "0x0d0707963952f2fba59dd06f2b425ace40b492fe": "huobi",
    "0x1062a747393198f70f71ec65a582423dba7e5ab3": "huobi",
    "0xab5c66752a9e8167967685f1450532fb96d5d24f": "huobi",
    
    # OKX
    "0x6cc5f688a315f3dc28a7781717a9a798a59fda7b": "okx",
    "0x236f9f97e0e62388479bf9e5ba4889e46b0273c3": "okx",
    "0x5041ed759dd4afc3a72b8192c143f72f4724081a": "okx",
    
    # Bitfinex
    "0x876eabf441b2ee5b5b0554fd502a8e0600950cfa": "bitfinex",
    "0x742d35cc6634c0532925a3b844bc454e4438f44e": "bitfinex",
    
    # Major DEXes
    "0x7a250d5630b4cf539739df2c5dacb4c659f2488d": "uniswap",
    "0xdef1c0ded9bec7f1a1670819833240f027b25eff": "0x_proxy",
    "0x11111112542d85b3ef69ae05771c2dccff4faa26": "1inch",
    "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f": "sushiswap",
    
    # Curve/Aave/Compound
    "0x5a6a4d54456819380173272a5e8e9b9904bdf41b": "curve",
    "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9": "aave",
    "0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b": "compound",

    # Binance
    "0x28c6c06298d514db089934071355e5743bf21d60": "binance",
    "0xdfd5293d8e347dfe59e90efd55b2956a1343963d": "binance",
    "0x5a52e96bacdabb82fd05763e25335261b270efcb": "binance",
    "0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be": "binance",
    "0x85b931a32a0725be14285b66f1a22178c672d69b": "binance",
    "0x708396f17127c42383e3b9014072679b2f60b82f": "binance",
    "0xe0f0cfde7ee664943906f17f7f14342e76a5cec7": "binance",
    
    # Coinbase
    "0x71660c4005ba85c37ccec55d0c4493e66fe775d3": "coinbase",
    "0x503828976d22510aad0201ac7ec88293211d23da": "coinbase",
    "0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740": "coinbase",
    "0xa090e606e30bd747d4e6245a1517ebe430f0057e": "coinbase",
    "0xf6c0aa7ebfe9992200c67e5388e546f7d1362713": "coinbase",
    "0x58553f5c5e55f2393cf6e65527847aef599e4a46": "coinbase",
    
    # Kraken
    "0x2910543af39aba0cd09dbb2d50200b3e800a63d2": "kraken",
    "0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13": "kraken",
    "0xa83b11093c858c86321fbc4c20fe82cdbd58e09e": "kraken",
    "0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0": "kraken",
    "0x53d284357ec70ce289d6d64134dfac8e511c8a3d": "kraken",
    
    # OKX
    "0x6cc5f688a315f3dc28a7781717a9a798a59fda7b": "okx",
    "0x236f9f97e0e62388479bf9e5ba4889e46b0273c3": "okx",
    "0x5041ed759dd4afc3a72b8192c143f72f4724081a": "okx",
    "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984": "okx",
    
    # KuCoin
    "0x2b5634c42055806a59e9107ed44d43c426e58258": "kucoin",
    "0xf16e9b0d03470827a95cdfd0cb8a8a3b46969b91": "kucoin",
    "0x689c56aef474df92d44a1b70850f808488f9769c": "kucoin",
    "0x0d0707963952f2fba59dd06f2b425ace40b492fe": "kucoin",
    
    # Gate.io
    "0x0d0707963952f2fba59dd06f2b425ace40b492fe": "gate.io",
    "0x1062a747393198f70f71ec65a582423dba7e5ab3": "gate.io",
    "0xd793281182a0e3e023116004778f45c29fc14f19": "gate.io",
    
    # Huobi
    "0xab5c66752a9e8167967685f1450532fb96d5d24f": "huobi",
    "0x6748f50f686bfbca6fe8ad62b22228b87f31ff2b": "huobi",
    "0xfdb16996831753d5331ff813c29a93c76834a0ad": "huobi",
    "0xeee28d484628d41a82d01e21d12e2e78d69920da": "huobi",
    
    # Bitfinex
    "0x876eabf441b2ee5b5b0554fd502a8e0600950cfa": "bitfinex",
    "0x742d35cc6634c0532925a3b844bc454e4438f44e": "bitfinex",
    "0x876eabf441b2ee5b5b0554fd502a8e0600950cfa": "bitfinex",
    "0x1151314c646ce4e0efd76d1af4760ae66a9fe30f": "bitfinex",
    
    # Gemini
    "0x5f65f7b609678448494de4c87521cdf6cef1e932": "gemini",
    "0xd24400ae8bfebb18ca49be86258a3c749cf46853": "gemini",
    "0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8": "gemini",
    
    # BitMart
    "0xe79eef9b9388a4ff70ed7ec5bccd5b928ebb8bd1": "bitmart",
    "0x68b22215ff8677f520d6d7789e78dd452f9686ac": "bitmart",
    
    # Crypto.com
    "0x6262998ced04146fa42253a5c0af90ca02dfd2a3": "crypto.com",
    "0x46340b20830761efd32832a74d7169b29feb9758": "crypto.com",
    "0x72a53cdbbcc1b9efa39c834a540550e23463aacb": "crypto.com",
}



solana_exchange_addresses = {
    "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1": "binance",
    "FdAXT4XPsswRhaJveaB45Lz9CFbKHGpyY3rSyRFRGArj": "bybit",
    "3Z4oLTsytjW5K2sgEYiDEbykerbYm6SnAJQm5kMQoZxd": "okx",
    "HSsAV4suBdFgaybUwzWDK1rR14FNQ73BcF3kAM7rRkbF": "kraken",
    "HU23r7UoZbqTUuh3vA7emAGztFtqwTeVips789vqxxBw": "kucoin",
    "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1": "Binance",
    "FdAXT4XPsswRhaJveaB45Lz9CFbKHGpyY3rSyRFRGArj": "Bybit",
    "3Z4oLTsytjW5K2sgEYiDEbykerbYm6SnAJQm5kMQoZxd": "OKX",
    "HSsAV4suBdFgaybUwzWDK1rR14FNQ73BcF3kAM7rRkbF": "Kraken",
    "HU23r7UoZbqTUuh3vA7emAGztFtqwTeVips789vqxxBw": "KuCoin",
    "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM": "Crypto.com",
    "73tF8uN3BwVzUzwETv59CvCF4oqzNtkUxKdJuFLHqmD9": "Gate.io",
    "AFrks6SxLK3FNKpKPdpx5DsFYhQZk8VKnz9BcVQxhYaY": "Huobi",
    # Binance
    "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1": "binance",
    "6QEJkDV8NhHc4pUCAP3v6n5h5osHUqR1xCEhUAX8e9bL": "binance",
    "BQcdHdAQW1hczDbBi9hiegXAR7A98Q9jx3X3iBBBDiq4": "binance",
    
    # Bybit
    "FdAXT4XPsswRhaJveaB45Lz9CFbKHGpyY3rSyRFRGArj": "bybit",
    "CxgKH1eNqR9yG6nxh8Fkf1ST1gN1fJ1Ve6i5Hs5qvmZw": "bybit",
    
    # OKX
    "3Z4oLTsytjW5K2sgEYiDEbykerbYm6SnAJQm5kMQoZxd": "okx",
    "9vqMJjqqH5zGvpVZgAzGpVksY1BWc5sKYyYrGnF6vv7E": "okx",
    
    # Kraken
    "HSsAV4suBdFgaybUwzWDK1rR14FNQ73BcF3kAM7rRkbF": "kraken",
    "2rsGk3LpJNfSGxeUGmwRm6YJ4KaBM8uH1puNHjk2BkEH": "kraken",
    
    # KuCoin
    "HU23r7UoZbqTUuh3vA7emAGztFtqwTeVips789vqxxBw": "kucoin",
    "3YBNs3aCY1mEPs3B7AgTHZhKUiJ8Y6w5ETwinK7jF3k9": "kucoin",
    
    # Crypto.com
    "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM": "crypto.com",
    
    # Gate.io
    "73tF8uN3BwVzUzwETv59CvCF4oqzNtkUxKdJuFLHqmD9": "gate.io",
    
    # Huobi
    "AFrks6SxLK3FNKpKPdpx5DsFYhQZk8VKnz9BcVQxhYaY": "huobi",
    
    # Major Solana DEXes
    "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin": "serum_dex",
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "raydium",
    "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1": "orca",
    # Binance
    "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1": "binance",
    "6QEJkDV8NhHc4pUCAP3v6n5h5osHUqR1xCEhUAX8e9bL": "binance",
    "BQcdHdAQW1hczDbBi9hiegXAR7A98Q9jx3X3iBBBDiq4": "binance",
    "73tF8uN3BwVzUzwETv59CvCF4oqzNtkUxKdJuFLHqmD9": "binance",
    
    # OKX
    "3Z4oLTsytjW5K2sgEYiDEbykerbYm6SnAJQm5kMQoZxd": "okx",
    "9vqMJjqqH5zGvpVZgAzGpVksY1BWc5sKYyYrGnF6vv7E": "okx",
    "GJR8H7HLkQHHgpX3HPAewCbYqcqcm8qQQvJe3gCNf3ut": "okx",
    
    # Kraken
    "HSsAV4suBdFgaybUwzWDK1rR14FNQ73BcF3kAM7rRkbF": "kraken",
    "2rsGk3LpJNfSGxeUGmwRm6YJ4KaBM8uH1puNHjk2BkEH": "kraken",
    "6WxEWKmYhQwqmNwGxfP3P6vp3ez5YkCsHEZ9cz6A3h5G": "kraken",
    
    # KuCoin
    "HU23r7UoZbqTUuh3vA7emAGztFtqwTeVips789vqxxBw": "kucoin",
    "3YBNs3aCY1mEPs3B7AgTHZhKUiJ8Y6w5ETwinK7jF3k9": "kucoin",
    "FGBqRKFJqoE95FqGqRxv6BiCevDR3t8jsNGm8hJEuhyM": "kucoin",
    
    # Bybit
    "FdAXT4XPsswRhaJveaB45Lz9CFbKHGpyY3rSyRFRGArj": "bybit",
    "CxgKH1eNqR9yG6nxh8Fkf1ST1gN1fJ1Ve6i5Hs5qvmZw": "bybit",
    "6gnCPhXtbNcZRJ6H9wVY7Febs2KgNNzBksBKxTNBKX7K": "bybit",
    
    # Gate.io
    "73tF8uN3BwVzUzwETv59CvCF4oqzNtkUxKdJuFLHqmD9": "gate.io",
    "B1bpzXfyM1SgWBUvpXjhgZBJeQEgZGZhbSy8CqnVZhE9": "gate.io",
    
    # Huobi
    "AFrks6SxLK3FNKpKPdpx5DsFYhQZk8VKnz9BcVQxhYaY": "huobi",
    "DxetQ1BMGSJm6tWce6aZcUjVWD5hUmCJmxWJ7NTVbS7P": "huobi"
}

xrp_exchange_addresses = {
    # Binance addresses
    "rLNaPoKeeBjZe2qs6x52yVPZpZ8td4dc6w": "binance",
    "rEb8TK3gBgk5auZkwc6sHnwrGVJH8DuaLh": "binance_hot",
    "rJb5KsHsDHF1YS5B5DU6QCkH5NsPaKQTcy": "binance_cold",
    
    # Other major exchanges
    "rEy8TFcrAPvhpKrwyrscNYyqBGUkE9hKaJ": "huobi",
    "rLW9gnQo7BQhU6igk5keqYnH3TVrCxGRzm": "bittrex",
    "rG6FZ31hDHN1K5Dkbma3PSB5uVCuVVRzfn": "bitso",
    "rJHb8RCfuB89HCwE7wo4W9d8qHEQhh7bVK": "bitstamp",
    "rNQEMJw3sAoXpYUe4gr9C1Js5EZK3cVUmJ": "coinbase",
    "rL9vUaa1p16YWWvkmXsQEGv9uQS65AXRPS": "kraken",
    "rHVLgqh1xS7PBWmhgAHMG9P1mnTRB269D8": "kraken_2",
    "rUobSiUpHCX1WEMRaZ8C1HTqpEqwQHC5Ns": "upbit",
    "r9x5KeWFx3mWHyPqB2NZAUPh1S7rcXK6CP": "gateio"
}
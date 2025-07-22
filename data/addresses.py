# Task 2: Address Type Confidence Modifiers for Whale Intelligence
# These modifiers are applied to confidence scores based on address categorization
ADDRESS_TYPE_CONFIDENCE_MODIFIERS = {
    'MARKET_MAKER': 0.20,        # High confidence boost for market maker activity
    'MIXER': -0.10,              # Reduce confidence for privacy coin mixers
    'MEGA_WHALE': 0.15,          # Strong confidence boost for mega whales
    'CEX_DEPOSIT_WALLET': 0.10,  # Moderate boost for exchange deposit patterns
    'HIGH_VOLUME_WHALE': 0.12,   # Moderate boost for high volume whales  
    'FREQUENT_TRADER': 0.08,     # Small boost for frequent trading activity
    'INSTITUTIONAL': 0.15,       # Strong boost for institutional addresses
    'PROTOCOL': 0.05,            # Small boost for protocol interactions
    'DEX_AGGREGATOR': 0.10,      # Moderate boost for DEX aggregator routing
    'BRIDGE': -0.05,             # Small reduction for bridge transfers
    'SCAMMER': -0.30,            # Large reduction for flagged scammer addresses
    'UNKNOWN': 0.00              # No modifier for unknown address types
}

# =============================================================================
# DEX ADDRESSES (DECENTRALIZED EXCHANGES) - SEPARATED FROM CEX
# =============================================================================
DEX_ADDRESSES = {
    # Uniswap v2/v3 (MOST IMPORTANT - These are DEX routers, NOT CEX!)
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
    
    # 0x Protocol
    "0xdef1c0ded9bec7f1a1670819833240f027b25eff": "0x_proxy",
    
    # New DEX addresses from user data
    "0xfb1b052756fc4f125d2daee48798fabde6fa6c00": "dex_evm_1",
    "0x372485b6d6ff398240ff18514986c656d0bbd9e1": "dex_evm_2", 
    "0xccd4422d695d8ee1a62698677cdb7d9515e90331": "dex_evm_3",
    "0x25e321109068675f915ea980a66bed48cdf6b0cc": "dex_evm_4",
    "0x232bdff6be025dbba928b6dbb74f3fc3b8670fa3": "dex_evm_5",
    "0xf64dac069752e3d8e65bc6967faa0696b898b1db": "dex_evm_6",
    "0x9fc3610ea95ece071dbc6b98be8d28abcc7ce6c2": "dex_evm_7",
    "0x02c9ec7d395278933cf8c284fb29e85817e6711b": "dex_evm_8",
    "0xd5bcc78bc531e047914cfd9ebb47dca5eb9ffed6": "dex_evm_9",
    "0x20a99b8881ac3b9112dd5b1a35414acc95fead30": "dex_evm_10",
    "0x84c5816ce47096f67ef2a1321672d433a33bb219": "dex_evm_11",
    "0x12a2ca712ad90e86fe6a49323feba7a724537cbf": "dex_evm_12",
    "0x74228b08958fa05738e24d06038ade9f5aa8b59c": "dex_evm_13",
    "0xecd256bc6eebd78f356aed5588d288cb66620968": "dex_evm_14",
    "0x5b99fd29728219db4f0e037c6b5de2d9d1d3261a": "dex_evm_15",
    "0xcfaaaf52d53681bedf9a631f2c1e7b0776729a78": "dex_evm_16",
    "0x9870f15bcacdab74639252f84912b391265f516b": "dex_evm_17",
    "0x68f0f1ad7c939056b47a471efb92ff387350d9a6": "dex_evm_18",
    "0x9c440a773697b66e4d07bb810a2d964aad7eb545": "dex_evm_19",
    "0x813a52e3621266fa6a357aadce72cf7907681bab": "dex_evm_20",
    "0x37c617cac9d0ee92289af2b54b36f91c65c1b3e7": "dex_evm_21",
    "0xf82e1f7ea7028c80cfd81dda300aec68d4c0a1be": "dex_evm_22",
    "0xcd07b1d8cf144a5ea465b2a245355830b64625f3": "dex_evm_23",
    "0x99a8c7b5dd648210ac2e7e67367d1d0c74572f05": "dex_evm_24",
    "0xcab056d315f0ef8c970331744b3eb030e303861d": "dex_evm_25",
    "0xae7fb50dce60fd1673fe820d3ca1e31312133d7a": "dex_evm_26",
    "0x76597623f1a6835799558f7025f71b0034b0b949": "dex_evm_27",
    "0x8b577c87237fa704a0fc00cc44261a19c6666451": "dex_evm_28",
    "0x0a1df6e0e02cfae21cd366d522fe57418c5e1217": "dex_evm_29",
    "0x29fefa4415a2164871eac304682e81068770c949": "dex_evm_30",
    "0xc95834898c3fe7f84dc9553a269dd7e60c155914": "dex_evm_31",
    "0x5112f32af90322b2bbf8967e8a10489da3ba04fc": "dex_evm_32",
    "0xc4f67a638e0bacb302ceb83480ef2f91e793d828": "dex_evm_33",
    "0xa966564f79aa1d6208f99d4e070fcad559b1b509": "dex_evm_34",
    "0x0951f18a606ebe29698fe87ce25a6c96d3816070": "dex_evm_35",
    "0x6737d59e6b63e264ee9645fadec777c96be96db3": "dex_evm_36",
    "0xd7e1fa206909a7f74e3a7057fa9e1e1566e6aef3": "dex_evm_37",
    "0x2a8f3b0f0eb5f196060afa772b690a7b1a65e966": "dex_evm_38",
    "0x820ce2f1d5839940dc4f61a08c5b1416bc484bb3": "dex_evm_39",
    "0xc3439250dd5c16cc1b11629aab51d3fe3583283a": "dex_evm_40",
    "0x068f888425227f46a736c04cf966fc8ad644e21e": "dex_evm_41",
    "0x7ec388ea154f5aea211bcac7b60dc3d09c14496e": "dex_evm_42",
    "0x5b18106e81fe44ebbfc3e6594b7171e3120b631e": "dex_evm_43",
    "0x335a4fad9d3c981e5eb6d6f64877a7bd0d48255d": "dex_evm_44",
    "0xeb454bc9be41ce321edc0b37725c8012b237b933": "dex_evm_45",
    "0x2f47648455d834da1f610043ac2a144fe0f79786": "dex_evm_46",
    "0x9ea04fdf473ac4a71c8053144a762d1200c88ec9": "dex_evm_47",
    "0x72039afa147528700974276c1a7b9276a373fd59": "dex_evm_48",
    "0x500592d8222259c405a8102a6626742e7db8c919": "dex_evm_49",
    "0xc1f06a718e37b697ae182de349ddc8008b529044": "dex_evm_50",
    "0x71ba50cc601cc5e76ad717e2b34f85f49bb4ea10": "dex_evm_51",
    "0x4a8914b7d529f94abf15934dc7157759e7d77645": "dex_evm_52",
    "0x8341e047aa2e04e165ad0a648022a411bf6b7a17": "dex_evm_53",
    "0x77160aa30a24d8c0778cdafc99b5342c26291bd1": "dex_evm_54",
    "0xa363963d855dcda205e3d42c36d83e8286876c23": "dex_evm_55",
    "0xa9a762084ae739a657418ac1da6fdf63a815620e": "dex_evm_56",
    "0x0cda817656808ed9fb0cda8c098bd3e94796f05b": "dex_evm_57",
    "0x14dc029cff478d9d539b5f391dc713594b0257d5": "dex_evm_58",
    "0xd29c08efd73ef264f569dbfe7d8779e9944a630f": "dex_evm_59",
    "0x52553fe6162f3a9553696368c0285b9506911dc3": "dex_evm_60",
    "0x8ea5a90db8924b4d8a7c0d340485e16519fab096": "dex_evm_61",
    "0x89025a2d5661bb291318cf2f4d14d3e56c244ac1": "dex_evm_62",
    "0xca8527ef6cd51446223a690a461c6d9070b1b04e": "dex_evm_63",
    "0xfa00887dedd5a81b42f5f8a19900fda553074658": "dex_evm_64",
    "0x799daef39f96f7911208a0bf58217924f7279f9b": "dex_evm_65",
    "0xed0068cbb49e343ed64bb33e4b1ee8d9879d970b": "dex_evm_66",
    "0x01ab362bd952618f6a14a70da8c338ba0537e730": "dex_evm_67",
    "0x6a9d96ccedd8acc09c62addcb5349ac320f406b6": "dex_evm_68",
    "0x3613d4a89eeef5ff4bdb6cd6df891230f7492cd6": "dex_evm_69",
    "0xda2421fd4db900861423c93dea4e2e65d5d09a66": "dex_evm_70",
    "0x2210af254ea3433c01c3bacd0731db3177de3d52": "dex_evm_71",
    "0xc90d8157a556145300d0d584f5712ee13e0d4937": "dex_evm_72",
    "0x3a26a7ebdb8a7229e26bd2af27e3c2c3ea61d5cb": "dex_evm_73",
    "0x124641208fa40ed1406aaff5bb3fecc6a836f14d": "dex_evm_74",
    "0x5cc3f72a2c97c49da4aab0ef6b0ea9d681a5a161": "dex_evm_75",
    "0x51b98d214e57b67913cfb3ea3bed2bb2d390b1cb": "dex_evm_76",
    "0x5dc1783c3a514da62478e57bb44a5103c6668f93": "dex_evm_77",
    "0x2b014375cd594be66ae62682129de515bebc98fa": "dex_evm_78",
    "0xf216ac179cd00272cf655333866d0b46b372ac17": "dex_evm_79",
    "0xebec3a0ddc0b1df96b9c7d26192b2dd071f2a839": "dex_evm_80",
    "0xc0e1bcead2395a12954af81a4ca7e3666800316c": "dex_evm_81",
    "0x914a0b151804a60436005f97fb536c9e145eecb6": "dex_evm_82",
    "0x969a71115fdaee223df69261ae6b7ce66d4f95fb": "dex_evm_83",
    "0x800f1e4d147e209db2eacd272e01f6ca8e40ef26": "dex_evm_84",
    "0xffa14c9dfb3e5f595a07d097fe30e1eaa6d485ae": "dex_evm_85",
    "0x0995552a9f05a72c9f0c8f5fc394d25494633331": "dex_evm_86",
    "0x40e7599f573b5182c4df09dbd295f344043cf3bb": "dex_evm_87",
    "0x36f422d766a268fa28143d5b4950549e57036047": "dex_evm_88",
    "0x3d12c8d09b6cf9b8c44121ac9872eab469a7df8a": "dex_evm_89",
    "0xa36460b9aefa17df00388675dadb9d7c9dcaff2e": "dex_evm_90",
    
    # Additional EVM DEX addresses from latest batch
    "0x7f2b82f59d1f42251eb85c5cfd3eece5741b1cda": "dex_evm_91",
    "0x5157699929a715f6cc5b50a7c7892f2a5889f7e8": "dex_evm_92",
    "0x481b9ff0472b7677601c7410662db43f5d52c7a3": "dex_evm_93",
    "0x26ac9fb3611b10b91d27e1677f5fda5c4f4ae289": "dex_evm_94",
    "0x589c9fbefaa699cb7a9d3959563ecd4768039593": "dex_evm_95",
    "0xb96826dfd43b18b48b091316a3850b9df74db604": "dex_evm_96",
    "0x9b31db690abfdd9b1509306bdb3923cec85b5078": "dex_evm_97",
    "0x7b643a69ed7ee399aaa1537131b08c1614d6e357": "dex_evm_98",
    "0xde6c0f0399d4383a69f04b61326c5b359ba8eb2c": "dex_evm_99",
    "0x600e74446e13e632e5847e41d123a674cae9f246": "dex_evm_100",
    "0xd5cad4402ef36862268f67a5c16b3e1b7aa1ac66": "dex_evm_101",
    "0x6e768d4922c059dc34495ead5bca1037e0d38799": "dex_evm_102",
    "0x434b8d1ba245b5b832397b409bebfe294e084c13": "dex_evm_103",
    "0x6c4e56b7b4232668308aac3f2a6a1c10edf6dd74": "dex_evm_104",
    "0x26751ba47ccec299992c81efd8b5cae40a7d881e": "dex_evm_105",
    "0x3f0b14129a2ee4bf7c2f70bc9181f8187ee721aa": "dex_evm_106",
    "0x0e4eee4ff5806e0c1ec763bf0944ad40c27c01d7": "dex_evm_107",
    "0xc6573e453704712cde7164da1f0b0973c3788547": "dex_evm_108",
    "0x8cd5dbbb3e15faa407a69e9934537f01e0532262": "dex_evm_109",
    "0x5147b919f0caae1cefc304bf16da728a7626a918": "dex_evm_110",
    "0xfa8dd3edfb08a456014e6f680bf2631e5e75dc2a": "dex_evm_111",
    "0xd1285b2550823c2c01f3777b61f8a0c00198973e": "dex_evm_112",
    "0xa6fcc06debb77aa79a094db4e2df3d869f52b7b6": "dex_evm_113",
    "0xbe90464b341e9f0db9a6bd1512bf416a297b2402": "dex_evm_114",
    "0x2864e37dce7e56d940ad118c3dd3369a3b04f6a4": "dex_evm_115",
    "0x433bef23e15e1a52e104b0cfef0e0c267214b7b3": "dex_evm_116",
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
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc": "orca_whirlpools",
    
    # New Solana DEX addresses from user data
    "yk7qq9ebfgoYBXdnv93BRr344CPB1TPzbfvr3": "solana_dex_1",
    "T2pqL35ryvNTWTRXx2EYTSNukNpSQbuxxG3Yw8bLE": "solana_dex_2",
    "Hgii2xiDbT4wQ7eqK9q4cDX2ozm2oc5hzQHdMSo5MH": "solana_dex_3",
    "QWCidht8rLwqUdkHNZF5fbKVNJncS2JcQzq": "solana_dex_4",
    "Hxz8bum1TLDvspgnzBRYKQtsHC9iXeUmvR5maRMSX": "solana_dex_5",
    "LUQiVEgKMfq2SGegb8WhMTaaNZ7x21rTqKszwtz": "solana_dex_6",
    "xYPJNmPBrksnMiyWxKwLPzaLCsJmnhCe277L7W13SQ": "solana_dex_7",
    
    # Additional Solana DEX addresses from latest data batch
    "6EF9ybLvn73v1KRTuw4SZndF4DDcdD58Xn": "solana_dex_8",
    "GndMUMdpbfoEHiWGoLPfopvWVFcnTdKPN6z7N": "solana_dex_9",
    "k3VN16PKXs8rt9dW5yJafWuV8PM6mZmeiBFLVGdQ": "solana_dex_10",
    "XXC8DboQQSDiWiqD6VL9Rf8vUhAtWQ1LDaSQTPYZjK9v": "solana_dex_11",
    "UcjTLGyPec48vkyAVQxcSPmDWJmkAVqtXgFM": "solana_dex_12",
    "8EYxxQt7ERPrED2msMQW7gLDifcPRfVxH": "solana_dex_13",
    "kcp8fMtzdrx3gZGM1y6ZEjN6se96CKVgMzU39eejvUu": "solana_dex_14",
    "GdcHwX9tf5x7B9ax3trDvb9thin7vh6A6GnSBPp3St": "solana_dex_15",
    "YuLJ64NpN6nFkWFtfpb5nVyWErSN8z4dNY": "solana_dex_16",
    "ZsqDRmioafVNCW5yzi1x3hsXhZmTAADQ9nQG76j": "solana_dex_17",
    
    # Additional Solana DEX addresses from latest batch
    "jpjDC1xPqkhFmGtAJFRDwwTuQd1kZ17wBfyhes": "solana_dex_18",
    "p9tR4fpfyGj2Ex7rqyKiTwHq8JrkTfPvK": "solana_dex_19",
}

# =============================================================================
# BITCOIN DEX ADDRESSES (NEW)
# =============================================================================
BITCOIN_DEX_ADDRESSES = {
    "3XCb9DBUGfpM12aSR3aRhVdqhvMUq1Nud": "bitcoin_dex_1",
    "3FmTw8FS5VNHuRz7QLw8SWpiHHKXAmaP": "bitcoin_dex_2",
    "1353KFNLPs9s6C2fHXkznXJkuRQ6wye": "bitcoin_dex_3",
    "1UCW96HV8eTi5iqtwMdmFZAfxcqVuejne9": "bitcoin_dex_4",
    "1K3GdbZH6jLMCj9LKA44dPgrLYx8": "bitcoin_dex_5",
    "1QsnjXC5pAL7w2UBnA51WK9Gtw": "bitcoin_dex_6",
    "3oENojDcD7cMmbmYH3DhbsE8ARe3ddL": "bitcoin_dex_7",
    "3DM1b683opoFdUJkmpovdUxiKYqM": "bitcoin_dex_8",
    "1kS7TXZoGdsUX1CToB1TBQQECDYfc6Gr": "bitcoin_dex_9",
    "3DT48we7BtSpbMJMqTcoM7CYdiMMG6uN1f": "bitcoin_dex_10",
    
    # Additional Bitcoin DEX addresses from latest batch
    "1ZcVJm3RcYUW5A3mMz8SvV1u6w": "bitcoin_dex_11",
    "1bKKfjxAhivZhQaJKXq22zL54gpyhwsHY63": "bitcoin_dex_12",
    "1rUaAh7iBu4YpA3t5Je9mGcCvxRCjUP": "bitcoin_dex_13",
    "3WmRbMc1zDYmHdRayqsNT3tn6hVsL": "bitcoin_dex_14",
    "1qWcifh9sPmvmTNdTkVxtXZMbx7jRMrS": "bitcoin_dex_15",
}

# =============================================================================
# MARKET MAKER ADDRESSES
# =============================================================================
MARKET_MAKER_ADDRESSES = {
    "0x56178a0d5f301baf6cf3e94a8a979c4b8c67fde9": "wintermute",
    "0xf584f8728b874a6a5c7a8d4d387c9aae9172d621": "jump_trading",
    "0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88": "alameda_research",
    "0x3ccdf48c5b8040526815e47322dfd0b524f390d9": "wintermute_2",
    "0x21b2be9090d1d319e67a981d42811ba5a4e9b35e": "dv_trading",
    "0x000000000000000000000000000000000000dead": "burn_address"
}

# =============================================================================
# CEX ADDRESSES (CENTRALIZED EXCHANGES ONLY) - CLEANED UP
# =============================================================================
known_exchange_addresses = {
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
    
    # KuCoin
    "0x2b5634c42055806a59e9107ed44d43c426e58258": "kucoin",
    "0x689c56aef474df92d44a1b70850f808488f9769c": "kucoin",
    "0xf16e9b0d03470827a95cdfd0cb8a8a3b46969b91": "kucoin",
    "0xd6216fc19db775df9774a6e33526131da7d19a2c": "kucoin",
    
    # OKX
    "0x6cc5f688a315f3dc28a7781717a9a798a59fda7b": "okx",
    "0x236f9f97e0e62388479bf9e5ba4889e46b0273c3": "okx",
    "0x5041ed759dd4afc3a72b8192c143f72f4724081a": "okx",
    
    # Crypto.com
    "0x6262998ced04146fa42253a5c0af90ca02dfd2a3": "crypto.com",
    "0x46340b20830761efd32832a74d7169b29feb9758": "crypto.com",
    "0xdc4c0fda463435d19962e8dd465d5eba86fd02ec": "crypto.com",
    "0x51ca21ed46a9df3cb6f34624cbd482d312996730": "crypto.com",
    
    # Huobi/Gate.io
    "0x0d0707963952f2fba59dd06f2b425ace40b492fe": "huobi",
    "0x1062a747393198f70f71ec65a582423dba7e5ab3": "huobi",
    "0xab5c66752a9e8167967685f1450532fb96d5d24f": "huobi",
    "0xdc76cd25977e0a5ae17155770273ad58648900d3": "huobi",
    
    # Bitfinex
    "0x876eabf441b2ee5b5b0554fd502a8e0600950cfa": "bitfinex",
    "0x742d35cc6634c0532925a3b844bc454e4438f44e": "bitfinex",
    "0x4fdd5eb2fb260149a3903859043e962ab89d8ed4": "bitfinex",
    
    # Bittrex
    "0xfbb1b73c4f0bda4f67dca266ce6ef42f520fbb98": "bittrex",
    
    # Gemini
    "0xd24400ae8bfebb18ca49be86258a3c749cf46853": "gemini",
    "0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8": "gemini",
    "0x5f65f7b609678448494de4c87521cdf6cef1e932": "gemini",
    
    # Gate.io
    "0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c": "gate.io",
    "0x0d0707963952f2fba59dd06f2b425ace40b492fe": "gate.io",
    "0xd793281182a0e3e023116004778f45c29fc14f19": "gate.io",
    
    # FTX (deprecated but keeping for historical data)
    "0x2faf487a4414fe77e2327f0bf4ae2a264a776ad2": "ftx",
    "0xc098b2a3aa256d2140208c3de6543aaef5cd3a94": "ftx",
    
    # New CEX addresses from user data
    "0xf44eae1ffbbc3420e414cf051f52fbc419a2bd9b": "cex_evm_1",
    "0xd8d5a132c74c80ec94acece5f5e1ba08b9ab66bd": "cex_evm_2",
    "0x4c2e37c0e316fcd00f70eebda8d04aba5f3185c0": "cex_evm_3",
    "0xd94a12f958704679fd2c41144912c565ccea7392": "cex_evm_4",
    "0x04c085926ea2f167168fc83f8659cd74671352c5": "cex_evm_5",
    "0x42c1f1b87eb77b9cae7c7825df1a420e02f1e1a4": "cex_evm_6",
    "0x82259f1a6be4b6f8113d5ba14a9dac830ad806f5": "cex_evm_7",
    "0x2dd7c6473ec146733b558230d13a93372edce7c0": "cex_evm_8",
    "0x9f6e460154ff2775e7fd34d4e14e0febf189e6c5": "cex_evm_9",
    "0xc2abb17bd2a08e9abea25d8bf2effa2b1cffe229": "cex_evm_10",
    "0xd8d97a76f9f09c87411381045c6bd3c3c0f44807": "cex_evm_11",
    "0x86e1d946273e3d2664865c06b40486a1964f7857": "cex_evm_12",
    "0xaabfa7979b51e7cdb45f8a94e66b747ae7ecdf07": "cex_evm_13",
    "0x2f8655af96c71ddf8dd31d9fdd4b23882a300eb9": "cex_evm_14",
    "0x69723a1cdd6b465c0730984b94a331593d65fe37": "cex_evm_15",
    "0xd44330753798b425ab99d40aac838570423a29c6": "cex_evm_16",
    "0x471af2a6a631ad49228c803c9c7345e1dface0ec": "cex_evm_17",
    "0xfc321be4b66f17ed98a0a90eed13f0f3e8f1f50e": "cex_evm_18",
    "0x28c9b6003ffbd5f730047f70e10c9f3c790d50f7": "cex_evm_19",
    "0x0926315662359efd26b9497758ce454b0a4ad28b": "cex_evm_20",
    "0x8e49dbc7d32b1ac993b2354fb2d165774904fde3": "cex_evm_21",
    "0xda12168e1098cac959165f127f882bc63f2bb9a2": "cex_evm_22",
    "0xcdaac9df82c2ac361d00583d95c7060795ee6275": "cex_evm_23",
    "0x1a133d2e01df6e245611cbc0f26fb2ade0404a87": "cex_evm_24",
    "0x03f79603fc7eedec585340bf02a62732a7851ac7": "cex_evm_25",
    "0x67c061cad8ea61605464c993c8569938257033df": "cex_evm_26",
    "0x4f7b3fa4f0dda7377f81b51f135e8f24941939fd": "cex_evm_27",
    "0x326b5eb3ea4d6db1dd852dfc1da1e9060c0e973a": "cex_evm_28",
    "0x7dcc6bf48d9230beac7942344bcecb7daf2848a3": "cex_evm_29",
    "0xdcd7808f418ad3af6ad1c5133481757452f86770": "cex_evm_30",
    "0x5f1f831054d10774040069a2a836e8e3075de08d": "cex_evm_31",
    "0xaf4c08d1cda2a351d27317368c3c700b23966fec": "cex_evm_32",
    "0xaab3e9da05ce7bf118aabd8cc5812a63b8b8cfd6": "cex_evm_33",
    "0x115a5b6f3b08b4dc9cb0228b2fa7d7fb02629b04": "cex_evm_34",
    "0x6b340a712daff7e29bafe0cedcd32a0aed878669": "cex_evm_35",
    "0x2cc04f7df274dd4deb881a2174d388b60b1c1e3d": "cex_evm_36",
    "0x483a79ff0370796bb8051af2ecbe7952e5e79ffb": "cex_evm_37",
    "0x9d3502cfbaaf640cbb042f564cbcc7db36cdf779": "cex_evm_38",
    "0x4ca0c06fe27ee8ae4c4e5625507dd2c4f576e25f": "cex_evm_39",
    "0x851e4ba1dd0b0077b478fcefa41ddce9bbfa7f16": "cex_evm_40",
    "0x4c769c59ae730bdd5ae40e4728e0f41e11b2db85": "cex_evm_41",
    "0x6aceede3eed74afe21fbf7d6a77cc9eafca2d56e": "cex_evm_42",
    "0xaa59183afd6aa87d98ad72cf386206ead547f54e": "cex_evm_43",
    "0xbd28aa86fd4e379000d5aaaf67fcc72e18d5d52f": "cex_evm_44",
    "0x416a8253a669b7231ceadb3ae96df82d02e32384": "cex_evm_45",
    "0xe6317d2d6d8e89e7693247c4aa9abab869a3cee1": "cex_evm_46",
    "0x7e098bf0de92962dc7608ffc6227e1aae9a6e1a0": "cex_evm_47",
    "0xdf94a07f0f177415417118a5f9733d002b9df84c": "cex_evm_48",
    "0xf6337c7ebc7eac441542492e30e83165a6a0526e": "cex_evm_49",
    "0x35b4d767146a83d7e23f934acf1528707d14b02e": "cex_evm_50",
    "0xf176ce1b07433257582c58c0dffc3f4ca2bb4d1d": "cex_evm_51",
    "0xebbb901469cf3fc50c3eed155e85ed889eddcdd5": "cex_evm_52",
    "0xbb5f9d0e30b61efd82a57096026405cb98891625": "cex_evm_53",
    "0x92c450ac36d14410ac44bbab124244ae1547877b": "cex_evm_54",
    "0xf4be587362967d2524f5be40cc13315c27179b63": "cex_evm_55",
    "0x8ee51a20bff5e9c3eeea9d0991ac93032e31599c": "cex_evm_56",
    "0x6b92791e7fc8c8e669288a5f4f70f17a0dafda66": "cex_evm_57",
    "0x3b41dea75d60b35fcf7899f7101a50ae8bc548c5": "cex_evm_58",
    "0xb0669a57f92c101e0901a3c2f07332790889589b": "cex_evm_59",
    "0x715a276c6c0c38557ae31a731a81f463094e5abf": "cex_evm_60",
    "0x967caeca3c26becc00c2dc810e63d57e36dbcc13": "cex_evm_61",
    "0x012a0767bcd9c01ff96b364e95b766831756f531": "cex_evm_62",
    "0xc7a67741f4d5fdd9b4a010321e17a550528fb9b6": "cex_evm_63",
    
    # Additional EVM CEX addresses from latest batch
    "0x49e9dc4a0bda512deb9d93dccd2ba37d9d37c06b": "cex_evm_64",
    "0xf684d41caa153f4825361581f9ab255d23e04e15": "cex_evm_65",
    "0x93a0258cd03d50152271f587a92a769051809b70": "cex_evm_66",
    "0x08490bb7c96e499848c00066040cfa0a329ce26b": "cex_evm_67",
    "0xe874b0002f697d1d753a9184ef6718d149fb1a6c": "cex_evm_68",
    "0xb73e250b719e3eee7cad040b647ec61daa182e8d": "cex_evm_69",
    "0xeef14c7ade937096f08bcc81572c7392e284a997": "cex_evm_70",
    "0xa8dbfa266617a5581b62ca0b79df11ac0fe3adcd": "cex_evm_71",
    "0xcc04dfd5280a811a8cfde47e58e71554c7e0b729": "cex_evm_72",
    "0x94aad76c79a1bc77c014586d331433235d478662": "cex_evm_73",
    "0x0f7cddd8fb2eff5e9c7fe03b3f797a399766eb97": "cex_evm_74",
    "0x0d684803b43e519e3b48940268f90e5edd1311ab": "cex_evm_75",
    "0x50181fe5c711cfa1fa2d140cb48687fea8a2b7ec": "cex_evm_76",
    "0xebbde079d07a206a993e91d2bd677296da564bcb": "cex_evm_77",
    "0xa2314b063184f41c6e2666de61aca7e2b9b48a69": "cex_evm_78",
    "0x44044ddee1e0808bc04ac242e19fda0263b39611": "cex_evm_79",
    "0xb9ed0276fb45da0ea0ccab667c88288cb6395633": "cex_evm_80",
    "0x29c3b314ae0269729d4dcc50d5ee1005f1f8ee18": "cex_evm_81",
    "0x47dfb3903272768b726e6c457e579325313ca3fb": "cex_evm_82",
    "0xd0a813515db9f577f21f751b99ad091155198b7a": "cex_evm_83",
    "0xd9b787cda92738b2221a26887d22d65abef1ae1c": "cex_evm_84",
    "0xeff30cd369874de0e458e94a7e4537230f58e691": "cex_evm_85",
    "0x893a56cc7eaa78542984afd8f0c8dc8cd4689411": "cex_evm_86",
    "0xcec531cc229b55e162ec29ca31fa53ac44227dc9": "cex_evm_87",
    "0xdb4189fd96f1be0f25886c2875eae88bfba5507e": "cex_evm_88",
    "0xedfe171a7818aec93c38179330e39800771304c5": "cex_evm_89",
    "0xb06b43b26129dd757ff31e41a09e8cfd263c3377": "cex_evm_90",
    "0x0054ed009656d719926cb58d18270cf41b1b0d0e": "cex_evm_91",
    "0xae1fd38e6234b65005146ca65c316c607948036d": "cex_evm_92",
    "0xf97f2f3dc9f5cd783cdafeb62e3c7a95dbcba7d2": "cex_evm_93",
}

# =============================================================================
# BITCOIN CEX ADDRESSES (NEW)
# =============================================================================
BITCOIN_CEX_ADDRESSES = {
    "3GZ7ieus8ZTXmRP461CMpGjUBSQW6o5mBB": "bitcoin_cex_1",
    "1gMwQKaHzxfjR8rMW6WWmVDRsdQBL38T": "bitcoin_cex_2",
    "3dYW1257tKPwQ5ieDur1u6V2HxXuzWT": "bitcoin_cex_3",
    
    # Additional Bitcoin CEX addresses from latest data batch
    "1nqBMv2SkWz7B6g3u1n5otDCXkAEGH2K1Q": "bitcoin_cex_4",
    "3qNdCi9VF92nscXEZ8FZUWPVnvmgptQ": "bitcoin_cex_5",
    "1YpeKkLnwcp8hHk5Zx1WM98Pb7EuSJnSD7": "bitcoin_cex_6",
    "1XRip7y3rvHvqtJwqr3KHUGLUd": "bitcoin_cex_7",
    "394sfp2WGPp7nxWkevmJ9r4w4UxrUG": "bitcoin_cex_8",
    "3b2nriqFyq5H3NiwyTmKRdjWuwE": "bitcoin_cex_9",
    
    # Additional Bitcoin CEX addresses from latest batch
    "3vPoPJM2YrxfqgXQXYoy8nQrwbvzuMj": "bitcoin_cex_10",
    "3MRo2V9ZizzkmQuLr6399qsdrpKMsCaof": "bitcoin_cex_11",
}

# =============================================================================
# SOLANA EXCHANGE ADDRESSES
# =============================================================================
solana_exchange_addresses = {
    "FdAXT4XPsswRhaJveaB45Lz9CFbKHGpyY3rSyRFRGArj": "bybit",
    "3Z4oLTsytjW5K2sgEYiDEbykerbYm6SnAJQm5kMQoZxd": "okx",
    "HSsAV4suBdFgaybUwzWDK1rR14FNQ73BcF3kAM7rRkbF": "kraken",
    "HU23r7UoZbqTUuh3vA7emAGztFtqwTeVips789vqxxBw": "kucoin",
    "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM": "crypto.com",
    "73tF8uN3BwVzUzwETv59CvCF4oqzNtkUxKdJuFLHqmD9": "gate.io",
    "AFrks6SxLK3FNKpKPdpx5DsFYhQZk8VKnz9BcVQxhYaY": "huobi",
    # Binance
    "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1": "binance",
    "6QEJkDV8NhHc4pUCAP3v6n5h5osHUqR1xCEhUAX8e9bL": "binance",
    "BQcdHdAQW1hczDbBi9hiegXAR7A98Q9jx3X3iBBBDiq4": "binance",
    
    # New Solana CEX addresses from user data
    "DVeJnY4fTnH6hauiBsCDJ59VCek1Yk3vRAKAxy67M": "solana_cex_1",
    "L2EEfrCNZcqxYdfdfs89zPcATWswpAiJtBwnQbbEMDh": "solana_cex_2",
    "vNg3zioiC8U65MCpX26aE5hSivxk8GtZdGD1F2aQu17Q": "solana_cex_3",
    "sSGpJy9R4dbtwVFh4kxnHTMjiwD31zATKPvr3": "solana_cex_4",
    "nH5gXtPmYPh7ktKAZprDupJBVkRj9aYmRfCTYH4rvw": "solana_cex_5",
    "AT9Sdjfwmk1i2cuCQT5Cu9btPYBR66H765xLd8": "solana_cex_6",
    "PBhjGEqVSEtoMMrcmFpiQz6H45RWPNQzsT6KQTe8": "solana_cex_7",
    "TdSHs3TXD8mKBE1wsDbLGaQGPLMijzMAJ": "solana_cex_8",
    "xSxJcujsbSTbxt2J7smaiLknsaRf7CUWrSSPau3": "solana_cex_9",
    "EG31HVrCUyhcjHpDp9nbrpbMFev3coRs7Ctxpn38TNX": "solana_cex_10",
    "siyrmSbKDgGXHkPc6g1DWRMNqVWjP5SHaMVNpwrNp": "solana_cex_11",
    "uqJU9HXARMHUxXomF4q4whjbCknmrc6KS": "solana_cex_12",
    
    # Additional Solana CEX addresses from latest data batch
    "iYjERzXDBaCwMAznLsMdymkFMuh5V6Tnzt37": "solana_cex_13",
    "bvLv4ZGaqABMPeZU7JUVB2YDCnQzoQ5S": "solana_cex_14",
    "EG31HVrCUyhcjHpDp9nbrpbMFev3coRs7Ctxpn38TNX": "solana_cex_15",
    "TdSHs3TXD8mKBE1wsDbLGaQGPLMijzMAJ": "solana_cex_16",
    "EAU11pkwpAJEKdyDQorYKw3nXpVvGRffHhex29": "solana_cex_17",
    "b3N311xbKdmPAatAHrdf2rf5eQJG9k9Xoyr6kjjVsqoY": "solana_cex_18",
    "xknH2ogi77DfyCcET3ybsbGP5mKumy9n": "solana_cex_19",
    "MYC5x9y3AJE1M3TutUDb86ZQ4Dg2VRMHG3vWBQMr": "solana_cex_20",
    "i6SaCRdNetMEZtcfbX5L5YhEMU3c15QsK": "solana_cex_21",
    "ewDY11Bj3tqSeVmWmjhGYTFGvNMw23HuGQ": "solana_cex_22",
    "Ndfrnuv6mqH1WPUxyf5eoHmhYZdbMAFRp": "solana_cex_23",
    "gVe7qhM4KwuFiwUYsZmFxF98FibJcBrt": "solana_cex_24",
    
    # Additional Solana CEX addresses from latest batch
    "P2owcmqpX1yvYxVjjJNqTYwbZAqsNhssu": "solana_cex_25",
    "oUs4s8u8RFCPUVBBUZBo3wnF2nbZUF8J8Yn9UDaurNY": "solana_cex_26",
}

# =============================================================================
# BRIDGE ADDRESSES (NEW)
# =============================================================================
BRIDGE_ADDRESSES = {
    # EVM Bridge addresses
    "0xa90f8caccf96f1d09456baf29da73118099d3818": "bridge_evm_1",
    "0xae27f5ac01287502568a7bbd136727a28a3ba869": "bridge_evm_2", 
    "0x1d2d9c80b1567d88defc7446c02e6def94754c9c": "bridge_evm_3",
    "0xa6590b962aa1056e6d3ad89b63f2ae78b421476e": "bridge_evm_4",
    "0xcc569496a3ee14c9021dab0de520d24a85e52728": "bridge_evm_5",
    "0xcb115983966b2686b86c980bf3f9841a89c694bd": "bridge_evm_6",
    "0xab2ae99d3eadc4a128b66bda1ba95f348e9d792e": "bridge_evm_7",
    "0xf612296db394575619566e562142ca72cb95232a": "bridge_evm_8",
    "0x1123f2806a86c3c9f060dc24f7c6b8a502de3cf1": "bridge_evm_9",
    "0xb087af49fdcfbbeb88c54fdcb1488046b0d69fca": "bridge_evm_10",
    "0xf0380c9d7c4e1014feaa9bf0866d7074d5c7633f": "bridge_evm_11",
    "0x178a45b2251bc4642a0764b4bd37333132c82ad5": "bridge_evm_12",
    "0x7b075151c294e5877f2986495298a878fb7fa366": "bridge_evm_13",
    "0xf08c78d40ec583e5e9a3f7855dc1e50a5b77d70a": "bridge_evm_14",
    "0xb1d2a297777ef164f43d0be221f3c1d522fcbc7a": "bridge_evm_15",
    "0x559ca215384c25a39014bd7f25a341e27a4b61cf": "bridge_evm_16",
    "0x3b4e27935f9524539e95e22d9d208464454277b4": "bridge_evm_17",
    "0x0abb375d7f030d4ffd70c4f7fb2d2d4365d00488": "bridge_evm_18",
    "0xfb9f9555bbaaae455f01b4bc3c6b1ceb80b43495": "bridge_evm_19",
    "0x22c5d469d08173901b0c69700b3c858e6d7c7b6a": "bridge_evm_20",
    "0x2ccbda13ac2e50c2cb3ad0974fd3a523985f1d5c": "bridge_evm_21",
    
    # Additional Bridge addresses from new user data
    "0xcf659e9a0186f7e4650ec0fc160e3614832d4d62": "bridge_evm_22",
    "0xb3fb12e85b94edde00d2497eb1d9bdc471c6ad3b": "bridge_evm_23",
    "0x0fdae5f3b9fefee84ddb4791f0fea15b60625016": "bridge_evm_24",
    "0x1e3a103cf96fd0b11d9957a9fc27cc00ba424be0": "bridge_evm_25",
    "0xfaea47c69f3d9699a00c31b300fbf0aa41128c4f": "bridge_evm_26",
    "0x5addc52f91b5f762c28383be513dd1375d2b79b6": "bridge_evm_27",
    "0x5dddc5dc6ea0c2f268bd3238d7c09d0759560c69": "bridge_evm_28",
    "0x5f5731ffd7c218c5d373e2781c6448f30ba6c0f7": "bridge_evm_29",
    "0x83bd8f5bcc350bf49e9b2a7034ac908afb8ebca8": "bridge_evm_30",
    "0xa289c44554f2d9fbe4f48af53ec6995ef4874346": "bridge_evm_31",
    "0x6386e8c15fc3c7fe7fea872a8cd3567cd12f9410": "bridge_evm_32",
    "0x8fd3b3b25a1eb31af5a622f59c8203e809fd0b69": "bridge_evm_33",
    "0x2bac8d487b079416f7c7e20a8d0533368a7854e6": "bridge_evm_34",
    "0xd93c5b68ccff4b75f7ff028487af0b46008805d1": "bridge_evm_35",
    "0x0c6cc5a902650cbe03ffd53b73af56c125e5970a": "bridge_evm_36",
    "0x149304371be77678177b97cd7476acf7cb1ce94d": "bridge_evm_37",
    "0x56804c127e9e2b8f50aacd432233e1d416325d27": "bridge_evm_38",
    "0x478174d0a4d310e47862cefbb957507efede3fde": "bridge_evm_39",
    "0x387139cf2ac8d14e01fb5b3d16e6da6011aa3753": "bridge_evm_40",
    "0x51651344fd9bdca5f59d2da2a8596e0780ab351c": "bridge_evm_41",
    "0x806640fe647d356613a8ca48b49cf8ffd346ef96": "bridge_evm_42",
    "0xc0bf35647c4430f425d6f5bb97a037a809fad078": "bridge_evm_43",
    "0xad0323ef463a6fa9840c1b9fc4c9b0a3f58f06ec": "bridge_evm_44",
    "0x4eccaa8b2147e2557419c20e5eaaac9ac642dc31": "bridge_evm_45",
    "0x341a2b2589d0575ce67574d4d182328ca89d1575": "bridge_evm_46",
    "0xc0738bb20370e067aa88294d9dd220bfa5e52d26": "bridge_evm_47",
    "0x366055dcb74f5e22ebd2ca7f4ee330b46dbfb167": "bridge_evm_48",
    
    # Additional EVM Bridge addresses from latest data batch
    "0x4e799b0323e2b929d50f2cf03cdb9de7d91b5b3f": "bridge_evm_49",
    "0x575f8fdfc5233903913bdc516b56b5b8229aef6c": "bridge_evm_50",
    "0x2a192c441b17b7e9cd9ed2a7b383857544ccdcc1": "bridge_evm_51",
    "0x8f573ca7a6c45da2256205f5e9a5199556af3bb0": "bridge_evm_52",
    "0xdce40011c335765ccf95438836dde8d89759675a": "bridge_evm_53",
    "0x543a6e76e5f3dd398b088b876ca633368ac8d498": "bridge_evm_54",
    "0x5523d8892899da73ece527031f12723c91f63886": "bridge_evm_55",
    "0x7b435f63c2c744ff18b7ecfd7f42fcb62b4734c3": "bridge_evm_56",
    "0x4e2473b3cbf2b3035fd58d09a54653296d40d811": "bridge_evm_57",
    "0x7b183750d10ef77dc3d837bbe0f33fd41d0bde69": "bridge_evm_58",
    "0xfa992315ecc7539dbf9865d7506674358930b4e2": "bridge_evm_59",
    "0xd32915d67e3a65587b69c04ef08a8abf3c98c7be": "bridge_evm_60",
    "0x5eef01a11f28a5dd9798a7ccb04a547cff72003d": "bridge_evm_61",
    "0x9f280767bc5b2d0da260eff6857aa1d3c9a0356e": "bridge_evm_62",
    "0x9676b0e3319dada9173937cc67839fd50d794bc0": "bridge_evm_63",
    "0xefc4c40b5be403f9280aa867e83983c42e1370f6": "bridge_evm_64",
    "0x1ab09e034536d99e6535e01e12764713c30f888d": "bridge_evm_65",
    "0xd5a52bd25f5c8d4b88ffa0a4b5632dfcfa7b54ac": "bridge_evm_66",
    "0x35f0dc736925a137b1de186b9b34d5a9b8d56d17": "bridge_evm_67",
    "0x81fb5ef4eaacb707e7e4adee47f126ee26ee568b": "bridge_evm_68",
    "0x3522b7524efd7c6f7998c2013ff29375ae876e84": "bridge_evm_69",
    "0x418c859ddd73f2ec0aa9bab63ed5a1be8d21c578": "bridge_evm_70",
    "0xead3382fab71ea5bd44f7bf6259c7a21256f04bc": "bridge_evm_71",
    "0x2bf8b29fbabd1b79e43a16fd299c39ff93f28a66": "bridge_evm_72",
    "0xa6f19e291496f217d26337a6a0fc40576a661504": "bridge_evm_73",
    "0xba278846e7d1e29654d8291bed829bc73fae4f0d": "bridge_evm_74",
    "0x949c65e15f2586df31924dceab9cb79677cf6d57": "bridge_evm_75",
    "0xd23c5ba13b673b034c41252b6eb073e13c83a2a5": "bridge_evm_76",
    "0x49b66eef209e78ef1f37e485008b40d85b29d863": "bridge_evm_77",
    "0x32b26e1b46d65e92151fc3cc3a4c20bf227dec23": "bridge_evm_78",
    "0x1859281a421185ca523a5aa3a7dacdae0753fd53": "bridge_evm_79",
    "0xde92f963c786aaeaed4d4bd1a024ffbaa9b362d2": "bridge_evm_80",
    "0x5829d9bf0b146771e312b66143530867d1f4a05e": "bridge_evm_81",
    "0xb5b0fa6cded937eca9a42681da7ea439f0789e7a": "bridge_evm_82",
    "0xf9358cab59a6f825be2b8b2e73f54b72f2bf6b2b": "bridge_evm_83",
    "0x004235bf5ba1a4347923102662052115c41f787a": "bridge_evm_84",
    "0xbd43c367919680ef9dfb97d71c378fbe98c2da58": "bridge_evm_85",
    "0x1befab7a99a38819e61a2bc61ca1d95af8d1314b": "bridge_evm_86",
    "0xa547ce9580aa73344ec4e549c291f982e04584f4": "bridge_evm_87",
    "0x6c6213ef8336c177fac1f10e2870c3824fe28391": "bridge_evm_88",
    "0x9454df1fdff44ba74370328de9e45c25c0548724": "bridge_evm_89",
    "0x3c5b702c5f1543530a5d118e45a7f623e4041425": "bridge_evm_90",
    "0x1fe3b92d8ec88f074564ae53aa942eddbc0278da": "bridge_evm_91",
    
    # Additional EVM Bridge addresses from latest batch
    "0x9776191b5a221f735bbfc7b2cb528f6da95c2a11": "bridge_evm_92",
    "0xe49ba13acee40eac1380edd0e65b0f9b01f5917a": "bridge_evm_93",
    "0x1d08b0d37a1ee563c0975a58425b174ebef91717": "bridge_evm_94",
    "0x637e915b15d67c7c71f5614152eca805d5a568b0": "bridge_evm_95",
    "0x813b0d4211677c600ecb1629ea6be6409a3cc562": "bridge_evm_96",
    "0x9c8ba0cae3112a5aee0def3be7536502771750c6": "bridge_evm_97",
    "0x396d75964c221ca311d28e1021ce03a044f637fe": "bridge_evm_98",
    "0xbf3ae40b68f416c8e42b3278c9bad919fc83a689": "bridge_evm_99",
    "0x242e91f08fae447c00a9bd3256e461f8351314b9": "bridge_evm_100",
    "0x7e7097b94075416b98a85604b1d1dbbbece5580b": "bridge_evm_101",
    "0xd2c97b73169d3b9a82e3dc717b54b58470d4316f": "bridge_evm_102",
    "0x9673dce714d6425051af98d4c29ec3a5baf7a6f0": "bridge_evm_103",
    "0x74e699e723b9c9c445ab4ec6507891d3aff4c454": "bridge_evm_104",
    "0x9d15fe6048d4355680918fdceda947863effd415": "bridge_evm_105",
    "0xffec50180b7cc455b7a0aea51462ae9990d77fe0": "bridge_evm_106",
    "0x465c76f12c9ee9d0807d507705ab71f29136cf9a": "bridge_evm_107",
    "0x8b2b8b2fa9f7a5246e144f96581634d81d9ff9ae": "bridge_evm_108",
    "0xa7c9278a47fb7ba62f25b8a4cdf61a82923f4257": "bridge_evm_109",
    
    # Solana Bridge addresses
    "NupghhKP6ezi1opfiJ5Jg115pc2AzmCJ9kLXSZje": "bridge_solana_1",
    "Bmh6UewZ9Q7Vz8eZjD4nqzEKQUpbXQFgLXi8jdWs": "bridge_solana_2",
    "fnLpHhu88QgfCppbYhWEfHfZok6upNjEUqWVuD79": "bridge_solana_3",
    "L5mwq8RAhAKM4nXNa77aU1LqNpiEsgzSCtDk": "bridge_solana_4",
    
    # Additional Solana Bridge addresses from new user data
    "2pPT7tzo4QRgSgWY9hXw4K3zBixN7dkZyXXYAbQ": "bridge_solana_5",
    "2mb69UGJC45z7o1GFKbo1Tgko4mBHUMpvoEgQ5Kgsa5": "bridge_solana_6",
    "soyWYasA3Eui7ZksdXmZwQPpcFRseVmmg": "bridge_solana_7",
    "mnASjw4mLfuLX9B6uvPhhynVa9VbGUWSuMbGGfgp84Q": "bridge_solana_8",
    "pY85QzJcP7yrv7wLcNZnfN6wGKRWfgncaxyxADeckv3m": "bridge_solana_9",
    "Bxit2Q3Prz3uL7iXXxmKQv2ce3Xrmop9EB9T7Lo": "bridge_solana_10",
    "6uWFdZra79Dq9FHFtoEhcXDTjBmX6nGk1QHMYK": "bridge_solana_11",
    "RgFDQGXjT9cAeV1bixr9pfK9fyq5339o": "bridge_solana_12",
    
    # Additional Solana Bridge addresses from latest data batch
    "QQFpuBMAddTHVEz4nCVqUNuNMVFuMAHcuCG": "bridge_solana_13",
    "XSUdLk4GmUL68tQ4WQpuK5tVmJG8T2yHZo16": "bridge_solana_14",
    "KSSCpfmq6agg9UCrSSKnx38zsbrVeTvcXuo": "bridge_solana_15",
    "CkixFL37HWk52u3152FiU1XrYxLx9BsUsie8J": "bridge_solana_16",
    "n5AU7dJP5bb4pPkGJsS4By7CNSaJp1KErG": "bridge_solana_17",
    "qmKgs3ybUWZZFMqGA78rEpjX7ZzEUEgaMT": "bridge_solana_18",
    "tmiLae9PpYk9ZaNThpJkUUT3i2dUq1f4rErWSYcPEy": "bridge_solana_19",
    "vDQwwTdyAX1E5HiogrLGZEya9BWSkfMcaaCFKQ2UAiEn": "bridge_solana_20",
    "gLJ2oJUctWMyqceTFvefH8yVvnMrzjpXD": "bridge_solana_21",
    "PhF5Hvnv3YgukX8XKzxmvMETdQwzErhfMWtc4n": "bridge_solana_22",
    "aHxWzu3bMLE8vFC7RcR4UfC1pNeV25x6EvjnGFVHqa": "bridge_solana_23",
    
    # Bitcoin Bridge addresses  
    "3zZbaxfgNBhtmeQn1BJ8V5eEDbSvXf7": "bridge_bitcoin_1",
    "3hayjgqJNa3SPpMZTwnayzM5i2KC": "bridge_bitcoin_2",
    "3Fz5CuXwixPZ3PSARqnGCkYshryrHWQgU": "bridge_bitcoin_3",
    "3Huoftz5DBqmB3ivtJYNucB7YCxY8m": "bridge_bitcoin_4",
    
    # Additional Bitcoin Bridge addresses from new user data
    "16ZJMqQcDrcxbP4sLeqXqrefbQxkbHEUEhF": "bridge_bitcoin_5",
    "16t4wn1hvmp2y3535oF1hViu7JGVuadG": "bridge_bitcoin_6",
    "3rpgme4a6Sgfi5owRkLdryWXtpsH": "bridge_bitcoin_7",
    "1vJ362TnjDi7jxq57KEPemSjX7WrYZ5aDFy": "bridge_bitcoin_8",
    "3W3ZxgkRG55aR9mX9WJGwjG8e4dcpQPjr": "bridge_bitcoin_9",
    
    # Additional Bitcoin Bridge addresses from latest data batch
    "3M5esxFeLrykaCx27QAE3pqayoa5nqzvHqM": "bridge_bitcoin_10",
    "3AVoDwbYC5xrstjdd1RXaCvZEp": "bridge_bitcoin_11",
    "122T37Y5PKviDfeYqA8qCh2jWHdX": "bridge_bitcoin_12",
    "3e7bJC9G7Q3TDozRTUcriUGBip88ve": "bridge_bitcoin_13",
    "13xrthEJsyNheYR15YvHD2rTKvMYwv": "bridge_bitcoin_14",
    "3cwGZfupoFAyd8PHTzEU6WLhSRiKEj2J6WW": "bridge_bitcoin_15",
    "3i9TPbco8oZFkRBhG3YbESJZRyzaJp": "bridge_bitcoin_16",
}

# =============================================================================
# CUSTODY ADDRESSES (NEW)
# =============================================================================
CUSTODY_ADDRESSES = {
    # EVM Custody addresses
    "0xb05980df68d06378411683a28415e5bc55cf7e6a": "custody_evm_1",
    "0xa3565769be0dff18875d572f68fc02e2175379e3": "custody_evm_2",
    "0x4e48cc8f8abcf2b2a79b21520d863c4301eb42de": "custody_evm_3",
    "0x2ca6c36fb32156927d9be263473afe6ab011128a": "custody_evm_4",
    "0x2aa2d0e0a542fd0ae01267e097861217c524fd67": "custody_evm_5",
    "0x841a66fa3293a827963804993e5ad9ab53139703": "custody_evm_6",
    "0xc0caf208b11afae361dfd11c0309726b1853775d": "custody_evm_7",
    "0xc7636f0129e81430bfef6e2f0952ec3083ffa0e0": "custody_evm_8",
    "0x4dcf3abf2247e5e2d693a5854710cc8f8e17fe8e": "custody_evm_9",
    "0x48123311cf5b468df333942aac19361c2cb9a075": "custody_evm_10",
    "0x91f96cbbd2db6b4fb0071476ec8fcc1e57b18780": "custody_evm_11",
    "0x23a4d3c7bf2e91de089537b948e10995d4fe2f04": "custody_evm_12",
    "0x3bbce7617b193cee8baaaa3954cb42e8d69af365": "custody_evm_13",
    
    # Additional EVM Custody addresses from new user data
    "0x1649827e226aded52aa8642947530546dd96105d": "custody_evm_14",
    "0x51551b557182eb1c98bcb8d0f4664b358382c0a0": "custody_evm_15",
    "0x6bb4a65b630fa998b3d45213e5cc31f2ce611089": "custody_evm_16",
    "0x55fb259ad33328c73e8118f453e79251eca6e675": "custody_evm_17",
    "0x43aeb2729a35eee0e2309fb788b03ba47fd4deb8": "custody_evm_18",
    "0xbf73d00b2fd4e7d0bad20aa13d8f9cfb0b59bf6c": "custody_evm_19",
    "0x420556afeeb6222418377e167956efcc324874ed": "custody_evm_20",
    "0x124f8e3b66555e10fbccd865f811b5f804b42168": "custody_evm_21",
    "0xf0ed641f2f70222dec96facd7d0383dab74c699e": "custody_evm_22",
    "0x0ecf5ba903b489544878a85d77ac28ed23fc4563": "custody_evm_23",
    "0xdab97c12e01092ec8ef627c06f14b9073c55bcd3": "custody_evm_24",
    "0x30ad1dee185ad041f308caebd5ce80b6128972ef": "custody_evm_25",
    "0x7b5f270fe14c5381b70f574f34db01e7461e858f": "custody_evm_26",
    "0x4c943490cb60bbc4b993e1f8c9e5bb1f2bc60c7d": "custody_evm_27",
    "0x93c25ab3e2b56f0a633a7c76dddca801b4ab40e9": "custody_evm_28",
    "0x33f83036da26972a711f522303e97eb8e78391d5": "custody_evm_29",
    "0xcd28ff0adc1170a5b896de1048423286ccd086f6": "custody_evm_30",
    "0xb9b7d03cd0d149738631c19b6e33da942801807d": "custody_evm_31",
    "0x7327cff77ead360d19920db11ea8d13e54fd4372": "custody_evm_32",
    "0x07dad67c2f38d30e547c1c197c527b3859295956": "custody_evm_33",
    "0x5e3a6985e39675d2d3ecaf78a36a5aa862ccbd20": "custody_evm_34",
    "0xa9c57df66439c0d529eb724de0c5c371cedddbb8": "custody_evm_35",
    "0x6b905cbd12589a523e184dade24949de93e6cbc2": "custody_evm_36",
    "0x8da5d58a4cf17b8219900654eb8b0ae9ce47502a": "custody_evm_37",
    "0x7e6ef90eba1ec420b4848278b8dd2022b86f5c43": "custody_evm_38",
    "0x19435e66f45eea5de672ad5d9937c7a45d88c690": "custody_evm_39",
    "0x115fa572c70e278ef76df2a984a974a5d8edbb16": "custody_evm_40",
    "0x916fcc602f8bc2d019b50656a6ac109b35794cc2": "custody_evm_41",
    "0xfa0e672ad0b20e557e33c314ae78060b0dad50df": "custody_evm_42",
    
    # Bitcoin Custody addresses
    "113169qxA4qsHRkcwaXwV6LYiKuiA7": "custody_bitcoin_1",
    "1TghUvRpYv3n5bNbJQmwx4VcQV1PJTJNshn": "custody_bitcoin_2",
    
    # Solana Custody addresses
    "sjV8LR7jM6tuFtyWnN6XagCTmb8bjM3uqps73": "custody_solana_1",
    "PRFfRUMQ1KbmUt7tGzqtCrqntwJWDcgSb57o": "custody_solana_2", 
    "cQGCuHTShadbX1R6bn4hpy7ACwzwXAwVUTJuR": "custody_solana_3",
    
    # Additional Solana Custody addresses from new user data
    "hcQgmrfkQGP56QvMwvXgJ7WWMFkbyfWRULR": "custody_solana_4",
    "4JmkSEKqvGnSySJeBE7roas3tD9iCrnXXSy8E9": "custody_solana_5",
    "ibGgGxktB45ZdKoMNstD8YXzhebD1womxw": "custody_solana_6",
    "esEiTM1DAPfVpsgzPRcvWhrRBXWk7RY7Phu": "custody_solana_7",
    "u9Z19WUVAbWm6Qabgd758xPH7j9HF9Vs3JtN": "custody_solana_8",
    "uFpv4ewstGCP5CksujJ6oxPLmAdsy1qkvj2rBT9": "custody_solana_9",
    "RERpEhUfRy5cqEmiBNsfoTqF7HQ2q4BPhsUBGwNJdr3": "custody_solana_10",
}

# =============================================================================
# TREASURY ADDRESSES (NEW)
# =============================================================================
TREASURY_ADDRESSES = {
    # EVM Treasury addresses
    "0x05dd4ed29a35345c07f462c3202889e39876bb15": "treasury_evm_1",
    "0x58dab908fa0ba39b2df58ec738cfed033d1ab0b6": "treasury_evm_2",
    "0x5c449bb630bf00ffa19f1cb02d8c967d93630555": "treasury_evm_3",
    "0x3c56d242be125885775e2c8bff6d0546e7c08a0e": "treasury_evm_4",
    "0xc97286370a8a594bc178978b0c5a0251f99e5e34": "treasury_evm_5",
    "0x929f261d3a88f4f217b51f8707916cb7a0c7c5ae": "treasury_evm_6",
    "0xe495c31c4588ca0026ec41157b9e85708d69c98e": "treasury_evm_7",
    "0x69d2ff287072610916f0d9e355d3cd2bbb25a9aa": "treasury_evm_8",
    "0x8110678b52248d2e1b72dac3850bd5b55f11fe42": "treasury_evm_9",
    "0xf7401c962e07b5c87668fb753cb3ba3c0ea47bfd": "treasury_evm_10",
    "0x284026cc2e494e312bd8a1bb02204ab97dfa4ee5": "treasury_evm_11",
    
    # Additional EVM Treasury addresses from new user data
    "0x451f7e2b7f672074ea0c8a2bd53e0322f4335913": "treasury_evm_12",
    "0x6be872ed04dc931c7341e785ce65315dc0d7ac7d": "treasury_evm_13",
    "0x7aaf862c7626f5eadb3e3fb34631cb33193402bc": "treasury_evm_14",
    "0xe3806a96cd51f382f12aac47cbc817faa9a6b8c1": "treasury_evm_15",
    "0xf40c2b4298b7cc06a784edbac0a3caa972696c6e": "treasury_evm_16",
    "0x20dd90987274ca0b00370615e96f7d95c454123f": "treasury_evm_17",
    "0xd1c9b2a03ac2bbaedcfa4c876984c004d105048e": "treasury_evm_18",
    "0x974fed3f9a33b3aa5e2129d966655a846e242967": "treasury_evm_19",
    "0x64e338ad8b060f80fda23ce32d87957355cfd7f2": "treasury_evm_20",
    "0x6c4ab53b8e10f1d11d10a94bfb0f069547978645": "treasury_evm_21",
    "0x75598fc5eed823f8af6c52305120aa4683f53f32": "treasury_evm_22",
    "0xc954e4b32f9b734368d0abd6a3f858e547f2c7d5": "treasury_evm_23",
    "0x6d5b917ac97a02529c84c52f67437e81f029ccbb": "treasury_evm_24",
    "0xd7d3055e20edfbf18ce8f43126d7b095cb4b594e": "treasury_evm_25",
    "0x2666d735d667a17daa281eaade740209ee6bc446": "treasury_evm_26",
    "0x413bc0f75631ea389751ebfd6de7fdf0981d65b8": "treasury_evm_27",
    "0x02aac09083958533dbe84d22bff6a94dc26e8a58": "treasury_evm_28",
    "0x7d58975d9236fef5d4946a13b75d54a0e696b99f": "treasury_evm_29",
    "0x317f73b799b5dd1cdc97caf2d49503cfa86ba861": "treasury_evm_30",
    "0x8208c513f7a4711d5dd0a204f87d47c0f9914277": "treasury_evm_31",
    "0x70c922740e9b9bb5a0cbce08b70b7773e6107587": "treasury_evm_32",
    "0xca45096a7cfcffa682dc11839be08de0e066ed0c": "treasury_evm_33",
    "0xdad6ae34612bdd15a9015e7a0e986e128de2f760": "treasury_evm_34",
    
    # Bitcoin Treasury addresses
    "3TdRnAsiegp8WgTVNd1C5YvQ62W": "treasury_bitcoin_1",
    "12YVWPDvJjTE2nP1bTsryBzX9J4cH": "treasury_bitcoin_2",
    
    # Solana Treasury addresses  
    "XnmuhXFwDWKBTPMrz5EMZhQfHLUzpz1QVSKGv1yXoX": "treasury_solana_1",
    "rd6KGYYESJNEMXDwFy9dW64A98TCTUVUjoojbVgm": "treasury_solana_2",
    "T2HWmyuYG4kRZqxcJLypEkCzfyAeh6Ee7LvtJ7": "treasury_solana_3",
    "AN3e61CQQnDqntUxTNq6Z24qWc4ezcvjCKaLAr": "treasury_solana_4",
    "fWBvcK9X2Cjhk1NsTkZ6s95pEVWczCw1CmEk": "treasury_solana_5",
    "BZ96hnz2vc6qg9asnzCitFRER18AnAnGEQyM": "treasury_solana_6",
    
    # Additional Solana Treasury addresses from new user data
    "XZFJz6kkTRG54v81rNWfZegVgb8QPqLpQUYkV": "treasury_solana_7",
    "woKBG74Sr9FdouF8JQCU992vfmb5zVRew": "treasury_solana_8",
    "88xoiStXmnsVUZ9huVUBHMook7ip2r2Ce9sJEuqw": "treasury_solana_9",
    "QN3gBN7k7FJMjG1np9RTAxu6jV6FZhkJAXFT": "treasury_solana_10",
    "RpmoHEhupLLLEVMBBoTLH6JL7vUHbj9gVS5TJcBw9ny": "treasury_solana_11",
    
    # Additional Bitcoin Treasury addresses from new user data  
    "3V1ap5kCsvUSJnZNpy1AfJtF19": "treasury_bitcoin_3",
    "1u9TrAkvHtWquXmWY1kmYwVbJyGi": "treasury_bitcoin_4",
}

# =============================================================================
# DEFI YIELD ADDRESSES (NEW)
# =============================================================================
DEFI_YIELD_ADDRESSES = {
    # EVM DeFi Yield addresses
    "0xd6ed8d77f89a3fad4b7db45600168b42661a0917": "defi_yield_evm_1",
    "0x8cee241fc0b4db870a2e37a4681dd2bb2a612f9b": "defi_yield_evm_2",
    "0xae27e945e8714a1891315f1578a519f4423a7570": "defi_yield_evm_3",
    "0x0c0504f3c1600ccd7328eff54d9eb4422e962fa3": "defi_yield_evm_4",
    "0x3bc5aea69792e9288a6204984236874a889fb322": "defi_yield_evm_5",
    "0xd2d46d713bbbb3675302f7662b2dc33d9ef0f236": "defi_yield_evm_6",
    "0xeaa1f517793ad31aa169f9258dc223250526f497": "defi_yield_evm_7",
    "0x42cc8527a96974378392437b0716a1e2e9248c20": "defi_yield_evm_8",
    "0x96fde32901d21ad9b030d86c9120feb90751fb67": "defi_yield_evm_9",
    "0xbcee25a31eabd02b87d160746d9cb9d3b1e05a79": "defi_yield_evm_10",
    "0x9c5e1ff5ee5e64d4dfb9b965d288e957fc1c243e": "defi_yield_evm_11",
    "0x7e41c0751202df742870407be6b7b10e674fc1ed": "defi_yield_evm_12",
    "0x3a9a8e1c6205b5c6f35b201a02e6ee398a2f60ca": "defi_yield_evm_13",
    
    # Solana DeFi Yield addresses
    "FFzBfpw3GxVE2a9Lvtt9ibWEzDoXbY5a8": "defi_yield_solana_1",
    "tGDmwtFLdae4nHUqgW9qtTEqKbqvKyjhdjQfK": "defi_yield_solana_2",
    "xnosJGsmgz7PCqj1CoT1J6hLpP7GNgfm7": "defi_yield_solana_3",
    
    # Additional EVM DeFi Yield addresses from new user data
    "0xfbc11b6c2a6f5e8d26b5a88a75bed8d3cef4a23d": "defi_yield_evm_14",
    "0xc03045f29862888fff5c399028547f71cb783a6a": "defi_yield_evm_15",
    "0xa3b3f93b5e04158fa7dad467c5cd795d67c184b2": "defi_yield_evm_16",
    "0x892c115c68478427de44b5c58cdf1cf2db6a55a1": "defi_yield_evm_17",
    "0x6b3f6dd279633a728376a8f18e841649e364d481": "defi_yield_evm_18",
    "0x27aa69564d8f9f79d019a0226d144a796690b8ce": "defi_yield_evm_19",
    "0x50ee289ada59c67e37531e25e51b046b99352f5e": "defi_yield_evm_20",
    "0xb48bf8fe01696af3e9af30bbeab61451b7c06e93": "defi_yield_evm_21",
    "0x6ba26d7b9b006ad1a43875a7f835de1096587dfe": "defi_yield_evm_22",
    "0x54c36e8a79301bf88a8b1dad1da90b61c114ca5d": "defi_yield_evm_23",
    "0x5bac14f82f0ce95151051df1f3006656a249512c": "defi_yield_evm_24",
    "0x3a7dbfedeb2295a905aabff9d6c4e3b5041f7e9d": "defi_yield_evm_25",
    "0x462fee49ed6d09a93d7f47493d10d9568b1a1fd8": "defi_yield_evm_26",
    "0xe68b3d38369387c2da01c286c5f954b2f4179035": "defi_yield_evm_27",
    "0x81d52ffabdc031d9f367651c4f07256272b6dd60": "defi_yield_evm_28",
    "0xcd8a855354f559be85abc4a9be945da9dd1b48c4": "defi_yield_evm_29",
    "0x60360d56fed50c7ac6353e2b4b1f0d1ccf7be6d3": "defi_yield_evm_30",
    "0xd6205ffcefecd76333425d46c4b5e5598588e269": "defi_yield_evm_31",
    "0xbad43e66fcf0675406cb52c4de8b11afafab25fc": "defi_yield_evm_32",
    "0x6fcc8f94d0bbe7a7e47f3f33b129a3c9a7979e96": "defi_yield_evm_33",
    "0xe01fe9dd7bf6d7fe165cf440813f14e8cc7a8058": "defi_yield_evm_34",
    "0xf47e0982375e2588a49f2388c984180e29fd0bdb": "defi_yield_evm_35",
    "0x464bb54d6940944e76159d4cecfa3dd16230afe3": "defi_yield_evm_36",
    "0xa02b0dec91ec396af580e888d3c49564c0c1cbd2": "defi_yield_evm_37",
    "0x0a9123c2baac473beaad0290c73023d6c1b74d3a": "defi_yield_evm_38",
    "0x493e0a694c02d4af20188c0098faf6319483f21b": "defi_yield_evm_39",
    "0x47998eca4d2d8aba0862e89d71db0ca66f6a38a6": "defi_yield_evm_40",
    "0x00455471d67e8019a42e292c017b42d250384b1b": "defi_yield_evm_41",
    "0xd446865d93bcc539d6cbdd954778026d31d8d9f8": "defi_yield_evm_42",
    "0xf153d547b86bc5c044438989d9c069f7f8c720d5": "defi_yield_evm_43",
    "0x6861ba9cbf7f12dc7fc98a31bfe01a01f277657a": "defi_yield_evm_44",
    "0xe6b52274be428efa01abe8054d76b3d091e54236": "defi_yield_evm_45",
    "0x06b522b269062851b0592e252bb169a794df2342": "defi_yield_evm_46",
    "0xc3477dc012eca7ea2f5755a631140d37f9d4fcef": "defi_yield_evm_47",
    "0xbc2a2c8eb73e972ba1b376c7423ab4990777c614": "defi_yield_evm_48",
    "0xa367691807b3847f04c509083b023041eb20d0ca": "defi_yield_evm_49",
    "0x3be7bd5bb0edf167acadadc3a0ca30a2791e6afb": "defi_yield_evm_50",
    "0x2a17a70d5da1cc2baddb4dbc1a2fa2abd9cabe3e": "defi_yield_evm_51",
    "0xb94c2b3217d785af87e10bb7b2e8644c883eadd2": "defi_yield_evm_52",
    
    # Additional Solana DeFi Yield addresses from new user data
    "5UoNqwJuM7ceuCmFhaxGmAbRP6cqZpxs6s6Sm": "defi_yield_solana_4",
    "QKQxBu7snk83ruMafQ66xiHJySukA4gfTwCtKFuTeTb": "defi_yield_solana_5",
    "piH2KPHQDcq9HvsV9cecRAjqJJRdmrbZoXA91v63": "defi_yield_solana_6",
    "qDRHXBGrMwU44QTjEXawzwSa6T1dU4gf4XAmbM": "defi_yield_solana_7",
    
    # Additional Bitcoin DeFi Yield addresses from new user data
    "1DJDetqvJYmE56EBBBXYRf3GBFxK": "defi_yield_bitcoin_1",
    "1UaXQpuTCBKZMhcW41y6WmStQgq": "defi_yield_bitcoin_2",
    "12rYyqrpfKC2s368eDDmLexTkD": "defi_yield_bitcoin_3",
    "1qgdtTV6VQiQ3UxqCgT7WkRS2wph3": "defi_yield_bitcoin_4",
    "1cKqdsPKRPTFTUjEsGCy66k1dd5QEEBn": "defi_yield_bitcoin_5",
    "1WC28kNYjnWynbKN7gVQrGxirFBRtX": "defi_yield_bitcoin_6",
}

# =============================================================================
# DEFI STAKING ADDRESSES (NEW)
# =============================================================================
DEFI_STAKING_ADDRESSES = {
    # EVM DeFi Staking addresses
    "0x275fc0d9cdccc7ddbcd36e5052630c7fd00b6c53": "defi_staking_evm_1",
    "0x23a516db559240014c179d3d31918e99b683e70a": "defi_staking_evm_2",
    "0x5525eaf98c4c142dcfa267f1ca92846e9183966b": "defi_staking_evm_3",
    "0x4144c2978a0bc4e18f86696f4ba7e4f76406cde8": "defi_staking_evm_4",
    "0x8877c0c5822de07a216d6f27bf56e2c359e369f3": "defi_staking_evm_5",
    "0xf4f8a4bd8ff32349a911d1ea0eeb429e175d6d94": "defi_staking_evm_6",
    "0xcd603cbaa6e6a4ca9c35327c0890f2f56eabeb5d": "defi_staking_evm_7",
    "0x09924a96e266d0dac56b54714bfda1a1144e039a": "defi_staking_evm_8",
    "0x67a2378bc9c9e73935bdd9eb89867eb099c2f37f": "defi_staking_evm_9",
    "0xa3e1c7817e1cc006482d08954c96b06768ce8dae": "defi_staking_evm_10",
    "0xafafe09782eb45c71ff26720c149c1f2061e79d0": "defi_staking_evm_11",
    "0xbaf990b01166b0f474e8eabc480866b8833bbc50": "defi_staking_evm_12",
    "0x714d3b8e7b023f42ceb9209c3013dba03b4d92f6": "defi_staking_evm_13",
    "0x76cde6491d39b8e398f0b60c052a9471bad3dd07": "defi_staking_evm_14",
    "0x8e6598ffc9b93e3069bc3c1c0aaa27859523c7ac": "defi_staking_evm_15",
    
    # Bitcoin DeFi Staking addresses
    "14AYGfLdsrt7UVLK4uv8A18zNHUd6M5Jf": "defi_staking_bitcoin_1",
    "3JhKMVbygZoy8jMU2qa2c6qJkQp": "defi_staking_bitcoin_2",
    
    # Solana DeFi Staking addresses
    "egiP3LvWZrxWpPsBsHDPv4QK9U7NJrZpBLBoC5w1sn": "defi_staking_solana_1",
    "9gwntGsPibCESe51tuybiwaSE2zgvZr1do6i5TCbd4": "defi_staking_solana_2",
    "nuk73irRr64inf5vpbHzDTftdXpEGTcsTjA": "defi_staking_solana_3",
    "Gw4y1bZZaDMMNvSbRmX9rNHMrCRBgxRVwGR": "defi_staking_solana_4",
    "ijwBAG4wUE67w7DFVZB1BHphb48S4ovQ": "defi_staking_solana_5",
    "nKXqHA52xYCNsBUBL4gdBsiZRcFTqBps": "defi_staking_solana_6",
    
    # Additional EVM DeFi Staking addresses from new user data
    "0x342fea0fd466df138ece1c9f2c9dac7aad0f4d25": "defi_staking_evm_16",
    "0x1f9f93341340e3bc6b04543065fdbf9f6deb8204": "defi_staking_evm_17",
    "0x93ad162ad64802583d4209c97f84934fd23224dc": "defi_staking_evm_18",
    "0x1a0b4d123341763179f0fee2c0ada2bd1f5e386d": "defi_staking_evm_19",
    "0xebe89ff517ae674c63d90c5b56ea791a407b73b6": "defi_staking_evm_20",
    "0x8657d259bcc5d36c308a82738aac35d17b515c36": "defi_staking_evm_21",
    "0x219df54d08f78d21cec8570c4907c33e267b1b41": "defi_staking_evm_22",
    "0x223d70d085ddd915861e6deef13fecff58d8a1bd": "defi_staking_evm_23",
    "0xd0b05f37d7b6f9e0918f8f4da828715999c1ef59": "defi_staking_evm_24",
    "0x172cb1d25c1e8545cb55188fb509f930323962da": "defi_staking_evm_25",
    "0x862f1d5bed73b5e15b80347caef3ebc59283a83e": "defi_staking_evm_26",
    "0xff3e27e3dd88101a7a1429fb5418c7e3e538f75a": "defi_staking_evm_27",
    "0x4bde4f8fe8e0eb1752d5c77d529ceff418af564c": "defi_staking_evm_28",
    "0x84c7046d09dc42efb5ea5c3bc08cf126999407fc": "defi_staking_evm_29",
    "0x16d71792ae72a114df6e005c641ace0451e277d9": "defi_staking_evm_30",
    "0xec3f7cfeef97a6cb167a7606b9a59b50324afe68": "defi_staking_evm_31",
    "0x22f51a8197f12b5e689bd47a0b8365a8d4b9ed7d": "defi_staking_evm_32",
    "0xab7e9e416f5e58543e079b1065507a20eab94887": "defi_staking_evm_33",
    
    # Additional Bitcoin DeFi Staking addresses from new user data
    "3emYF2WqbMfB1Pjgy8krEpsm548KPoJr5U7": "defi_staking_bitcoin_3",
    
    # Additional Solana DeFi Staking addresses from new user data  
    "wdye3QGyKThajKe4cQymcdLjNpN5kmUJtqwRcchK9gNJ": "defi_staking_solana_7",
    "vp8SqAqEUTbmCjwyStNdh5Hsq8VTTXbCt": "defi_staking_solana_8",
    "pjngJ5YS44gbX8JRrf9WUkoeeM4NKBf9E7": "defi_staking_solana_9",
    "ZLAmEM9cCdfb4fVimqyt4TiS4y5NoyAHY3AxHxGX": "defi_staking_solana_10",
    "ev2eDpq7t7qABHJ2WB4TauNg8kMYhZ9j": "defi_staking_solana_11",
    "63yNhNE1nHMnmhBMMvF64jX2ZKC4Q1ycgKRBHT": "defi_staking_solana_12",
    "6kZdXkwptT13ULx4GNF4XvYKWwaq5Jd4HQLXf": "defi_staking_solana_13",
    "rR1AVCAF44Tua4VVHvecYiCvmZKubu2t": "defi_staking_solana_14",
    "69eJfE5PpfXRRPfhsUZUyukGxN2US5Rudv": "defi_staking_solana_15",
    "MYt79SYvu7SHzzL5Y4g1FjHsT56D5qUpKX7K7oV": "defi_staking_solana_16",
}

# =============================================================================
# DEFI LENDING ADDRESSES (NEW)
# =============================================================================
DEFI_LENDING_ADDRESSES = {
    # EVM DeFi Lending addresses
    "0x6f6061fc749d22cdacd81fd9cad6c47229d2f845": "defi_lending_evm_1",
    "0x296fde413b879ae8e721bdbb2dde3e03e87777b1": "defi_lending_evm_2",
    "0xe3d1c7b9f9e3e05b638bc22ec19503a62d770e6d": "defi_lending_evm_3",
    "0x3c57779e16322e8e557e9ac9a588a226fbba1b3f": "defi_lending_evm_4",
    "0xe6ee2463df62fe4c75874552fcf8bb74ed08ef66": "defi_lending_evm_5",
    "0x3ea911358e3e1713f861c1501a42a9659bc0472a": "defi_lending_evm_6",
    "0xc2c0cf33d6d65a4b2c58dcdb559ee5e00152c511": "defi_lending_evm_7",
    "0x7fc72d6dfb24eff6ddc8600250b7afe90fd67c52": "defi_lending_evm_8",
    "0xd055d348662fe70370fa4aad0b0580ee3fa18403": "defi_lending_evm_9",
    "0xe42dd915497c4121e4c8df76e8ab2c48a2ffe4ef": "defi_lending_evm_10",
    "0xdcbe9f3860c3ab86c6c4d303ce48b4c77b1e1976": "defi_lending_evm_11",
    
    # Solana DeFi Lending addresses
    "StA9uD6XvLQm85b9fNFHhtZaymjLuVUtfrRZhdJHGy": "defi_lending_solana_1",
    "YeTu7Bq9TpnCsS9pJYcgBisuABUHvXDPAKD8E2EXjnbj": "defi_lending_solana_2",
    "mP9mXG6xA4hmhLHcnoB6Jm3WpJZiXThZDPz9XS": "defi_lending_solana_3",
    
    # Additional EVM DeFi Lending addresses from new user data
    "0xde88a095bec691cef3156099bcae0ec964fe144c": "defi_lending_evm_12",
    "0xa8328d9832c7032289dc1c2834b70f6d1360d908": "defi_lending_evm_13",
    "0xe6f141a6cbdc86fff434be930408bce2f80592b8": "defi_lending_evm_14",
    "0x8dedcf68b5a4f70fca733b48c7ae754f0ca6654d": "defi_lending_evm_15",
    "0xcda8313f891b9a233b58447eda1dda8410aa455d": "defi_lending_evm_16",
    "0xdf0d674bcf260bb8457b7d10cef9491f18395cc3": "defi_lending_evm_17",
    "0xe62f1cd3f003241f9b62d8419d1ba4eeef02cb48": "defi_lending_evm_18",
    "0xd83bb530e31b1250787830f4cb96a8916a0292e8": "defi_lending_evm_19",
    "0x5f2308281b856b2f7376eb31d71317ed91ac5d5c": "defi_lending_evm_20",
    "0x0cdac2cfab4a1395449c079a1df9b6080c38b3d6": "defi_lending_evm_21",
    "0xcbcd724f4fdfeb69347d516b564cdd5965b58f66": "defi_lending_evm_22",
    "0x42be7ea91917b53e6f89e14400716e7a600fba91": "defi_lending_evm_23",
    "0xaa1a4d7a0fafd41148ce817f9f00576e73a32445": "defi_lending_evm_24",
    "0xb9f816fc213550093258589d913521e54ca56809": "defi_lending_evm_25",
    "0xa17c141f54caa50ba07d4b806dbabcef6d22962b": "defi_lending_evm_26",
    
    # Additional Bitcoin DeFi Lending addresses from new user data
    "1CHovK21Yf6f7h7Tf8pmnZeXWTgpvHYkww": "defi_lending_bitcoin_1",
    "1w8rdrVTV4rWUPPhQFRytnZ7ZiB": "defi_lending_bitcoin_2",
    "39pXU5eYcskBGtdxmivFCP47ksQQF3J": "defi_lending_bitcoin_3",
    "1oVdXCgyYAzWFR6TQJi6w8ReVRTcmTdvf": "defi_lending_bitcoin_4",
    "31Kdqnpcbc42yQXrxoHHvsXVreKY": "defi_lending_bitcoin_5",
    
    # Additional Solana DeFi Lending addresses from new user data
    "uy31b2S8DLP6K6L3fiynJQUSVQkfQjjvniVZUi7": "defi_lending_solana_4",
    "z5uN92Y4obWTq7w6CfHVBK9izPi8tb3YGHho": "defi_lending_solana_5",
    "C6oEC8wT7dyZD8HB6kFd7M1ZWVqHExk67": "defi_lending_solana_6",
    "GBL9JE51oeQ34KzQpbCe9XEpuVhjkBsxUN": "defi_lending_solana_7",
    "ziz6azMqvqH2nJyU81ZLBLr6cWW7pHzQcNh": "defi_lending_solana_8",
    "o37ynXqEP15Sk2RvPJaLpUM4sWPDBqewp": "defi_lending_solana_9",
    "fH2VST8iPCfzxkDCvCXrwhKq6oszLtCTEVaJamP8dYKq": "defi_lending_solana_10",
    "eR4vKdnrkf5VCTgVLFdQ16MfEHZCycbN7cP": "defi_lending_solana_11",
}

# =============================================================================
# XRP EXCHANGE ADDRESSES  
# =============================================================================
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
    "r9x5KeWFx3mWHyPqB2NZAUPh1S7rcXK6CP": "gateio",
}

# Unified protocol addresses combining all known DeFi protocols
PROTOCOL_ADDRESSES = {
    **DEX_ADDRESSES,
    **MARKET_MAKER_ADDRESSES,
    **BRIDGE_ADDRESSES,
    **DEFI_YIELD_ADDRESSES,
    **DEFI_STAKING_ADDRESSES,
    **DEFI_LENDING_ADDRESSES,
    # Additional known protocol addresses
    "0xa0b86a33e6ba4c8e3d0b6fd533dcb73dfa346c19": "compound_eth",
    "0x5d3a536e4d6dbd6114cc1ead35777bab948e3643": "compound_dai", 
    "0x39aa39c021dfbae8fac545936693ac917d5e7563": "compound_usdc",
    "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9": "aave_token",
    "0x3ed3b47dd13ec9a98b44e6204a523e766b225811": "aave_usdt",
    "0xbcca60bb61934080951369a648fb03df4f96263c": "aave_usdc",
    "0x030ba81f1c18d280636f32af80b9aad02cf0854e": "aave_weth",
}
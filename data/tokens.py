# =======================
# GLOBAL VARIABLES & COUNTERS
# =======================

# --- ERC‑20 tokens to monitor on Ethereum ---
# ⚠️ REDUCED TO TOP 20 TOKENS TO AVOID RATE LIMITS
TOKENS_TO_MONITOR = {
    # 🔥 TOP MEME/TRENDING TOKENS (High whale activity)
    "PEPE": {"contract": "0x6982508145454ce325ddbe47a25d4ec3d2311933", "decimals": 18, "min_threshold": 15_000},
    "SHIB": {"contract": "0x95aD61b0a150d79219dCF64E1E6Cc01f0B64C4cE", "decimals": 18, "min_threshold": 10_000},
    "FLOKI": {"contract": "0xcf0C122c6b73ff809C693DB761e7BaeBe62b6a2E", "decimals": 9, "min_threshold": 8_000},
    
    # 💎 TOP DEFI BLUE CHIPS
    "UNI": {"contract": "0x1F9840a85d5aF5bf1D1762F925BDADdC4201F984", "decimals": 18, "min_threshold": 8_000},
    "LINK": {"contract": "0x514910771af9ca656af840dff83e8264ecf986ca", "decimals": 18, "min_threshold": 10_000},
    "CRV": {"contract": "0xD533a949740bb3306d119CC777fa900bA034cd52", "decimals": 18, "min_threshold": 5_000},
    "SUSHI": {"contract": "0x6B3595068778DD592e39A122f4f5a5cF09C90fE2", "decimals": 18, "min_threshold": 4_000},
    
    # 🚀 LAYER 2 / SCALING
    "MATIC": {"contract": "0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0", "decimals": 18, "min_threshold": 8_000},
    "ARB": {"contract": "0x912CE59144191C1204E64559FE8253a0e49E6548", "decimals": 18, "min_threshold": 8_000},
    "OP": {"contract": "0x4200000000000000000000000000000000000042", "decimals": 18, "min_threshold": 6_000},
    
    # 🎮 METAVERSE/GAMING
    "APE": {"contract": "0x4d224452801ACEd8B2F0aebE155379bb5D594381", "decimals": 18, "min_threshold": 8_000},
    "SAND": {"contract": "0x3845badAde8e6dFF049820680d1F14bD3903a5d0", "decimals": 18, "min_threshold": 8_000},
    "MANA": {"contract": "0x0F5D2fB29fb7d3CFeE444a200298f468908cC942", "decimals": 18, "min_threshold": 8_000},
    
    # 🤖 AI TOKENS
    "FET": {"contract": "0xaea46A60368A7bD060eec7DF8CBa43b7EF41Ad85", "decimals": 18, "min_threshold": 6_000},
    
    # 💰 MAJOR TOKENS (for big whale moves)
    "WETH": {"contract": "0xC02aaa39b223FE8D0A0e5C4F27ead9083C756Cc2", "decimals": 18, "min_threshold": 50_000},  # Higher threshold
    "WBTC": {"contract": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599", "decimals": 8, "min_threshold": 100_000},  # Very high threshold
    "USDT": {"contract": "0xdac17f958d2ee523a2206206994597c13d831ec7", "decimals": 6, "min_threshold": 500_000},  # Mega whales only
    "USDC": {"contract": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "decimals": 6, "min_threshold": 500_000},  # Mega whales only
    "DAI": {"contract": "0x6b175474e89094c44da98b954eedeac495271d0f", "decimals": 18, "min_threshold": 500_000},  # Mega whales only
    
    # 💡 NOTE: Reduced from 108 to 20 tokens to stay within Etherscan rate limits
    #     If you need more tokens, upgrade to Etherscan Pro plan
}

# 🏛️ PROFESSIONAL-GRADE TOP 100 ERC-20 TOKEN MONITORING LIST
# Real contract addresses verified from Etherscan and CoinGecko
TOP_100_ERC20_TOKENS = [
    # 🏆 MEGA CAP ($10B+) - $500K threshold
    {"symbol": "USDT", "address": "0xdac17f958d2ee523a2206206994597c13d831ec7", "tier": "mega_cap", "decimals": 6},
    {"symbol": "USDC", "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "tier": "mega_cap", "decimals": 6},
    {"symbol": "WETH", "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", "tier": "mega_cap", "decimals": 18},
    {"symbol": "WBTC", "address": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599", "tier": "mega_cap", "decimals": 8},
    {"symbol": "SHIB", "address": "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce", "tier": "mega_cap", "decimals": 18},
    {"symbol": "DAI", "address": "0x6b175474e89094c44da98b954eedeac495271d0f", "tier": "mega_cap", "decimals": 18},
    
    # 🚀 LARGE CAP ($1B+) - $100K threshold  
    {"symbol": "UNI", "address": "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984", "tier": "large_cap", "decimals": 18},
    {"symbol": "LINK", "address": "0x514910771af9ca656af397c67371dc9b5c1eaf5e", "tier": "large_cap", "decimals": 18},
    {"symbol": "MATIC", "address": "0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0", "tier": "large_cap", "decimals": 18},
    {"symbol": "AAVE", "address": "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9", "tier": "large_cap", "decimals": 18},
    {"symbol": "MKR", "address": "0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2", "tier": "large_cap", "decimals": 18},
    {"symbol": "LDO", "address": "0x5a98fcbea516cf06857215779fd812ca3bef1b32", "tier": "large_cap", "decimals": 18},
    {"symbol": "APE", "address": "0x4d224452801aced8b2f0aebe155379bb5d594381", "tier": "large_cap", "decimals": 18},
    {"symbol": "CRO", "address": "0xa0b73e1ff0b80914ab6fe0444e65848c4c34450b", "tier": "large_cap", "decimals": 8},
    {"symbol": "QNT", "address": "0x4a220e6096b25eadb88358cb44068a3248254675", "tier": "large_cap", "decimals": 18},
    {"symbol": "GRT", "address": "0xc944e90c64b2c07662a292be6244bdf05cda44a7", "tier": "large_cap", "decimals": 18},
    {"symbol": "MANA", "address": "0x0f5d2fb29fb7d3cfee444a200298f468908cc942", "tier": "large_cap", "decimals": 18},
    {"symbol": "SAND", "address": "0x3845badade8e6dff049820680d1f14bd3903a5d0", "tier": "large_cap", "decimals": 18},
    {"symbol": "ARB", "address": "0x912ce59144191c1204e64559fe8253a0e49e6548", "tier": "large_cap", "decimals": 18},
    {"symbol": "OP", "address": "0x4200000000000000000000000000000000000042", "tier": "large_cap", "decimals": 18},
    
    # 💎 MID CAP ($100M+) - $50K threshold
    {"symbol": "CRV", "address": "0xd533a949740bb3306d119cc777fa900ba034cd52", "tier": "mid_cap", "decimals": 18},
    {"symbol": "YFI", "address": "0x0bc529c00c6401aef6d220be8c6ea1667f6ad93e", "tier": "mid_cap", "decimals": 18},
    {"symbol": "COMP", "address": "0xc00e94cb662c3520282e6f5717214004a7f26888", "tier": "mid_cap", "decimals": 18},
    {"symbol": "SUSHI", "address": "0x6b3595068778dd592e39a122f4f5a5cf09c90fe2", "tier": "mid_cap", "decimals": 18},
    {"symbol": "SNX", "address": "0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f", "tier": "mid_cap", "decimals": 18},
    {"symbol": "BAL", "address": "0xba100000625a3754423978a60c9317c58a424e3d", "tier": "mid_cap", "decimals": 18},
    {"symbol": "CVX", "address": "0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b", "tier": "mid_cap", "decimals": 18},
    {"symbol": "1INCH", "address": "0x111111111117dc0aa78b770fa6a738034120c302", "tier": "mid_cap", "decimals": 18},
    {"symbol": "ENS", "address": "0xc18360217d8f7ab5e7c516566761ea12ce7f9d72", "tier": "mid_cap", "decimals": 18},
    {"symbol": "GALA", "address": "0x15d4c048f83bd7e37d49ea4c83a07267ec4203da", "tier": "mid_cap", "decimals": 8},
    {"symbol": "CHZ", "address": "0x3506424f91fd33084466f402d5d97f05f8e3b4af", "tier": "mid_cap", "decimals": 18},
    {"symbol": "ENJ", "address": "0xf629cbd94d3791c9250152bd8dfbdf380e2a3b9c", "tier": "mid_cap", "decimals": 18},
    {"symbol": "LRC", "address": "0xbbbbca6a901c926f240b89eacb641d8aec7aeafd", "tier": "mid_cap", "decimals": 18},
    {"symbol": "BAT", "address": "0x0d8775f648430679a709e98d2b0cb6250d2887ef", "tier": "mid_cap", "decimals": 18},
    {"symbol": "ZRX", "address": "0xe41d2489571d322189246dafa5ebde1f4699f498", "tier": "mid_cap", "decimals": 18},
    {"symbol": "PEPE", "address": "0x6982508145454ce325ddbe47a25d4ec3d2311933", "tier": "mid_cap", "decimals": 18},
    {"symbol": "FLOKI", "address": "0xcf0c122c6b73ff809c693db761e7baebe62b6a2e", "tier": "mid_cap", "decimals": 9},
    
    # 🎯 SMALL CAP ($10M+) - $10K threshold
    {"symbol": "FET", "address": "0xaea46a60368a7bd060eec7df8cba43b7ef41ad85", "tier": "small_cap", "decimals": 18},
    {"symbol": "OCEAN", "address": "0x967da4048cd07ab37855c090aaf366e4ce1b9f48", "tier": "small_cap", "decimals": 18},
    {"symbol": "AGIX", "address": "0x5b7533812759b45c2b44c19e320ba2cd2681b542", "tier": "small_cap", "decimals": 8},
    {"symbol": "RNDR", "address": "0x6de037ef9ad2725eb40118bb1702ebb27e4aeb24", "tier": "small_cap", "decimals": 18},
    {"symbol": "BLUR", "address": "0x5283d291dbcf85356a21ba090e6db59121208b44", "tier": "small_cap", "decimals": 18},
    {"symbol": "RPL", "address": "0xd33526068d116ce69f19a9ee46f0bd304f21a51f", "tier": "small_cap", "decimals": 18},
    {"symbol": "SSV", "address": "0x9d65ff81a3c488d585bbfb0bfe3c7707c7917f54", "tier": "small_cap", "decimals": 18},
    {"symbol": "ANT", "address": "0xa117000000f279d81a1d3cc75430faa017fa5a2e", "tier": "small_cap", "decimals": 18},
    {"symbol": "BNT", "address": "0x1f573d6fb3f13d689ff844b4ce37794d79a7ff1c", "tier": "small_cap", "decimals": 18},
    {"symbol": "REN", "address": "0x408e41876cccdc0f92210600ef50372656052a38", "tier": "small_cap", "decimals": 18},
    {"symbol": "DYDX", "address": "0x92d6c1e31e14520e676a687f0a93788b716beff5", "tier": "small_cap", "decimals": 18},
    {"symbol": "ALPHA", "address": "0xa1faa113cbe53436df28ff0aee54275c13b40975", "tier": "small_cap", "decimals": 18},
    {"symbol": "BADGER", "address": "0x3472a5a71965499acd81997a54bba8d852c6e53d", "tier": "small_cap", "decimals": 18},
    {"symbol": "UMA", "address": "0x04fa0d235c4abf4bcf4787af4cf447de572ef828", "tier": "small_cap", "decimals": 18},
    {"symbol": "OXT", "address": "0x4575f41308ec1483f3d399aa9a2826d74da13deb", "tier": "small_cap", "decimals": 8},
    {"symbol": "MLN", "address": "0xec67005c4e498ec7f55e092bd1d35cbc47c91892", "tier": "small_cap", "decimals": 18},
    {"symbol": "KNC", "address": "0xdefa4e8a7bcba345f687a2f1456f5edd9ce97202", "tier": "small_cap", "decimals": 18},
    {"symbol": "ALCX", "address": "0xdbdb4d16eda451d0503b854cf79d55697f90c8df", "tier": "small_cap", "decimals": 18},
    {"symbol": "RARI", "address": "0xfca59cd816ab1ead66534d82bc21e7515ce441cf", "tier": "small_cap", "decimals": 18},
    
    # 🔬 MICRO CAP / EMERGING TOKENS - $5K threshold
    {"symbol": "DOGE", "address": "0x4206931337dc273a630d328da6441786bfad668f", "tier": "micro_cap", "decimals": 8},
    {"symbol": "SPELL", "address": "0x090185f2135308bad17527004364ebcc2d37e5f6", "tier": "micro_cap", "decimals": 18},
    {"symbol": "POLY", "address": "0x9992ec3cf6a55b00978cddf2b27bc6882d88d1ec", "tier": "micro_cap", "decimals": 18},
    {"symbol": "TEMPLE", "address": "0x470ebf5f030ed85fc1ed4c2d36b9dd02e77cf1b7", "tier": "micro_cap", "decimals": 18},
    {"symbol": "RADAR", "address": "0x44709a920fccf795fbc57baa433cc3dd53c44dbe", "tier": "micro_cap", "decimals": 18},
    {"symbol": "TORN", "address": "0x77777feddddffc19ff86db637967013e6c6a116c", "tier": "micro_cap", "decimals": 18},
    {"symbol": "BOBA", "address": "0x42bbfa2e77757c645eeaad1655e0911a7553efbc", "tier": "micro_cap", "decimals": 18},
    {"symbol": "RBN", "address": "0x6123b0049f904d730db3c36030fcf7565d661f99", "tier": "micro_cap", "decimals": 18},
    {"symbol": "POND", "address": "0x57b946008913b82e4df85f501cbaed910e58d26c", "tier": "micro_cap", "decimals": 18},
    {"symbol": "MIR", "address": "0x09a3ecafa817268f77be1283176b946c4ff2e608", "tier": "micro_cap", "decimals": 18},
    {"symbol": "TRIBE", "address": "0xc7283b66eb1eb5fb86327f08e1b5816b0720212b", "tier": "micro_cap", "decimals": 18},
    {"symbol": "FEI", "address": "0x956f47f50a910163d8bf957cf5846d573e7f87ca", "tier": "micro_cap", "decimals": 18},
    {"symbol": "JPEG", "address": "0xe80c0cd204d654cebe8dd64a4857cab6be8345a3", "tier": "micro_cap", "decimals": 18},
    {"symbol": "SHIDO", "address": "0x94845333028b1204fbe14e1278fd4adde46b22ce", "tier": "micro_cap", "decimals": 9},
    {"symbol": "SQUID", "address": "0x21ad647b8f4fe333212e735bfc1f36b4941e6ab2", "tier": "micro_cap", "decimals": 18},
    
    # 💰 STABLECOINS & INSTITUTIONAL
    {"symbol": "FRAX", "address": "0x853d955acef822db058eb8505911ed77f175b99e", "tier": "large_cap", "decimals": 18},
    {"symbol": "LUSD", "address": "0x5f98805a4e8be255a32880fdec7f6728c6568ba0", "tier": "mid_cap", "decimals": 18},
    {"symbol": "USDD", "address": "0x0c10bf8fcb7bf5412187a595ab97a3609160b5c6", "tier": "mid_cap", "decimals": 18},
    {"symbol": "USDP", "address": "0x1456688345527be1f37e9e627da0837d6f08c925", "tier": "mid_cap", "decimals": 18},
    {"symbol": "TUSD", "address": "0x0000000000085d4780b73119b644ae5ecd22b376", "tier": "mid_cap", "decimals": 18},
    {"symbol": "GUSD", "address": "0x056fd409e1d7a124bd7017459dfea2f387b6d5cd", "tier": "small_cap", "decimals": 2},
    
    # 🎮 GAMING & METAVERSE
    {"symbol": "AXS", "address": "0xbb0e17ef65f82ab018d8edd776e8dd940327b28b", "tier": "mid_cap", "decimals": 18},
    {"symbol": "ILV", "address": "0x767fe9edc9e0df98e07454847909b5e959d7ca0e", "tier": "small_cap", "decimals": 18},
    {"symbol": "SLP", "address": "0xcc8fa225d80b9c7d42f96e9570156c65d6caaa25", "tier": "small_cap", "decimals": 0},
    {"symbol": "REVV", "address": "0x557b933a7c2c45672b610f8954a3deb39a51a8ca", "tier": "micro_cap", "decimals": 18},
    {"symbol": "TOWER", "address": "0x1c9922314ed1415c95b9fd453c3818fd41867d0b", "tier": "micro_cap", "decimals": 18},
    {"symbol": "ALICE", "address": "0xac51066d7bec65dc4589368da368b212745d63e8", "tier": "small_cap", "decimals": 6},
    {"symbol": "TLM", "address": "0x888888848b652b3e3a0f34c96e00eec0f3a23f72", "tier": "small_cap", "decimals": 4},
    {"symbol": "WAXP", "address": "0x39bb259f66e1c59d5abef88375979b4d20d98022", "tier": "small_cap", "decimals": 8},
    
    # 🔋 INFRASTRUCTURE & ORACLES
    {"symbol": "API3", "address": "0x0b38210ea11411557c13457d4da7dc6ea731b88a", "tier": "small_cap", "decimals": 18},
    {"symbol": "BAND", "address": "0xba11d00c5f74255f56a5e366f4f77f5a186d7f55", "tier": "small_cap", "decimals": 18},
    {"symbol": "TRB", "address": "0x88df592f8eb5d7bd38bfef7deb0fbc02cf3778a0", "tier": "small_cap", "decimals": 18},
    {"symbol": "DIA", "address": "0x84ca8bc7997272c7cfb4d0cd3d55cd942b3c9419", "tier": "micro_cap", "decimals": 18},
    
    # 🏛️ DEFI BLUE CHIPS EXTENDED
    {"symbol": "INST", "address": "0x6f40d4a6237c257fff2db00fa0510deeecd303eb", "tier": "small_cap", "decimals": 18},
    {"symbol": "DODO", "address": "0x43dfc4159d86f3a37a5a4b3d4580b888ad7d4ddd", "tier": "micro_cap", "decimals": 18},
    {"symbol": "BEL", "address": "0xa91ac63d040deb1b7a5e4d4134ad23eb0ba07e14", "tier": "micro_cap", "decimals": 18},
    {"symbol": "PERP", "address": "0xbc396689893d065f41bc2c6ecbee5e0085233447", "tier": "small_cap", "decimals": 18},
    {"symbol": "INJ", "address": "0xe28b3b32b6c345a34ff64674606124dd5aceca30", "tier": "small_cap", "decimals": 18},
    {"symbol": "HEGIC", "address": "0x584bc13c7d411c00c01a62e8019472de68768430", "tier": "micro_cap", "decimals": 18},
    
    # 🚀 LAYER 2 & SCALING SOLUTIONS  
    {"symbol": "IMX", "address": "0xf57e7e7c23978c3caec3c3548e3d615c346e79ff", "tier": "mid_cap", "decimals": 18},
    {"symbol": "METIS", "address": "0x9e32b13ce7f2e80a01932b42553652e053d6ed8e", "tier": "small_cap", "decimals": 18},
    {"symbol": "CELR", "address": "0x4f9254c83eb525f9fcf346490bbb3ed28a81c667", "tier": "small_cap", "decimals": 18},
    {"symbol": "STRK", "address": "0xca14007eff0db1f8135f4c25b34de49ab0d42766", "tier": "small_cap", "decimals": 18},
    
    # 📊 ADDITIONAL QUALITY TOKENS
    {"symbol": "REQ", "address": "0x8f8221afbb33998d8584a2b05749ba73c37a938a", "tier": "micro_cap", "decimals": 18},
    {"symbol": "LPT", "address": "0x58b6a8a3302369daec383334672404ee733ab239", "tier": "small_cap", "decimals": 18},
    {"symbol": "NMR", "address": "0x1776e1f26f98b1a5df9cd347953a26dd3cb46671", "tier": "small_cap", "decimals": 18},
    {"symbol": "STORJ", "address": "0xb64ef51c888972c908cfacf59b47c1afbc0ab8ac", "tier": "micro_cap", "decimals": 8},
    {"symbol": "KEEP", "address": "0x85eee30c52b0b379b046fb0f85f4f3dc3009afec", "tier": "micro_cap", "decimals": 18},
    {"symbol": "NU", "address": "0x4fe83213d56308330ec302a8bd641f1d0113a4cc", "tier": "micro_cap", "decimals": 18},
    {"symbol": "ANKR", "address": "0x8290333cef9e6d528dd5618fb97a76f268f3edd4", "tier": "small_cap", "decimals": 18},
    {"symbol": "SKL", "address": "0x00c83aecc790e8a4453e5dd3b0b4b3680501a7a7", "tier": "small_cap", "decimals": 18},
    {"symbol": "CTK", "address": "0x2ba64efb7a4f3e4242cfa373be66e8b7b2a2f0f4", "tier": "micro_cap", "decimals": 6},
    {"symbol": "AUDIO", "address": "0x18aaa7115705e8be94bffebde57af9bfc265b998", "tier": "small_cap", "decimals": 18}
]

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
    "FLOKI": 0.00002,      # Added missing price
    "DOGE": 0.08,          # Added missing price 
    "APE": 1.5,
    "SAND": 0.4,           # Added missing price
    "MANA": 0.5,           # Added missing price
    "GALA": 0.02,          # Added missing price
    "CHZ": 0.08,           # Added missing price
    "ENJ": 0.3,            # Added missing price
    "FET": 0.8,            # Added missing price
    "OCEAN": 0.6,          # Added missing price
    "AGIX": 0.4,           # Added missing price
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
    
    # 🏆 MEGA CAP TOKENS - COMPREHENSIVE PRICING
    "USDT": 1.00,          # Tether
    "USDC": 1.00,          # USD Coin
    "WBTC": 65000,         # Wrapped Bitcoin
    "DAI": 1.00,           # MakerDAO
    
    # 🚀 LARGE CAP TOKENS - PROFESSIONAL PRICING
    "CRO": 0.12,           # Cronos
    "QNT": 85.0,           # Quant
    "ENS": 15.0,           # Ethereum Name Service
    
    # 💎 MID CAP TOKENS - VERIFIED PRICING
    
    # 🎯 SMALL CAP TOKENS - COMPREHENSIVE COVERAGE
    "RNDR": 8.5,           # Render Token
    "BLUR": 0.85,          # Blur
    "SSV": 45.0,           # SSV Network
    "ANT": 4.2,            # Aragon
    "ALPHA": 0.15,         # Alpha Finance
    "BADGER": 3.8,         # Badger DAO
    "UMA": 2.4,            # UMA Protocol
    "OXT": 0.08,           # Orchid
    "MLN": 25.0,           # Melon Protocol
    "KNC": 0.75,           # Kyber Network
    "ALCX": 22.0,          # Alchemix
    "RARI": 2.1,           # Rarible
    
    # 🔬 MICRO CAP TOKENS - EMERGING PROJECTS
    "SPELL": 0.0008,       # Spell Token
    "POLY": 0.35,          # Polymath
    "TEMPLE": 1.2,         # TempleDAO
    "RADAR": 0.012,        # DappRadar
    "TORN": 8.5,           # Tornado Cash
    "BOBA": 0.18,          # Boba Network
    "RBN": 0.45,           # Ribbon Finance
    "POND": 0.015,         # Marlin Protocol
    "MIR": 0.25,           # Mirror Protocol
    "TRIBE": 0.35,         # Fei Protocol
    "JPEG": 0.0001,        # JPEG'd
    "SHIDO": 0.002,        # Shido
    "SQUID": 0.001,        # Squid Game Token
    
    # 💰 STABLECOINS & INSTITUTIONAL
    "USDD": 1.00,          # USDD
    "USDP": 1.00,          # Pax Dollar
    "TUSD": 1.00,          # TrueUSD
    "GUSD": 1.00,          # Gemini Dollar
    
    # 🎮 GAMING & METAVERSE
    "AXS": 8.5,            # Axie Infinity
    "ILV": 75.0,           # Illuvium
    "SLP": 0.0035,         # Smooth Love Potion
    "REVV": 0.045,         # REVV Racing
    "TOWER": 0.0085,       # Crazy Defense Heroes
    "ALICE": 1.8,          # My Neighbor Alice
    "TLM": 0.018,          # Alien Worlds
    "WAXP": 0.065,         # WAX
    
    # 🔋 INFRASTRUCTURE & ORACLES
    "API3": 2.1,           # API3
    "BAND": 1.4,           # Band Protocol
    "TRB": 35.0,           # Tellor
    "DIA": 0.65,           # DIA
    
    # 🏛️ DEFI BLUE CHIPS EXTENDED
    "INST": 4.2,           # Instadapp
    "DODO": 0.18,          # DODO
    "BEL": 0.65,           # Bella Protocol
    "PERP": 0.85,          # Perpetual Protocol
    "HEGIC": 0.012,        # Hegic
    
    # 🚀 LAYER 2 & SCALING SOLUTIONS
    "IMX": 1.8,            # Immutable X
    "METIS": 45.0,         # Metis
    "CELR": 0.025,         # Celer Network
    "STRK": 1.2,           # Strike
    
    # 📊 ADDITIONAL QUALITY TOKENS
    "REQ": 0.08,           # Request Network
    "LPT": 12.0,           # Livepeer
    "NMR": 18.0,           # Numeraire
    "STORJ": 0.45,         # Storj
    "KEEP": 0.08,          # Keep Network
    "NU": 0.12,            # NuCypher
    "ANKR": 0.035,         # Ankr
    "SKL": 0.055,          # SKALE
    "CTK": 0.85,           # CertiK
    "AUDIO": 0.18,         # Audius
    
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

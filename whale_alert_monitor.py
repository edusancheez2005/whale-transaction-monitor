import datetime
import json
import requests
import time
import threading
from collections import defaultdict
import matplotlib.pyplot as plt
import numpy as np
import websocket  # pip install websocket-client

# -------------------------------------------------------------------------
# 1) WHALE ALERT SETUP
# -------------------------------------------------------------------------
WHALE_ALERT_API_KEY = "TSVOXnkH32evQo1KiddPIJscfksPkWIo"
WS_URL = f"wss://leviathan.whale-alert.io/ws?api_key={WHALE_ALERT_API_KEY}"

# Dictionaries to track whale alert counts
whale_buy_counts = defaultdict(int)
whale_sell_counts = defaultdict(int)
whale_trending_counts = defaultdict(int)

whale_known_exchanges = {
    "binance", "coinbase", "kraken", "huobi", "bitfinex", 
    "bittrex", "poloniex", "okex", "bitstamp", "gemini",
    "mexc", "bybit", "uniswap", "kucoin", "gate.io",
    "binanceus", "bitmex", "wazirx", "lbank", "bitget",
    "coinex", "phemex", "zb", "upbit", "bigone",
    "xt.com", "crypto.com", "probit", "indodax", "hitbtc",
    "coinlist", "coinsbit", "bithumb", "bitso", "tokocrypto",
    "ftx", "woo", "deribit", "dex-trade", "pancakeswap",
    "sushiswap", "bisq", "changelly", "bitvavo", "indacoin",
    "whitebit", "stormgain", "cex.io", "nicehash",
    "bitmart", "ascendex"
}

# --- New: Define stablecoins to ignore in Whale Alert ---
STABLE_COINS = {"usdt", "usdc", "dai", "tusd", "busd"}

def classify_whale_transaction(from_addr, to_addr):
    from_lower = from_addr.lower() if from_addr else ""
    to_lower   = to_addr.lower() if to_addr else ""
    from_is_exchange = any(exch in from_lower for exch in whale_known_exchanges)
    to_is_exchange   = any(exch in to_lower for exch in whale_known_exchanges)
    if from_is_exchange and not to_is_exchange:
        return "buy"
    elif to_is_exchange and not from_is_exchange:
        return "sell"
    else:
        return "unknown"

def on_whale_message(ws, message):
    print(f"\nReceived raw message: {message}\n") 
    try:
        data = json.loads(message)
        if data.get("type") == "alert":
            amounts = data.get("amounts", [])
            coins_in_alert = set()
            for amt in amounts:
                symbol = amt.get("symbol")
                # Skip stablecoins (e.g. USDT, USDC, DAI, etc.)
                if symbol and symbol.lower() in STABLE_COINS:
                    continue
                if symbol:
                    coins_in_alert.add(symbol.lower())
            
            tx_from = data.get("from", "")
            tx_to = data.get("to", "")
            classification = classify_whale_transaction(tx_from, tx_to)
            
            if classification in ("buy", "sell"):
                for coin in coins_in_alert:
                    whale_trending_counts[coin] += 1
                    if classification == "buy":
                        whale_buy_counts[coin] += 1
                    else:
                        whale_sell_counts[coin] += 1
                
                # Enhanced transaction display
                print("\n" + "="*50)
                print("🐋 WHALE ALERT DETECTED:")
                print("="*50)
                print(f"📅 Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(data.get('timestamp', 0)))}")
                print(f"🔗 Blockchain: {data.get('blockchain', 'N/A')}")
                print(f"📝 Type: {data.get('transaction_type', 'N/A')}")
                print(f"📤 From: {tx_from}")
                print(f"📥 To: {tx_to}")
                print(f"🏷️  Classification: {classification.upper()}")
                
                print("\n💰 Amounts Transferred:")
                total_usd_value = 0
                for amt in amounts:
                    sym = amt.get("symbol", "N/A")
                    # Again, skip stablecoins in the printout
                    if sym.lower() in STABLE_COINS:
                        continue
                    val = amt.get("amount", 0)
                    usd = amt.get("value_usd", 0)
                    total_usd_value += usd
                    print(f"   • {sym}: {val:,.2f} (${usd:,.2f} USD)")
                
                print(f"\n💵 Total USD Value: ${total_usd_value:,.2f}")
                
                if data.get("transaction", {}).get("hash"):
                    print(f"\n🔍 Transaction Hash: {data['transaction']['hash']}")
                
                print("\n📊 Market Impact:")
                print(f"   • Transaction Size: {'VERY LARGE' if total_usd_value > 10000000 else 'LARGE'}")
                print(f"   • Direction: {classification.upper()}")
                
                print("\n📝 Summary:")
                print(data.get("text", ""))
                print("="*50 + "\n")
    except Exception as e:
        print("Error processing Whale Alert message:", e)
        print("Raw message:", message)

def on_whale_error(ws, error):
    print(f"\nWhale Alert WebSocket Error at {time.strftime('%Y-%m-%d %H:%M:%S')}:")
    print(f"Error type: {type(error)}")
    print(f"Error details: {str(error)}")
    if "429" in str(error):
        print("Rate limit encountered. Waiting for 60 seconds before reconnecting.")
        time.sleep(60)
    elif "401" in str(error):
        print("Authentication error - please check your API key")
    elif "403" in str(error):
        print("Authorization error - please check your subscription status")

def on_whale_close(ws, close_status_code, close_msg):
    print("Whale Alert WebSocket closed. Code:", close_status_code, "Message:", close_msg)
    # If no code is provided, assume it might be due to rate limiting.
    wait_time = 60 if close_status_code is None else 10
    print(f"Attempting to reconnect in {wait_time} seconds...")
    time.sleep(wait_time)
    connect_whale_websocket()

def on_whale_open(ws):
    print("Whale Alert WebSocket connection established.")
    subscription_request = {
        "type": "subscribe_alerts",
        "min_value_usd": 50000,  # Lowered to catch more movements
        "blockchain": [
            "ethereum",    # ETH and ERC-20 tokens
            "bitcoin",     # BTC
            "solana",      # SOL and SPL tokens
            "ripple",      # XRP (using 'ripple' instead of 'xrp')
            "polygon",     # MATIC and tokens
            "tron"        # TRX and tokens
        ]
    }
    ws.send(json.dumps(subscription_request))
    print("Subscription request sent:")
    print(json.dumps(subscription_request, indent=4))
    print("\nMonitoring major chains for whale movements...")

def connect_whale_websocket():
    ws_app = websocket.WebSocketApp(
        WS_URL,
        on_open=on_whale_open,
        on_message=on_whale_message,
        on_error=on_whale_error,
        on_close=on_whale_close
    )
    ws_app.run_forever()

# -------------------------------------------------------------------------
# 2) ETHEREUM (ERC-20) SETUP
# -------------------------------------------------------------------------
ETHERSCAN_API_KEY = "QY23IJ4D4EJTGFQNSNJHAD4G1IUEQYUJTN"

# Updated list: Removed stablecoins and added 18 more tokens so that there are 20 tokens in total.
TOKENS_TO_MONITOR = {
    "WETH": {"contract": "0xC02aaa39b223FE8D0A0e5C4F27ead9083C756Cc2", "decimals": 18, "min_threshold": 63},
    "LINK": {"contract": "0x514910771af9ca656af840dff83e8264ecf986ca", "decimals": 18, "min_threshold": 14286},
    "UNI": {"contract": "0x1F9840a85d5aF5bf1D1762F925BDADdC4201F984", "decimals": 18, "min_threshold": 20000},
    "AAVE": {"contract": "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9", "decimals": 18, "min_threshold": 1333},
    "COMP": {"contract": "0xc00e94Cb662C3520282E6f5717214004A7f26888", "decimals": 18, "min_threshold": 1667},
    "SNX": {"contract": "0xC011a73ee8576Fb46F5E1c5751cA3B9Fe0Af2a6F", "decimals": 18, "min_threshold": 40000},
    "MKR": {"contract": "0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2", "decimals": 18, "min_threshold": 67},
    "YFI": {"contract": "0x0bc529c00C6401aEF6D220BE8C6Ea1667F6Ad93e", "decimals": 18, "min_threshold": 4},
    "SUSHI": {"contract": "0x6B3595068778DD592e39A122f4f5a5cF09C90fE2", "decimals": 18, "min_threshold": 40000},
    "CRV": {"contract": "0xD533a949740bb3306d119CC777fa900bA034cd52", "decimals": 18, "min_threshold": 40000},
    "BAL": {"contract": "0xba100000625a3754423978a60c9317c58a424e3D", "decimals": 18, "min_threshold": 10000},
    "BNT": {"contract": "0x1F573D6Fb3F13d689FF844B4cE37794d79A7FF1C", "decimals": 18, "min_threshold": 50000},
    "REN": {"contract": "0x408e41876cCCDC0F92210600ef50372656052a38", "decimals": 18, "min_threshold": 333333},
    "OMG": {"contract": "0xd26114cd6EE289AccF82350c8d8487fedB8A0C07", "decimals": 18, "min_threshold": 66667},
    "ZRX": {"contract": "0xE41d2489571d322189246DaFA5ebDe1F4699F498", "decimals": 18, "min_threshold": 250000},
    "BAT": {"contract": "0x0D8775F648430679A709E98d2b0Cb6250d2887EF", "decimals": 18, "min_threshold": 166667},
    "GRT": {"contract": "0xC944E90C64B2c07662A292be6244BDf05Cda44a7", "decimals": 18, "min_threshold": 200000},
    "LRC": {"contract": "0xBBbbCA6A901c926F240b89EacB641d8Aec7AEafD", "decimals": 18, "min_threshold": 333333},
    "1INCH": {"contract": "0x111111111117dC0aa78b770fA6A738034120C302", "decimals": 18, "min_threshold": 40000},
    "MATIC": {"contract": "0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0", "decimals": 18, "min_threshold": 100000},
    # Ethereum Ecosystem
    "WETH": {"contract": "0xC02aaa39b223FE8D0A0e5C4F27ead9083C756Cc2", "decimals": 18},
    "LINK": {"contract": "0x514910771af9ca656af840dff83e8264ecf986ca", "decimals": 18},
    "UNI": {"contract": "0x1F9840a85d5aF5bf1D1762F925BDADdC4201F984", "decimals": 18},
    "AAVE": {"contract": "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9", "decimals": 18},
    
    # DeFi Blue Chips
    "MKR": {"contract": "0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2", "decimals": 18},
    "YFI": {"contract": "0x0bc529c00C6401aEF6D220BE8C6Ea1667F6Ad93e", "decimals": 18},
    "SUSHI": {"contract": "0x6B3595068778DD592e39A122f4f5a5cF09C90fE2", "decimals": 18},
    "CRV": {"contract": "0xD533a949740bb3306d119CC777fa900bA034cd52", "decimals": 18},
    
    # Gaming & Metaverse
    "AXS": {"contract": "0xBB0E17EF65F82Ab018d8EDd776e8DD940327B28b", "decimals": 18},
    "SAND": {"contract": "0x3845badAde8e6dFF049820680d1F14bD3903a5d0", "decimals": 18},
    "MANA": {"contract": "0x0F5D2fB29fb7d3CFeE444a200298f468908cC942", "decimals": 18},
    
    # Layer 2 & Scaling
    "MATIC": {"contract": "0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0", "decimals": 18},
    "ARB": {"contract": "0xB50721BCf8d664c30412Cfbc6cf7a15145234ad1", "decimals": 18},
    "OP": {"contract": "0x4200000000000000000000000000000000000042", "decimals": 18}
}

# Updated approximate static token prices (in USD) for the above tokens.
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

# Global variables to track processed blocks and buy/sell counts
last_processed_block = {symbol: 0 for symbol in TOKENS_TO_MONITOR}
buy_counts = defaultdict(int)
sell_counts = defaultdict(int)
GLOBAL_USD_THRESHOLD = 100_000

def compute_buy_percentage(buys, sells):
    total = buys + sells
    return buys / total if total else 0

def fetch_erc20_transfers(contract_address, sort="desc"):
    url = "https://api.etherscan.io/api"
    params = {
        "module": "account",
        "action": "tokentx",
        "contractaddress": contract_address,
        "startblock": 0,
        "endblock": 99999999,
        "sort": sort,
        "apikey": ETHERSCAN_API_KEY
    }
    try:
        r = requests.get(url, params=params, timeout=20)
        data = r.json()
        if data.get("status") == "1":
            return data.get("result", [])
        else:
            msg = data.get("message", "No message")
            print(f"Etherscan returned status != 1 for {contract_address}. Msg: {msg}")
            return []
    except Exception as e:
        print(f"Error calling Etherscan for {contract_address}:", e)
        return []

# -------------------------------------------------------------------------
# Global Known Exchange Addresses (REAL EXAMPLES)
# -------------------------------------------------------------------------
known_exchange_addresses = {
# Binance
    "0x28c6c06298d514db089934071355e5743bf21d60": "binance",
    "0xdfd5293d8e347dfe59e90efd55b2956a1343963d": "binance",
    "0x5a52e96bacdabb82fd05763e25335261b270efcb": "binance",
    "0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be": "binance",
    
    # Coinbase
    "0x71660c4005ba85c37ccec55d0c4493e66fe775d3": "coinbase",
    "0x503828976d22510aad0201ac7ec88293211d23da": "coinbase",
    "0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740": "coinbase",
    
    # FTX
    "0x2faf487a4414fe77e2327f0bf4ae2a264a776ad2": "ftx",
    "0xc098b2a3aa256d2140208c3de6543aaef5cd3a94": "ftx",
    
    # Kraken
    "0x2910543af39aba0cd09dbb2d50200b3e800a63d2": "kraken",
    "0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13": "kraken",
    "0xa83b11093c858c86321fbc4c20fe82cdbd58e09e": "kraken",
    
    # KuCoin
    "0x2b5634c42055806a59e9107ed44d43c426e58258": "kucoin",
    "0x689c56aef474df92d44a1b70850f808488f9769c": "kucoin",
    
    # Huobi
    "0x0d0707963952f2fba59dd06f2b425ace40b492fe": "huobi",
    "0x1062a747393198f70f71ec65a582423dba7e5ab3": "huobi",
    
    # Gate.io
    "0x0d0707963952f2fba59dd06f2b425ace40b492fe": "gate.io",
    "0x1062a747393198f70f71ec65a582423dba7e5ab3": "gate.io",
    
    # OKX
    "0x6cc5f688a315f3dc28a7781717a9a798a59fda7b": "okx",
    "0x236f9f97e0e62388479bf9e5ba4889e46b0273c3": "okx",

    # BitMart
    "0xe79eef9b9388a4ff70ed7ec5bccd5b928ebb8bd1": "bitmart",
    
    # MEXC
    "0x75e89d5979e4f6fba9f97c104c2f0afb3f1dcb88": "mexc",
    
    # Crypto.com
    "0x6262998ced04146fa42253a5c0af90ca02dfd2a3": "crypto.com",
    
    # Gemini
    "0xd24400ae8bfebb18ca49be86258a3c749cf46853": "gemini",

    # Uniswap v2 Router
    "0x7a250d5630b4cf539739df2c5dacb4c659f2488d": "uniswap",
    
    # Uniswap v3 Router
    "0xe592427a0aece92de3edee1f18e0157c05861564": "uniswap",
    
    # SushiSwap Router 
    "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f": "sushiswap",

    # More Binance addresses
    "0x9696f59e4d72e237be84ffd425dcad154bf96976": "binance",
    "0x4e9ce36e442e55ecd9025b9a6e0d88485d628a67": "binance",
    "0x8894e0a0c962cb723c1976a4421c95949be2d4e3": "binance",
    
    # More Coinbase addresses
    "0xa090e606e30bd747d4e6245a1517ebe430f0057e": "coinbase",
    "0xf6874c88757721a02f47592140905c4336dfbc61": "coinbase",
    
    # More OKX addresses
    "0x5041ed759dd4afc3a72b8192c143f72f4724081a": "okx",
    "0x4b4e140d1f131fdad6fb59c13af796fd194e4135": "okx",
    
    # Popular DEX aggregators
    "0x220bda5c8994804ac96ebe4df184d25e5c2196d4": "dex_aggregator",
    "0x11111112542d85b3ef69ae05771c2dccff4faa26": "dex_aggregator",
}

def classify_buy_sell(from_addr, to_addr):
    from_lower = from_addr.lower()
    to_lower = to_addr.lower()
    
    # Known DEX routers
    dex_routers = {
        "0x7a250d5630b4cf539739df2c5dacb4c659f2488d": "uniswap_v2",
        "0xe592427a0aece92de3edee1f18e0157c05861564": "uniswap_v3",
        "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f": "sushiswap"
    }
    
    from_type = get_address_type(from_addr)
    to_type = get_address_type(to_addr)
    
    # Check exchange interactions
    if from_addr.lower() in known_exchange_addresses and to_type != "exchange":
        return "buy"
    elif to_addr.lower() in known_exchange_addresses and from_type != "exchange":
        return "sell"
    
    # Check DEX interactions
    if to_lower in dex_routers:
        return "sell"  # Sending to DEX router typically means selling
    elif from_lower in dex_routers:
        return "buy"   # Receiving from DEX router typically means buying
    
    # Check contract interactions
    if from_type == "contract" and to_type == "wallet":
        return "buy"
    elif from_type == "wallet" and to_type == "contract":
        return "sell"
    
    return "unknown"

def is_contract_address(address):
    # Most contract addresses start with specific patterns
    contract_patterns = [
        "0x000000000000000000000000",
        "0xeeeeeeeeeeeeeeeeeeeeeeee",
        "0xffffffffffffffffffffffff",
        # Add more common patterns
        "0x000000000000000000000001",
        "0x000000000000000000000002",
        "0xdeaddeaddeaddeaddeaddead",
        "0xdeadbeefdeadbeefdeadbeef"
    ]
    # Also check if address is in our known DEX or token contract list
    dex_and_token_contracts = {
        "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",  # Uniswap V2
        "0xe592427a0aece92de3edee1f18e0157c05861564",  # Uniswap V3
        "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f",  # SushiSwap
    }
    dex_and_token_contracts.update(info["contract"] for info in TOKENS_TO_MONITOR.values())
    
    return any(address.lower().startswith(pattern) for pattern in contract_patterns) or address.lower() in dex_and_token_contracts

def get_address_type(address):
    if address.lower() in known_exchange_addresses:
        return "exchange"
    elif is_contract_address(address):
        return "contract"
    else:
        return "wallet"

def print_new_erc20_transfers():
    global last_processed_block
    for symbol, info in TOKENS_TO_MONITOR.items():
        contract = info["contract"]
        decimals = info["decimals"]
        price = TOKEN_PRICES.get(symbol, 0)
        if price == 0:
            continue
        transfers = fetch_erc20_transfers(contract, sort="desc")
        if not transfers:
            continue
        new_transfers = []
        for tx in transfers:
            block_num = int(tx["blockNumber"])
            if block_num <= last_processed_block.get(symbol, 0):
                break
            new_transfers.append(tx)
        if new_transfers:
            highest_block = max(int(t["blockNumber"]) for t in new_transfers)
            last_processed_block[symbol] = max(last_processed_block.get(symbol, 0), highest_block)
        for tx in reversed(new_transfers):
            raw_value = int(tx["value"])
            token_amount = raw_value / (10 ** decimals)
            estimated_usd = token_amount * price
            if estimated_usd >= GLOBAL_USD_THRESHOLD:
                tx_from = tx["from"]
                tx_to = tx["to"]
                classification = classify_buy_sell(tx_from, tx_to)
                if classification == "buy":
                    buy_counts[symbol] += 1
                elif classification == "sell":
                    sell_counts[symbol] += 1
                timestamp = int(tx["timeStamp"])
                human_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
                print(f"\n[{symbol} > ${GLOBAL_USD_THRESHOLD:,} USD] Block {tx['blockNumber']} | Tx {tx['hash']}")
                print(f"  Time:   {human_time}")
                print(f"  From:   {tx_from}")
                print(f"  To:     {tx_to}")
                print(f"  Amount: {token_amount:,.2f} {symbol} (~${estimated_usd:,.2f} USD)")
                print(f"  Classification: {classification}")

def print_final_summary():
    tokens = sorted(TOKENS_TO_MONITOR.keys())
    summary_data = {}
    print("\n\n=== FINAL ETHEREUM TOKEN SUMMARY (NAIVE BUY PERCENTAGE) ===")
    for sym in tokens:
        bcount = buy_counts[sym]
        scount = sell_counts[sym]
        if bcount == 0 and scount == 0:
            continue
        buy_percentage = compute_buy_percentage(bcount, scount)
        if buy_percentage > 0.55:
            trend = "More Buys"
        elif buy_percentage < 0.45:
            trend = "More Sells"
        else:
            trend = "Neutral"
        summary_data[sym] = {"buy": bcount, "sell": scount, "buy_percentage": buy_percentage, "trend": trend}
        print(f"{sym}: BUY: {bcount}, SELL: {scount}  => {trend} (Buy % = {buy_percentage*100:.2f}%)")
    print("===========================================\n")
    tokens_with_data = [(sym, d["buy_percentage"]) for sym, d in summary_data.items() if (buy_counts[sym] + sell_counts[sym]) > 0]
    top5_buys = sorted(tokens_with_data, key=lambda x: x[1], reverse=True)[:5]
    top5_sells = sorted(tokens_with_data, key=lambda x: x[1])[:5]
    print("Top 5 tokens (highest buy %):")
    for sym, pct in top5_buys:
        print(f"  {sym}: Buy % = {pct*100:.2f}% ({summary_data[sym]['trend']})")
    print("\nTop 5 tokens (lowest buy %):")
    for sym, pct in top5_sells:
        print(f"  {sym}: Buy % = {pct*100:.2f}% ({summary_data[sym]['trend']})")
    get_news_for_tokens([sym for sym, _ in top5_buys + top5_sells])

# -------------------------------------------------------------------------
# 4) FREE NEWS API FUNCTIONALITY (using NewsAPI.org, restricted to English)
# -------------------------------------------------------------------------
NEWS_API_KEY = "b7c1fdbffb8842f18a495bf8d32df7cf"

def get_news_for_token(token_symbol):
    # Add specific cryptocurrency terms for better relevance
    crypto_terms = {
        "WETH": "Wrapped Ethereum",
        "LINK": "Chainlink",
        "UNI": "Uniswap UNI",
        "AAVE": "Aave",
        "COMP": "Compound COMP",
        "SNX": "Synthetix",
        "MKR": "Maker MKR",
        "YFI": "Yearn Finance",
        "SUSHI": "SushiSwap",
        "CRV": "Curve DAO",
        "BAL": "Balancer",
        "BNT": "Bancor",
        "REN": "Ren",
        "OMG": "OMG Network",
        "ZRX": "0x ZRX",
        "BAT": "Basic Attention Token",
        "GRT": "The Graph",
        "LRC": "Loopring",
        "1INCH": "1inch",
        "MATIC": "MATIC"
    }
    
    specific_term = crypto_terms.get(token_symbol.upper(), token_symbol)
    query = f'"{specific_term}" AND (cryptocurrency OR crypto OR blockchain)'
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": query,
        "sortBy": "publishedAt",
        "pageSize": 3,
        "language": "en",
        "domains": "cointelegraph.com,coindesk.com,decrypt.co,theblock.co",
        "apiKey": NEWS_API_KEY
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        if data.get("status") == "ok":
            articles = data.get("articles", [])
            if not articles:
                return ["No recent crypto-specific news found."]
            
            formatted_news = []
            for article in articles:
                from datetime import datetime
                pub_date = datetime.strptime(
                    article["publishedAt"], 
                    "%Y-%m-%dT%H:%M:%SZ"
                ).strftime("%Y-%m-%d")
                formatted_news.append(f"[{pub_date}] {article['title']} - {article['source']['name']}")
            return formatted_news
        else:
            return [f"Error: {data.get('message', 'Unknown error')}"]
    except Exception as e:
        return [f"Error fetching news: {e}"]

def get_news_for_tokens(token_list):
    print("\n=== NEWS HEADLINES FOR TOP TOKENS ===")
    for token in token_list:
        headlines = get_news_for_token(token)
        print(f"\n{token}:")
        for idx, headline in enumerate(headlines, start=1):
            print(f"  {idx}. {headline}")
    print("======================================\n")

def print_final_whale_summary():
    summary_data = {}
    print("\n\n" + "="*50)
    print("🐋 WHALE ALERT ACTIVITY SUMMARY")
    print("="*50)
    
    all_coins = set(whale_buy_counts.keys()) | set(whale_sell_counts.keys())
    if not all_coins:
        print("No Whale Alert events received during this session.")
        return
    
    print("\n📊 Transaction Analysis:")
    for coin in sorted(all_coins):
        bcount = whale_buy_counts[coin]
        scount = whale_sell_counts[coin]
        total = bcount + scount
        if total == 0:
            continue
        
        buy_percentage = bcount / total
        if buy_percentage > 0.55:
            trend = "🟢 More Buys"
        elif buy_percentage < 0.45:
            trend = "🔴 More Sells"
        else:
            trend = "⚪ Neutral"
            
        summary_data[coin] = {
            "buy": bcount,
            "sell": scount,
            "buy_percentage": buy_percentage,
            "trend": trend
        }
        
        print(f"\n{coin.upper()}:")
        print(f"   • Buy Transactions:  {bcount}")
        print(f"   • Sell Transactions: {scount}")
        print(f"   • Buy Percentage:    {buy_percentage*100:.2f}%")
        print(f"   • Trend:            {trend}")
    
    # Print top movers
    print("\n🔝 TOP MOVERS:")
    tokens_with_data = [(coin, d["buy_percentage"]) for coin, d in summary_data.items()]
    
    print("\nStrongest Buy Pressure:")
    top5_buys = sorted(tokens_with_data, key=lambda x: x[1], reverse=True)[:5]
    for coin, pct in top5_buys:
        print(f"   • {coin.upper()}: {pct*100:.2f}% buys ({summary_data[coin]['trend']})")
    
    print("\nStrongest Sell Pressure:")
    top5_sells = sorted(tokens_with_data, key=lambda x: x[1])[:5]
    for coin, pct in top5_sells:
        print(f"   • {coin.upper()}: {pct*100:.2f}% buys ({summary_data[coin]['trend']})")
    
    # Get news for all significant movers
    get_news_for_tokens([sym for sym, _ in top5_buys + top5_sells])

# -------------------------------------------------------------------------
# 5) MAIN LOOP
# -------------------------------------------------------------------------
if __name__ == "__main__":
    # Start Whale Alert WebSocket in a separate thread.
    def run_whale_alert():
        connect_whale_websocket()
    whale_thread = threading.Thread(target=run_whale_alert)
    whale_thread.daemon = True
    whale_thread.start()

    print("Starting monitoring for Ethereum ERC-20 tokens (each tx >= $100,000 USD) and Whale Alert events...\n")
    start_time = time.time()
    RUN_DURATION = 1 * 60  # Adjust run duration (in seconds) as needed

    while True:
        print_new_erc20_transfers()
        elapsed = time.time() - start_time
        if elapsed >= RUN_DURATION:
            break
        time.sleep(60)

    print_final_summary()
    print_final_whale_summary()
    print("Exiting program now.\n")

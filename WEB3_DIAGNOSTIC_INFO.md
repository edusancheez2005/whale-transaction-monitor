# web3 connection diagnostic

## what i added

detailed error logging to show exactly why web3/alchemy connections are failing.

## new diagnostics at startup

when you run `python enhanced_monitor.py`, you'll now see:

```
================================================================================
🔍 DIAGNOSING RPC CONNECTIONS
================================================================================

📡 ETHEREUM RPC Configuration:
   URL: https://eth-mainnet.alchemyapi.io/v2/...
   Available providers: 5
   ✅ Connected! Latest block: 12345678
   
📡 POLYGON RPC Configuration:
   URL: https://polygon-mainnet.g.alchemy.com/v2/...
   Available providers: 4
   ❌ Cannot get blocks: HTTPError: 429 Too Many Requests
   
🔑 API KEY STATUS:
   ✅ Alchemy API key configured: fo8NbYyCOV...cUpes
================================================================================
```

## detailed error messages

for each failed connection, you'll now see:

### rate limit errors
```
❌ Web3 connection error for ethereum:
   Error type: HTTPError
   Error message: 429 Client Error: Too Many Requests
   🚨 RATE LIMIT DETECTED - Too many API requests
   💡 Solution: Wait a few minutes or use a different RPC provider
```

### timeout errors
```
❌ Web3 connection error for polygon:
   Error type: ReadTimeout
   Error message: HTTPSConnectionPool read timed out
   ⏱️  TIMEOUT - RPC endpoint not responding
   💡 Solution: Check internet connection or try a different RPC
```

### authentication errors
```
❌ Web3 connection error for ethereum:
   Error type: HTTPError
   Error message: 403 Forbidden
   🔒 UNAUTHORIZED - Check your API key
   💡 Current RPC: https://eth-mainnet.alchemyapi.io/v2/fo8NbYy...
```

### connection errors
```
❌ Web3 connection error for ethereum:
   Error type: ConnectionError
   Error message: Connection refused
   🌐 CONNECTION ERROR - Network issue
```

## what to look for

when you run the monitor, check for:

1. **startup diagnostics** - shows if rpc connections work at all
2. **connection attempt logs** - shows each monitor trying to connect
3. **error type** - tells you exactly what failed (rate limit, timeout, auth, etc.)
4. **solutions** - suggests what to do

## common issues and fixes

### issue: rate limit detected (429)
**cause:** too many api requests to alchemy/infura

**fix:**
1. wait 5-10 minutes for rate limit to reset
2. use multiple api keys (already configured in your `api_keys.py`)
3. use free public rpcs (already have fallbacks configured)

### issue: timeout errors
**cause:** rpc endpoint too slow or unreachable

**fix:**
1. check internet connection
2. try different rpc provider
3. increase timeout (already set to 10 seconds)

### issue: unauthorized (403)
**cause:** api key invalid or missing

**fix:**
1. check `config/api_keys.py`
2. verify alchemy api key is correct: `fo8NbYyCOVp35sUKYle2JS9TdiJcUpes`
3. get new key from https://dashboard.alchemy.com

### issue: connection refused
**cause:** network/firewall blocking connection

**fix:**
1. check firewall settings
2. try different network
3. use vpn if blocked

## your current configuration

from `config/api_keys.py`:

**alchemy key:**
```python
ALCHEMY_API_KEY = "fo8NbYyCOVp35sUKYle2JS9TdiJcUpes"
```

**ethereum rpc providers (fallback chain):**
1. alchemy (330k requests/day free)
2. infura (100k requests/day free)
3. cloudflare (no api key needed)
4. publicnode (no api key needed)
5. ankr (backup)

**polygon rpc providers:**
1. alchemy
2. infura
3. publicnode
4. ankr

## if it's definitely rate limits

you have two options:

### option 1: wait it out
alchemy free tier: 330k compute units/day
- wait 1 hour for some quota to free up
- or wait until tomorrow for full reset

### option 2: use free public rpcs
edit `config/api_keys.py` and move public rpcs first:

```python
ETHEREUM_RPC_PROVIDERS = [
    "https://ethereum.publicnode.com",  # free, no api key
    "https://cloudflare-eth.com",       # free, no api key
    ALCHEMY_ETHEREUM_RPC,                # your alchemy key
    INFURA_ETHEREUM_RPC,                 # your infura key
]
```

## next steps

1. run `python enhanced_monitor.py`
2. watch for the diagnostic output
3. look for specific error messages
4. share the error output if you need help

the system will automatically fall back to etherscan api (which is working great for you already).


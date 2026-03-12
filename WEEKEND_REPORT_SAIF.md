# Weekend Engineering Report for Saif
## Whale Transaction Monitor - Status & Next Steps
**Date:** March 12, 2026  
**Prepared by:** Eduardo (Engineering)

---

## Current System Status: PRODUCTION READY (4/5 chains)

### Architecture
```
enhanced_monitor.py (main entry point)
    |
    |-- Ethereum:  Alchemy WebSocket (real-time, 108 ERC-20 tokens)
    |-- Polygon:   Alchemy WebSocket (real-time, 24 tokens + native MATIC)
    |-- Bitcoin:   Alchemy HTTPS polling (every 30s, block scanning)
    |-- XRP:       Ripple WebSocket (real-time, all transactions)
    |-- Solana:    Alchemy HTTPS block scanning (every 15s, 20 SPL tokens)
    |
    v
    Classification Engine -> Dedup Pipeline -> Per-chain Supabase Tables
                                                |
                                                |-- ethereum_transactions
                                                |-- bitcoin_transactions
                                                |-- solana_transactions
                                                |-- polygon_transactions
                                                |-- xrp_transactions
```

### 10-Minute Test Results (Latest)
| Chain | Transactions | BUY | SELL | TRANSFER | Directional Signal | Status |
|-------|-------------|-----|------|----------|-------------------|--------|
| **Ethereum** | 99 | 51% | 24% | 25% | **75%** | EXCELLENT |
| **Polygon** | 116 | 4% | 9% | 87% | 13% | NEEDS CLASSIFICATION WORK |
| **XRP** | 84 | 0% | 10% | 90% | 10% | NEEDS BUY DETECTION |
| **Bitcoin** | 9 | 22% | 0% | 78% | 22% | OK (limited by block time) |
| **Solana** | 6 | 0% | 0% | 100% | 0% | NEEDS YELLOWSTONE gRPC |
| **Total** | 314 | | | | **32%** | 31.4 txs/min |

### Thresholds
| Chain | Minimum Transaction USD | Reasoning |
|-------|------------------------|-----------|
| Ethereum | $20,000 | Captures whale ERC-20 swaps across 108 tokens |
| Bitcoin | $200,000 | Only mega-whale BTC movements |
| Polygon | $5,000 | Lower threshold because Polygon has smaller average tx size |
| Solana | $500 | DeFi swaps on Solana are typically smaller |
| XRP | $50,000 | Filters out noise, captures institutional flows |

---

## Weekend Tasks for Saif

### 1. CRITICAL: Solana Yellowstone gRPC Integration

**Why:** Current Solana monitoring only captures 6 transactions in 10 minutes. Yellowstone gRPC would give us real-time streaming of ALL Solana transactions, matching what we have for Ethereum and Polygon.

**What Alchemy provides:**
- Endpoint: `https://solana-mainnet.g.alchemy.com/`
- Auth: `X-Token: DwkUQYC8okkQxComPim-h`
- Protocol: gRPC with Yellowstone protobuf definitions

**Steps:**
1. Clone the proto files from [yellowstone-grpc repo](https://github.com/rpcpool/yellowstone-grpc/tree/master/yellowstone-grpc-proto/proto)
2. Compile protos for Python: `python -m grpc_tools.protoc -I./proto --python_out=. --grpc_python_out=. proto/geyser.proto proto/solana-storage.proto`
3. Create `chains/solana_grpc.py` that:
   - Connects to `https://solana-mainnet.g.alchemy.com/` with TLS
   - Sends `SubscribeRequest` with `transactions` filter:
     - `vote: false` (exclude validator votes)
     - `failed: false` (only successful txs)
     - `account_include: [list of 20 SPL token mint addresses]`
   - Processes `SubscribeUpdateTransaction` messages
   - Extracts `preTokenBalances`/`postTokenBalances` from `meta`
   - Matches against monitored token mints
   - Classifies and routes through `handle_event()`
4. Replace the HTTPS block scanner in `enhanced_monitor.py`

**Official Python example:** https://github.com/rpcpool/yellowstone-grpc/tree/master/examples/python

**Expected result:** Real-time Solana data comparable to Ethereum WS (hundreds of transactions per 10 minutes instead of 6)

---

### 2. HIGH: Classification Engine Improvements

**Problem:** Polygon is 87% TRANSFER, XRP is 90% TRANSFER. The classification only works well for Ethereum because it has the largest CEX/DEX address database.

**Fix for each chain:**

#### Polygon Classification
- **Current addresses:** 15 CEX + 11 DEX 
- **Need to add:** More bridge contracts (Polygon Bridge `0xA0c68C638235ee32657e8f720a23ceC1bFc77C77`, Hop Protocol, Stargate, Across)
- **Logic improvement:** USDC/USDT transfers TO/FROM known bridge addresses should be classified as BUY (bridging in) or SELL (bridging out)
- **File:** `chains/polygon_ws.py` → `_classify_polygon()` function

#### XRP Classification
- **Current issue:** 0% BUY detection. DestinationTag heuristic catches SELLs (42% in earlier test) but no mechanism for BUYs
- **Fix ideas:**
  - XRP `OfferCreate` transactions on the XRPL DEX are actual trades — classify based on `TakerPays`/`TakerGets` fields
  - If from_address matches exchange AND no DestinationTag → likely withdrawal → BUY
  - Track known OTC desk addresses (Ripple Labs ODL corridors)
- **File:** `chains/xrp.py` → classification block starting around line 70

#### Bitcoin Classification  
- **Current:** 22% directional (heuristic-based: input/output patterns)
- **Improvement:** Add more exchange addresses from BitInfoCharts (currently ~80, could be 200+)
- **File:** `chains/bitcoin_alchemy.py` → `BTC_EXCHANGE_ADDRESSES` dict

#### Solana Classification
- **Current:** 0% directional (all TRANSFER)
- **Fix:** Add Jupiter V6/V4, Raydium, Orca addresses to the classifier so DEX swaps get classified
- **File:** `chains/solana_api.py` → `_classify_solana_transfer()` function
- **Consider:** Parse Jupiter swap logs to determine if SOL was bought or sold

---

### 3. MEDIUM: Expand Exchange/DEX Address Databases

Sources for verified exchange addresses:
- **Bitcoin:** https://www.walletexplorer.com/ (clusters addresses by exchange)
- **Ethereum:** https://etherscan.io/labelcloud (labeled addresses)
- **XRP:** https://xrpscan.com/balances (top XRP holders with labels)
- **Polygon:** https://polygonscan.com/accounts (top POL holders with labels)
- **Solana:** https://solscan.io/account (labeled accounts)

Target: 200+ addresses per chain for CEX, 50+ for DEX

---

### 4. NEXT PHASE: Whale Wallet Tracking

After the classification engine is solid, the next value-add is tracking specific whale wallets:

**Approach:**
1. Use the `addresses` table in Supabase (already exists with `whale_score`, `signal_potential` fields)
2. Identify wallets that consistently make $100K+ BUY/SELL transactions
3. Label known entities: Wintermute, Jump Trading, Alameda (remnants), Galaxy Digital, Pantera, a16z, etc.
4. Build alerts when these specific wallets make moves
5. Create a "whale leaderboard" showing most active whale wallets per token

**Data already available:**
- Every transaction has `from_address` and `to_address`
- The classification engine already determines direction (BUY/SELL)
- The `whale_address` and `counterparty_address` fields exist in each per-chain table

---

### 5. LOW: Ethereum Token Coverage Question

**Issue:** We're subscribed to 108 ERC-20 tokens via WebSocket, getting 99 transactions in 10 minutes at $20K threshold. This is GOOD but could be more.

**Investigation needed:**
- Are there popular ERC-20 tokens NOT in our `TOP_100_ERC20_TOKENS` list? 
- Check against CoinGecko top 100 by market cap and top 100 by 24h volume
- Some high-volume tokens like TRUMP, AI tokens (TAO, NEAR wrapped), and new meme coins may be missing
- The stablecoin exclusion (USDT, USDC, DAI) removes the highest-volume tokens — consider tracking them on Ethereum where they carry genuine whale signal

---

## Config & Keys Reference

| Service | Key/URL |
|---------|---------|
| Alchemy API Key | `DwkUQYC8okkQxComPim-h` |
| Alchemy Plan | Pay As You Go, 10K CU/s, $0.45/M CU |
| Supabase URL | `https://fwbwfvqzomipoftgodof.supabase.co` |
| Supabase Project ID | `fwbwfvqzomipoftgodof` |
| CoinGecko API Key | `CG-nuPrNk1hMydULrhAGsZZJ2bD` (Pro plan) |
| Entry point | `python enhanced_monitor.py` |

## Files Modified This Week
- `enhanced_monitor.py` — Main entry point, Ethereum WS integration
- `chains/ethereum_ws.py` — **NEW** — Real-time Ethereum WebSocket monitor
- `chains/polygon_ws.py` — **NEW** — Real-time Polygon WebSocket monitor
- `chains/bitcoin_alchemy.py` — Heuristic classification, expanded addresses
- `chains/solana_api.py` — Block scanner with 150 slots/cycle
- `chains/xrp.py` — DestinationTag heuristic, Ripple treasury filter
- `utils/alchemy_rpc.py` — Rate limiter updated for Pay As You Go plan
- `utils/supabase_writer.py` — Per-chain table routing
- `utils/dedup.py` — Bitcoin dedup key fix, Polygon stablecoin passthrough
- `config/api_keys.py` — WebSocket URLs, Bitcoin hostname fix
- `config/settings.py` — Per-chain thresholds
- `data/tokens.py` — Expanded Polygon tokens, OXT removed
- `data/addresses.py` — Expanded XRP exchange addresses
- `models/classes.py` — CoinGecko Pro API batch pricing

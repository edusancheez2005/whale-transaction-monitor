"""
API Keys Configuration for Address Enrichment Service

This file contains API keys and configuration for external services.
In production, consider using environment variables instead of hardcoded values.

IMPORTANT: Never commit real API keys to version control.
          Replace these placeholder values with your actual API keys,
          or set them as environment variables.
"""

import os
import logging

# Set up a logger for this module
logger = logging.getLogger(__name__)

# Nansen API (https://nansen.ai)
# Used for wallet labeling and address enrichment
NANSEN_API_KEY = ""  # Replace with actual key or use env var

# Arkham Intelligence API (https://arkhamintelligence.com)
# Used for entity resolution and address labeling
ARKHAM_API_KEY = ""  # Replace with actual key or use env var

# Default admin key for maintenance endpoints
ADMIN_API_KEY = "dev_key"  # Replace with a secure key in production

# Existing API keys
WHALE_ALERT_API_KEY = os.getenv("WHALE_ALERT_API_KEY", "Kj7GlLsRpxoCBz1zZBINUUqBCgDFdHyV")

# Provider API Keys (for professional RPC endpoints)
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY", "")
INFURA_PROJECT_ID = os.getenv("INFURA_PROJECT_ID", "")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "MZVFRTZVZXQCK178Z5PXN3ZCN1MBN9GJWT")  # SONAR key
NEWS_API_KEY = "b7c1fdbffb8842f18a495bf8d32df7cf"
DUNE_API_KEY = "OMOjlnPKwqG1OiLvZ4bTcs3V6EYX9ymF"  # Updated Dune Analytics API key for whale discovery
BITQUERY_API_KEY = "ory_at_1z07_FgKYYRlTvrAaUzSxTdelBd--L-IyVTM3LGxbho.SCYADMgS1bCW_xNTI-wh_i049B8DjgpI5OeCjb3TXOo"
HELIUS_API_KEY = "0bd8a69e-4bd7-4557-b4c6-1240a2185b6b"

# CoinGecko API for price feeds (free tier)
COINGECKO_API_KEY = ""  # Optional - free tier works without key, pro tier for rate limits

# New API keys for address enrichment service integrations
COVALENT_API_KEY = os.getenv("COVALENT_API_KEY", "cqt_rQfrBY93cfvpfwYDMKXwYjTXJppC")  # Alternative working key
MORALIS_API_KEY = os.getenv("MORALIS_API_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJub25jZSI6ImYyZDdiMTI1LWU1NWEtNGI1Yi05MTU3LWE3MDVjZDdhNGViMyIsIm9yZ0lkIjoiNDQzMDI5IiwidXNlcklkIjoiNDU1ODE5IiwidHlwZUlkIjoiZWNiYmYxZTUtNDhjNS00MjcwLWFiODgtMDA3ZWYzMThkYzZkIiwidHlwZSI6IlBST0pFQ1QiLCJpYXQiOjE3NDUyMzQ5NTksImV4cCI6NDkwMDk5NDk1OX0.Ot3EhOWatv_PSYYBoo57OhoEHqhErYIwcIuU_SR-zoo")
QUICKNODE_API_KEY = "fbc432f872c1649d2ed5c1ccaa63fc4a4584ab6b"
THEGRAPH_API_KEY = "your_thegraph_api_key"  # Replace with actual key
SOLSCAN_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjcmVhdGVkQXQiOjE3NDUyNzAzODkyODMsImVtYWlsIjoiZWR1YXJkb3NhbmNoZXo0ODQ4QGdtYWlsLmNvbSIsImFjdGlvbiI6InRva2VuLWFwaSIsImFwaVZlcnNpb24iOiJ2MiIsImlhdCI6MTc0NTI3MDM4OX0.UN-p-bPdediy-wNtLKcrKQ1U1wwKmdrlZMWPgHgaLcU"
POLYGONSCAN_API_KEY = os.getenv("POLYGONSCAN_API_KEY", "67NMD5V4YSB972TDG26AEIH4TFUUF7DTCP")

# Alternative API Keys for Fallback (ONLY USER'S WORKING KEYS)
FALLBACK_API_KEYS = {
    "etherscan": [
        "UMMRGSP6BM6DZ3UZ2AWBFJ2M9X9BAPS47I",  # SONAR2 key (primary fallback)
    ],
    "covalent": [
        "cqt_rQfrBY93cfvpfwYDMKXwYjTXJppC",  # Alternative key
        "cqt_rQGBMmYG6Prt3C8DxrDPx7pmQQT9"   # Original key
    ],
    "moralis": [
        # Add alternative Moralis keys here when available
    ],
    "zerion": [
        "zk_dev_aaf4e06cb16a4d5caa46bb3d421b7098"  # From settings
    ]
}

# Add a check for the Moralis API key and log a warning if it's not set
if not MORALIS_API_KEY:
    logger.warning("MORALIS_API_KEY environment variable not set. Moralis functionality will be disabled.")

# Blockfrost API (Cardano)
BLOCKFROST_PROJECT_ID = "mainnetTPHK50nzvdXBP1nlyV3h2FtqoTjTAzMb"

# Blockstream API (Bitcoin, Liquid - Note: Public Esplora API usually doesn't require keys)
# These might be for a specific Blockstream service or commercial offering.
BLOCKSTREAM_SONAR_CLIENT_ID = "ffafe2ed-2ade-4a42-bdad-3a5628d38885"
BLOCKSTREAM_SONAR_CLIENT_SECRET = "olPDXrbyC1mmnDGeW7ndyJ7gIaDJ4ydD"

# Supabase Configuration
SUPABASE_URL = "https://fwbwfvqzomipoftgodof.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZ3YndmdnF6b21pcG9mdGdvZG9mIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDc5Mjc3MzMsImV4cCI6MjA2MzUwMzczM30.Fw0Ejr7yrMRjP1WFXjSnJxwNQUe8O_Dzhv96E1OvEl8"
SUPABASE_SERVICE_ROLE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZ3YndmdnF6b21pcG9mdGdvZG9mIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0NzkyNzczMywiZXhwIjoyMDYzNTAzNzMzfQ.L2e_VICxQ_aumt8KmvJaClwK4W2rQLA1QZ3EfvdVYXM"

# Google Cloud Configuration  
GOOGLE_APPLICATION_CREDENTIALS = "config/bigquery_credentials.json"  # Path to BigQuery service account JSON

# Direct ETL Provider URIs (for ethereum-etl and bitcoin-etl)
# These are loaded from environment variables for security
ETHEREUM_NODE_PROVIDER_URI = os.getenv('ETHEREUM_NODE_PROVIDER_URI', '')  # Infura, Alchemy, or local node
BITCOIN_NODE_PROVIDER_URI = os.getenv('BITCOIN_NODE_PROVIDER_URI', '')    # Local node or RPC endpoint

# RPC URLs for real-time transaction monitoring and decoding
# PROFESSIONAL MULTI-PROVIDER SETUP (Based on 2024/2025 research)

# Primary providers (highest reliability)
ALCHEMY_ETHEREUM_RPC = f"https://eth-mainnet.alchemyapi.io/v2/{ALCHEMY_API_KEY}" if ALCHEMY_API_KEY and ALCHEMY_API_KEY != "YourApiKeyToken" else None
INFURA_ETHEREUM_RPC = f"https://mainnet.infura.io/v3/{INFURA_PROJECT_ID}" if INFURA_PROJECT_ID and INFURA_PROJECT_ID != "YourProjectId" else None

# Secondary providers (free but reliable)
CLOUDFLARE_ETHEREUM_RPC = "https://cloudflare-eth.com"
ANKR_ETHEREUM_RPC = "https://rpc.ankr.com/eth"
PUBLICNODE_ETHEREUM_RPC = "https://ethereum.publicnode.com"

# Ordered list of RPC providers (primary to fallback)
ETHEREUM_RPC_PROVIDERS = [
    provider for provider in [
        ALCHEMY_ETHEREUM_RPC,      # Best reliability, 330k req/day free
        INFURA_ETHEREUM_RPC,       # High reliability, 100k req/day free  
        CLOUDFLARE_ETHEREUM_RPC,   # No API key needed, good uptime
        ANKR_ETHEREUM_RPC,         # No API key needed, 25 req/sec
        PUBLICNODE_ETHEREUM_RPC    # No API key needed, backup
    ] if provider is not None
]

# Primary RPC URL (for backward compatibility)
ETHEREUM_RPC_URL = ETHEREUM_RPC_PROVIDERS[0] if ETHEREUM_RPC_PROVIDERS else "https://ethereum.publicnode.com"

# Polygon setup (similar pattern)
ALCHEMY_POLYGON_RPC = f"https://polygon-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}" if ALCHEMY_API_KEY and ALCHEMY_API_KEY != "YourApiKeyToken" else None
INFURA_POLYGON_RPC = f"https://polygon-mainnet.infura.io/v3/{INFURA_PROJECT_ID}" if INFURA_PROJECT_ID and INFURA_PROJECT_ID != "YourProjectId" else None

POLYGON_RPC_PROVIDERS = [
    provider for provider in [
        ALCHEMY_POLYGON_RPC,
        INFURA_POLYGON_RPC,
        "https://polygon.publicnode.com",
        "https://rpc.ankr.com/polygon"
    ] if provider is not None
]

POLYGON_RPC_URL = POLYGON_RPC_PROVIDERS[0] if POLYGON_RPC_PROVIDERS else "https://polygon.publicnode.com"

# Construct URLs that depend on API keys
WHALE_WS_URL = f"wss://leviathan.whale-alert.io/ws?api_key={WHALE_ALERT_API_KEY}"
HELIUS_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
# IMPORTANT: The following is the CORRECT Helius WebSocket URL. The api.helius.xyz endpoint returns 404 errors
HELIUS_WS_URL = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
HELIUS_ENHANCED_API_BASE_URL = "https://api.helius.xyz/v0"  # Enhanced API endpoint
DUNE_API_BASE_URL = "https://api.dune.com/api/v1"
BITQUERY_API_BASE_URL = "https://graphql.bitquery.io"

# API base URLs
COVALENT_API_BASE_URL = "https://api.covalenthq.com/v1"
MORALIS_API_BASE_URL = "https://deep-index.moralis.io/api/v2"
QUICKNODE_ETHEREUM_RPC = "https://ethereum-mainnet.core.quiknode.pro"  # Generic endpoint
QUICKNODE_SOLANA_RPC = "https://warmhearted-dark-panorama.solana-mainnet.quiknode.pro/fbc432f872c1649d2ed5c1ccaa63fc4a4584ab6b/"  # Solana specific endpoint
THEGRAPH_API_BASE_URL = "https://gateway.thegraph.com/api"
SOLSCAN_API_BASE_URL = "https://pro-api.solscan.io/v2.0"  # Updated to v2 API
ETHERSCAN_API_BASE_URL = "https://api.etherscan.io/api"  # Explicitly defining this
POLYGONSCAN_API_BASE_URL = "https://api.polygonscan.com/api"
# BigQuery Configuration - Updated to match new credentials
BIGQUERY_PROJECT_ID = "peak-seat-465413-u9"  # Updated to match service account
BIGQUERY_DATASET = "crypto_intelligence"

# Add GCP_PROJECT_ID for backwards compatibility
GCP_PROJECT_ID = "peak-seat-465413-u9"

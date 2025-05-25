"""
API Keys Configuration for Address Enrichment Service

This file contains API keys and configuration for external services.
In production, consider using environment variables instead of hardcoded values.

IMPORTANT: Never commit real API keys to version control.
          Replace these placeholder values with your actual API keys,
          or set them as environment variables.
"""

# Nansen API (https://nansen.ai)
# Used for wallet labeling and address enrichment
NANSEN_API_KEY = ""  # Replace with actual key or use env var

# Arkham Intelligence API (https://arkhamintelligence.com)
# Used for entity resolution and address labeling
ARKHAM_API_KEY = ""  # Replace with actual key or use env var

# Default admin key for maintenance endpoints
ADMIN_API_KEY = "dev_key"  # Replace with a secure key in production

# Existing API keys
WHALE_ALERT_API_KEY = "Kj7GlLsRpxoCBz1zZBINUUqBCgDFdHyV"
ETHERSCAN_API_KEY = "QY23IJ4D4EJTGFQNSNJHAD4G1IUEQYUJTN"
NEWS_API_KEY = "b7c1fdbffb8842f18a495bf8d32df7cf"
DUNE_API_KEY = "BDG2PbmFAQIRLgfhZyXxrQ1P5t8LU8EK"
BITQUERY_API_KEY = "ory_at_1z07_FgKYYRlTvrAaUzSxTdelBd--L-IyVTM3LGxbho.SCYADMgS1bCW_xNTI-wh_i049B8DjgpI5OeCjb3TXOo"
HELIUS_API_KEY = "0bd8a69e-4bd7-4557-b4c6-1240a2185b6b"

# New API keys for address enrichment service integrations
COVALENT_API_KEY = "cqt_rQfrBY93cfvpfwYDMKXwYjTXJppC"  # Goldrush API key for Covalent
MORALIS_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJub25jZSI6ImYyZDdiMTI1LWU1NWEtNGI1Yi05MTU3LWE3MDVjZDdhNGViMyIsIm9yZ0lkIjoiNDQzMDI5IiwidXNlcklkIjoiNDU1ODE5IiwidHlwZUlkIjoiZWNiYmYxZTUtNDhjNS00MjcwLWFiODgtMDA3ZWYzMThkYzZkIiwidHlwZSI6IlBST0pFQ1QiLCJpYXQiOjE3NDUyMzQ5NTksImV4cCI6NDkwMDk5NDk1OX0.Ot3EhOWatv_PSYYBoo57OhoEHqhErYIwcIuU_SR-zoo"
QUICKNODE_API_KEY = "fbc432f872c1649d2ed5c1ccaa63fc4a4584ab6b"
THEGRAPH_API_KEY = "your_thegraph_api_key"  # Replace with actual key
SOLSCAN_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjcmVhdGVkQXQiOjE3NDUyNzAzODkyODMsImVtYWlsIjoiZWR1YXJkb3NhbmNoZXo0ODQ4QGdtYWlsLmNvbSIsImFjdGlvbiI6InRva2VuLWFwaSIsImFwaVZlcnNpb24iOiJ2MiIsImlhdCI6MTc0NTI3MDM4OX0.UN-p-bPdediy-wNtLKcrKQ1U1wwKmdrlZMWPgHgaLcU"
POLYGONSCAN_API_KEY = "67NMD5V4YSB972TDG26AEIH4TFUUF7DTCP"  # Polygonscan API key

# Supabase Configuration
SUPABASE_URL = "https://fwbwfvqzomipoftgodof.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZ3YndmdnF6b21pcG9mdGdvZG9mIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDc5Mjc3MzMsImV4cCI6MjA2MzUwMzczM30.Fw0Ejr7yrMRjP1WFXjSnJxwNQUe8O_Dzhv96E1OvEl8"
SUPABASE_SERVICE_ROLE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZ3YndmdnF6b21pcG9mdGdvZG9mIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0NzkyNzczMywiZXhwIjoyMDYzNTAzNzMzfQ.Ej8Ej7yrMRjP1WFXjSnJxwNQUe8O_Dzhv96E1OvEl8"

# Google Cloud Configuration
GOOGLE_APPLICATION_CREDENTIALS = "gcp-service-account-key.json"  # Path to BigQuery service account JSON

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
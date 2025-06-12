# Whale Transaction Monitor

A comprehensive multi-chain whale discovery and monitoring system that identifies and tracks high-value cryptocurrency addresses across multiple blockchains.

## Features

### 🐋 **Multi-Chain Whale Discovery**
- **Ethereum**: Etherscan Rich Lists, BigQuery public datasets, API integrations
- **Bitcoin**: BigQuery public datasets, high-value transaction analysis
- **Solana**: Flipside BigQuery datasets, Helius API integration
- **Polygon**: Polygonscan Rich Lists, BigQuery datasets
- **Avalanche**: BigQuery public datasets (when available)
- **Arbitrum**: BigQuery public datasets
- **Optimism**: BigQuery public datasets

### 📊 **Data Sources**
- **BigQuery Public Datasets**: Comprehensive blockchain transaction analysis
- **Dune Analytics**: Working verified queries for whale identification
- **GitHub Repositories**: Curated whale address lists from reputable sources
- **Rich List APIs**: Real-time top holder data from blockchain explorers
- **Custom APIs**: Multiple blockchain API integrations

### 🎯 **Advanced Discovery Methods**
- **New Whale Detection**: Find addresses not previously in database
- **Expanded Discovery**: Comprehensive search across ALL data sources
- **Pattern Analysis**: Analyze trends in newly discovered whales
- **Balance Enrichment**: Real-time balance verification and USD conversion

## Quick Start

### Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd whale-transaction-monitor-2
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure API keys** in `config/api_keys.py`:
   ```python
   API_KEYS = {
       'SUPABASE_URL': 'your_supabase_url',
       'SUPABASE_KEY': 'your_supabase_key',
       'ETHERSCAN_API_KEY': 'your_etherscan_key',
       'DUNE_API_KEY': 'your_dune_key',
       # ... other API keys
   }
   ```

### Basic Usage

#### Standard Whale Discovery
```bash
python whale_discovery_agent.py --chains ethereum polygon solana --min-balance 1000000 --verbose
```

#### Find NEW Whales (not in database)
```bash
python whale_discovery_agent.py --discover-new --since-date "2025-01-20" --chains ethereum polygon solana --min-balance 1000000 --analyze-patterns --verbose
```

#### Expanded Comprehensive Discovery
```bash
python whale_discovery_agent.py --expand-discovery --chains ethereum polygon solana avalanche arbitrum optimism --min-balance 1000000 --analyze-patterns --verbose
```

## Automated Daily Execution

The system includes automated daily whale discovery to continuously find new whales and update your database.

### Setting Up Daily Automation

1. **Make the script executable**:
   ```bash
   chmod +x run_daily_discovery.sh
   ```

2. **Test the daily script**:
   ```bash
   ./run_daily_discovery.sh
   ```

3. **Set up automated execution with cron**:
   ```bash
   crontab -e
   ```

4. **Add the following line to run daily at 2 AM**:
   ```cron
   # Run comprehensive whale discovery every day at 2 AM
   0 2 * * * /bin/bash /path/to/your/project/run_daily_discovery.sh >> /path/to/your/project/logs/cron.log 2>&1
   ```

   **Replace `/path/to/your/project/` with your actual project path.**

### Daily Script Features

The `run_daily_discovery.sh` script automatically:
- ✅ Activates virtual environment (if available)
- ✅ Checks Python dependencies
- ✅ Runs expanded whale discovery for new whales since yesterday
- ✅ Analyzes patterns in discovered whales
- ✅ Logs all output with timestamps
- ✅ Saves results to dated JSON files
- ✅ Cleans up old logs (keeps 30 days)
- ✅ Cleans up old output files (keeps 7 days)
- ✅ Provides optional webhook notifications

### Monitoring Daily Execution

- **View recent logs**: `tail -f logs/daily_discovery_$(date +%Y%m%d).log`
- **Check cron logs**: `tail -f logs/cron.log`
- **View daily results**: `ls -la output/daily_whales_*.json`

## Command Line Options

### Discovery Modes
- `--discover-new`: Find NEW whale addresses not in the database
- `--expand-discovery`: Run comprehensive discovery using ALL data sources
- No flags: Standard whale discovery

### Chain Selection
- `--chains ethereum polygon solana`: Specify target blockchains
- Supports: `ethereum`, `bitcoin`, `polygon`, `solana`, `avalanche`, `arbitrum`, `optimism`

### Balance Filtering
- `--min-balance 1000000`: Minimum USD balance (default: $1M)
- `--max-balance 50000000`: Maximum USD balance to avoid exchanges

### Analysis & Output
- `--analyze-patterns`: Analyze trends in discovered whales
- `--since-date "2025-01-20"`: Find whales since specific date
- `--output-file results.json`: Save results to file
- `--verbose`: Enable detailed logging
- `--dry-run`: Test without storing to database

## Database Schema

The system stores whale addresses in Supabase with comprehensive metadata:

```sql
-- Core whale address data
address VARCHAR(255) NOT NULL
blockchain VARCHAR(50) NOT NULL  
balance_usd DECIMAL(20,2)
balance_native DECIMAL(30,10)
source VARCHAR(255)
confidence DECIMAL(3,2)
discovery_method VARCHAR(100)
analysis_tags JSONB
-- ... additional fields
```

## Data Sources Explained

### BigQuery Public Datasets
- **Ethereum**: `bigquery-public-data.crypto_ethereum`
- **Bitcoin**: `bigquery-public-data.crypto_bitcoin`
- **Solana**: `flipside.crypto_solana.transactions`
- **Polygon**: `bigquery-public-data.crypto_polygon`
- **Layer 2s**: Arbitrum, Optimism, Avalanche datasets

### Dune Analytics (Working Queries)
- **Top ETH Holders** (Query ID: 2857442)
- **High-value ETH Transfers** (Query ID: 3055014)
- **MATIC Top Holders** (Query ID: 3260658)
- **Bridged BTC Holders** (Query ID: 3424379)

### GitHub Repositories
- **whale-watching**: Ethereum whale activity tracking
- **defillama-server**: DeFi protocol and whale metrics
- **arkham-token-lists**: Tagged whale and exchange addresses
- **explorer-labels**: Labeled address datasets
- **ethereum_whale_watcher**: Python whale tracking scripts
- **More repositories**: See `utils/github_data_extractor.py`

## Architecture

```
whale_discovery_agent.py          # Main orchestrator
├── utils/
│   ├── api_integrations.py        # Dune, Etherscan, APIs (3,053 lines)
│   ├── bigquery_public_data_extractor.py  # BigQuery datasets
│   ├── github_data_extractor.py   # GitHub repository parsing
│   └── dedup.py                   # Deduplication logic
├── config/
│   └── api_keys.py                # API configuration
└── run_daily_discovery.sh         # Automated execution
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add new data sources or improve existing ones
4. Test thoroughly with `--dry-run` mode
5. Submit a pull request

## License

MIT License - see LICENSE file for details.

---

**🚀 Ready to discover whales?** Start with:
```bash
python whale_discovery_agent.py --expand-discovery --chains ethereum polygon solana --min-balance 1000000 --verbose
```
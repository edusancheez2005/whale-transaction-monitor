#!/bin/bash

# Whale Discovery Daily Automation Script
# This script runs the comprehensive whale discovery system daily
# to find new whales and update the database

# Set error handling
set -e

# Get the directory of the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Set up logging
LOG_FILE="$DIR/logs/daily_discovery_$(date +%Y%m%d).log"
mkdir -p "$DIR/logs"

# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "🚀 Starting daily whale discovery automation..."

# Activate virtual environment if it exists
if [ -f "$DIR/venv/bin/activate" ]; then
    log "📦 Activating virtual environment..."
    source "$DIR/venv/bin/activate"
elif [ -f "$DIR/.venv/bin/activate" ]; then
    log "📦 Activating virtual environment..."
    source "$DIR/.venv/bin/activate"
else
    log "⚠️  No virtual environment found, using system Python"
fi

# Check if Python dependencies are available
log "🔍 Checking Python dependencies..."
python -c "import supabase, google.cloud.bigquery, pandas" 2>/dev/null || {
    log "❌ Required Python dependencies not found. Please install requirements.txt"
    exit 1
}

# Run comprehensive whale discovery
log "🐋 Running EXPANDED whale discovery..."

# Get yesterday's date for discovering new whales
YESTERDAY=$(date -d '1 day ago' '+%Y-%m-%d')

# Execute the whale discovery with comprehensive settings
python "$DIR/whale_discovery_agent.py" \
    --expand-discovery \
    --discover-new \
    --since-date "$YESTERDAY" \
    --chains ethereum polygon solana avalanche arbitrum optimism \
    --min-balance 1000000 \
    --analyze-patterns \
    --verbose \
    --output-file "$DIR/output/daily_whales_$(date +%Y%m%d).json" \
    2>&1 | tee -a "$LOG_FILE"

# Check exit status
if [ $? -eq 0 ]; then
    log "✅ Daily whale discovery completed successfully"
    
    # Optional: Send success notification (uncomment and configure as needed)
    # curl -X POST "YOUR_WEBHOOK_URL" -H "Content-Type: application/json" \
    #     -d '{"text":"🐋 Daily whale discovery completed successfully"}'
    
else
    log "❌ Daily whale discovery failed with exit code $?"
    
    # Optional: Send failure notification (uncomment and configure as needed)
    # curl -X POST "YOUR_WEBHOOK_URL" -H "Content-Type: application/json" \
    #     -d '{"text":"❌ Daily whale discovery failed. Check logs for details."}'
    
    exit 1
fi

# Clean up old logs (keep last 30 days)
log "🧹 Cleaning up old log files..."
find "$DIR/logs" -name "daily_discovery_*.log" -mtime +30 -delete 2>/dev/null || true

# Clean up old output files (keep last 7 days)
log "🧹 Cleaning up old output files..."
find "$DIR/output" -name "daily_whales_*.json" -mtime +7 -delete 2>/dev/null || true

log "🎉 Daily whale discovery automation completed!" 
#!/bin/bash

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -r enrichment_service/requirements.txt
pip install -r rule_engine/requirements.txt 
pip install -r training_pipeline/requirements.txt

# Create necessary directories
mkdir -p data output

# Start services in the background
echo "Starting Address Enrichment Service..."
python -m enrichment_service.api.main &
ENRICHMENT_PID=$!

echo "Starting Rule Engine..."
python -m rule_engine.api.main &
RULE_ENGINE_PID=$!

echo "Services started!"
echo "Address Enrichment Service is running on http://localhost:8000"
echo "Rule Engine is running on http://localhost:8001"
echo
echo "To generate training data, run:"
echo "python -m training_pipeline.main --days 180 --chains ethereum solana polygon xrp --min-value 500000"
echo
echo "Press Ctrl+C to stop all services"

# Wait for user interrupt
trap "kill $ENRICHMENT_PID $RULE_ENGINE_PID; exit" INT
wait 
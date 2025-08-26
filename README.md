# Whale Transaction Monitor - Enhanced Classification System

This enhanced system provides an advanced approach to classifying cryptocurrency transactions. It uses address enrichment from multiple sources, rule-based classification, and supports training data generation for machine learning models.

## Features

- **Address Enrichment**: Fetches and caches address metadata from Nansen, Arkham, Chainalysis, and block explorers
- **Rule-Based Classification**: Classifies transactions based on address entity types and transaction patterns
- **Training Data Generation**: Creates labeled datasets for training ML models
- **Cross-Chain Support**: Works with Ethereum, Solana, XRP, Polygon and other chains
- **Caching**: Redis-based caching for performance optimization
- **Confidence Scoring**: Provides confidence scores and explanations for classifications

## Architecture

The system consists of three main components:

1. **Address Enrichment**: Identifies the entity type of addresses involved in transactions
2. **Rule-Based Classification**: Applies intelligent rules to classify obvious cases with high confidence
3. **Training Data Generation**: Creates high-quality labeled datasets for ML model training

## Requirements

- Python 3.8+
- Redis (for caching address metadata)
- API keys for metadata sources (Nansen, Arkham, etc.)

## Installation

```bash
# Clone the repository
git clone https://github.com/your-username/whale-transaction-monitor.git
cd whale-transaction-monitor

# Install dependencies
pip install -r requirements.txt

# Set environment variables for API keys
export NANSEN_API_KEY="your_nansen_api_key"
export ARKHAM_API_KEY="your_arkham_api_key"
export CHAINALYSIS_API_KEY="your_chainalysis_api_key" 
export REDIS_URL="redis://localhost:6379/0"
```

## Usage

### Direct Classification

```python
import asyncio
from transaction_classifier import TransactionClassifier

async def classify_example():
    # Initialize the classifier
    classifier = TransactionClassifier()
    
    # Classify a transaction
    result = await classifier.classify_transaction(
        tx_hash="0x123456789abcdef",
        from_address="0x28c6c06298d514db089934071355e5743bf21d60",
        to_address="0x3f5CE5FBFe3E9af3971dD833D26bA9b5C936f0bE",
        chain="ethereum",
        token="ETH",
        amount=2.5,
        usd_value=4500.0
    )
    
    # Get summary
    summary = classifier.generate_classification_summary(result)
    print(f"Classification: {result.classification}")
    print(f"Confidence: {result.confidence} ({result.confidence_level})")
    print(f"Explanation: {result.explanation}")
    
    # Clean up
    await classifier.close()

# Run the example
asyncio.run(classify_example())
```

### Batch Classification

```python
import asyncio
from transaction_classifier import TransactionClassifier

async def classify_batch():
    # Initialize the classifier
    classifier = TransactionClassifier()
    
    # Prepare batch of transactions
    transactions = [
        {
            "tx_hash": "0x123456789abcdef1",
            "from_address": "0x28c6c06298d514db089934071355e5743bf21d60",
            "to_address": "0x3f5CE5FBFe3E9af3971dD833D26bA9b5C936f0bE",
            "chain": "ethereum",
            "token": "ETH",
            "amount": 2.5,
            "usd_value": 4500.0
        },
        {
            "tx_hash": "0x123456789abcdef2",
            "from_address": "0x3f5CE5FBFe3E9af3971dD833D26bA9b5C936f0bE",
            "to_address": "0x28c6c06298d514db089934071355e5743bf21d60",
            "chain": "ethereum",
            "token": "ETH",
            "amount": 1.5,
            "usd_value": 2700.0
        }
    ]
    
    # Classify batch
    results = await classifier.classify_transactions(transactions)
    
    # Process results
    for result in results:
        print(f"Transaction {result.transaction.tx_hash}: {result.classification} ({result.confidence:.2f})")
    
    # Clean up
    await classifier.close()

# Run the example
asyncio.run(classify_batch())
```

### Generating Training Data

```python
import asyncio
from training_data_generator import TrainingDataGenerator

async def generate_training_data():
    # Initialize the generator
    generator = TrainingDataGenerator()
    
    # Input files (JSON or CSV with transaction data)
    input_files = ["data/transactions.json", "data/whale_alert.csv"]
    
    # Generate training data
    result = await generator.generate_training_data(
        input_files=input_files,
        min_confidence=0.7,  # Only include high-confidence labels
        target_per_class=100  # Balance classes to have 100 examples each
    )
    
    # Show statistics
    print("Training data statistics:")
    for split_name, stats in result["statistics"].items():
        print(f"\n{split_name.upper()} Set:")
        print(f"Total transactions: {stats['total_transactions']}")
        print(f"Label distribution: {stats['label_distribution']}")
    
    # Clean up
    await generator.close()

# Run the example
asyncio.run(generate_training_data())
```

## API Configuration

Configure API keys via environment variables:

- `NANSEN_API_KEY`: Nansen API key
- `NANSEN_API_URL`: Nansen API URL (default: https://api.nansen.ai/v1)
- `ARKHAM_API_KEY`: Arkham Intelligence API key
- `ARKHAM_API_URL`: Arkham API URL (default: https://api.arkhamintelligence.com/v1)
- `CHAINALYSIS_API_KEY`: Chainalysis API key
- `CHAINALYSIS_API_URL`: Chainalysis API URL (default: https://api.chainalysis.com/kyt/v1)
- `REDIS_URL`: Redis connection URL (default: redis://localhost:6379/0)
- `CACHE_TTL`: Cache TTL in seconds (default: 86400, i.e., 24 hours)

## File Structure

```
whale-transaction-monitor/
├── address_enrichment.py    # Address enrichment service
├── rule_engine.py           # Rule-based classification engine
├── transaction_classifier.py # Unified transaction classifier
├── training_data_generator.py # Training data generation
├── requirements.txt         # Project dependencies
└── README.md                # This file
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request
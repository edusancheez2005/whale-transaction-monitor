"""
Rule Engine API Module

This module provides a FastAPI application for the rule-based transaction classification engine.
"""
import logging
import httpx
import json
import os
import sys

# Add parent directory to path to make imports work when run as a script
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import time
from typing import Optional, Dict, Any, List

from rule_engine.models.transaction import TransactionRequest, ClassificationResult, AddressMetadata
from rule_engine.rules.base import RuleEngine
from rule_engine.rules.common_rules import (
    ExchangeDepositRule,
    ExchangeWithdrawalRule,
    DexSwapRule,
    BridgeTransactionRule,
    MarketMakerTransferRule
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Transaction Classification Engine",
    description="API for classifying crypto transactions using enhanced rule-based algorithms",
    version="1.0.0"
)

# Initialize the rule engine
rule_engine = RuleEngine()

# Register rules
rule_engine.register_rules([
    ExchangeDepositRule(),
    ExchangeWithdrawalRule(),
    DexSwapRule(),
    BridgeTransactionRule(),
    MarketMakerTransferRule()
])

# Environment variables
ENRICHMENT_SERVICE_URL = os.getenv("ENRICHMENT_SERVICE_URL", "http://localhost:8000")

@app.post("/classify-transaction", response_model=ClassificationResult)
async def classify_transaction(transaction: TransactionRequest):
    """
    Classify a crypto transaction using the enhanced rule engine
    
    This endpoint:
    1. Enriches the transaction addresses with metadata
    2. Applies classification rules to determine the transaction type
    3. Returns the classification with confidence and explanation
    
    Args:
        transaction: The transaction to classify
        
    Returns:
        ClassificationResult: The classification result
    """
    start_time = time.time()
    
    try:
        # If address metadata is not provided, try to enrich it
        if not transaction.from_address_metadata or not transaction.to_address_metadata:
            transaction = await enrich_transaction_addresses(transaction)
        
        # Classify the transaction using the rule engine
        result = rule_engine.classify(transaction)
        
        # Calculate total processing time
        processing_time_ms = (time.time() - start_time) * 1000
        result.rule_processing_time_ms = processing_time_ms
        
        logger.info(f"Classified transaction in {processing_time_ms:.2f}ms: {result.classification}")
        
        return result
    
    except Exception as e:
        logger.error(f"Error classifying transaction: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def enrich_transaction_addresses(transaction: TransactionRequest) -> TransactionRequest:
    """
    Enrich the transaction addresses with metadata from the enrichment service
    
    Args:
        transaction: The transaction to enrich
        
    Returns:
        TransactionRequest: The enriched transaction
    """
    async with httpx.AsyncClient() as client:
        # Enrich the from address
        try:
            response = await client.post(
                f"{ENRICHMENT_SERVICE_URL}/enrich-address",
                json={
                    "address": transaction.from_address,
                    "chain": transaction.chain.value
                },
                timeout=5.0
            )
            
            if response.status_code == 200:
                data = response.json()
                transaction.from_address_metadata = AddressMetadata(
                    address=data["address"],
                    label=data["primary_label"],
                    entity_type=data["primary_label"],
                    confidence=data["confidence"]
                )
                logger.info(f"Enriched from address {transaction.from_address} as {data['primary_label']}")
        except Exception as e:
            logger.error(f"Error enriching from address: {e}")
        
        # Enrich the to address
        try:
            response = await client.post(
                f"{ENRICHMENT_SERVICE_URL}/enrich-address",
                json={
                    "address": transaction.to_address,
                    "chain": transaction.chain.value
                },
                timeout=5.0
            )
            
            if response.status_code == 200:
                data = response.json()
                transaction.to_address_metadata = AddressMetadata(
                    address=data["address"],
                    label=data["primary_label"],
                    entity_type=data["primary_label"],
                    confidence=data["confidence"]
                )
                logger.info(f"Enriched to address {transaction.to_address} as {data['primary_label']}")
        except Exception as e:
            logger.error(f"Error enriching to address: {e}")
    
    return transaction


@app.get("/health")
async def health_check():
    """Health check endpoint to verify the service is running"""
    return {"status": "healthy", "rules_loaded": len(rule_engine.rules)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("rule_engine.api.main:app", host="0.0.0.0", port=8001, reload=True) 
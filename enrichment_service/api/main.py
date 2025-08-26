"""
Rule Engine API Module

This module provides a FastAPI application for the rule-based transaction classification engine.
"""
import logging
import os
import sys

# Add parent directory to path to make imports work when run as a script
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import time
from typing import Optional, Dict, Any, List

from enrichment_service.services.enrichment_service import AddressEnrichmentService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Address Enrichment Service",
    description="API for enriching blockchain addresses with metadata from various sources",
    version="1.0.0"
)

# Initialize the enrichment service
enrichment_service = AddressEnrichmentService(
    redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0")
)

@app.post("/enrich-address")
async def enrich_address(request: Dict[str, Any]):
    """
    Enrich a blockchain address with metadata from various sources
    
    Args:
        request: Dictionary containing address and chain information
    
    Returns:
        Dict: Enriched address data
    """
    try:
        address = request["address"]
        chain = request["chain"]
        force_refresh = request.get("force_refresh", False)
        
        result = await enrichment_service.get_address_enrichment(
            address=address,
            chain=chain,
            force_refresh=force_refresh
        )
        
        return result.dict()
    
    except Exception as e:
        logger.error(f"Error enriching address: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint to verify the service is running"""
    return {"status": "healthy", "service": "address_enrichment"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("enrichment_service.api.main:app", host="0.0.0.0", port=8000, reload=True) 
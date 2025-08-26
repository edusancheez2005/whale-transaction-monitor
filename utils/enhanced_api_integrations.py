"""
Enhanced API Integrations - Professional Google-level Implementation

This module provides a unified interface for all external API integrations with:
- Proper error handling and rate limiting
- Structured response handling
- Comprehensive logging
- Production-ready reliability
"""

import logging
import time
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import requests
from ratelimit import limits, sleep_and_retry
import base64
import json

from config.settings import API_CONFIG
from config.api_keys import COVALENT_API_KEY, DUNE_API_KEY, MORALIS_API_KEY
from .base_helpers import log_error
from .zerion_enricher import ZerionEnricher

logger = logging.getLogger(__name__)


@dataclass
class APIResponse:
    """Standardized API response structure."""
    success: bool
    data: Any
    error_message: Optional[str] = None
    status_code: Optional[int] = None
    response_time_ms: Optional[float] = None


class EnhancedAPIIntegrations:
    """Professional API integration layer with comprehensive error handling."""
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
        # Initialize API clients
        self.covalent_session = self._setup_covalent_session()
        self.dune_session = self._setup_dune_session()
        self.moralis_session = self._setup_moralis_session()
        self.whale_alert_session = self._setup_whale_alert_session()
        
        # Initialize Zerion enricher
        self.zerion_enricher = ZerionEnricher(API_CONFIG["zerion"]["api_key"])
        
        # API status tracking
        self.api_status = {
            "covalent": {"healthy": True, "last_success": None, "error_count": 0},
            "dune": {"healthy": True, "last_success": None, "error_count": 0},
            "moralis": {"healthy": True, "last_success": None, "error_count": 0},
            "whale_alert": {"healthy": True, "last_success": None, "error_count": 0},
            "zerion": {"healthy": True, "last_success": None, "error_count": 0}
        }
    
    def _setup_covalent_session(self) -> requests.Session:
        """Setup Covalent API session with Basic Auth."""
        session = requests.Session()
        
        # Basic Auth: base64 encode "api_key:"
        auth_string = f"{COVALENT_API_KEY}:"
        encoded_auth = base64.b64encode(auth_string.encode()).decode()
        
        session.headers.update({
            'Authorization': f'Basic {encoded_auth}',
            'Accept': 'application/json',
            'User-Agent': 'WhaleTransactionMonitor/1.0'
        })
        return session
    
    def _setup_dune_session(self) -> requests.Session:
        """Setup Dune Analytics v2 API session."""
        session = requests.Session()
        session.headers.update({
            'X-DUNE-API-KEY': DUNE_API_KEY,
            'Content-Type': 'application/json',
            'User-Agent': 'WhaleTransactionMonitor/1.0'
        })
        return session
    
    def _setup_moralis_session(self) -> requests.Session:
        """Setup Moralis Streams API session."""
        session = requests.Session()
        session.headers.update({
            'X-API-Key': MORALIS_API_KEY,
            'Accept': 'application/json',
            'User-Agent': 'WhaleTransactionMonitor/1.0'
        })
        return session
    
    def _setup_whale_alert_session(self) -> requests.Session:
        """Setup WhaleAlert API session."""
        session = requests.Session()
        session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'WhaleTransactionMonitor/1.0'
        })
        return session
    
    @sleep_and_retry
    @limits(calls=5, period=1)
    def get_covalent_portfolio(self, address: str, chain_name: str = "eth-mainnet") -> APIResponse:
        """Get portfolio data from Covalent v3 free tier."""
        start_time = time.time()
        
        try:
            # Use free tier balances endpoint
            url = f"{API_CONFIG['covalent']['base_url']}/{chain_name}/address/{address}/balances_v2/"
            
            params = {
                'nft': 'false',  # Skip NFTs for faster response
                'no-nft-fetch': 'true'
            }
            
            response = self.covalent_session.get(url, params=params, timeout=30)
            response_time = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                data = response.json()
                self._update_api_status("covalent", success=True)
                self.logger.info(f"Covalent portfolio fetch successful for {address}")
                
                return APIResponse(
                    success=True,
                    data=data,
                    status_code=response.status_code,
                    response_time_ms=response_time
                )
            else:
                error_msg = f"Covalent API error {response.status_code}: {response.text}"
                self._update_api_status("covalent", success=False, error=error_msg)
                log_error(error_msg)
                
                return APIResponse(
                    success=False,
                    data=None,
                    error_message=error_msg,
                    status_code=response.status_code,
                    response_time_ms=response_time
                )
                
        except Exception as e:
            error_msg = f"Covalent request failed for {address}: {e}"
            self._update_api_status("covalent", success=False, error=error_msg)
            log_error(error_msg)
            
            return APIResponse(
                success=False,
                data=None,
                error_message=error_msg,
                response_time_ms=(time.time() - start_time) * 1000
            )
    
    @sleep_and_retry
    @limits(calls=20, period=60)
    def execute_dune_query(self, query_id: str, parameters: Dict[str, Any] = None) -> APIResponse:
        """Execute Dune Analytics v2 query."""
        start_time = time.time()
        
        try:
            # Execute query
            execute_url = f"{API_CONFIG['dune']['base_url']}/query/{query_id}/execute"
            
            payload = {"parameters": parameters or {}}
            
            response = self.dune_session.post(execute_url, json=payload, timeout=30)
            response_time = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                execution_data = response.json()
                execution_id = execution_data.get('execution_id')
                
                if execution_id:
                    # Poll for results
                    results = self._poll_dune_results(execution_id)
                    self._update_api_status("dune", success=True)
                    
                    return APIResponse(
                        success=True,
                        data=results,
                        status_code=response.status_code,
                        response_time_ms=response_time
                    )
                else:
                    error_msg = "Dune query execution failed - no execution ID"
                    self._update_api_status("dune", success=False, error=error_msg)
                    log_error(error_msg)
                    
                    return APIResponse(
                        success=False,
                        data=None,
                        error_message=error_msg,
                        status_code=response.status_code
                    )
            else:
                error_msg = f"Dune API error {response.status_code}: {response.text}"
                self._update_api_status("dune", success=False, error=error_msg)
                log_error(error_msg)
                
                return APIResponse(
                    success=False,
                    data=None,
                    error_message=error_msg,
                    status_code=response.status_code,
                    response_time_ms=response_time
                )
                
        except Exception as e:
            error_msg = f"Dune query execution failed: {e}"
            self._update_api_status("dune", success=False, error=error_msg)
            log_error(error_msg)
            
            return APIResponse(
                success=False,
                data=None,
                error_message=error_msg,
                response_time_ms=(time.time() - start_time) * 1000
            )
    
    def _poll_dune_results(self, execution_id: str, max_wait_seconds: int = 60) -> Optional[Dict]:
        """Poll Dune Analytics for query results."""
        start_time = time.time()
        
        while time.time() - start_time < max_wait_seconds:
            try:
                results_url = f"{API_CONFIG['dune']['base_url']}/execution/{execution_id}/results"
                response = self.dune_session.get(results_url, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get('state') == 'QUERY_STATE_COMPLETED':
                        return data
                    elif data.get('state') in ['QUERY_STATE_FAILED', 'QUERY_STATE_CANCELLED']:
                        self.logger.error(f"Dune query failed: {data}")
                        return None
                
                time.sleep(2)  # Wait 2 seconds before next poll
                
            except Exception as e:
                self.logger.error(f"Error polling Dune results: {e}")
                return None
        
        self.logger.warning(f"Dune query timeout after {max_wait_seconds} seconds")
        return None
    
    @sleep_and_retry
    @limits(calls=25, period=1)
    def get_moralis_wallet_history(self, address: str, chain: str = "eth") -> APIResponse:
        """Get wallet transaction history from Moralis Streams API."""
        start_time = time.time()
        
        try:
            url = f"{API_CONFIG['moralis']['base_url']}/{address}/history"
            
            params = {
                'chain': chain,
                'limit': 100,
                'order': 'desc'
            }
            
            response = self.moralis_session.get(url, params=params, timeout=30)
            response_time = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                data = response.json()
                self._update_api_status("moralis", success=True)
                self.logger.info(f"Moralis wallet history successful for {address}")
                
                return APIResponse(
                    success=True,
                    data=data,
                    status_code=response.status_code,
                    response_time_ms=response_time
                )
            else:
                error_msg = f"Moralis API error {response.status_code}: {response.text}"
                self._update_api_status("moralis", success=False, error=error_msg)
                log_error(error_msg)
                
                return APIResponse(
                    success=False,
                    data=None,
                    error_message=error_msg,
                    status_code=response.status_code,
                    response_time_ms=response_time
                )
                
        except Exception as e:
            error_msg = f"Moralis request failed for {address}: {e}"
            self._update_api_status("moralis", success=False, error=error_msg)
            log_error(error_msg)
            
            return APIResponse(
                success=False,
                data=None,
                error_message=error_msg,
                response_time_ms=(time.time() - start_time) * 1000
            )
    
    @sleep_and_retry
    @limits(calls=10, period=60)
    def get_whale_alert_transactions(self, start_time: int = None) -> APIResponse:
        """Get whale transactions from WhaleAlert API with proper parameters."""
        api_start_time = time.time()
        
        try:
            url = f"{API_CONFIG['whale_alert']['base_url']}/transactions"
            
            # Use proper parameters
            params = {
                'api_key': 'demo-api-key',  # Use demo key for testing
                'min_value': API_CONFIG['whale_alert']['default_params']['min_value'],
                'limit': API_CONFIG['whale_alert']['default_params']['limit'],
                'currency': API_CONFIG['whale_alert']['default_params']['currency']
            }
            
            if start_time:
                params['start'] = start_time
            
            response = self.whale_alert_session.get(url, params=params, timeout=30)
            response_time = (time.time() - api_start_time) * 1000
            
            if response.status_code == 200:
                data = response.json()
                self._update_api_status("whale_alert", success=True)
                self.logger.info("WhaleAlert transactions fetch successful")
                
                return APIResponse(
                    success=True,
                    data=data,
                    status_code=response.status_code,
                    response_time_ms=response_time
                )
            else:
                error_msg = f"WhaleAlert API error {response.status_code}: {response.text}"
                self._update_api_status("whale_alert", success=False, error=error_msg)
                log_error(error_msg)
                
                return APIResponse(
                    success=False,
                    data=None,
                    error_message=error_msg,
                    status_code=response.status_code,
                    response_time_ms=response_time
                )
                
        except Exception as e:
            error_msg = f"WhaleAlert request failed: {e}"
            self._update_api_status("whale_alert", success=False, error=error_msg)
            log_error(error_msg)
            
            return APIResponse(
                success=False,
                data=None,
                error_message=error_msg,
                response_time_ms=(time.time() - api_start_time) * 1000
            )
    
    @sleep_and_retry
    @limits(calls=10, period=60)
    def get_zerion_portfolio(self, address: str) -> APIResponse:
        """Get portfolio data from Zerion API using existing enricher."""
        start_time = time.time()
        
        try:
            # Use existing zerion enricher with correct method name
            portfolio_data = self.zerion_enricher.get_wallet_portfolio(address)
            response_time = (time.time() - start_time) * 1000
            
            if portfolio_data:
                self._update_api_status("zerion", success=True)
                self.logger.info(f"Zerion portfolio fetch successful for {address}")
                
                return APIResponse(
                    success=True,
                    data=portfolio_data.to_dict() if hasattr(portfolio_data, 'to_dict') else portfolio_data,
                    response_time_ms=response_time
                )
            else:
                error_msg = "Zerion portfolio data not available"
                self._update_api_status("zerion", success=False, error=error_msg)
                
                return APIResponse(
                    success=False,
                    data=None,
                    error_message=error_msg,
                    response_time_ms=response_time
                )
                
        except Exception as e:
            error_msg = f"Zerion portfolio request failed for {address}: {e}"
            self._update_api_status("zerion", success=False, error=error_msg)
            log_error(error_msg)
            
            return APIResponse(
                success=False,
                data=None,
                error_message=error_msg,
                response_time_ms=(time.time() - start_time) * 1000
            )
    
    @sleep_and_retry
    @limits(calls=10, period=60)
    def fetch_zerion_transaction_details(self, address: str, tx_hash: str) -> APIResponse:
        """
        Fetch transaction details from Zerion API for a specific transaction hash.
        
        Args:
            address: Wallet address to query
            tx_hash: Specific transaction hash to find
            
        Returns:
            APIResponse with transaction details including sends/receives arrays
        """
        start_time = time.time()
        
        try:
            # Setup Basic Auth for Zerion API
            api_key = API_CONFIG["zerion"]["api_key"]
            auth_string = f"{api_key}:"
            encoded_auth = base64.b64encode(auth_string.encode()).decode()
            
            headers = {
                "Authorization": f"Basic {encoded_auth}",
                "Content-Type": "application/json",
                "User-Agent": "WhaleTransactionMonitor/1.0"
            }
            
            # Query transactions endpoint with proper parameters
            url = f"{API_CONFIG['zerion']['base_url']}/wallets/{address}/transactions/"
            params = {
                "currency": "usd",
                "page[size]": "50"  # Get recent 50 transactions
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response_time = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                data = response.json()
                
                # Search for the specific transaction hash in the results
                transactions = data.get('data', [])
                matching_transaction = None
                
                for tx in transactions:
                    tx_attributes = tx.get('attributes', {})
                    if tx_attributes.get('hash', '').lower() == tx_hash.lower():
                        matching_transaction = tx_attributes
                        break
                
                if matching_transaction:
                    # Extract sends and receives for analysis
                    sends = matching_transaction.get('sends', [])
                    receives = matching_transaction.get('receives', [])
                    
                    # Structure the response for classification analysis
                    transaction_data = {
                        'hash': tx_hash,
                        'sends': sends,
                        'receives': receives,
                        'transaction_type': matching_transaction.get('type'),
                        'status': matching_transaction.get('status'),
                        'fee': matching_transaction.get('fee'),
                        'mined_at': matching_transaction.get('mined_at')
                    }
                    
                    self._update_api_status("zerion", success=True)
                    self.logger.info(f"Zerion transaction details found for {tx_hash}")
                    
                    return APIResponse(
                        success=True,
                        data=transaction_data,
                        status_code=response.status_code,
                        response_time_ms=response_time
                    )
                else:
                    # Transaction not found in recent history
                    error_msg = f"Transaction {tx_hash} not found in recent Zerion data"
                    self.logger.warning(error_msg)
                    
                    return APIResponse(
                        success=False,
                        data=None,
                        error_message=error_msg,
                        status_code=404,
                        response_time_ms=response_time
                    )
                    
            else:
                error_msg = f"Zerion transactions API error {response.status_code}: {response.text}"
                self._update_api_status("zerion", success=False, error=error_msg)
                log_error(error_msg)
                
                return APIResponse(
                    success=False,
                    data=None,
                    error_message=error_msg,
                    status_code=response.status_code,
                    response_time_ms=response_time
                )
                
        except Exception as e:
            error_msg = f"Zerion transaction request failed for {address}/{tx_hash}: {e}"
            self._update_api_status("zerion", success=False, error=error_msg)
            log_error(error_msg)
            
            return APIResponse(
                success=False,
                data=None,
                error_message=error_msg,
                response_time_ms=(time.time() - start_time) * 1000
            )
    
    def _update_api_status(self, api_name: str, success: bool, error: str = None):
        """Update API health status tracking."""
        if success:
            self.api_status[api_name]["healthy"] = True
            self.api_status[api_name]["last_success"] = datetime.utcnow()
            self.api_status[api_name]["error_count"] = 0
        else:
            self.api_status[api_name]["error_count"] += 1
            if self.api_status[api_name]["error_count"] >= 5:
                self.api_status[api_name]["healthy"] = False
    
    def get_api_health_summary(self) -> Dict[str, Any]:
        """Get comprehensive API health summary."""
        healthy_apis = sum(1 for status in self.api_status.values() if status["healthy"])
        total_apis = len(self.api_status)
        
        return {
            "overall_health": f"{healthy_apis}/{total_apis} APIs healthy",
            "healthy_percentage": (healthy_apis / total_apis) * 100,
            "api_details": self.api_status,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def comprehensive_enrichment(self, address: str, chain: str = "ethereum") -> Dict[str, Any]:
        """Perform comprehensive enrichment using all available APIs."""
        enrichment_data = {
            "address": address,
            "chain": chain,
            "enrichment_timestamp": datetime.utcnow().isoformat(),
            "sources": {}
        }
        
        # Zerion enrichment (highest priority)
        zerion_response = self.get_zerion_portfolio(address)
        if zerion_response.success:
            enrichment_data["sources"]["zerion"] = zerion_response.data
            enrichment_data["whale_score"] = zerion_response.data.get("whale_score", 0)
            enrichment_data["classification"] = zerion_response.data.get("user_classification", "Unknown")
        
        # Covalent enrichment
        if chain in ["ethereum", "polygon"]:
            chain_name = "eth-mainnet" if chain == "ethereum" else "matic-mainnet"
            covalent_response = self.get_covalent_portfolio(address, chain_name)
            if covalent_response.success:
                enrichment_data["sources"]["covalent"] = covalent_response.data
        
        # Moralis enrichment
        moralis_chain = "eth" if chain == "ethereum" else "polygon"
        moralis_response = self.get_moralis_wallet_history(address, moralis_chain)
        if moralis_response.success:
            enrichment_data["sources"]["moralis"] = moralis_response.data
        
        return enrichment_data 
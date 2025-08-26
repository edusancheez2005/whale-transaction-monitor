"""
Zerion API Integration for Portfolio Enrichment

This module integrates with Zerion's API to enrich whale transaction data with
comprehensive portfolio information, whale scoring, and user classification.

Features:
- Portfolio value calculation
- Whale score computation (0-10 scale)
- User classification (Mega Whale, Large Whale, Whale, Small Whale, Retail)
- Rate limiting and error handling
- Structured data models
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime
import requests
from ratelimit import limits, sleep_and_retry
import base64

# Import the centralized error logging function
from .base_helpers import log_error

logger = logging.getLogger(__name__)


@dataclass
class ZerionPortfolioData:
    """Structured portfolio data from Zerion API."""
    address: str
    total_value_usd: float
    position_count: int
    whale_score: float
    user_classification: str
    collected_at: datetime
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            'address': self.address,
            'total_value_usd': self.total_value_usd,
            'position_count': self.position_count,
            'whale_score': self.whale_score,
            'user_classification': self.user_classification,
            'collected_at': self.collected_at.isoformat()
        }


class ZerionEnricher:
    """Zerion API integration for portfolio enrichment."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.zerion.io/v1"
        self.session = requests.Session()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
        # Set headers for Zerion API (proper Basic Auth format)
        # Encode API key properly: base64(api_key + ":")
        auth_string = f"{self.api_key}:"
        encoded_auth = base64.b64encode(auth_string.encode()).decode()
        
        self.session.headers.update({
            'Authorization': f'Basic {encoded_auth}',
            'Accept': 'application/json',
            'User-Agent': 'WhaleTransactionMonitor/1.0'
        })
        
        # Whale thresholds based on portfolio value
        self.WHALE_THRESHOLDS = {
            'mega_whale': 50_000_000,    # $50M+
            'large_whale': 10_000_000,   # $10M+
            'whale': 1_000_000,          # $1M+
            'small_whale': 100_000,      # $100K+
            'retail': 0                  # Below $100K
        }
    
    @sleep_and_retry
    @limits(calls=10, period=60)  # 10 requests per minute
    def _make_request(self, endpoint: str) -> Dict[str, Any]:
        """Make a rate-limited request to Zerion API."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            result = response.json()
            self.logger.info(f"Zerion API request successful: {endpoint}")
            return result
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Zerion API request failed for {endpoint}: {e}"
            self.logger.error(error_msg)
            log_error(error_msg)
            return {}
        except ValueError as e:
            error_msg = f"Zerion JSON parsing failed for {endpoint}: {e}"
            self.logger.error(error_msg)
            log_error(error_msg)
            return {}
    
    def get_wallet_portfolio(self, address: str) -> Optional[ZerionPortfolioData]:
        """Fetch comprehensive portfolio data for a wallet address."""
        try:
            # Use the correct Zerion API endpoint for wallet positions
            portfolio_data = self._make_request(f'wallets/{address}/positions/')
            
            if not portfolio_data or 'data' not in portfolio_data:
                self.logger.warning(f"No portfolio data found for {address}")
                return None
            
            # Extract portfolio metrics from positions
            positions = portfolio_data['data']
            if not isinstance(positions, list):
                positions = [positions] if positions else []
            
            # Calculate total portfolio value in USD
            total_value_usd = 0.0
            position_count = len(positions)
            
            for position in positions:
                if isinstance(position, dict):
                    attributes = position.get('attributes', {})
                    value = attributes.get('value', 0)
                    if isinstance(value, (int, float)):
                        total_value_usd += value
            
            # Calculate whale score and classification
            whale_score = self._calculate_whale_score(total_value_usd, positions)
            user_classification = self._classify_user(total_value_usd, positions)
            
            return ZerionPortfolioData(
                address=address,
                total_value_usd=total_value_usd,
                position_count=position_count,
                whale_score=whale_score,
                user_classification=user_classification,
                collected_at=datetime.utcnow()
            )
            
        except Exception as e:
            error_msg = f"Error in Zerion portfolio fetch for {address}: {e}"
            self.logger.error(error_msg)
            log_error(error_msg)
            return None
    
    def _calculate_whale_score(self, total_value: float, positions: List[Dict]) -> float:
        """
        Calculate whale score (0-10) based on portfolio value and composition.
        
        Scoring algorithm:
        - Portfolio value: Primary factor
        - Position diversity: Secondary factor
        - Asset composition: Tertiary factor
        """
        if total_value <= 0:
            return 0.0
        
        # Base score from portfolio value
        if total_value >= self.WHALE_THRESHOLDS['mega_whale']:
            base_score = 10.0
        elif total_value >= self.WHALE_THRESHOLDS['large_whale']:
            base_score = 8.5
        elif total_value >= self.WHALE_THRESHOLDS['whale']:
            base_score = 7.0
        elif total_value >= self.WHALE_THRESHOLDS['small_whale']:
            base_score = 5.0
        else:
            # Graduated scoring for smaller portfolios
            base_score = min(5.0, (total_value / self.WHALE_THRESHOLDS['small_whale']) * 5.0)
        
        # Position diversity bonus (up to 0.5 points)
        position_count = len(positions)
        diversity_bonus = min(0.5, position_count * 0.05)
        
        return min(10.0, base_score + diversity_bonus)
    
    def _classify_user(self, total_value: float, positions: List[Dict]) -> str:
        """Classify user based on portfolio value thresholds."""
        if total_value >= self.WHALE_THRESHOLDS['mega_whale']:
            return 'Mega Whale'
        elif total_value >= self.WHALE_THRESHOLDS['large_whale']:
            return 'Large Whale'
        elif total_value >= self.WHALE_THRESHOLDS['whale']:
            return 'Whale'
        elif total_value >= self.WHALE_THRESHOLDS['small_whale']:
            return 'Small Whale'
        else:
            return 'Retail Investor'
    
    def get_whale_score_only(self, address: str) -> Tuple[float, str]:
        """Quick whale score calculation without full portfolio data."""
        try:
            portfolio_data = self.get_wallet_portfolio(address)
            if portfolio_data:
                return portfolio_data.whale_score, portfolio_data.user_classification
            else:
                return 0.0, 'Unknown'
        except Exception as e:
            self.logger.error(f"Failed to get whale score for {address}: {e}")
            return 0.0, 'Error'
    
    def enrich_transaction_with_zerion(self, tx_data: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich transaction data with Zerion portfolio information."""
        enriched_tx = tx_data.copy()
        
        # Add Zerion data for both from and to addresses
        for address_field in ['from_address', 'to_address']:
            if address_field in tx_data:
                address = tx_data[address_field]
                portfolio_data = self.get_wallet_portfolio(address)
                
                if portfolio_data:
                    field_prefix = address_field.replace('_address', '')
                    enriched_tx[f'{field_prefix}_zerion_whale_score'] = portfolio_data.whale_score
                    enriched_tx[f'{field_prefix}_zerion_classification'] = portfolio_data.user_classification
                    enriched_tx[f'{field_prefix}_zerion_portfolio_value'] = portfolio_data.total_value_usd
                    enriched_tx[f'{field_prefix}_zerion_position_count'] = portfolio_data.position_count
        
        return enriched_tx 
#!/usr/bin/env python3
"""
Enhanced Transaction Classification with Etherscan Labels, Token Intelligence, and Whale Registry
Wraps the existing classification system with additional intelligence layers
"""

import logging
import asyncio
from typing import Dict, Any, Optional
from utils.classification_final import process_and_enrich_transaction
from utils.etherscan_labels import label_provider
from utils.token_intelligence import token_intelligence
from utils.whale_registry import whale_registry

logger = logging.getLogger(__name__)

async def enhanced_transfer_classification(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Phase 1: Enhance transfer events with CEX flow detection BEFORE whale intelligence.
    
    This adds:
    1. Etherscan address labels for from/to addresses
    2. Conservative CEX detection (only classify if verified exchange)
    3. Token quality assessment
    4. Whale registry tracking
    
    Args:
        event: Raw transaction event
        
    Returns:
        Enhanced event with additional metadata
    """
    try:
        from_addr = event.get('from', '')
        to_addr = event.get('to', '')
        chain = event.get('blockchain', 'ethereum')
        token_symbol = event.get('symbol', '')
        usd_value = event.get('estimated_usd', event.get('value_usd', 0))
        
        # Step 1: Get Etherscan labels for both addresses
        from_label = None
        to_label = None
        
        if from_addr:
            from_label = await label_provider.get_address_label(from_addr, chain)
            if from_label:
                event['from_label'] = from_label
                event['from_address_type'] = label_provider.get_address_type(from_label)
                logger.info(f"From address label: {from_addr[:10]}... = {from_label}")
        
        if to_addr:
            to_label = await label_provider.get_address_label(to_addr, chain)
            if to_label:
                event['to_label'] = to_label
                event['to_address_type'] = label_provider.get_address_type(to_label)
                logger.info(f"To address label: {to_addr[:10]}... = {to_label}")
        
        # Step 2: CONSERVATIVE CEX Classification
        # Only classify as BUY/SELL if we have verified CEX address
        classification, confidence, reason = label_provider.classify_transfer_by_label(
            from_label, 
            to_label
        )
        
        if classification:
            # Found a CEX flow - override default TRANSFER classification
            event['cex_flow_detected'] = True
            event['cex_classification'] = classification
            event['cex_confidence'] = confidence
            event['cex_reason'] = reason
            logger.info(f"🏦 CEX Flow: {reason} → {classification} (confidence: {confidence})")
        
        # Step 3: Token Quality Assessment (for alerts/filtering)
        token_address = event.get('contract_address', event.get('token_address'))
        if token_address and usd_value >= 50000:  # Only check for high-value trades
            token_assessment = await token_intelligence.should_alert_on_token(
                token_address=token_address,
                chain=chain,
                trade_size_usd=usd_value
            )
            
            event['token_risk_assessment'] = token_assessment['risk_assessment']
            event['should_alert'] = token_assessment['should_alert']
            
            if not token_assessment['should_alert']:
                logger.warning(
                    f"⚠️ Token quality filter triggered for {token_symbol}: "
                    f"{token_assessment['risk_assessment']['risk_level']} risk"
                )
        
        # Step 4: Whale Registry Tracking
        # Track both from and to addresses if they're making large moves
        if usd_value >= 50000:
            # Track sender
            if from_addr and classification:
                whale_registry.track_transaction(
                    address=from_addr,
                    transaction={
                        'classification': classification,
                        'token': token_symbol,
                        'usd_value': usd_value,
                        'timestamp': event.get('timestamp'),
                        'confidence': confidence
                    }
                )
                
                # Check if this is a proven whale
                if whale_registry.is_proven_whale(from_addr):
                    event['is_proven_whale'] = True
                    event['whale_confidence_boost'] = whale_registry.get_whale_confidence_boost(from_addr)
                    logger.info(f"🐋 Proven whale detected: {from_addr[:10]}...")
        
        return event
        
    except Exception as e:
        logger.error(f"Enhanced classification failed: {e}")
        # Return original event if enhancement fails (graceful degradation)
        return event


def process_with_enhanced_intelligence(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Main entry point: Process transaction with enhanced intelligence layers.
    
    Flow:
    1. Run async enhanced classification (Etherscan labels, token intelligence)
    2. Pass to existing whale intelligence engine
    3. Return enriched result
    
    Args:
        event: Raw transaction event
        
    Returns:
        Fully enriched transaction or None
    """
    try:
        # Run async enhancement in sync context
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If already in async context, create new task
            future = asyncio.ensure_future(enhanced_transfer_classification(event))
            # Wait with timeout
            enhanced_event = asyncio.wait_for(future, timeout=5.0)
        else:
            # Create new event loop for sync context
            enhanced_event = loop.run_until_complete(enhanced_transfer_classification(event))
        
        # Pass to existing whale intelligence pipeline
        result = process_and_enrich_transaction(enhanced_event)
        
        # If CEX flow was detected, use that classification
        if result and enhanced_event.get('cex_flow_detected'):
            result['classification'] = enhanced_event.get('cex_classification', result.get('classification'))
            result['confidence'] = enhanced_event.get('cex_confidence', result.get('confidence'))
            result['classification_method'] = 'cex_label_detection'
            
            # Add proven whale boost to confidence
            if enhanced_event.get('whale_confidence_boost'):
                result['confidence'] = min(
                    0.99, 
                    result['confidence'] + enhanced_event['whale_confidence_boost']
                )
        
        return result
        
    except Exception as e:
        logger.error(f"Enhanced intelligence processing failed: {e}")
        # Fallback to standard processing
        return process_and_enrich_transaction(event)



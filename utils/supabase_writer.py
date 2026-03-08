"""
Supabase Writer - Persists whale transactions to Supabase for the Sonar dashboard.

Hooks into the deduplication pipeline so every unique transaction is written
to the whale_transactions table that Sonar reads from.
"""

import time
import threading
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# Lazy-initialized Supabase client
_supabase_client = None
_client_lock = threading.Lock()


def _get_client():
    """Lazy-init Supabase client (thread-safe)."""
    global _supabase_client
    if _supabase_client is None:
        with _client_lock:
            if _supabase_client is None:
                try:
                    from supabase import create_client
                    from config.api_keys import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
                    _supabase_client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
                    logger.info("Supabase writer client initialized")
                except Exception as e:
                    logger.error(f"Failed to initialize Supabase client: {e}")
                    return None
    return _supabase_client


def _map_event_to_whale_row(event: Dict[str, Any], classification_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Map a dedup event dict to the whale_transactions row schema that Sonar expects.

    Sonar reads these columns:
        transaction_hash, timestamp, blockchain, token_symbol, classification,
        usd_value, whale_score, whale_address, counterparty_address,
        counterparty_type, from_address, to_address, reasoning, confidence,
        from_label, to_label
    """
    tx_hash = event.get('tx_hash', event.get('hash', ''))

    # Build timestamp — prefer ISO string for Supabase timestamptz
    raw_ts = event.get('timestamp')
    if isinstance(raw_ts, (int, float)) and raw_ts > 0:
        if raw_ts > 1e12:  # milliseconds
            raw_ts = raw_ts / 1000
        ts_iso = datetime.fromtimestamp(raw_ts, tz=timezone.utc).isoformat()
    else:
        ts_iso = datetime.now(tz=timezone.utc).isoformat()

    # Classification — pull from classification_data or event itself
    classification = 'TRANSFER'
    confidence = 0.0
    whale_score = 0.0
    reasoning = ''

    if classification_data:
        classification = classification_data.get('classification', 'TRANSFER')
        confidence = classification_data.get('confidence', 0.0)
        whale_score = classification_data.get('whale_score', 0.0)
        reasoning = classification_data.get('reasoning', '')
    elif 'classification' in event:
        classification = event['classification']
        confidence = event.get('confidence', 0.0)
        whale_score = event.get('whale_score', 0.0)
        reasoning = event.get('reasoning', '')

    # Normalize classification to uppercase and map to BUY/SELL/TRANSFER
    classification = str(classification).upper()
    if classification.startswith('PROBABLE_'):
        classification = classification.replace('PROBABLE_', '')
    # Map granular buy/sell variants to their base classification
    if 'BUY' in classification:
        classification = 'BUY'
    elif 'SELL' in classification:
        classification = 'SELL'
    elif classification not in ('BUY', 'SELL', 'TRANSFER'):
        classification = 'TRANSFER'

    from_addr = event.get('from', event.get('from_address', ''))
    to_addr = event.get('to', event.get('to_address', ''))

    # Whale address / counterparty — use classification_data if available
    whale_address = ''
    counterparty_address = ''
    counterparty_type = ''
    from_label = ''
    to_label = ''

    if classification_data:
        whale_address = classification_data.get('whale_address', '')
        counterparty_address = classification_data.get('counterparty_address', '')
        counterparty_type = classification_data.get('counterparty_type', '')
        from_label = classification_data.get('from_label', '')
        to_label = classification_data.get('to_label', '')

    # Fallback: if no whale_address, use from_address for BUY, to_address for SELL
    if not whale_address:
        if classification == 'BUY':
            whale_address = to_addr
            counterparty_address = from_addr
        elif classification == 'SELL':
            whale_address = from_addr
            counterparty_address = to_addr
        else:
            whale_address = from_addr

    usd_value = float(
        event.get('estimated_usd', 0) or
        event.get('usd_value', 0) or
        event.get('value_usd', 0) or 0
    )

    return {
        'transaction_hash': tx_hash,
        'timestamp': ts_iso,
        'blockchain': event.get('blockchain', 'unknown').lower(),
        'token_symbol': event.get('symbol', event.get('token_symbol', '')).upper(),
        'classification': classification,
        'usd_value': usd_value,
        'whale_score': float(whale_score),
        'whale_address': whale_address.lower() if whale_address else '',
        'counterparty_address': counterparty_address.lower() if counterparty_address else '',
        'counterparty_type': counterparty_type,
        'from_address': from_addr.lower() if from_addr else '',
        'to_address': to_addr.lower() if to_addr else '',
        'reasoning': reasoning[:1000] if reasoning else '',
        'confidence': float(confidence),
        'from_label': from_label,
        'to_label': to_label,
    }


def store_transaction(event: Dict[str, Any], classification_data: Optional[Dict[str, Any]] = None) -> bool:
    """
    Persist a whale transaction to Supabase.

    Called after deduplication confirms a unique transaction.
    Thread-safe — can be called from any chain monitoring thread.

    Args:
        event: The raw event dict from chain modules
        classification_data: Optional enrichment data (classification, whale_address, etc.)

    Returns:
        True if stored successfully, False otherwise
    """
    client = _get_client()
    if client is None:
        return False

    # Stablecoins to exclude — high-volume noise that drowns out real whale activity
    EXCLUDED_STABLECOINS = {'USDT', 'USDC', 'DAI', 'BUSD', 'TUSD', 'USDP', 'FDUSD'}

    try:
        row = _map_event_to_whale_row(event, classification_data)

        # Skip if no transaction hash
        if not row['transaction_hash']:
            return False

        # Skip if no token symbol
        if not row['token_symbol']:
            return False

        # Skip pure stablecoin transfers — they flood the database
        if row['token_symbol'] in EXCLUDED_STABLECOINS and row['classification'] == 'TRANSFER':
            logger.debug(f"Skipped stablecoin transfer: {row['token_symbol']} ${row['usd_value']:,.0f}")
            return False

        result = client.table('whale_transactions').upsert(
            row,
            on_conflict='transaction_hash'
        ).execute()

        if result.data:
            logger.info(
                f"Stored: {row['blockchain']} {row['token_symbol']} "
                f"${row['usd_value']:,.0f} {row['classification']}"
            )
            return True
        return False

    except Exception as e:
        logger.error(f"Failed to store transaction {event.get('tx_hash', '?')}: {e}")
        return False

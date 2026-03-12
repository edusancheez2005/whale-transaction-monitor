"""Supabase Writer - Persists whale transactions to per-blockchain Supabase tables.

Routes each transaction to its chain-specific table:
  ethereum_transactions, bitcoin_transactions, solana_transactions,
  polygon_transactions, tron_transactions, xrp_transactions

All tables share the same schema as whale_transactions / alchemy_transactions.
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

# Blockchain -> table name routing
_CHAIN_TABLE_MAP = {
    'ethereum': 'ethereum_transactions',
    'bitcoin':  'bitcoin_transactions',
    'solana':   'solana_transactions',
    'polygon':  'polygon_transactions',
    'xrp':      'xrp_transactions',
}


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


def _map_event_to_row(event: Dict[str, Any], classification_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Map a dedup event dict to the per-chain table row schema.

    Matches whale_transactions columns exactly:
        transaction_hash, token_symbol, token_address, classification,
        confidence, usd_value, whale_score, blockchain, from_address,
        to_address, timestamp, analysis_phases, reasoning, monitoring_group,
        whale_address, counterparty_address, counterparty_type,
        is_cex_transaction, from_label, to_label
    """
    tx_hash = event.get('tx_hash', event.get('hash', ''))

    # Build timestamp - prefer ISO string for Supabase timestamptz
    raw_ts = event.get('timestamp')
    if isinstance(raw_ts, (int, float)) and raw_ts > 0:
        if raw_ts > 1e12:  # milliseconds
            raw_ts = raw_ts / 1000
        ts_iso = datetime.fromtimestamp(raw_ts, tz=timezone.utc).isoformat()
    else:
        ts_iso = datetime.now(tz=timezone.utc).isoformat()

    # Classification - pull from classification_data or event itself
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
    if 'BUY' in classification:
        classification = 'BUY'
    elif 'SELL' in classification:
        classification = 'SELL'
    elif classification not in ('BUY', 'SELL', 'TRANSFER'):
        classification = 'TRANSFER'

    from_addr = event.get('from', event.get('from_address', ''))
    to_addr = event.get('to', event.get('to_address', ''))

    # Whale address / counterparty
    whale_address = ''
    counterparty_address = ''
    counterparty_type = ''
    from_label = ''
    to_label = ''
    is_cex_transaction = False

    if classification_data:
        whale_address = classification_data.get('whale_address', '')
        counterparty_address = classification_data.get('counterparty_address', '')
        counterparty_type = classification_data.get('counterparty_type', '')
        from_label = classification_data.get('from_label', '')
        to_label = classification_data.get('to_label', '')
        is_cex_transaction = classification_data.get('is_cex_transaction', False)

    # Fallback: if no whale_address, use from/to based on classification
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
        'token_address': event.get('token_address', ''),
        'classification': classification,
        'usd_value': usd_value,
        'whale_score': float(whale_score),
        'confidence': float(confidence),
        'whale_address': whale_address.lower() if whale_address else '',
        'counterparty_address': counterparty_address.lower() if counterparty_address else '',
        'counterparty_type': counterparty_type,
        'is_cex_transaction': bool(is_cex_transaction),
        'from_address': from_addr.lower() if from_addr else '',
        'to_address': to_addr.lower() if to_addr else '',
        'reasoning': reasoning[:1000] if reasoning else '',
        'from_label': from_label,
        'to_label': to_label,
        'analysis_phases': int(event.get('analysis_phases', 0)),
        'monitoring_group': event.get('monitoring_group', ''),
    }


def store_transaction(event: Dict[str, Any], classification_data: Optional[Dict[str, Any]] = None) -> bool:
    """
    Persist a whale transaction to the per-blockchain Supabase table.

    Routes based on event['blockchain']:
      ethereum -> ethereum_transactions
      bitcoin  -> bitcoin_transactions
      solana   -> solana_transactions
      polygon  -> polygon_transactions
      tron     -> tron_transactions
      xrp      -> xrp_transactions

    Thread-safe - can be called from any chain monitoring thread.
    """
    client = _get_client()
    if client is None:
        return False

    EXCLUDED_STABLECOINS = {'USDT', 'USDC', 'DAI', 'BUSD', 'TUSD', 'USDP', 'FDUSD'}

    try:
        row = _map_event_to_row(event, classification_data)

        if not row['transaction_hash']:
            return False
        if not row['token_symbol']:
            return False
        if row['token_symbol'] in EXCLUDED_STABLECOINS and row['blockchain'] not in ('polygon', 'solana'):
            logger.debug(f"Skipped stablecoin: {row['token_symbol']} {row['classification']} ${row['usd_value']:,.0f}")
            return False

        # Route to per-blockchain table
        blockchain = row['blockchain']
        table_name = _CHAIN_TABLE_MAP.get(blockchain)
        if not table_name:
            logger.warning(f"Unknown blockchain '{blockchain}', skipping storage")
            return False

        result = client.table(table_name).upsert(
            row,
            on_conflict='transaction_hash'
        ).execute()

        if result.data:
            logger.info(
                f"Stored -> {table_name}: {row['token_symbol']} "
                f"${row['usd_value']:,.0f} {row['classification']}"
            )
            return True
        return False

    except Exception as e:
        logger.error(f"Failed to store transaction {event.get('tx_hash', '?')}: {e}")
        return False


# Backwards-compatible alias - some chain modules still call this
store_alchemy_transaction = store_transaction

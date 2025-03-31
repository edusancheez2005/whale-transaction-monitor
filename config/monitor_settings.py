# config/monitor_settings.py

# Monitoring intervals (in seconds)
INTERVALS = {
    'main_loop': 60,
    'price_update': 300,
    'health_check': 30,
    'status_print': 600
}

# Transaction filtering
FILTERING = {
    'min_usd_value': 3_000,  # Minimum USD value to track
    'exclude_stablecoins': True,
    'min_confidence_score': 2,  # Increased from 1
    'detect_circular_flows': True,  # New setting
    'min_trend_threshold': 10,  # Percentage difference to mark a trend
}

# Deduplication settings
DEDUP_SETTINGS = {
    'time_window': 3600,  # Time window for temporal deduplication (1 hour)
    'address_tracking': True,  # Track address interactions
    'chain_specific': True,  # Use chain-specific deduplication rules
    'circular_detection': True,  # Detect A->B->C->A patterns
    'window_for_similar_tx': 1800  # 30 minutes
}

# Chain-specific settings
CHAIN_SETTINGS = {
    'ethereum': {
        'enabled': True,
        'block_confirmations': 12,
        'max_blocks_back': 10000
    },
    'solana': {
        'enabled': True,
        'commitment': 'confirmed'
    },
    'xrp': {
        'enabled': True,
        'min_ledger_index': -1000  # How far back to look
    }
}

# Reporting settings
REPORT_SETTINGS = {
    'include_dedup_stats': True,
    'show_volume_analysis': True,
    'show_market_momentum': True,
    'max_trending_tokens': 5
}

# Error handling
ERROR_HANDLING = {
    'max_retries': 3,
    'retry_delay': 5,
    'log_errors': True
}

# Display settings
DISPLAY_SETTINGS = {
    'use_emoji': True,
    'show_timestamps': True,
    'debug_mode': False,
    'color_output': True
}
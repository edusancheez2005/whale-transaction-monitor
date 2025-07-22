#!/usr/bin/env python3
"""
Production-Grade Logging Configuration
=====================================

Implements structured JSON logging with transaction traceability for the
Whale Intelligence Engine. This enables deep observability into every
transaction's analysis pipeline in production environments.

Features:
- Structured JSON output for log aggregation tools (Datadog, Splunk, ELK)
- Transaction-specific trace IDs for filtering and debugging
- Rich contextual information for each analysis phase
- Error tracking with full stack traces
"""

import logging
import json
import uuid
from typing import Dict, Any, Optional
from pythonjsonlogger import jsonlogger
from datetime import datetime

class WhaleIntelligenceFormatter(jsonlogger.JsonFormatter):
    """
    Custom JSON formatter for whale intelligence logging with enhanced context.
    """
    
    def add_fields(self, log_record: Dict[str, Any], record: logging.LogRecord, message_dict: Dict[str, Any]) -> None:
        """Add custom fields to each log record."""
        super().add_fields(log_record, record, message_dict)
        
        # Always include timestamp in ISO format
        log_record['timestamp'] = datetime.utcnow().isoformat() + 'Z'
        
        # Include service identification
        log_record['service'] = 'whale-intelligence-engine'
        log_record['version'] = '1.0.0'
        
        # Ensure level is included
        if not log_record.get('level'):
            log_record['level'] = record.levelname

class TransactionLogger:
    """
    Transaction-aware logger that automatically includes transaction context
    in all log messages for a specific transaction analysis.
    """
    
    def __init__(self, base_logger: logging.Logger, transaction_hash: str, trace_id: Optional[str] = None):
        """
        Initialize transaction logger.
        
        Args:
            base_logger: The base logger instance
            transaction_hash: Transaction hash for traceability
            trace_id: Optional custom trace ID, generates one if not provided
        """
        self.base_logger = base_logger
        self.transaction_hash = transaction_hash
        self.trace_id = trace_id or str(uuid.uuid4())[:8]
        
        # Base context that will be included in every log message
        self.base_context = {
            'transaction_hash': self.transaction_hash,
            'trace_id': self.trace_id
        }
    
    def _log_with_context(self, level: int, message: str, extra_context: Optional[Dict[str, Any]] = None) -> None:
        """Log message with transaction context."""
        context = self.base_context.copy()
        if extra_context:
            context.update(extra_context)
        
        # Create extra dict for structured logging
        extra = {'extra_fields': context}
        self.base_logger.log(level, message, extra=extra)
    
    def info(self, message: str, **kwargs) -> None:
        """Log info message with transaction context."""
        self._log_with_context(logging.INFO, message, kwargs)
    
    def debug(self, message: str, **kwargs) -> None:
        """Log debug message with transaction context."""
        self._log_with_context(logging.DEBUG, message, kwargs)
    
    def warning(self, message: str, **kwargs) -> None:
        """Log warning message with transaction context."""
        self._log_with_context(logging.WARNING, message, kwargs)
    
    def error(self, message: str, **kwargs) -> None:
        """Log error message with transaction context."""
        self._log_with_context(logging.ERROR, message, kwargs)
    
    def phase_complete(self, phase_name: str, classification: str, confidence: float, evidence: str) -> None:
        """
        Log completion of an analysis phase with structured data.
        
        This is the primary method for logging phase results and is critical
        for debugging the master classification logic.
        """
        self.info(
            f"Phase Analysis Complete: {phase_name}",
            phase=phase_name,
            classification=classification,
            confidence=confidence,
            evidence=evidence
        )
    
    def phase_error(self, phase_name: str, error: str, exception: Optional[Exception] = None) -> None:
        """Log phase analysis error with context."""
        error_context = {
            'phase': phase_name,
            'error_message': error
        }
        
        if exception:
            error_context['exception_type'] = type(exception).__name__
            # Include stack trace for debugging
            import traceback
            error_context['stack_trace'] = traceback.format_exc()
        
        self.error(f"Phase Analysis Failed: {phase_name}", **error_context)
    
    def master_classification(self, final_classification: str, final_confidence: float, reasoning: str) -> None:
        """Log master classification decision with reasoning."""
        self.info(
            f"Master Classification Decision: {final_classification}",
            master_classification=final_classification,
            master_confidence=final_confidence,
            master_reasoning=reasoning
        )

def setup_production_logging(log_level: str = 'INFO') -> logging.Logger:
    """
    Set up production-grade structured logging for the Whale Intelligence Engine.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        
    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger('whale_intelligence')
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Create console handler with JSON formatting
    console_handler = logging.StreamHandler()
    
    # Set up JSON formatter
    formatter = WhaleIntelligenceFormatter(
        '%(timestamp)s %(level)s %(service)s %(version)s %(message)s %(trace_id)s %(transaction_hash)s %(phase)s %(classification)s %(confidence)s %(evidence)s'
    )
    
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Prevent propagation to root logger to avoid duplicate logs
    logger.propagate = False
    
    return logger

def get_transaction_logger(transaction_hash: str, trace_id: Optional[str] = None) -> TransactionLogger:
    """
    Get a transaction-aware logger for analyzing a specific transaction.
    
    Args:
        transaction_hash: Transaction hash for traceability
        trace_id: Optional custom trace ID
        
    Returns:
        TransactionLogger instance
    """
    base_logger = logging.getLogger('whale_intelligence')
    return TransactionLogger(base_logger, transaction_hash, trace_id)

# Initialize the production logger when module is imported
production_logger = setup_production_logging() 
# utils/base_helpers.py
"""Base helper functions to avoid circular imports"""
import sys
from threading import Lock
from config.settings import print_lock, RUNTIME_ERRORS

def safe_print(*args, **kwargs):
    """Thread-safe print function that survives Windows cp1252 consoles."""
    with print_lock:
        try:
            print(*args, **kwargs)
        except UnicodeEncodeError:
            sep = kwargs.get('sep', ' ')
            end = kwargs.get('end', '\n')
            text = sep.join(str(arg) for arg in args)
            encoding = getattr(sys.stdout, 'encoding', None) or 'utf-8'
            safe_text = text.encode(encoding, errors='replace').decode(encoding, errors='replace')
            sys.stdout.write(safe_text + end)
            sys.stdout.flush()

def log_error(error_message: str):
    """
    Logs a unique error message to the global runtime error set.
    Truncates long messages to keep the log clean.
    """
    # To keep the log clean, truncate long messages
    if len(error_message) > 250:
        error_message = error_message[:250] + "..."
    RUNTIME_ERRORS.add(error_message)

def print_error_summary():
    """
    Prints a summary of all unique runtime errors that were logged during execution.
    """
    if not RUNTIME_ERRORS:
        safe_print("✅ No runtime errors detected!")
        return
    
    safe_print(f"\n{'='*60}")
    safe_print(f"🚨 RUNTIME ERROR SUMMARY ({len(RUNTIME_ERRORS)} unique errors)")
    safe_print(f"{'='*60}")
    
    for i, error in enumerate(sorted(RUNTIME_ERRORS), 1):
        safe_print(f"{i:2d}. {error}")
    
    safe_print(f"{'='*60}")
    safe_print(f"Total unique errors logged: {len(RUNTIME_ERRORS)}")
    safe_print(f"{'='*60}\n")
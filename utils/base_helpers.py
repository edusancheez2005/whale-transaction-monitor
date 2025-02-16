# utils/base_helpers.py
"""Base helper functions to avoid circular imports"""
from threading import Lock
from config.settings import print_lock

def safe_print(*args, **kwargs):
    """Thread-safe print function"""
    with print_lock:
        print(*args, **kwargs)
"""
Solana Yellowstone gRPC Streaming Client

Real-time Solana transaction monitoring via Alchemy's Yellowstone gRPC endpoint.
Subscribes to transactions involving monitored SPL token mints and streams
whale-sized transfers for classification and persistence.
"""

import sys
import os
import time
import threading
import traceback
import base58
from collections import defaultdict

import grpc

# Add project root to path for proto imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proto import geyser_pb2, geyser_pb2_grpc
from config.api_keys import ALCHEMY_API_KEY
from config.settings import (
    solana_previous_balances,
    solana_buy_counts,
    solana_sell_counts,
    shutdown_flag,
    print_lock,
)
from data.tokens import SOL_TOKENS_TO_MONITOR, TOKEN_PRICES
from utils.classification_final import enhanced_solana_classification
from utils.base_helpers import safe_print, log_error
from utils.dedup import handle_event

# --- Configuration ---
GRPC_ENDPOINT = "solana-mainnet.g.alchemy.com"
PING_INTERVAL_SECONDS = 30
MAX_RETRIES = 5

# Build mint-to-symbol lookup and per-token USD thresholds
MINT_TO_SYMBOL = {}
MINT_THRESHOLD = {}  # Per-token minimum USD value from SOL_TOKENS_TO_MONITOR
for _sym, _info in SOL_TOKENS_TO_MONITOR.items():
    MINT_TO_SYMBOL[_info["mint"]] = _sym
    MINT_THRESHOLD[_info["mint"]] = _info.get("min_threshold", 1_000)

# List of all monitored mint addresses for the subscription filter
MONITORED_MINTS = list(MINT_TO_SYMBOL.keys())

# SPL Token Program IDs
SPL_TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
SPL_TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"

# Stats
_stats = {
    "updates_received": 0,
    "transactions_processed": 0,
    "whale_transfers_found": 0,
    "errors": 0,
}


class AlchemyAuthPlugin(grpc.AuthMetadataPlugin):
    """Injects the X-Token header for Alchemy gRPC authentication."""

    def __init__(self, api_key: str):
        self.api_key = api_key

    def __call__(self, context, callback):
        metadata = (("x-token", self.api_key),)
        callback(metadata, None)


def _build_channel():
    """Create an authenticated gRPC channel to Alchemy Yellowstone."""
    auth_plugin = AlchemyAuthPlugin(ALCHEMY_API_KEY)
    ssl_creds = grpc.ssl_channel_credentials()
    call_creds = grpc.metadata_call_credentials(auth_plugin)
    combined_creds = grpc.composite_channel_credentials(ssl_creds, call_creds)

    channel = grpc.secure_channel(
        GRPC_ENDPOINT,
        credentials=combined_creds,
        options=[
            ("grpc.max_receive_message_length", 64 * 1024 * 1024),  # 64MB
            ("grpc.keepalive_time_ms", 30_000),
            ("grpc.keepalive_timeout_ms", 10_000),
            ("grpc.keepalive_permit_without_calls", 1),
        ],
    )
    return channel


def _build_subscribe_request():
    """Build a SubscribeRequest that filters for transactions involving our monitored mints.

    Uses the transactions filter with account_include set to our SPL token mint addresses.
    This catches any transaction that touches these mint accounts (transfers, swaps, etc).
    """
    request = geyser_pb2.SubscribeRequest(
        transactions={
            "spl_monitor": geyser_pb2.SubscribeRequestFilterTransactions(
                vote=False,
                failed=False,
                account_include=MONITORED_MINTS,
            )
        },
        commitment=geyser_pb2.CONFIRMED,
    )
    return request


def _bytes_to_base58(data: bytes) -> str:
    """Convert raw bytes to a base58 string (Solana address format)."""
    return base58.b58encode(data).decode("utf-8")


def _process_transaction_update(update):
    """Process a SubscribeUpdateTransaction and extract SPL token transfers.

    Uses pre/post token balances from TransactionStatusMeta — same approach
    as the existing HTTPS poller (solana_api.py) but in real-time.
    """
    _stats["transactions_processed"] += 1

    # update.transaction = SubscribeUpdateTransaction (has .transaction, .slot)
    # update.transaction.transaction = SubscribeUpdateTransactionInfo (has .signature, .transaction, .meta)
    tx_wrapper = update.transaction           # SubscribeUpdateTransaction
    tx_info = tx_wrapper.transaction          # SubscribeUpdateTransactionInfo

    sig_bytes = tx_info.signature
    tx_sig = _bytes_to_base58(sig_bytes)

    tx_msg = tx_info.transaction              # solana.storage.ConfirmedBlock.Transaction
    tx_meta = tx_info.meta                    # TransactionStatusMeta

    if tx_meta is None:
        return

    # Skip failed transactions
    if tx_meta.err and tx_meta.err.err:
        return

    # Build account keys list (static + loaded addresses)
    account_keys = [_bytes_to_base58(k) for k in tx_msg.message.account_keys]
    for addr in tx_meta.loaded_writable_addresses:
        account_keys.append(_bytes_to_base58(addr))
    for addr in tx_meta.loaded_readonly_addresses:
        account_keys.append(_bytes_to_base58(addr))

    # Build pre/post token balance maps: {account_index: TokenBalance}
    pre_balances = {}
    for tb in tx_meta.pre_token_balances:
        pre_balances[tb.account_index] = tb

    post_balances = {}
    for tb in tx_meta.post_token_balances:
        post_balances[tb.account_index] = tb

    # Find all account indices with token balance changes
    all_indices = set(pre_balances.keys()) | set(post_balances.keys())

    for idx in all_indices:
        pre = pre_balances.get(idx)
        post = post_balances.get(idx)

        # Determine the mint
        mint = None
        if post and post.mint:
            mint = post.mint
        elif pre and pre.mint:
            mint = pre.mint

        if not mint or mint not in MINT_TO_SYMBOL:
            continue

        symbol = MINT_TO_SYMBOL[mint]

        # Get amounts
        pre_amount = pre.ui_token_amount.ui_amount if pre else 0.0
        post_amount = post.ui_token_amount.ui_amount if post else 0.0

        amount_change = post_amount - pre_amount
        if abs(amount_change) < 0.0001:
            continue

        # Get owner
        owner = ""
        if post and post.owner:
            owner = post.owner
        elif pre and pre.owner:
            owner = pre.owner

        if not owner:
            continue

        # Calculate USD value and apply per-token threshold
        price = TOKEN_PRICES.get(symbol, 0)
        usd_value = abs(amount_change) * price
        min_threshold = MINT_THRESHOLD.get(mint, 1_000)

        if usd_value < min_threshold:
            continue

        _stats["whale_transfers_found"] += 1

        # Determine from/to based on balance change direction
        prev_owner = None
        if owner in solana_previous_balances:
            prev_owner = solana_previous_balances.get(owner, {}).get("last_counterparty")

        # Build event matching the existing Solana monitor format
        unique_id = f"{tx_sig}_{owner}_{amount_change:.6f}"
        event = {
            "blockchain": "solana",
            "tx_hash": unique_id,
            "original_hash": tx_sig,
            "from": prev_owner or "unknown",
            "to": owner,
            "amount": abs(amount_change),
            "symbol": symbol,
            "usd_value": usd_value,
            "timestamp": time.time(),
            "source": "solana_grpc",
        }

        # Classify before dedup
        classification, confidence = enhanced_solana_classification(
            owner=owner,
            prev_owner=prev_owner,
            amount_change=amount_change,
            tx_hash=tx_sig,
            token=symbol,
            source="solana_grpc",
        )
        event["classification"] = classification

        # Dedup check
        if not handle_event(event):
            continue

        # Update buy/sell counts
        if confidence >= 2:
            if classification == "buy":
                solana_buy_counts[symbol] += 1
            elif classification == "sell":
                solana_sell_counts[symbol] += 1

            current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            safe_print(f"\n[{symbol} | ${usd_value:,.2f} USD] Solana gRPC {classification.upper()}")
            safe_print(f"  Time: {current_time}")
            safe_print(f"  TX: {tx_sig[:16]}...")
            safe_print(f"  Amount: {abs(amount_change):,.2f} {symbol}")
            safe_print(f"  Classification: {classification} (confidence: {confidence})")

        # Persist to Supabase
        if confidence >= 2:
            try:
                from utils.supabase_writer import store_transaction

                classification_data = {
                    "classification": classification.upper(),
                    "confidence": float(confidence) / 10.0,
                    "whale_score": 0.0,
                    "reasoning": f"Solana gRPC classification: {classification}",
                }
                store_transaction(event, classification_data)
            except Exception:
                pass

        # Update balance tracking
        if owner not in solana_previous_balances:
            solana_previous_balances[owner] = {}
        solana_previous_balances[owner][mint] = post_amount


def _subscribe_stream(stub):
    """Open a bidirectional Subscribe stream and process updates.

    Sends the initial SubscribeRequest, then yields pings periodically
    to keep the connection alive while consuming the response stream.
    """

    def request_iterator():
        # Send the subscription request first
        yield _build_subscribe_request()

        # Periodically send pings to keep the stream alive
        ping_id = 0
        while not shutdown_flag.is_set():
            time.sleep(PING_INTERVAL_SECONDS)
            ping_id += 1
            yield geyser_pb2.SubscribeRequest(
                ping=geyser_pb2.SubscribeRequestPing(id=ping_id)
            )

    response_stream = stub.Subscribe(request_iterator())

    for update in response_stream:
        if shutdown_flag.is_set():
            break

        _stats["updates_received"] += 1

        # Route by update type
        which = update.WhichOneof("update_oneof")

        if which == "transaction":
            try:
                _process_transaction_update(update)
            except Exception as e:
                _stats["errors"] += 1
                log_error(f"Solana gRPC tx processing error: {e}")
                if _stats["errors"] <= 5:
                    traceback.print_exc()

        elif which == "ping":
            pass  # Server ping, no action needed

        elif which == "pong":
            pass  # Response to our ping, connection is alive


def _run_grpc_stream(retry_count=0):
    """Main loop: connect, subscribe, and reconnect on failure with exponential backoff."""
    if not ALCHEMY_API_KEY or len(ALCHEMY_API_KEY) < 10:
        safe_print("Solana gRPC: ALCHEMY_API_KEY missing or invalid.")
        return

    safe_print(f"Solana gRPC: Connecting to {GRPC_ENDPOINT} (monitoring {len(MONITORED_MINTS)} SPL mints)...")

    while retry_count <= MAX_RETRIES and not shutdown_flag.is_set():
        try:
            channel = _build_channel()
            stub = geyser_pb2_grpc.GeyserStub(channel)

            # Quick connectivity check
            try:
                slot_resp = stub.GetSlot(
                    geyser_pb2.GetSlotRequest(commitment=geyser_pb2.CONFIRMED),
                    timeout=10,
                )
                safe_print(f"Solana gRPC: Connected. Current slot: {slot_resp.slot}")
            except Exception as e:
                safe_print(f"Solana gRPC: Slot check failed ({e}), proceeding anyway...")

            retry_count = 0  # Reset on successful connection
            _subscribe_stream(stub)

        except grpc.RpcError as e:
            _stats["errors"] += 1
            code = e.code() if hasattr(e, "code") else "UNKNOWN"
            details = e.details() if hasattr(e, "details") else str(e)
            safe_print(f"Solana gRPC error [{code}]: {details}")

        except Exception as e:
            _stats["errors"] += 1
            safe_print(f"Solana gRPC unexpected error: {type(e).__name__}: {str(e)[:200]}")
            traceback.print_exc()

        finally:
            try:
                channel.close()
            except Exception:
                pass

        if shutdown_flag.is_set():
            break

        retry_count += 1
        if retry_count <= MAX_RETRIES:
            wait_time = min(30, 2 ** retry_count)
            safe_print(
                f"Solana gRPC: Reconnecting in {wait_time}s ({retry_count}/{MAX_RETRIES})..."
            )
            time.sleep(wait_time)
        else:
            safe_print(f"Solana gRPC: Max retries ({MAX_RETRIES}) reached. Giving up.")


def start_solana_grpc_thread():
    """Start the Solana gRPC monitoring thread. Drop-in replacement for start_solana_thread()."""
    try:
        thread = threading.Thread(target=_run_grpc_stream, daemon=True)
        thread.name = "Solana-gRPC"
        thread.start()
        safe_print("Solana gRPC: Monitoring thread started.")
        return thread
    except Exception as e:
        safe_print(f"Solana gRPC: Failed to start thread: {e}")
        traceback.print_exc()
        return None


def get_grpc_stats():
    """Return current gRPC monitoring statistics."""
    return dict(_stats)

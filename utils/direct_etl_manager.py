#!/usr/bin/env python3
"""
Direct ETL Manager - Advanced Blockchain Data Extraction
Leverages ethereum-etl and bitcoin-etl for targeted address discovery

This module provides direct access to blockchain data extraction tools for:
- Specific block range analysis
- Smart contract interaction discovery
- Custom event log extraction
- Recent data that may not be in public datasets

Author: Whale Transaction Monitor System
Version: 1.0.0
"""

import os
import csv
import json
import logging
import subprocess
import tempfile
import shutil
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


class DirectETLManager:
    """
    Direct ETL Manager for targeted blockchain data extraction
    
    Uses ethereum-etl and bitcoin-etl tools to extract specific data ranges,
    smart contract interactions, and custom events for address discovery.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize DirectETLManager with configuration
        
        Args:
            config: Configuration dictionary containing provider URIs and settings
        """
        self.config = config
        self.ethereum_provider_uri = config.get('ETHEREUM_NODE_PROVIDER_URI', '')
        self.bitcoin_provider_uri = config.get('BITCOIN_NODE_PROVIDER_URI', '')
        
        # Validate provider URIs
        if not self.ethereum_provider_uri:
            logger.warning("No Ethereum provider URI configured - Ethereum ETL features disabled")
        if not self.bitcoin_provider_uri:
            logger.warning("No Bitcoin provider URI configured - Bitcoin ETL features disabled")
    
    def extract_ethereum_data_range(
        self, 
        start_block: int, 
        end_block: int, 
        provider_uri: Optional[str] = None,
        output_dir: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Extract Ethereum data for a specific block range
        
        Args:
            start_block: Starting block number
            end_block: Ending block number
            provider_uri: Ethereum node provider URI (optional, uses config if not provided)
            output_dir: Output directory for temporary files (optional, creates temp dir)
            
        Returns:
            List of address data dictionaries
        """
        provider_uri = provider_uri or self.ethereum_provider_uri
        if not provider_uri:
            raise ValueError("No Ethereum provider URI available")
        
        # Create temporary directory if not provided
        temp_dir_created = False
        if output_dir is None:
            output_dir = tempfile.mkdtemp(prefix='eth_etl_')
            temp_dir_created = True
        
        try:
            os.makedirs(output_dir, exist_ok=True)
            addresses = []
            
            # Extract blocks and transactions
            transactions_file = os.path.join(output_dir, 'transactions.csv')
            blocks_file = os.path.join(output_dir, 'blocks.csv')
            
            logger.info(f"Extracting Ethereum blocks {start_block}-{end_block}")
            
            # Run ethereum-etl export_blocks_and_transactions
            cmd = [
                'ethereumetl', 'export_blocks_and_transactions',
                '--start-block', str(start_block),
                '--end-block', str(end_block),
                '--provider-uri', provider_uri,
                '--blocks-output', blocks_file,
                '--transactions-output', transactions_file
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode != 0:
                logger.error(f"ethereum-etl failed: {result.stderr}")
                raise RuntimeError(f"ethereum-etl export failed: {result.stderr}")
            
            # Parse transactions for addresses
            if os.path.exists(transactions_file):
                addresses.extend(self._parse_ethereum_transactions(transactions_file))
            
            # Extract token transfers
            token_transfers_file = os.path.join(output_dir, 'token_transfers.csv')
            logger.info(f"Extracting Ethereum token transfers {start_block}-{end_block}")
            
            cmd = [
                'ethereumetl', 'export_token_transfers',
                '--start-block', str(start_block),
                '--end-block', str(end_block),
                '--provider-uri', provider_uri,
                '--output', token_transfers_file
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode == 0 and os.path.exists(token_transfers_file):
                addresses.extend(self._parse_ethereum_token_transfers(token_transfers_file))
            
            # Extract logs
            logs_file = os.path.join(output_dir, 'logs.csv')
            logger.info(f"Extracting Ethereum logs {start_block}-{end_block}")
            
            cmd = [
                'ethereumetl', 'export_logs',
                '--start-block', str(start_block),
                '--end-block', str(end_block),
                '--provider-uri', provider_uri,
                '--output', logs_file
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode == 0 and os.path.exists(logs_file):
                addresses.extend(self._parse_ethereum_logs(logs_file))
            
            logger.info(f"Extracted {len(addresses)} addresses from Ethereum blocks {start_block}-{end_block}")
            return addresses
            
        except Exception as e:
            logger.error(f"Error extracting Ethereum data: {e}")
            raise
        finally:
            # Clean up temporary directory if we created it
            if temp_dir_created and os.path.exists(output_dir):
                shutil.rmtree(output_dir)
    
    def extract_bitcoin_data_range(
        self,
        start_date: str,
        end_date: str,
        provider_uri: Optional[str] = None,
        output_dir: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Extract Bitcoin data for a specific date range
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            provider_uri: Bitcoin node provider URI (optional, uses config if not provided)
            output_dir: Output directory for temporary files (optional, creates temp dir)
            
        Returns:
            List of address data dictionaries
        """
        provider_uri = provider_uri or self.bitcoin_provider_uri
        if not provider_uri:
            raise ValueError("No Bitcoin provider URI available")
        
        # Create temporary directory if not provided
        temp_dir_created = False
        if output_dir is None:
            output_dir = tempfile.mkdtemp(prefix='btc_etl_')
            temp_dir_created = True
        
        try:
            os.makedirs(output_dir, exist_ok=True)
            addresses = []
            
            # Extract blocks and transactions
            transactions_file = os.path.join(output_dir, 'transactions.csv')
            blocks_file = os.path.join(output_dir, 'blocks.csv')
            
            logger.info(f"Extracting Bitcoin data {start_date} to {end_date}")
            
            # Run bitcoin-etl export_blocks_and_transactions
            cmd = [
                'bitcoinetl', 'export_blocks_and_transactions',
                '--start-date', start_date,
                '--end-date', end_date,
                '--provider-uri', provider_uri,
                '--blocks-output', blocks_file,
                '--transactions-output', transactions_file
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            if result.returncode != 0:
                logger.error(f"bitcoin-etl failed: {result.stderr}")
                raise RuntimeError(f"bitcoin-etl export failed: {result.stderr}")
            
            # Parse transactions for addresses
            if os.path.exists(transactions_file):
                addresses.extend(self._parse_bitcoin_transactions(transactions_file))
            
            logger.info(f"Extracted {len(addresses)} addresses from Bitcoin {start_date} to {end_date}")
            return addresses
            
        except Exception as e:
            logger.error(f"Error extracting Bitcoin data: {e}")
            raise
        finally:
            # Clean up temporary directory if we created it
            if temp_dir_created and os.path.exists(output_dir):
                shutil.rmtree(output_dir)
    
    def extract_ethereum_contract_interactions(
        self,
        contract_address: str,
        start_block: int,
        end_block: int,
        provider_uri: Optional[str] = None,
        output_dir: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Extract all interactions with a specific Ethereum smart contract
        
        Args:
            contract_address: The smart contract address to analyze
            start_block: Starting block number
            end_block: Ending block number
            provider_uri: Ethereum node provider URI (optional, uses config if not provided)
            output_dir: Output directory for temporary files (optional, creates temp dir)
            
        Returns:
            List of address data dictionaries for all interacting addresses
        """
        provider_uri = provider_uri or self.ethereum_provider_uri
        if not provider_uri:
            raise ValueError("No Ethereum provider URI available")
        
        # Create temporary directory if not provided
        temp_dir_created = False
        if output_dir is None:
            output_dir = tempfile.mkdtemp(prefix='eth_contract_')
            temp_dir_created = True
        
        try:
            os.makedirs(output_dir, exist_ok=True)
            addresses = []
            
            # Extract transactions to/from the contract
            transactions_file = os.path.join(output_dir, 'contract_transactions.csv')
            
            logger.info(f"Extracting interactions with contract {contract_address}")
            
            # First get all transactions in the block range
            all_transactions_file = os.path.join(output_dir, 'all_transactions.csv')
            blocks_file = os.path.join(output_dir, 'blocks.csv')
            
            cmd = [
                'ethereumetl', 'export_blocks_and_transactions',
                '--start-block', str(start_block),
                '--end-block', str(end_block),
                '--provider-uri', provider_uri,
                '--blocks-output', blocks_file,
                '--transactions-output', all_transactions_file
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode != 0:
                logger.error(f"ethereum-etl failed: {result.stderr}")
                raise RuntimeError(f"ethereum-etl export failed: {result.stderr}")
            
            # Filter transactions for the specific contract
            if os.path.exists(all_transactions_file):
                addresses.extend(self._parse_contract_transactions(
                    all_transactions_file, contract_address.lower()
                ))
            
            # Extract logs from the contract
            logs_file = os.path.join(output_dir, 'contract_logs.csv')
            
            cmd = [
                'ethereumetl', 'export_logs',
                '--start-block', str(start_block),
                '--end-block', str(end_block),
                '--provider-uri', provider_uri,
                '--output', logs_file
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode == 0 and os.path.exists(logs_file):
                addresses.extend(self._parse_contract_logs(logs_file, contract_address.lower()))
            
            logger.info(f"Found {len(addresses)} addresses interacting with contract {contract_address}")
            return addresses
            
        except Exception as e:
            logger.error(f"Error extracting contract interactions: {e}")
            raise
        finally:
            # Clean up temporary directory if we created it
            if temp_dir_created and os.path.exists(output_dir):
                shutil.rmtree(output_dir)
    
    def extract_ethereum_custom_events(
        self,
        event_topic0: str,
        start_block: int,
        end_block: int,
        contract_address: Optional[str] = None,
        provider_uri: Optional[str] = None,
        output_dir: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Extract addresses from custom Ethereum event logs
        
        Args:
            event_topic0: The event signature hash (topic0)
            start_block: Starting block number
            end_block: Ending block number
            contract_address: Specific contract address to filter (optional)
            provider_uri: Ethereum node provider URI (optional, uses config if not provided)
            output_dir: Output directory for temporary files (optional, creates temp dir)
            
        Returns:
            List of address data dictionaries from event participants
        """
        provider_uri = provider_uri or self.ethereum_provider_uri
        if not provider_uri:
            raise ValueError("No Ethereum provider URI available")
        
        # Create temporary directory if not provided
        temp_dir_created = False
        if output_dir is None:
            output_dir = tempfile.mkdtemp(prefix='eth_events_')
            temp_dir_created = True
        
        try:
            os.makedirs(output_dir, exist_ok=True)
            addresses = []
            
            # Extract logs with specific topic0
            logs_file = os.path.join(output_dir, 'custom_events.csv')
            
            logger.info(f"Extracting custom events with topic0 {event_topic0}")
            
            cmd = [
                'ethereumetl', 'export_logs',
                '--start-block', str(start_block),
                '--end-block', str(end_block),
                '--provider-uri', provider_uri,
                '--output', logs_file
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode != 0:
                logger.error(f"ethereum-etl failed: {result.stderr}")
                raise RuntimeError(f"ethereum-etl export failed: {result.stderr}")
            
            # Parse logs for the specific event
            if os.path.exists(logs_file):
                addresses.extend(self._parse_custom_event_logs(
                    logs_file, event_topic0.lower(), contract_address
                ))
            
            logger.info(f"Found {len(addresses)} addresses from custom events")
            return addresses
            
        except Exception as e:
            logger.error(f"Error extracting custom events: {e}")
            raise
        finally:
            # Clean up temporary directory if we created it
            if temp_dir_created and os.path.exists(output_dir):
                shutil.rmtree(output_dir)
    
    def _parse_ethereum_transactions(self, transactions_file: str) -> List[Dict[str, Any]]:
        """Parse Ethereum transactions CSV for addresses"""
        addresses = []
        try:
            with open(transactions_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # From address
                    if row.get('from_address'):
                        addresses.append({
                            'address': row['from_address'].lower(),
                            'label': 'Transaction Sender',
                            'source_system': 'direct_etl_ethereum_transactions',
                            'blockchain': 'ethereum',
                            'metadata': {
                                'block_number': row.get('block_number'),
                                'transaction_hash': row.get('hash'),
                                'value': row.get('value')
                            }
                        })
                    
                    # To address
                    if row.get('to_address'):
                        addresses.append({
                            'address': row['to_address'].lower(),
                            'label': 'Transaction Recipient',
                            'source_system': 'direct_etl_ethereum_transactions',
                            'blockchain': 'ethereum',
                            'metadata': {
                                'block_number': row.get('block_number'),
                                'transaction_hash': row.get('hash'),
                                'value': row.get('value')
                            }
                        })
        except Exception as e:
            logger.error(f"Error parsing transactions file: {e}")
        
        return addresses
    
    def _parse_ethereum_token_transfers(self, token_transfers_file: str) -> List[Dict[str, Any]]:
        """Parse Ethereum token transfers CSV for addresses"""
        addresses = []
        try:
            with open(token_transfers_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # From address
                    if row.get('from_address'):
                        addresses.append({
                            'address': row['from_address'].lower(),
                            'label': 'Token Transfer Sender',
                            'source_system': 'direct_etl_ethereum_token_transfers',
                            'blockchain': 'ethereum',
                            'metadata': {
                                'block_number': row.get('block_number'),
                                'transaction_hash': row.get('transaction_hash'),
                                'token_address': row.get('token_address'),
                                'value': row.get('value')
                            }
                        })
                    
                    # To address
                    if row.get('to_address'):
                        addresses.append({
                            'address': row['to_address'].lower(),
                            'label': 'Token Transfer Recipient',
                            'source_system': 'direct_etl_ethereum_token_transfers',
                            'blockchain': 'ethereum',
                            'metadata': {
                                'block_number': row.get('block_number'),
                                'transaction_hash': row.get('transaction_hash'),
                                'token_address': row.get('token_address'),
                                'value': row.get('value')
                            }
                        })
                    
                    # Token contract address
                    if row.get('token_address'):
                        addresses.append({
                            'address': row['token_address'].lower(),
                            'label': 'Token Contract',
                            'source_system': 'direct_etl_ethereum_token_transfers',
                            'blockchain': 'ethereum',
                            'metadata': {
                                'block_number': row.get('block_number'),
                                'transaction_hash': row.get('transaction_hash')
                            }
                        })
        except Exception as e:
            logger.error(f"Error parsing token transfers file: {e}")
        
        return addresses
    
    def _parse_ethereum_logs(self, logs_file: str) -> List[Dict[str, Any]]:
        """Parse Ethereum logs CSV for addresses"""
        addresses = []
        try:
            with open(logs_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Contract address that emitted the log
                    if row.get('address'):
                        addresses.append({
                            'address': row['address'].lower(),
                            'label': 'Event Emitter Contract',
                            'source_system': 'direct_etl_ethereum_logs',
                            'blockchain': 'ethereum',
                            'metadata': {
                                'block_number': row.get('block_number'),
                                'transaction_hash': row.get('transaction_hash'),
                                'log_index': row.get('log_index'),
                                'topic0': row.get('topic0')
                            }
                        })
                    
                    # Extract addresses from indexed topics (topic1, topic2, topic3)
                    for i in range(1, 4):
                        topic_key = f'topic{i}'
                        if row.get(topic_key):
                            topic_value = row[topic_key]
                            # Check if topic looks like an address (32 bytes with leading zeros)
                            if (topic_value.startswith('0x') and 
                                len(topic_value) == 66 and 
                                topic_value[2:26] == '000000000000000000000000'):
                                address = '0x' + topic_value[26:]
                                addresses.append({
                                    'address': address.lower(),
                                    'label': f'Event Indexed Address (topic{i})',
                                    'source_system': 'direct_etl_ethereum_logs',
                                    'blockchain': 'ethereum',
                                    'metadata': {
                                        'block_number': row.get('block_number'),
                                        'transaction_hash': row.get('transaction_hash'),
                                        'log_index': row.get('log_index'),
                                        'topic0': row.get('topic0'),
                                        'topic_position': i
                                    }
                                })
        except Exception as e:
            logger.error(f"Error parsing logs file: {e}")
        
        return addresses
    
    def _parse_bitcoin_transactions(self, transactions_file: str) -> List[Dict[str, Any]]:
        """Parse Bitcoin transactions CSV for addresses"""
        addresses = []
        try:
            with open(transactions_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Parse inputs and outputs for addresses
                    # Bitcoin transactions have complex input/output structures
                    # This is a simplified parser - real implementation would need
                    # to handle the full UTXO model
                    
                    if row.get('inputs'):
                        # Parse JSON inputs
                        try:
                            inputs = json.loads(row['inputs'])
                            for inp in inputs:
                                if inp.get('addresses'):
                                    for addr in inp['addresses']:
                                        addresses.append({
                                            'address': addr,
                                            'label': 'Bitcoin Input Address',
                                            'source_system': 'direct_etl_bitcoin_transactions',
                                            'blockchain': 'bitcoin',
                                            'metadata': {
                                                'block_number': row.get('block_number'),
                                                'transaction_hash': row.get('hash'),
                                                'value': inp.get('value')
                                            }
                                        })
                        except (json.JSONDecodeError, TypeError):
                            pass
                    
                    if row.get('outputs'):
                        # Parse JSON outputs
                        try:
                            outputs = json.loads(row['outputs'])
                            for out in outputs:
                                if out.get('addresses'):
                                    for addr in out['addresses']:
                                        addresses.append({
                                            'address': addr,
                                            'label': 'Bitcoin Output Address',
                                            'source_system': 'direct_etl_bitcoin_transactions',
                                            'blockchain': 'bitcoin',
                                            'metadata': {
                                                'block_number': row.get('block_number'),
                                                'transaction_hash': row.get('hash'),
                                                'value': out.get('value')
                                            }
                                        })
                        except (json.JSONDecodeError, TypeError):
                            pass
        except Exception as e:
            logger.error(f"Error parsing Bitcoin transactions file: {e}")
        
        return addresses
    
    def _parse_contract_transactions(self, transactions_file: str, contract_address: str) -> List[Dict[str, Any]]:
        """Parse transactions for a specific contract"""
        addresses = []
        contract_address = contract_address.lower()
        
        try:
            with open(transactions_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    to_addr = row.get('to_address', '').lower()
                    from_addr = row.get('from_address', '').lower()
                    
                    # Check if transaction involves the contract
                    if to_addr == contract_address or from_addr == contract_address:
                        # Add the other party in the transaction
                        if to_addr == contract_address and from_addr:
                            addresses.append({
                                'address': from_addr,
                                'label': 'Contract Caller',
                                'source_system': 'direct_etl_contract_interactions',
                                'blockchain': 'ethereum',
                                'metadata': {
                                    'block_number': row.get('block_number'),
                                    'transaction_hash': row.get('hash'),
                                    'contract_address': contract_address,
                                    'value': row.get('value')
                                }
                            })
                        elif from_addr == contract_address and to_addr:
                            addresses.append({
                                'address': to_addr,
                                'label': 'Contract Recipient',
                                'source_system': 'direct_etl_contract_interactions',
                                'blockchain': 'ethereum',
                                'metadata': {
                                    'block_number': row.get('block_number'),
                                    'transaction_hash': row.get('hash'),
                                    'contract_address': contract_address,
                                    'value': row.get('value')
                                }
                            })
        except Exception as e:
            logger.error(f"Error parsing contract transactions: {e}")
        
        return addresses
    
    def _parse_contract_logs(self, logs_file: str, contract_address: str) -> List[Dict[str, Any]]:
        """Parse logs from a specific contract"""
        addresses = []
        contract_address = contract_address.lower()
        
        try:
            with open(logs_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    log_address = row.get('address', '').lower()
                    
                    # Only process logs from the specific contract
                    if log_address == contract_address:
                        # Extract addresses from indexed topics
                        for i in range(1, 4):
                            topic_key = f'topic{i}'
                            if row.get(topic_key):
                                topic_value = row[topic_key]
                                # Check if topic looks like an address
                                if (topic_value.startswith('0x') and 
                                    len(topic_value) == 66 and 
                                    topic_value[2:26] == '000000000000000000000000'):
                                    address = '0x' + topic_value[26:]
                                    addresses.append({
                                        'address': address.lower(),
                                        'label': f'Contract Event Participant (topic{i})',
                                        'source_system': 'direct_etl_contract_interactions',
                                        'blockchain': 'ethereum',
                                        'metadata': {
                                            'block_number': row.get('block_number'),
                                            'transaction_hash': row.get('transaction_hash'),
                                            'contract_address': contract_address,
                                            'log_index': row.get('log_index'),
                                            'topic0': row.get('topic0'),
                                            'topic_position': i
                                        }
                                    })
        except Exception as e:
            logger.error(f"Error parsing contract logs: {e}")
        
        return addresses
    
    def _parse_custom_event_logs(
        self, 
        logs_file: str, 
        event_topic0: str, 
        contract_address: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Parse logs for custom events"""
        addresses = []
        event_topic0 = event_topic0.lower()
        if contract_address:
            contract_address = contract_address.lower()
        
        try:
            with open(logs_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    log_address = row.get('address', '').lower()
                    topic0 = row.get('topic0', '').lower()
                    
                    # Filter by event signature and optionally by contract
                    if topic0 == event_topic0:
                        if contract_address is None or log_address == contract_address:
                            # Add the contract that emitted the event
                            addresses.append({
                                'address': log_address,
                                'label': 'Custom Event Emitter',
                                'source_system': 'direct_etl_custom_events',
                                'blockchain': 'ethereum',
                                'metadata': {
                                    'block_number': row.get('block_number'),
                                    'transaction_hash': row.get('transaction_hash'),
                                    'log_index': row.get('log_index'),
                                    'event_topic0': event_topic0
                                }
                            })
                            
                            # Extract addresses from indexed topics
                            for i in range(1, 4):
                                topic_key = f'topic{i}'
                                if row.get(topic_key):
                                    topic_value = row[topic_key]
                                    # Check if topic looks like an address
                                    if (topic_value.startswith('0x') and 
                                        len(topic_value) == 66 and 
                                        topic_value[2:26] == '000000000000000000000000'):
                                        address = '0x' + topic_value[26:]
                                        addresses.append({
                                            'address': address.lower(),
                                            'label': f'Custom Event Participant (topic{i})',
                                            'source_system': 'direct_etl_custom_events',
                                            'blockchain': 'ethereum',
                                            'metadata': {
                                                'block_number': row.get('block_number'),
                                                'transaction_hash': row.get('transaction_hash'),
                                                'log_index': row.get('log_index'),
                                                'event_topic0': event_topic0,
                                                'topic_position': i
                                            }
                                        })
        except Exception as e:
            logger.error(f"Error parsing custom event logs: {e}")
        
        return addresses
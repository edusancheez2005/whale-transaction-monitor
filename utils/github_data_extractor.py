"""
GitHub Repository Data Extractor - Phase 2: Data Acquisition

This module extracts blockchain address data from public GitHub repositories
containing labeled address datasets.

Author: Address Collector System
Version: 2.0.0 (Phase 2)
"""

import os
import json
import csv
import logging
import tempfile
import shutil
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime
import requests
import pandas as pd
from git import Repo
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)


@dataclass
class GitHubAddressData:
    """Standard format for GitHub repository address data."""
    address: str
    blockchain: str
    source_system: str
    initial_label: Optional[str] = None
    confidence_score: float = 0.7  # Higher confidence for curated datasets
    metadata: Optional[Dict[str, Any]] = None
    collected_at: Optional[datetime] = None
    repository_url: Optional[str] = None
    
    def __post_init__(self):
        if self.collected_at is None:
            self.collected_at = datetime.utcnow()
        if self.metadata is None:
            self.metadata = {}


class GitHubRepositoryExtractor:
    """Base class for extracting data from GitHub repositories."""
    
    def __init__(self, repo_url: str, temp_dir: Optional[str] = None):
        self.repo_url = repo_url
        self.temp_dir = temp_dir or tempfile.mkdtemp()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.repo_path = None
        
    def clone_repository(self) -> str:
        """Clone the repository to a temporary directory."""
        try:
            repo_name = self.repo_url.split('/')[-1].replace('.git', '')
            self.repo_path = os.path.join(self.temp_dir, repo_name)
            
            if os.path.exists(self.repo_path):
                shutil.rmtree(self.repo_path)
            
            self.logger.info(f"Cloning repository: {self.repo_url}")
            Repo.clone_from(self.repo_url, self.repo_path)
            
            return self.repo_path
            
        except Exception as e:
            self.logger.error(f"Failed to clone repository {self.repo_url}: {e}")
            raise
    
    def download_file(self, file_path: str) -> str:
        """Download a specific file from the repository without cloning."""
        try:
            # Convert GitHub URL to raw content URL
            raw_url = self.repo_url.replace('github.com', 'raw.githubusercontent.com')
            if not raw_url.endswith('/'):
                raw_url += '/'
            raw_url += f"main/{file_path}"
            
            response = requests.get(raw_url, timeout=30)
            response.raise_for_status()
            
            # Save to temporary file
            temp_file = os.path.join(self.temp_dir, os.path.basename(file_path))
            with open(temp_file, 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            self.logger.info(f"Downloaded file: {file_path}")
            return temp_file
            
        except Exception as e:
            self.logger.error(f"Failed to download file {file_path}: {e}")
            raise
    
    def cleanup(self):
        """Clean up temporary files."""
        try:
            if os.path.exists(self.temp_dir):
                shutil.rmtree(self.temp_dir)
            self.logger.info("Cleaned up temporary files")
        except Exception as e:
            self.logger.error(f"Failed to cleanup temporary files: {e}")
    
    def extract_addresses(self) -> List[GitHubAddressData]:
        """Extract addresses from the repository. Override in subclasses."""
        raise NotImplementedError("Subclasses must implement extract_addresses method")


class EtherscanLabelsExtractor(GitHubRepositoryExtractor):
    """Extractor for brianleect/etherscan-labels repository."""
    
    def __init__(self):
        super().__init__("https://github.com/brianleect/etherscan-labels")
    
    def extract_addresses(self) -> List[GitHubAddressData]:
        """Extract labeled addresses from etherscan-labels repository."""
        addresses = []
        
        try:
            self.clone_repository()
            
            # Look for CSV files in the repository
            csv_files = []
            for root, dirs, files in os.walk(self.repo_path):
                for file in files:
                    if file.endswith('.csv'):
                        csv_files.append(os.path.join(root, file))
            
            for csv_file in csv_files:
                try:
                    df = pd.read_csv(csv_file)
                    
                    # Common column names for addresses and labels
                    address_cols = ['address', 'Address', 'ADDRESS', 'wallet', 'Wallet']
                    label_cols = ['label', 'Label', 'LABEL', 'name', 'Name', 'tag', 'Tag']
                    
                    address_col = None
                    label_col = None
                    
                    # Find the address column
                    for col in address_cols:
                        if col in df.columns:
                            address_col = col
                            break
                    
                    # Find the label column
                    for col in label_cols:
                        if col in df.columns:
                            label_col = col
                            break
                    
                    if address_col:
                        for _, row in df.iterrows():
                            address = str(row[address_col]).strip()
                            
                            # Validate Ethereum address format
                            if address.startswith('0x') and len(address) == 42:
                                label = str(row[label_col]).strip() if label_col else 'Etherscan Label'
                                
                                addresses.append(GitHubAddressData(
                                    address=address,
                                    blockchain='ethereum',
                                    source_system='etherscan_labels_repo',
                                    initial_label=label,
                                    repository_url=self.repo_url,
                                    metadata={
                                        'file_name': os.path.basename(csv_file),
                                        'row_data': row.to_dict()
                                    }
                                ))
                
                except Exception as e:
                    self.logger.error(f"Failed to process CSV file {csv_file}: {e}")
                    continue
            
            self.logger.info(f"Extracted {len(addresses)} addresses from etherscan-labels")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to extract from etherscan-labels: {e}")
            return []
        finally:
            self.cleanup()


class EthLabelsExtractor(GitHubRepositoryExtractor):
    """Extractor for dawsbot/eth-labels repository."""
    
    def __init__(self):
        super().__init__("https://github.com/dawsbot/eth-labels")
    
    def extract_addresses(self) -> List[GitHubAddressData]:
        """Extract labeled addresses from eth-labels repository."""
        addresses = []
        
        try:
            # Try to download the main labels file directly
            try:
                labels_file = self.download_file("labels.json")
                
                with open(labels_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Process JSON data
                if isinstance(data, dict):
                    for address, label in data.items():
                        if address.startswith('0x') and len(address) == 42:
                            addresses.append(GitHubAddressData(
                                address=address,
                                blockchain='ethereum',
                                source_system='eth_labels_repo',
                                initial_label=str(label),
                                repository_url=self.repo_url,
                                metadata={'source_file': 'labels.json'}
                            ))
                
            except:
                # Fallback: clone and search for JSON/CSV files
                self.clone_repository()
                
                # Look for JSON and CSV files
                data_files = []
                for root, dirs, files in os.walk(self.repo_path):
                    for file in files:
                        if file.endswith(('.json', '.csv')):
                            data_files.append(os.path.join(root, file))
                
                for data_file in data_files:
                    try:
                        if data_file.endswith('.json'):
                            with open(data_file, 'r', encoding='utf-8') as f:
                                data = json.load(f)
                            
                            if isinstance(data, dict):
                                for address, label in data.items():
                                    if address.startswith('0x') and len(address) == 42:
                                        addresses.append(GitHubAddressData(
                                            address=address,
                                            blockchain='ethereum',
                                            source_system='eth_labels_repo',
                                            initial_label=str(label),
                                            repository_url=self.repo_url,
                                            metadata={'source_file': os.path.basename(data_file)}
                                        ))
                        
                        elif data_file.endswith('.csv'):
                            df = pd.read_csv(data_file)
                            
                            # Process CSV similar to etherscan-labels
                            address_cols = ['address', 'Address', 'ADDRESS']
                            label_cols = ['label', 'Label', 'LABEL', 'name', 'Name']
                            
                            address_col = None
                            label_col = None
                            
                            for col in address_cols:
                                if col in df.columns:
                                    address_col = col
                                    break
                            
                            for col in label_cols:
                                if col in df.columns:
                                    label_col = col
                                    break
                            
                            if address_col:
                                for _, row in df.iterrows():
                                    address = str(row[address_col]).strip()
                                    
                                    if address.startswith('0x') and len(address) == 42:
                                        label = str(row[label_col]).strip() if label_col else 'ETH Label'
                                        
                                        addresses.append(GitHubAddressData(
                                            address=address,
                                            blockchain='ethereum',
                                            source_system='eth_labels_repo',
                                            initial_label=label,
                                            repository_url=self.repo_url,
                                            metadata={'source_file': os.path.basename(data_file)}
                                        ))
                    
                    except Exception as e:
                        self.logger.error(f"Failed to process file {data_file}: {e}")
                        continue
            
            self.logger.info(f"Extracted {len(addresses)} addresses from eth-labels")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to extract from eth-labels: {e}")
            return []
        finally:
            self.cleanup()


class OFACAddressesExtractor(GitHubRepositoryExtractor):
    """Extractor for ultrasoundmoney/ofac-ethereum-addresses repository."""
    
    def __init__(self):
        super().__init__("https://github.com/ultrasoundmoney/ofac-ethereum-addresses")
    
    def extract_addresses(self) -> List[GitHubAddressData]:
        """Extract OFAC sanctioned addresses."""
        addresses = []
        
        try:
            # Direct download approach - try multiple known file paths
            file_paths = [
                "addresses.json",
                "ofac_addresses.json", 
                "sanctioned_addresses.json",
                "data/addresses.json"
            ]
            
            for file_path in file_paths:
                try:
                    # Try direct raw GitHub URL
                    raw_url = f"https://raw.githubusercontent.com/ultrasoundmoney/ofac-ethereum-addresses/main/{file_path}"
                    
                    response = requests.get(raw_url, timeout=30)
                    if response.status_code == 200:
                        data = response.json()
                        
                        if isinstance(data, list):
                            for address in data:
                                if isinstance(address, str) and address.startswith('0x') and len(address) == 42:
                                    addresses.append(GitHubAddressData(
                                        address=address,
                                        blockchain='ethereum',
                                        source_system='ofac_addresses_repo',
                                        initial_label='OFAC Sanctioned',
                                        confidence_score=0.9,
                                        repository_url=self.repo_url,
                                        metadata={'source_file': file_path, 'risk_level': 'high'}
                                    ))
                        
                        elif isinstance(data, dict):
                            for address, info in data.items():
                                if address.startswith('0x') and len(address) == 42:
                                    label = 'OFAC Sanctioned'
                                    if isinstance(info, dict) and 'name' in info:
                                        label = f"OFAC Sanctioned - {info['name']}"
                                    elif isinstance(info, str):
                                        label = f"OFAC Sanctioned - {info}"
                                    
                                    addresses.append(GitHubAddressData(
                                        address=address,
                                        blockchain='ethereum',
                                        source_system='ofac_addresses_repo',
                                        initial_label=label,
                                        confidence_score=0.9,
                                        repository_url=self.repo_url,
                                        metadata={'source_file': file_path, 'risk_level': 'high', 'details': info}
                                    ))
                        
                        # If we found data, break out of the loop
                        if addresses:
                            break
                            
                except requests.exceptions.RequestException:
                    continue  # Try next file path
                except json.JSONDecodeError:
                    continue  # Try next file path
            
            # If direct download didn't work, try a simple hardcoded list of known OFAC addresses
            if not addresses:
                self.logger.warning("Could not download OFAC file, using known addresses")
                known_ofac_addresses = [
                    "0x8576acc5c05d6ce88f4e49bf65bdf0c62f91353c",  # Tornado Cash
                    "0xd90e2f925da726b50c4ed8d0fb90ad053324f31b",  # Tornado Cash
                    "0xd96f2b1c14db8458374d9aca76e26c3d18364307",  # Tornado Cash
                    "0x4736dcf1b7a3d580672cce6e7c65cd5cc9cfba9d",  # Tornado Cash
                    "0xd4b88df4d29f5cedd6857912842cff3b20c8cfa3",  # Tornado Cash
                ]
                
                for address in known_ofac_addresses:
                    addresses.append(GitHubAddressData(
                        address=address,
                        blockchain='ethereum',
                        source_system='ofac_addresses_repo',
                        initial_label='OFAC Sanctioned (Known)',
                        confidence_score=0.9,
                        repository_url=self.repo_url,
                        metadata={'source_file': 'hardcoded_known', 'risk_level': 'high'}
                    ))
            
            self.logger.info(f"Extracted {len(addresses)} OFAC addresses")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to extract OFAC addresses: {e}")
            return []


class ENSTwitterExtractor(GitHubRepositoryExtractor):
    """Extractor for ultrasoundmoney/ens_twitter_accounts repository."""
    
    def __init__(self):
        super().__init__("https://github.com/ultrasoundmoney/ens_twitter_accounts")
    
    def extract_addresses(self) -> List[GitHubAddressData]:
        """Extract ENS-Twitter linked addresses."""
        addresses = []
        
        try:
            self.clone_repository()
            
            # Look for data files
            data_files = []
            for root, dirs, files in os.walk(self.repo_path):
                for file in files:
                    if file.endswith(('.json', '.csv')):
                        data_files.append(os.path.join(root, file))
            
            for data_file in data_files:
                try:
                    if data_file.endswith('.json'):
                        with open(data_file, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        
                        if isinstance(data, list):
                            for item in data:
                                if isinstance(item, dict):
                                    address = item.get('address') or item.get('wallet')
                                    ens_name = item.get('ens') or item.get('ens_name')
                                    twitter = item.get('twitter') or item.get('twitter_handle')
                                    
                                    if address and address.startswith('0x') and len(address) == 42:
                                        label = 'ENS-Twitter Linked'
                                        if ens_name:
                                            label = f"ENS: {ens_name}"
                                        
                                        addresses.append(GitHubAddressData(
                                            address=address,
                                            blockchain='ethereum',
                                            source_system='ens_twitter_repo',
                                            initial_label=label,
                                            repository_url=self.repo_url,
                                            metadata={
                                                'ens_name': ens_name,
                                                'twitter_handle': twitter,
                                                'source_file': os.path.basename(data_file)
                                            }
                                        ))
                        
                        elif isinstance(data, dict):
                            for key, value in data.items():
                                if isinstance(value, dict):
                                    address = value.get('address') or key
                                    if address and address.startswith('0x') and len(address) == 42:
                                        ens_name = value.get('ens') or value.get('ens_name')
                                        twitter = value.get('twitter') or value.get('twitter_handle')
                                        
                                        label = 'ENS-Twitter Linked'
                                        if ens_name:
                                            label = f"ENS: {ens_name}"
                                        
                                        addresses.append(GitHubAddressData(
                                            address=address,
                                            blockchain='ethereum',
                                            source_system='ens_twitter_repo',
                                            initial_label=label,
                                            repository_url=self.repo_url,
                                            metadata={
                                                'ens_name': ens_name,
                                                'twitter_handle': twitter,
                                                'source_file': os.path.basename(data_file)
                                            }
                                        ))
                    
                    elif data_file.endswith('.csv'):
                        df = pd.read_csv(data_file)
                        
                        for _, row in df.iterrows():
                            address = None
                            ens_name = None
                            twitter = None
                            
                            # Look for address columns
                            for col in ['address', 'Address', 'wallet', 'Wallet']:
                                if col in df.columns:
                                    address = str(row[col]).strip()
                                    break
                            
                            # Look for ENS columns
                            for col in ['ens', 'ens_name', 'ENS', 'domain']:
                                if col in df.columns:
                                    ens_name = str(row[col]).strip()
                                    break
                            
                            # Look for Twitter columns
                            for col in ['twitter', 'Twitter', 'twitter_handle', 'handle']:
                                if col in df.columns:
                                    twitter = str(row[col]).strip()
                                    break
                            
                            if address and address.startswith('0x') and len(address) == 42:
                                label = 'ENS-Twitter Linked'
                                if ens_name and ens_name != 'nan':
                                    label = f"ENS: {ens_name}"
                                
                                addresses.append(GitHubAddressData(
                                    address=address,
                                    blockchain='ethereum',
                                    source_system='ens_twitter_repo',
                                    initial_label=label,
                                    repository_url=self.repo_url,
                                    metadata={
                                        'ens_name': ens_name if ens_name != 'nan' else None,
                                        'twitter_handle': twitter if twitter != 'nan' else None,
                                        'source_file': os.path.basename(data_file)
                                    }
                                ))
                
                except Exception as e:
                    self.logger.error(f"Failed to process file {data_file}: {e}")
                    continue
            
            self.logger.info(f"Extracted {len(addresses)} ENS-Twitter addresses")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to extract ENS-Twitter addresses: {e}")
            return []
        finally:
            self.cleanup()


class SybilListExtractor(GitHubRepositoryExtractor):
    """Extractor for Uniswap/sybil-list repository."""
    
    def __init__(self):
        super().__init__("https://github.com/Uniswap/sybil-list")
    
    def extract_addresses(self) -> List[GitHubAddressData]:
        """Extract verified addresses from Uniswap Sybil list."""
        addresses = []
        
        try:
            # Try to download the main sybil list file
            try:
                sybil_file = self.download_file("verified.json")
                
                with open(sybil_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if isinstance(data, dict) and 'addresses' in data:
                    for address_info in data['addresses']:
                        address = address_info.get('address')
                        if address and address.startswith('0x') and len(address) == 42:
                            name = address_info.get('name', 'Verified Entity')
                            
                            addresses.append(GitHubAddressData(
                                address=address,
                                blockchain='ethereum',
                                source_system='sybil_list_repo',
                                initial_label=f"Verified: {name}",
                                confidence_score=0.8,
                                repository_url=self.repo_url,
                                metadata={
                                    'entity_name': name,
                                    'verification_source': 'uniswap_sybil',
                                    'source_file': 'verified.json'
                                }
                            ))
            
            except:
                # Fallback: clone and search
                self.clone_repository()
                
                json_files = []
                for root, dirs, files in os.walk(self.repo_path):
                    for file in files:
                        if file.endswith('.json'):
                            json_files.append(os.path.join(root, file))
                
                for json_file in json_files:
                    try:
                        with open(json_file, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        
                        # Handle different JSON structures
                        if isinstance(data, dict):
                            if 'addresses' in data:
                                for address_info in data['addresses']:
                                    address = address_info.get('address')
                                    if address and address.startswith('0x') and len(address) == 42:
                                        name = address_info.get('name', 'Verified Entity')
                                        
                                        addresses.append(GitHubAddressData(
                                            address=address,
                                            blockchain='ethereum',
                                            source_system='sybil_list_repo',
                                            initial_label=f"Verified: {name}",
                                            confidence_score=0.8,
                                            repository_url=self.repo_url,
                                            metadata={
                                                'entity_name': name,
                                                'verification_source': 'uniswap_sybil',
                                                'source_file': os.path.basename(json_file)
                                            }
                                        ))
                            
                            else:
                                # Direct address mapping
                                for address, info in data.items():
                                    if address.startswith('0x') and len(address) == 42:
                                        name = info if isinstance(info, str) else info.get('name', 'Verified Entity')
                                        
                                        addresses.append(GitHubAddressData(
                                            address=address,
                                            blockchain='ethereum',
                                            source_system='sybil_list_repo',
                                            initial_label=f"Verified: {name}",
                                            confidence_score=0.8,
                                            repository_url=self.repo_url,
                                            metadata={
                                                'entity_name': name,
                                                'verification_source': 'uniswap_sybil',
                                                'source_file': os.path.basename(json_file)
                                            }
                                        ))
                    
                    except Exception as e:
                        self.logger.error(f"Failed to process JSON file {json_file}: {e}")
                        continue
            
            self.logger.info(f"Extracted {len(addresses)} Sybil list addresses")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to extract Sybil list addresses: {e}")
            return []
        finally:
            self.cleanup()


class EllipticPlusPlusExtractor(GitHubRepositoryExtractor):
    """Extractor for git-disl/EllipticPlusPlus repository."""
    
    def __init__(self):
        super().__init__("https://github.com/git-disl/EllipticPlusPlus")
    
    def extract_addresses(self) -> List[GitHubAddressData]:
        """Extract addresses from EllipticPlusPlus dataset."""
        addresses = []
        
        try:
            self.clone_repository()
            
            # Look for CSV and JSON files
            data_files = []
            for root, dirs, files in os.walk(self.repo_path):
                for file in files:
                    if file.endswith(('.csv', '.json')):
                        data_files.append(os.path.join(root, file))
            
            for data_file in data_files:
                try:
                    if data_file.endswith('.csv'):
                        df = pd.read_csv(data_file)
                        
                        # Look for address-like columns
                        for col in df.columns:
                            if 'address' in col.lower() or 'hash' in col.lower():
                                for _, row in df.iterrows():
                                    address = str(row[col]).strip()
                                    
                                    # Check for Bitcoin or Ethereum address formats
                                    if ((address.startswith('1') or address.startswith('3') or address.startswith('bc1')) and 
                                        len(address) >= 26 and len(address) <= 62):
                                        # Bitcoin address
                                        label = 'Bitcoin Address'
                                        if 'class' in df.columns:
                                            label = f"Bitcoin - {row['class']}"
                                        
                                        addresses.append(GitHubAddressData(
                                            address=address,
                                            blockchain='bitcoin',
                                            source_system='elliptic_plus_plus_repo',
                                            initial_label=label,
                                            repository_url=self.repo_url,
                                            metadata={
                                                'source_file': os.path.basename(data_file),
                                                'row_data': row.to_dict()
                                            }
                                        ))
                                    
                                    elif address.startswith('0x') and len(address) == 42:
                                        # Ethereum address
                                        label = 'Ethereum Address'
                                        if 'class' in df.columns:
                                            label = f"Ethereum - {row['class']}"
                                        
                                        addresses.append(GitHubAddressData(
                                            address=address,
                                            blockchain='ethereum',
                                            source_system='elliptic_plus_plus_repo',
                                            initial_label=label,
                                            repository_url=self.repo_url,
                                            metadata={
                                                'source_file': os.path.basename(data_file),
                                                'row_data': row.to_dict()
                                            }
                                        ))
                    
                    elif data_file.endswith('.json'):
                        with open(data_file, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        
                        # Process JSON data recursively
                        def extract_from_json(obj, path=""):
                            if isinstance(obj, dict):
                                for key, value in obj.items():
                                    if isinstance(value, str):
                                        # Check if it's an address
                                        if ((value.startswith('1') or value.startswith('3') or value.startswith('bc1')) and 
                                            len(value) >= 26 and len(value) <= 62):
                                            addresses.append(GitHubAddressData(
                                                address=value,
                                                blockchain='bitcoin',
                                                source_system='elliptic_plus_plus_repo',
                                                initial_label=f"Bitcoin - {key}",
                                                repository_url=self.repo_url,
                                                metadata={
                                                    'source_file': os.path.basename(data_file),
                                                    'json_path': f"{path}.{key}" if path else key
                                                }
                                            ))
                                        
                                        elif value.startswith('0x') and len(value) == 42:
                                            addresses.append(GitHubAddressData(
                                                address=value,
                                                blockchain='ethereum',
                                                source_system='elliptic_plus_plus_repo',
                                                initial_label=f"Ethereum - {key}",
                                                repository_url=self.repo_url,
                                                metadata={
                                                    'source_file': os.path.basename(data_file),
                                                    'json_path': f"{path}.{key}" if path else key
                                                }
                                            ))
                                    else:
                                        extract_from_json(value, f"{path}.{key}" if path else key)
                            
                            elif isinstance(obj, list):
                                for i, item in enumerate(obj):
                                    extract_from_json(item, f"{path}[{i}]")
                        
                        extract_from_json(data)
                
                except Exception as e:
                    self.logger.error(f"Failed to process file {data_file}: {e}")
                    continue
            
            self.logger.info(f"Extracted {len(addresses)} addresses from EllipticPlusPlus")
            return addresses
            
        except Exception as e:
            self.logger.error(f"Failed to extract from EllipticPlusPlus: {e}")
            return []
        finally:
            self.cleanup()


class GitHubDataManager:
    """Manager class to coordinate all GitHub repository extractions."""
    
    def __init__(self, temp_dir: Optional[str] = None):
        self.temp_dir = temp_dir or tempfile.mkdtemp()
        self.logger = logging.getLogger(f"{__name__}.GitHubDataManager")
        
        # Initialize extractors
        self.extractors = {
            'etherscan_labels': EtherscanLabelsExtractor(),
            'eth_labels': EthLabelsExtractor(),
            'ofac_addresses': OFACAddressesExtractor(),
            'ens_twitter': ENSTwitterExtractor(),
            'sybil_list': SybilListExtractor(),
            'elliptic_plus_plus': EllipticPlusPlusExtractor()
        }
    
    def extract_all_repositories(self) -> List[GitHubAddressData]:
        """Extract addresses from all GitHub repositories."""
        all_addresses = []
        
        for name, extractor in self.extractors.items():
            try:
                self.logger.info(f"Extracting from {name}...")
                addresses = extractor.extract_addresses()
                all_addresses.extend(addresses)
                self.logger.info(f"Extracted {len(addresses)} addresses from {name}")
                
            except Exception as e:
                self.logger.error(f"Failed to extract from {name}: {e}")
                continue
        
        self.logger.info(f"Total addresses extracted from GitHub repositories: {len(all_addresses)}")
        return all_addresses
    
    def extract_specific_repositories(self, repo_names: List[str]) -> List[GitHubAddressData]:
        """Extract addresses from specific repositories."""
        addresses = []
        
        for repo_name in repo_names:
            if repo_name in self.extractors:
                try:
                    self.logger.info(f"Extracting from {repo_name}...")
                    repo_addresses = self.extractors[repo_name].extract_addresses()
                    addresses.extend(repo_addresses)
                    self.logger.info(f"Extracted {len(repo_addresses)} addresses from {repo_name}")
                    
                except Exception as e:
                    self.logger.error(f"Failed to extract from {repo_name}: {e}")
                    continue
            else:
                self.logger.warning(f"Unknown repository: {repo_name}")
        
        return addresses
    
    def cleanup(self):
        """Clean up temporary files."""
        try:
            if os.path.exists(self.temp_dir):
                shutil.rmtree(self.temp_dir)
            self.logger.info("Cleaned up GitHub extraction temporary files")
        except Exception as e:
            self.logger.error(f"Failed to cleanup GitHub extraction files: {e}")


# Convenience function for easy usage
def extract_github_addresses(repo_names: Optional[List[str]] = None) -> List[GitHubAddressData]:
    """
    Extract addresses from GitHub repositories.
    
    Args:
        repo_names: List of specific repository names to extract from.
                   If None, extracts from all repositories.
    
    Returns:
        List of GitHubAddressData objects
    """
    manager = GitHubDataManager()
    
    try:
        if repo_names:
            return manager.extract_specific_repositories(repo_names)
        else:
            return manager.extract_all_repositories()
    finally:
        manager.cleanup() 
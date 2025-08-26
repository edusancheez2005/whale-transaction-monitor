#!/usr/bin/env python3
"""
Emergency surgical fix for evm_parser.py indentation issues.
"""

# Read the file
with open('utils/evm_parser.py', 'r') as f:
    lines = f.readlines()

# Fix specific lines with incorrect indentation
fixes = [
    (957, "            pair_key = pair_address.lower()\n"),  # Inside try block
    (958, "            if pair_key in known_pairs:\n"),      # Inside try block
    (959, "                logger.debug(f\"✅ Found cached pair info for {pair_address}: {known_pairs[pair_key]}\")\n"),  # Inside if
    (960, "                return known_pairs[pair_key]\n"), # Inside if
    (961, "            \n"),                                 # Empty line, properly indented
    (962, "            # 🚀 NEW: Try to get token info via Web3 calls for unknown pairs\n"),  # Comment, inside try
]

# Apply fixes
for line_num, new_content in fixes:
    if line_num <= len(lines):
        lines[line_num - 1] = new_content

# Write the fixed file
with open('utils/evm_parser.py', 'w') as f:
    f.writelines(lines)

print("✅ Emergency indentation fixes applied")

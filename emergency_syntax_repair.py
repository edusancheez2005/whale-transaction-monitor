#!/usr/bin/env python3
"""Emergency syntax repair for critical files"""

# Fix test_live_transactions.py indentation issues
with open('test_live_transactions.py', 'r') as f:
    lines = f.readlines()

# Fix specific problematic lines
fixes = [
    (158, "                    # Try to get token transactions if internal fails\n"),
    (159, "                    if endpoint_index == 1:\n"),
    (160, "                        params['action'] = 'tokentx'\n"),
    (161, "                        params['contractaddress'] = token_contract\n"),
]

for line_num, new_content in fixes:
    if line_num <= len(lines):
        lines[line_num - 1] = new_content

with open('test_live_transactions.py', 'w') as f:
    f.writelines(lines)

print("✅ test_live_transactions.py repaired")

# Test compilation
import py_compile
try:
    py_compile.compile('test_live_transactions.py', doraise=True)
    print("✅ test_live_transactions.py syntax VALIDATED")
except Exception as e:
    print(f"❌ test_live_transactions.py still has errors: {e}")

try:
    py_compile.compile('utils/classification.py', doraise=True)
    print("✅ utils/classification.py syntax VALIDATED")
except Exception as e:
    print(f"❌ utils/classification.py still has errors: {e}")

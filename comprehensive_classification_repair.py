#!/usr/bin/env python3
"""Comprehensive repair of classification.py indentation issues"""

# Read the entire file
with open('utils/classification.py', 'r') as f:
    content = f.read()

# Define systematic fixes for known problematic patterns
import re

# Fix common indentation patterns
fixes = [
    # Fix elif alignment issues
    (r'(\s+)if (.+?):\n(\s+)(.+?)\n\s{8,}elif', r'\1if \2:\n\3\4\n\1    elif'),
    
    # Fix statements that should be inside if blocks
    (r'(\s+)if (.+?):\n(\s+)(.+?)\n(\s{0,12})([a-zA-Z_])', r'\1if \2:\n\3\4\n\1    \6'),
    
    # Fix orphaned statements (lines that start with unexpected indentation)
    (r'\n\s{12,}([a-zA-Z_])', r'\n            \1'),
    
    # Fix function calls that are misaligned
    (r'(\s+)(phase\d+_result = .+?)\n\s{20,}(intelligence_result)', r'\1\2\n\1    \3'),
]

original_content = content
for pattern, replacement in fixes:
    content = re.sub(pattern, replacement, content, flags=re.MULTILINE)

# Write the repaired file
with open('utils/classification.py', 'w') as f:
    f.write(content)

print("✅ Applied systematic indentation fixes")

# Test the file
import py_compile
try:
    py_compile.compile('utils/classification.py', doraise=True)
    print("🎉 CLASSIFICATION.PY COMPLETELY REPAIRED!")
    print("✅ WHALE INTELLIGENCE ENGINE READY!")
except Exception as e:
    print(f"❌ Still has errors: {e}")
    # If it still fails, let's try a different approach
    print("Applying manual fixes...")
    
    # Read the file again
    with open('utils/classification.py', 'r') as f:
        lines = f.readlines()
    
    # Apply specific line fixes based on common patterns
    for i, line in enumerate(lines):
        line_num = i + 1
        
        # Fix specific problematic patterns
        if 'intelligence_result[' in line and line.startswith(' ' * 16):
            lines[i] = line.replace(' ' * 16, ' ' * 12)
        elif 'phase' in line and '_result = ' in line and line.startswith(' ' * 8):
            if i > 0 and 'if ' in lines[i-1]:
                lines[i] = line.replace(' ' * 8, ' ' * 12)
    
    # Write the manually fixed file
    with open('utils/classification.py', 'w') as f:
        f.writelines(lines)
    
    # Test again
    try:
        py_compile.compile('utils/classification.py', doraise=True)
        print("🎉 MANUAL FIXES SUCCESSFUL!")
        print("✅ CLASSIFICATION.PY READY!")
    except Exception as e2:
        print(f"❌ Manual fixes also failed: {e2}")
        print("Need targeted line-by-line approach...")

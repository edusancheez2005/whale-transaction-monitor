# Fix indentation in utils/classification_final.py
with open('utils/classification_final.py', 'r') as f:
    lines = f.readlines()

# Fix the specific problematic section around line 4362-4380
for i in range(len(lines)):
    line_num = i + 1
    
    # Fix line 4362 - should be properly indented inside try block
    if line_num == 4362:
        lines[i] = '            # Fetch comprehensive market data\n'
    # Fix lines 4363-4365 - should be properly indented inside try block  
    elif line_num in [4363, 4364, 4365]:
        lines[i] = '            ' + lines[i].lstrip()
    # Fix line 4367 onwards - should be properly indented inside try block
    elif line_num >= 4367 and line_num <= 4400:
        if lines[i].strip() and not lines[i].startswith('        '):
            lines[i] = '            ' + lines[i].lstrip()

# Write back the fixed content
with open('utils/classification_final.py', 'w') as f:
    f.writelines(lines)

print("Fixed indentation issues in classification_final.py")

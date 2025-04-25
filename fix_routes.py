#!/usr/bin/env python3
"""
Quick script to fix route parameter issues in routes.py
"""

import re

# Read the file
with open('web/routes.py', 'r') as f:
    content = f.read()

# Fix the route parameters
content = content.replace("'/profiles/edit/<n>'", "'/profiles/edit/<name>'")
content = content.replace("'/profiles/delete/<n>'", "'/profiles/delete/<name>'")

# Write back to file
with open('web/routes.py', 'w') as f:
    f.write(content)

print("Routes fixed!")

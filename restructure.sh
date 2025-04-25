#!/bin/bash
# Script to restructure the codebase

# Step 1: Create the directory structure (already done)
echo "Ensuring directory structure exists..."
mkdir -p cli connectors core storage templates static/css static/js web uploads

# Step 2: Copy files to new structure
echo "Copying files to new structure..."

# Copy templates
echo "  - Copying templates..."
cp -r csv_to_iceberg/web/templates/* templates/

# Copy CLI module
echo "  - Copying CLI module..."
cp csv_to_iceberg/cli/*.py cli/

# Copy connectors module
echo "  - Copying connectors module..."
cp csv_to_iceberg/connectors/*.py connectors/

# Copy core module
echo "  - Copying core module..."
cp csv_to_iceberg/core/*.py core/

# Copy storage module
echo "  - Copying storage module..."
cp csv_to_iceberg/storage/*.py storage/

# Copy web module
echo "  - Copying web module..."
cp csv_to_iceberg/web/*.py web/

# Step 3: Update imports in all files (this will be done manually after copying)
echo "Files copied. You'll need to update the imports manually."

# Step 4: Create symlinks for backward compatibility
echo "Creating symlinks for backward compatibility..."
ln -sf cli_main.py csv_to_iceberg.py

echo "Restructuring complete!"
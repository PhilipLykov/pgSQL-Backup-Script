#!/bin/bash
# Quick fix for config directory permissions
# Run as root: bash FIX_CONFIG_PERMISSIONS.sh

set -e

CONFIG_DIR="/etc/pgbackup"
POSTGRES_USER="postgres"
POSTGRES_GROUP="postgres"

echo "Fixing config directory permissions..."

# Ensure directory exists
mkdir -p "$CONFIG_DIR"

# Set group ownership
chgrp "$POSTGRES_GROUP" "$CONFIG_DIR"

# Set directory permissions (750 = owner rwx, group rx)
chmod 750 "$CONFIG_DIR"

# Fix file permissions if they exist
if [ -f "$CONFIG_DIR/.encryption_key" ]; then
    chgrp "$POSTGRES_GROUP" "$CONFIG_DIR/.encryption_key"
    chmod 640 "$CONFIG_DIR/.encryption_key"
    echo "✓ Fixed .encryption_key permissions"
fi

if [ -f "$CONFIG_DIR/config.enc" ]; then
    chgrp "$POSTGRES_GROUP" "$CONFIG_DIR/config.enc"
    chmod 640 "$CONFIG_DIR/config.enc"
    echo "✓ Fixed config.enc permissions"
fi

echo ""
echo "Verifying permissions:"
ls -la "$CONFIG_DIR"

echo ""
echo "✓ Config directory permissions fixed!"
echo "The postgres user should now be able to read the config files."


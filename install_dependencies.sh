#!/bin/bash
# Install Python dependencies for PostgreSQL Backup Script
# For Debian 13 with externally-managed environment

set -e

echo "Installing Python dependencies for PostgreSQL Backup Script..."
echo ""

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo "ERROR: This script must be run as root (use sudo)"
    exit 1
fi

# Install cryptography via apt
echo "Installing python3-cryptography via apt..."
apt-get update
apt-get install -y python3-cryptography

# Check if cryptography is installed
if python3 -c "import cryptography" 2>/dev/null; then
    echo "✓ cryptography is installed"
else
    echo "ERROR: Failed to import cryptography"
    exit 1
fi

# Install azure-storage-blob
echo ""
echo "Installing azure-storage-blob..."
echo "Note: Using --break-system-packages flag for system-wide installation"
echo "This is acceptable for system services running as root."
echo ""

pip3 install --break-system-packages --root-user-action=ignore azure-storage-blob>=12.19.0

# Verify installation
echo ""
echo "Verifying installation..."
if python3 -c "from azure.storage.blob import BlobServiceClient" 2>/dev/null; then
    echo "✓ azure-storage-blob is installed"
else
    echo "ERROR: Failed to import azure.storage.blob"
    exit 1
fi

if python3 -c "from cryptography.fernet import Fernet" 2>/dev/null; then
    echo "✓ cryptography.fernet is available"
else
    echo "ERROR: Failed to import cryptography.fernet"
    exit 1
fi

echo ""
echo "✓ All dependencies installed successfully!"
echo ""
echo "Next steps:"
echo "1. cd /opt/pgSQL-bck-script"
echo "2. sudo python3 pg_backup_setup.py"


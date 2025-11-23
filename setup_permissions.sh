#!/bin/bash
# Setup permissions for PostgreSQL backup script
# This is needed when using Unix socket authentication (running as postgres user)

set -e

echo "=========================================="
echo "PostgreSQL Backup Permissions Setup"
echo "=========================================="
echo ""

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo "ERROR: This script must be run as root (use sudo)"
    exit 1
fi

LOG_DIR="/var/log/pgbackup"
BACKUP_DIR="/var/backups/pgbackup"
CONFIG_DIR="/etc/pgbackup"

echo "Setting up permissions for backup script..."
echo ""

# Get postgres user and group
POSTGRES_USER="postgres"
POSTGRES_GROUP="postgres"

# Check if postgres user exists
if ! id "$POSTGRES_USER" &>/dev/null; then
    echo "WARNING: User '$POSTGRES_USER' not found. Please specify PostgreSQL user:"
    read -p "PostgreSQL user [postgres]: " POSTGRES_USER
    POSTGRES_USER=${POSTGRES_USER:-postgres}
    
    if ! id "$POSTGRES_USER" &>/dev/null; then
        echo "ERROR: User '$POSTGRES_USER' does not exist"
        exit 1
    fi
fi

# Get group for postgres user
POSTGRES_GROUP=$(id -gn "$POSTGRES_USER")

echo "Using PostgreSQL user: $POSTGRES_USER"
echo "Using PostgreSQL group: $POSTGRES_GROUP"
echo ""

# Create directories if they don't exist
echo "Creating directories..."
mkdir -p "$LOG_DIR"
mkdir -p "$BACKUP_DIR"
mkdir -p "$CONFIG_DIR"

# Set ownership and permissions for log directory
echo "Setting permissions for log directory: $LOG_DIR"
chown -R "$POSTGRES_USER:$POSTGRES_GROUP" "$LOG_DIR"
chmod 755 "$LOG_DIR"
echo "✓ Log directory permissions set"

# Set ownership and permissions for backup directory
echo "Setting permissions for backup directory: $BACKUP_DIR"
chown -R "$POSTGRES_USER:$POSTGRES_GROUP" "$BACKUP_DIR"
chmod 750 "$BACKUP_DIR"
echo "✓ Backup directory permissions set"

# Create temp subdirectory
mkdir -p "$BACKUP_DIR/tmp"
chown -R "$POSTGRES_USER:$POSTGRES_GROUP" "$BACKUP_DIR/tmp"
chmod 750 "$BACKUP_DIR/tmp"
echo "✓ Temp directory permissions set"

# Clean up any existing lock files (from previous root runs)
if [ -f "$BACKUP_DIR/pgbackup.lock" ]; then
    echo "Removing existing lock file..."
    rm -f "$BACKUP_DIR/pgbackup.lock"
    echo "✓ Lock file cleaned up"
fi
if [ -f "/tmp/pgbackup.lock" ]; then
    echo "Removing existing fallback lock file..."
    rm -f "/tmp/pgbackup.lock"
    echo "✓ Fallback lock file cleaned up"
fi

# Config directory: readable by postgres user, writable only by root
echo "Setting permissions for config directory: $CONFIG_DIR"
# First, ensure directory exists
mkdir -p "$CONFIG_DIR"
# Set group ownership to postgres group so postgres user can read
chgrp "$POSTGRES_GROUP" "$CONFIG_DIR"
# Make directory readable by postgres user (750 = owner read/write/execute, group read/execute)
chmod 750 "$CONFIG_DIR"
# Ensure files are readable by postgres user
if [ -f "$CONFIG_DIR/.encryption_key" ]; then
    chgrp "$POSTGRES_GROUP" "$CONFIG_DIR/.encryption_key"
    chmod 640 "$CONFIG_DIR/.encryption_key"  # owner read/write, group read
fi
if [ -f "$CONFIG_DIR/config.enc" ]; then
    chgrp "$POSTGRES_GROUP" "$CONFIG_DIR/config.enc"
    chmod 640 "$CONFIG_DIR/config.enc"  # owner read/write, group read
fi
echo "✓ Config directory permissions set (readable by $POSTGRES_USER, writable only by root)"

echo ""
echo "=========================================="
echo "✓ Permissions setup complete!"
echo "=========================================="
echo ""
echo "The backup script can now run as $POSTGRES_USER user."
echo ""
echo "Test it with:"
echo "  sudo -u $POSTGRES_USER python3 /opt/pgSQL-bck-script/pg_backup_main.py"
echo ""


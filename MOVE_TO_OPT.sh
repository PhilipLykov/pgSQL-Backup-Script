#!/bin/bash
# Script to move backup scripts to /opt directory
# Run with: sudo bash MOVE_TO_OPT.sh

set -e

SOURCE_DIR="/path/to/pgSQL-bck-script"
TARGET_DIR="/opt/pgSQL-bck-script"

echo "Moving PostgreSQL backup scripts to /opt..."
echo "Source: $SOURCE_DIR"
echo "Target: $TARGET_DIR"

# Create target directory
mkdir -p "$TARGET_DIR"

# Copy all files
cp -r "$SOURCE_DIR"/* "$TARGET_DIR/"

# Set ownership and permissions
chown -R root:root "$TARGET_DIR"
chmod +x "$TARGET_DIR"/pg_backup_*.py "$TARGET_DIR"/pg_backup_cron.sh "$TARGET_DIR"/install_dependencies.sh "$TARGET_DIR"/setup_pg_auth.sh "$TARGET_DIR"/setup_permissions.sh
chmod 644 "$TARGET_DIR"/*.md "$TARGET_DIR"/*.txt "$TARGET_DIR"/*.service "$TARGET_DIR"/*.timer

echo "Files moved successfully!"
echo ""
echo "Next steps:"
echo "1. cd $TARGET_DIR"
echo "2. bash install_dependencies.sh"
echo "3. python3 pg_backup_setup.py"
echo "4. Set up cron job or systemd timer"

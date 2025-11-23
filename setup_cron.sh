#!/bin/bash
# Automated cron setup script for PostgreSQL backup
# This script sets up the cron job to run backups daily at 1:00 AM

set -e

SCRIPT_DIR="/opt/pgSQL-bck-script"
CRON_SCRIPT="$SCRIPT_DIR/pg_backup_cron.sh"
CRON_ENTRY="0 1 * * * $CRON_SCRIPT"

echo "============================================================"
echo "Setting up PostgreSQL Backup Cron Job"
echo "============================================================"
echo ""

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "ERROR: This script must be run as root"
    echo "Usage: sudo bash setup_cron.sh"
    exit 1
fi

# Check if cron script exists
if [ ! -f "$CRON_SCRIPT" ]; then
    echo "ERROR: Cron wrapper script not found at $CRON_SCRIPT"
    echo "Please ensure the script is installed in /opt/pgSQL-bck-script/"
    exit 1
fi

# Make cron script executable
echo "Making cron script executable..."
chmod +x "$CRON_SCRIPT"
echo "✓ Cron script is executable"

# Determine which user should run the cron job
# Check if using Unix socket (postgres user) or TCP/IP (root user)
CONFIG_DIR="/etc/pgbackup"
if [ -f "$CONFIG_DIR/config.enc" ]; then
    echo ""
    echo "Checking configuration..."
    # Try to determine connection method (this is a best guess)
    # User should verify manually
    echo "Note: Please verify your connection method:"
    echo "  - Unix socket: Run cron as 'postgres' user"
    echo "  - TCP/IP with .pgpass: Can run as 'root' user"
    echo ""
fi

# Ask user which method to use
echo "Which authentication method are you using?"
echo "1) Unix socket (peer authentication) - Run as postgres user"
echo "2) TCP/IP with .pgpass file - Run as root user"
read -p "Enter choice [1 or 2]: " choice

case $choice in
    1)
        CRON_USER="postgres"
        echo ""
        echo "Setting up cron job for user: $CRON_USER"
        ;;
    2)
        CRON_USER="root"
        echo ""
        echo "Setting up cron job for user: $CRON_USER"
        ;;
    *)
        echo "Invalid choice. Exiting."
        exit 1
        ;;
esac

# Check if cron entry already exists
if sudo -u "$CRON_USER" crontab -l 2>/dev/null | grep -q "$CRON_SCRIPT"; then
    echo ""
    echo "WARNING: Cron job already exists for user $CRON_USER"
    echo "Current crontab:"
    sudo -u "$CRON_USER" crontab -l
    echo ""
    read -p "Do you want to replace it? [y/N]: " replace
    if [[ ! "$replace" =~ ^[Yy]$ ]]; then
        echo "Exiting without changes."
        exit 0
    fi
    # Remove existing entry
    sudo -u "$CRON_USER" crontab -l 2>/dev/null | grep -v "$CRON_SCRIPT" | sudo -u "$CRON_USER" crontab -
fi

# Add cron job
echo ""
echo "Adding cron job..."
(sudo -u "$CRON_USER" crontab -l 2>/dev/null; echo "$CRON_ENTRY") | sudo -u "$CRON_USER" crontab -

echo "✓ Cron job added successfully"
echo ""
echo "Current crontab for user $CRON_USER:"
sudo -u "$CRON_USER" crontab -l
echo ""
echo "============================================================"
echo "Cron job setup complete!"
echo "============================================================"
echo ""
echo "The backup will run daily at 1:00 AM"
echo ""
echo "To verify:"
echo "  sudo -u $CRON_USER crontab -l"
echo ""
echo "To test manually:"
echo "  sudo -u $CRON_USER $CRON_SCRIPT"
echo ""
echo "To check logs after first run:"
echo "  sudo tail -f /var/log/pgbackup/pgbackup_\$(date +%Y%m%d).log"
echo "  sudo tail -f /var/log/pgbackup/cron_*.log"
echo ""


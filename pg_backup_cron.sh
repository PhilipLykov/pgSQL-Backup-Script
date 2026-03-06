#!/bin/bash
# Cron wrapper script for PostgreSQL backup
# This script ensures proper environment and error handling for cron execution.
#
# NOTE: The Python backup script already writes detailed logs to
#   /var/log/pgbackup/pgbackup_YYYYMMDD.log
# This cron log only captures the wrapper's own status messages and any
# stderr from the Python process to avoid giant duplicate log files.

# Set script directory
SCRIPT_DIR="/opt/pgSQL-bck-script"
SCRIPT="$SCRIPT_DIR/pg_backup_main.py"

# Set up logging (lightweight cron-only log)
LOG_DIR="/var/log/pgbackup"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
CRON_LOG="$LOG_DIR/cron_${TIMESTAMP}.log"

# Ensure log directory exists and is writable
mkdir -p "$LOG_DIR" 2>/dev/null || {
    CRON_LOG="/tmp/pgbackup_cron_${TIMESTAMP}.log"
}

# Function to log messages
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$CRON_LOG"
}

# Check if script exists
if [ ! -f "$SCRIPT" ]; then
    log_message "ERROR: Backup script not found at $SCRIPT"
    exit 1
fi

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    log_message "ERROR: python3 not found in PATH"
    exit 1
fi

# Set PATH to include common locations (cron has minimal PATH)
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

# Set Python path if needed
export PYTHONPATH="$SCRIPT_DIR:$PYTHONPATH"

log_message "Starting PostgreSQL backup (cron job)"
log_message "User: $(whoami)"

# Execute the backup script.
# stdout goes to /dev/null (already captured by the Python logger to its own log file).
# Only stderr is captured in the cron log for debugging unexpected crashes.
if python3 "$SCRIPT" > /dev/null 2>> "$CRON_LOG"; then
    log_message "Backup completed successfully"
    exit 0
else
    EXIT_CODE=$?
    log_message "ERROR: Backup failed (exit code: $EXIT_CODE)"
    log_message "Check the main log file for details: $LOG_DIR/pgbackup_$(date +%Y%m%d).log"
    exit $EXIT_CODE
fi

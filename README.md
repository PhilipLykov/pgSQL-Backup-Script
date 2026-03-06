# PostgreSQL Backup Script

A production-ready PostgreSQL backup and maintenance script with Azure Blob Storage integration, automated scheduling, and email notifications.

## Features

- **Secure**: Encrypted configuration, password sanitization, OWASP Top 10 compliant
- **Cloud Storage**: Automatic upload to Azure Blob Storage with retry logic
- **Network-Aware**: Detects offline state and skips Azure operations instead of hanging for hours on DNS timeouts
- **Disk-Safe**: Hard cap on local backups (`MAX_LOCAL_BACKUPS`) and emergency disk-space cleanup prevent the server from running out of space during extended outages
- **Notifications**: Email alerts for backup failures and warnings
- **Automated**: Cron job and systemd timer support for scheduled backups
- **Maintenance**: Pre-backup VACUUM/CHECKPOINT and optional post-backup REINDEX
- **Health Checks**: PostgreSQL connectivity and long-running query detection
- **Retry Logic**: Failed uploads are tracked, retried, and eventually cleaned up after `MAX_UPLOAD_RETRIES`
- **Cleanup**: Automatic cleanup of old backups (local retention + Azure retention)
- **Quiet Logs**: Azure SDK verbose HTTP logging is suppressed -- logs contain only meaningful backup information

## Requirements

- Python 3.8+
- PostgreSQL 9.6+ (tested with PostgresPro 1C 17)
- Azure Storage Account
- SMTP server (optional, for email notifications)
- Debian/Ubuntu Linux (uses `fcntl` for file locking)

## Installation

### 1. Clone and Install

```bash
# Clone the repository
git clone https://github.com/PhilipLykov/pgSQL-Backup-Script.git
cd pgSQL-Backup-Script

# Move to /opt (recommended)
sudo bash MOVE_TO_OPT.sh
# Or manually:
sudo mkdir -p /opt/pgSQL-bck-script
sudo cp -r * /opt/pgSQL-bck-script/
cd /opt/pgSQL-bck-script

# Install dependencies
sudo bash install_dependencies.sh
```

**Note:** On Debian 13+, Python uses an externally-managed environment. The `install_dependencies.sh` script handles this automatically.

### 2. Run Setup Script

```bash
cd /opt/pgSQL-bck-script
sudo python3 pg_backup_setup.py
```

The setup script will guide you through:
- PostgreSQL connection method (Unix socket or TCP/IP)
- Azure Storage account name, key, and container
- Email notification settings (SMTP server, credentials)
- Databases to skip, maintenance options (VACUUM, CHECKPOINT, REINDEX)

### 3. Configure PostgreSQL Authentication

#### Option A: Unix Socket (Recommended)

- No password needed, uses peer authentication
- Script must run as `postgres` user

```bash
sudo bash setup_permissions.sh
sudo -u postgres python3 /opt/pgSQL-bck-script/pg_backup_main.py
```

#### Option B: TCP/IP with .pgpass

- Requires password authentication
- Can run as root or dedicated user

```bash
sudo bash setup_pg_auth.sh
```

All configuration is encrypted and stored in `/etc/pgbackup/config.enc`.

### 4. Set Up Automated Backups

**Option A: Cron (Recommended)**
```bash
sudo bash setup_cron.sh
```

**Option B: systemd Timer**
```bash
sudo cp pg_backup.service pg_backup.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now pg_backup.timer
```

### 5. Install Log Rotation

```bash
sudo cp pgbackup.logrotate /etc/logrotate.d/pgbackup
```

## Backup Process

1. **Lock**: Acquire file lock to prevent concurrent runs
2. **Network Check**: TCP connect to Azure endpoint (5s timeout)
3. **Health Checks**: PostgreSQL service status and connectivity
4. **Retry Failed Uploads**: Re-upload previously failed backups (skipped if offline)
5. **Pre-Backup Maintenance**: VACUUM ANALYZE + CHECKPOINT (per database)
6. **Backup**: `pg_dump` + gzip compression (per database)
7. **Upload**: Upload to Azure Blob Storage (skipped if offline, marked for retry)
8. **Post-Backup Maintenance**: Optional REINDEX (per database)
9. **Azure Cleanup**: Delete blobs older than `RETENTION_DAYS` (skipped if offline)
10. **Local Cleanup**: Retention-based + `MAX_LOCAL_BACKUPS` cap + emergency disk-space cleanup
11. **Notification**: Email summary on errors/warnings

## Constants

| Constant | Default | Description |
|----------|---------|-------------|
| `RETENTION_DAYS` | 365 | Azure blob retention (days) |
| `LOCAL_RETENTION_DAYS` | 7 | Local file retention after successful upload |
| `MAX_UPLOAD_RETRIES` | 3 | Upload retry attempts before giving up |
| `MAX_LOCAL_BACKUPS` | 30 | Hard cap on local backup files |
| `MIN_DISK_SPACE_MB` | 1024 | Emergency cleanup disk threshold (MB) |
| `NETWORK_CHECK_TIMEOUT` | 5 | Network connectivity check timeout (seconds) |

## Offline Behavior

The script checks network connectivity before any Azure operations. When offline:

- All upload retries are **skipped entirely** (no more hours of DNS timeouts)
- New backups are still created locally and marked for retry on the next run
- Azure cleanup is skipped
- Local cleanup still runs, enforcing `MAX_LOCAL_BACKUPS` and disk-space limits

## Directory Layout (on server)

```
/opt/pgSQL-bck-script/       Scripts (this repo)
/etc/pgbackup/               Encrypted config + encryption key
/var/log/pgbackup/           Log files (daily rotation recommended)
/var/backups/pgbackup/       Local backup files (.sql.gz)
/var/backups/pgbackup/tmp/   Temp files during pg_dump
```

## Project Structure

```
pgSQL-Backup-Script/
├── pg_backup_main.py          # Main backup script
├── pg_backup_config.py        # Encrypted configuration manager
├── pg_backup_setup.py         # Interactive first-time setup
├── pg_backup_cron.sh          # Lightweight cron wrapper
├── pg_backup.service          # systemd oneshot service
├── pg_backup.timer            # systemd daily timer (01:00)
├── pgbackup.logrotate         # Logrotate configuration
├── install_dependencies.sh    # Dependency installer
├── setup_permissions.sh       # Fix permissions for postgres user
├── setup_cron.sh              # Cron job installer
├── setup_pg_auth.sh           # PostgreSQL auth helper
├── FIX_CONFIG_PERMISSIONS.sh  # Quick-fix for /etc/pgbackup permissions
├── MOVE_TO_OPT.sh             # Deploy scripts to /opt
└── requirements.txt           # Python dependencies
```

## Security

- Configuration encrypted at rest (Fernet symmetric encryption)
- Passwords never logged or exposed in error messages
- Secure authentication methods (Unix socket peer auth or .pgpass)
- Input validation and SQL injection prevention on all identifiers
- OWASP Top 10 compliance

## Troubleshooting

### Permission Errors
```bash
sudo bash setup_permissions.sh
```

### Authentication Failures

**Unix Socket:** Ensure script runs as `postgres` user; verify peer auth in `pg_hba.conf`.

**TCP/IP:** Check `.pgpass` format (`hostname:port:database:username:password`), permissions (`chmod 600`), and test with `psql -h localhost -U postgres -l`.

### Azure Upload Failures
- Verify account name and key (base64-encoded from Azure Portal > Storage Account > Access Keys)
- Check network connectivity
- Review `/var/log/pgbackup/pgbackup_YYYYMMDD.log`

### Email Notifications Not Working
- For Gmail: Use App Password (https://myaccount.google.com/apppasswords)
- Verify SMTP server, port, and credentials in setup

## License

MIT

---

**Status**: Production-ready
**Last Updated**: 2026-03-06

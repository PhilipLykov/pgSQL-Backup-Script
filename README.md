# PostgreSQL Backup Script

A production-ready PostgreSQL backup and maintenance script with Azure Blob Storage integration, automated scheduling, and email notifications.

## Features

- 🔒 **Secure**: Encrypted configuration, password sanitization, OWASP Top 10 compliant
- ☁️ **Cloud Storage**: Automatic upload to Azure Blob Storage
- 📧 **Notifications**: Email alerts for backup status
- 🔄 **Automated**: Cron job support for scheduled backups
- 🛡️ **Maintenance**: Pre-backup VACUUM/CHECKPOINT and post-backup REINDEX
- 🔍 **Health Checks**: PostgreSQL connectivity and long-running query detection
- 📊 **Retry Logic**: Failed uploads are tracked and retried
- 🧹 **Cleanup**: Automatic cleanup of old backups (local and Azure)

## Requirements

- Python 3.8+
- PostgreSQL 9.6+
- Azure Storage Account (optional, for cloud backups)
- SMTP server (optional, for email notifications)

## Installation

### 1. Clone and Install

```bash
# Clone the repository
git clone https://github.com/PhilipLykov/pgSQL-Backup-Script.git
cd pgSQL-Backup-Script

# Move to /opt (recommended)
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
- Backup options (databases to backup, retention policies)
- Maintenance options (VACUUM, CHECKPOINT, REINDEX)

**Note:** During setup, you'll choose your PostgreSQL authentication method. The script can help create the `.pgpass` file if you choose TCP/IP authentication.

### 3. Configure PostgreSQL Authentication

Based on your choice during setup, complete the authentication configuration:

#### Option A: Unix Socket (Recommended)

- No password needed
- Script must run as `postgres` user
- Uses peer authentication

**Setup:**
```bash
# Set up permissions (required for postgres user)
sudo bash setup_permissions.sh

# Test (must run as postgres user)
sudo -u postgres python3 /opt/pgSQL-bck-script/pg_backup_main.py
```

#### Option B: TCP/IP with .pgpass

- Requires password authentication
- More flexible user management
- Can run as root or dedicated user

**Setup:**

The setup script can create the `.pgpass` file automatically when you choose TCP/IP authentication. If you need to create it manually:

```bash
# Create dedicated backup user (optional but recommended)
sudo bash setup_pg_auth.sh

# Or manually create .pgpass file
echo "localhost:5432:*:postgres:YOUR_PASSWORD" | sudo tee /root/.pgpass
sudo chmod 600 /root/.pgpass
```

All configuration is encrypted and stored in `/etc/pgbackup/config.enc`.

**Important:** Complete the authentication setup (Step 3) before setting up automated backups.

### 4. Set Up Automated Backups

**Option A: Automated Setup (Recommended)**
```bash
sudo bash setup_cron.sh
```

**Option B: Manual Cron Setup**

For Unix socket authentication:
```bash
sudo -u postgres crontab -e
# Add: 0 1 * * * /opt/pgSQL-bck-script/pg_backup_cron.sh
```

For TCP/IP authentication:
```bash
sudo crontab -e
# Add: 0 1 * * * /opt/pgSQL-bck-script/pg_backup_cron.sh
```

The cron job runs daily at 1:00 AM. To change the schedule, edit the crontab entry:
- `0 1 * * *` = Every day at 1:00 AM
- `0 2 * * 0` = Every Sunday at 2:00 AM
- `30 3 * * 1-5` = Monday-Friday at 3:30 AM

## Usage

### Manual Backup

**Unix Socket (as postgres user):**
```bash
sudo -u postgres python3 /opt/pgSQL-bck-script/pg_backup_main.py
```

**TCP/IP (as root):**
```bash
sudo python3 /opt/pgSQL-bck-script/pg_backup_main.py
```

### Check Logs

```bash
# Main backup log
sudo tail -f /var/log/pgbackup/pgbackup_$(date +%Y%m%d).log

# Cron execution log
sudo tail -f /var/log/pgbackup/cron_*.log
```

### Verify Backups

```bash
# Local backups
ls -lh /var/backups/pgbackup/

# Check Azure (via Azure Portal)
```

## Backup Process

The script performs the following steps:

1. **Health Checks**: Verifies PostgreSQL connectivity
2. **Pre-Backup Maintenance** (optional):
   - VACUUM ANALYZE
   - CHECKPOINT
3. **Backup**: Creates compressed backups for each database
4. **Upload**: Uploads to Azure Blob Storage (if configured)
5. **Post-Backup Maintenance** (optional):
   - REINDEX
6. **Cleanup**: Removes old backups (local and Azure)
7. **Notifications**: Sends email alerts (if configured)

## Configuration Files

- **Configuration**: `/etc/pgbackup/config.enc` (encrypted)
- **Encryption Key**: `/etc/pgbackup/.encryption_key`
- **Logs**: `/var/log/pgbackup/`
- **Backups**: `/var/backups/pgbackup/`
- **Lock File**: `/var/backups/pgbackup/pgbackup.lock`

## Security

- ✅ Configuration encrypted at rest (Fernet symmetric encryption)
- ✅ Passwords never logged or exposed
- ✅ Secure authentication methods (Unix socket or .pgpass)
- ✅ Input validation and sanitization
- ✅ OWASP Top 10 compliance

## Common Issues

### Permission Errors

```bash
sudo bash setup_permissions.sh
```

### Authentication Failures

**Unix Socket:**
- Ensure script runs as `postgres` user
- Verify peer authentication is enabled in `pg_hba.conf`

**TCP/IP:**
- Verify `.pgpass` file format: `hostname:port:database:username:password`
- Check file permissions: `chmod 600 /root/.pgpass`
- Test connection: `psql -h localhost -U postgres -l`

### Azure Upload Failures

- Verify Azure storage account name and key
- Check network connectivity
- Review error logs for details
- Ensure Azure key is base64-encoded (from Azure Portal)

### Email Notifications Not Working

- For Gmail: Use App Password, not regular password
- Generate App Password: https://myaccount.google.com/apppasswords
- Verify SMTP server and port settings

## Project Structure

```
pgSQL-bck-script/
├── pg_backup_main.py          # Main backup script
├── pg_backup_setup.py          # Interactive setup
├── pg_backup_config.py         # Configuration management
├── pg_backup_cron.sh           # Cron wrapper script
├── install_dependencies.sh    # Dependency installer
├── setup_permissions.sh        # Permission setup
├── setup_cron.sh              # Cron setup automation
├── setup_pg_auth.sh           # PostgreSQL auth helper
└── requirements.txt           # Python dependencies
```

## License

[Add your license here]

---

**Status**: ✅ Production-ready  
**Last Updated**: 2025-11-21

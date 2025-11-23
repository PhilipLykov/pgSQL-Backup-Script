#!/usr/bin/env python3
"""
PostgreSQL Backup and Maintenance Script
Performs routine checks, backups, and uploads to Azure Blob Storage
"""

import os
import sys
import subprocess
import logging
import smtplib
import ssl
import fcntl
import shutil
import re
import json
import base64
import pwd
import socket
from datetime import datetime, timedelta
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Dict, Optional, Tuple
import tempfile
import gzip

try:
    from azure.storage.blob import BlobServiceClient, BlobClient
    from azure.core.exceptions import AzureError
except ImportError:
    print("ERROR: azure-storage-blob package not installed. Run: pip3 install azure-storage-blob")
    sys.exit(1)

try:
    from cryptography.fernet import Fernet
except ImportError:
    print("ERROR: cryptography package not installed. Run: pip3 install cryptography")
    sys.exit(1)

# Add script directory to Python path for imports
SCRIPT_DIR = Path(__file__).parent.absolute()
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from pg_backup_config import SecureConfig

# Constants (SCRIPT_DIR already set above for imports)
LOG_DIR = Path("/var/log/pgbackup")
BACKUP_DIR = Path("/var/backups/pgbackup")
TEMP_DIR = BACKUP_DIR / "tmp"  # Use backup directory for temp files, not /tmp
CONFIG_DIR = Path("/etc/pgbackup")
# Lock file location: Use backup directory so postgres user can write to it
# Fallback to /var/run if backup directory not accessible
LOCK_FILE = BACKUP_DIR / "pgbackup.lock"
FAILED_UPLOADS_FILE = BACKUP_DIR / ".failed_uploads.json"
RETENTION_DAYS = 365  # 1 year (Azure)
LOCAL_RETENTION_DAYS = 7  # 7 days (local, after successful upload)
MIN_DISK_SPACE_MB = 1024  # Minimum 1GB free space required
MAX_UPLOAD_RETRIES = 3  # Maximum retry attempts for failed uploads

# Setup logging
LOG_FILE = None
try:
    # Try to create log directory if it doesn't exist
    LOG_DIR.mkdir(mode=0o755, exist_ok=True)
    # Check if we can write to the log directory
    test_file = LOG_DIR / ".write_test"
    try:
        test_file.touch()
        test_file.unlink()
        # We can write, use the standard log directory
        LOG_FILE = LOG_DIR / f"pgbackup_{datetime.now().strftime('%Y%m%d')}.log"
    except (PermissionError, OSError):
        # Can't write to /var/log/pgbackup, use fallback
        # This happens when running as postgres user (Unix socket auth)
        LOG_FILE = Path(f"/tmp/pgbackup_{datetime.now().strftime('%Y%m%d')}.log")
        print(f"WARNING: Cannot write to {LOG_DIR}, using {LOG_FILE}", file=sys.stderr)
        print("NOTE: If running as postgres user, ensure /var/log/pgbackup has proper permissions", file=sys.stderr)
except Exception as e:
    # Fallback to /tmp if log directory can't be created
    LOG_FILE = Path(f"/tmp/pgbackup_{datetime.now().strftime('%Y%m%d')}.log")
    print(f"WARNING: Could not create log directory, using {LOG_FILE}: {e}", file=sys.stderr)

# Setup logging handlers
handlers = [logging.StreamHandler(sys.stdout)]
if LOG_FILE:
    try:
        handlers.append(logging.FileHandler(LOG_FILE, mode='a'))
    except (PermissionError, OSError) as e:
        print(f"WARNING: Cannot write to log file {LOG_FILE}: {e}", file=sys.stderr)
        print("Continuing with console output only...", file=sys.stderr)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=handlers
)
logger = logging.getLogger(__name__)

class PostgreSQLBackup:
    """PostgreSQL backup and maintenance manager"""
    
    @staticmethod
    def _validate_identifier(identifier: str, identifier_type: str = "identifier") -> bool:
        """
        Validate PostgreSQL identifier to prevent SQL injection.
        PostgreSQL identifiers must start with letter or underscore and contain only
        letters, numbers, underscores, and dollar signs.
        """
        if not identifier:
            return False
        # PostgreSQL identifier rules: start with letter/underscore, then alphanumeric/underscore/dollar
        pattern = r'^[a-zA-Z_][a-zA-Z0-9_$]*$'
        if not re.match(pattern, identifier):
            logger.error(f"Invalid {identifier_type}: '{identifier}' - contains invalid characters")
            return False
        # Limit length (PostgreSQL has a limit, but we'll be conservative)
        if len(identifier) > 63:
            logger.error(f"Invalid {identifier_type}: '{identifier}' - too long (max 63 characters)")
            return False
        return True
    
    def __init__(self):
        self.config_manager = SecureConfig(str(CONFIG_DIR))
        self.config = self._load_config()
        self.errors = []
        self.warnings = []
        self.backup_dir = BACKUP_DIR
        try:
            self.backup_dir.mkdir(mode=0o750, exist_ok=True)
            # Create temp directory for backup operations (not /tmp to avoid disk space issues)
            TEMP_DIR.mkdir(mode=0o750, exist_ok=True)
        except Exception as e:
            logger.error(f"Failed to create backup directory: {str(e)}")
            raise
        self.failed_uploads = self._load_failed_uploads()
        # Get PostgreSQL user from config (default to 'postgres' for backward compatibility)
        pg_user = self.config.get('pg_user', 'postgres')
        # Validate PostgreSQL username to prevent injection
        if not self._validate_identifier(pg_user, "PostgreSQL username"):
            logger.error(f"Invalid PostgreSQL username in config: {pg_user}")
            raise ValueError(f"Invalid PostgreSQL username: {pg_user}")
        self.pg_user = pg_user
        # Get connection method: 'unix_socket' (peer auth) or 'tcp' (password auth)
        self.pg_connection_method = self.config.get('pg_connection_method', 'tcp')
    
    def _build_pg_connection_args(self, user: str, database: str = None) -> list:
        """
        Build PostgreSQL connection arguments based on configuration.
        Returns list of arguments for psql/pg_dump commands.
        
        If pg_connection_method is 'unix_socket', uses peer authentication (no password needed).
        If 'tcp', uses TCP/IP connection to localhost (requires password or .pgpass).
        
        Args:
            user: PostgreSQL username (validated)
            database: Database name (optional, validated if provided)
        """
        # Validate user to prevent injection
        if not self._validate_identifier(user, "PostgreSQL username"):
            raise ValueError(f"Invalid PostgreSQL username: {user}")
        
        # Validate database name if provided
        if database and not self._validate_identifier(database, "database name"):
            raise ValueError(f"Invalid database name: {database}")
        
        args = []
        
        if self.pg_connection_method == 'unix_socket':
            # Use Unix socket - peer authentication, no password needed
            # Don't specify -h, which makes psql use Unix socket
            args.extend(['-U', user])
        else:
            # Use TCP/IP connection - requires password or .pgpass
            args.extend(['-h', 'localhost', '-U', user])
        
        if database:
            args.extend(['-d', database])
        
        return args
    
    def _build_pg_env(self, user: str) -> dict:
        """
        Build environment variables for PostgreSQL commands.
        Sets PGPASSWORD if configured, otherwise relies on .pgpass file.
        
        SECURITY NOTE: PGPASSWORD in environment is less secure than .pgpass file
        as it can be visible in process lists. Prefer .pgpass file when possible.
        """
        env = {**os.environ, 'PGUSER': user}
        # Only set PGPASSWORD if explicitly configured (not recommended for production)
        # Prefer .pgpass file for better security
        if 'PGPASSWORD' in self.config:
            env['PGPASSWORD'] = self.config['PGPASSWORD']
            # Log warning about using environment variable (but don't log the password!)
            logger.debug("Using PGPASSWORD from config (less secure than .pgpass file)")
        return env
    
    def _sanitize_filename(self, name: str) -> str:
        """Sanitize database name for use in filename"""
        # Remove or replace characters that are problematic in filenames
        sanitized = re.sub(r'[^a-zA-Z0-9_-]', '_', name)
        # Limit length to prevent filesystem issues
        return sanitized[:50]
    
    def _check_disk_space(self, required_mb: int = MIN_DISK_SPACE_MB, warn_only: bool = False) -> bool:
        """Check if enough disk space is available
        
        Args:
            required_mb: Minimum required free space in MB
            warn_only: If True, only warn instead of failing
        """
        try:
            stat = shutil.disk_usage(self.backup_dir)
            free_mb = stat.free / (1024 * 1024)
            if free_mb < required_mb:
                error_msg = f"Insufficient disk space: {free_mb:.2f} MB free, need at least {required_mb} MB"
                if warn_only:
                    logger.warning(error_msg)
                    self.warnings.append(error_msg)
                    return True  # Continue but warn
                else:
                    logger.error(error_msg)
                    self.errors.append(error_msg)
                    return False
            logger.info(f"Disk space check: {free_mb:.2f} MB available")
            return True
        except Exception as e:
            logger.warning(f"Could not check disk space: {str(e)}")
            return True  # Continue if check fails
    
    def _acquire_lock(self) -> bool:
        """Acquire file lock to prevent concurrent execution"""
        # Store which lock file we're using
        self.actual_lock_file = LOCK_FILE
        
        try:
            # Ensure lock file directory exists and is writable
            LOCK_FILE.parent.mkdir(mode=0o755, exist_ok=True)
            # Try to create/open lock file
            self.lock_file = open(LOCK_FILE, 'w')
            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            # Write PID to lock file
            self.lock_file.write(str(os.getpid()))
            self.lock_file.flush()
            return True
        except (IOError, OSError, PermissionError) as e:
            # Check if it's a permission error vs actual lock conflict
            if "Permission denied" in str(e) or isinstance(e, PermissionError):
                # Try fallback location
                fallback_lock = Path("/tmp/pgbackup.lock")
                try:
                    self.lock_file = open(fallback_lock, 'w')
                    fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self.lock_file.write(str(os.getpid()))
                    self.lock_file.flush()
                    self.actual_lock_file = fallback_lock
                    logger.warning(f"Using fallback lock file: {fallback_lock}")
                    return True
                except (IOError, OSError) as e2:
                    logger.error(f"Cannot acquire lock (tried {LOCK_FILE} and {fallback_lock}): {str(e2)}")
                    self.errors.append("Cannot acquire lock - check permissions")
                    return False
            else:
                # Actual lock conflict (another process running)
                logger.error(f"Another backup process is already running (lock file: {LOCK_FILE}): {str(e)}")
                self.errors.append("Backup already in progress - another instance is running")
                return False
    
    def _release_lock(self):
        """Release file lock"""
        try:
            if hasattr(self, 'lock_file'):
                fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_UN)
                self.lock_file.close()
                # Remove the actual lock file we used (could be primary or fallback)
                lock_file_to_remove = getattr(self, 'actual_lock_file', LOCK_FILE)
                if lock_file_to_remove.exists():
                    lock_file_to_remove.unlink()
        except Exception as e:
            logger.warning(f"Error releasing lock: {str(e)}")
    
    def _load_failed_uploads(self) -> Dict:
        """Load list of failed uploads from previous runs"""
        if FAILED_UPLOADS_FILE.exists():
            try:
                with open(FAILED_UPLOADS_FILE, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load failed uploads list: {str(e)}")
                return {}
        return {}
    
    def _save_failed_uploads(self):
        """Save list of failed uploads for retry on next run"""
        try:
            with open(FAILED_UPLOADS_FILE, 'w') as f:
                json.dump(self.failed_uploads, f, indent=2)
            FAILED_UPLOADS_FILE.chmod(0o600)
        except Exception as e:
            logger.warning(f"Could not save failed uploads list: {str(e)}")
    
    def _mark_upload_failed(self, backup_file: Path):
        """Mark a backup file as failed to upload"""
        file_str = str(backup_file)
        if file_str not in self.failed_uploads:
            self.failed_uploads[file_str] = {
                'file': str(backup_file),
                'first_failure': datetime.now().isoformat(),
                'retry_count': 0,
                'last_attempt': datetime.now().isoformat()
            }
        else:
            self.failed_uploads[file_str]['retry_count'] += 1
            self.failed_uploads[file_str]['last_attempt'] = datetime.now().isoformat()
        self._save_failed_uploads()
    
    def _mark_upload_success(self, backup_file: Path):
        """Mark a backup file as successfully uploaded"""
        file_str = str(backup_file)
        if file_str in self.failed_uploads:
            del self.failed_uploads[file_str]
            self._save_failed_uploads()
    
    def _should_retry_upload(self, backup_file: Path) -> bool:
        """Check if a failed upload should be retried"""
        file_str = str(backup_file)
        if file_str not in self.failed_uploads:
            return False
        
        upload_info = self.failed_uploads[file_str]
        retry_count = upload_info.get('retry_count', 0)
        
        # Don't retry if exceeded max retries
        if retry_count >= MAX_UPLOAD_RETRIES:
            return False
        
        # Check if file still exists
        if not backup_file.exists():
            # File was deleted, remove from failed list
            del self.failed_uploads[file_str]
            self._save_failed_uploads()
            return False
        
        return True
    
    def _load_config(self) -> Dict:
        """Load configuration from secure storage"""
        config = self.config_manager.load_config()
        if not config:
            logger.error("Configuration not found. Please run setup script first.")
            sys.exit(1)
        return config
    
    def check_postgresql_health(self) -> Tuple[bool, List[str]]:
        """Perform routine PostgreSQL health checks"""
        logger.info("Starting PostgreSQL health checks...")
        issues = []
        
        # Check if PostgreSQL service is running
        try:
            result = subprocess.run(
                ['systemctl', 'is-active', 'postgrespro-1c-17.service'],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode != 0:
                issues.append(f"PostgreSQL service is not active: {result.stderr}")
                logger.error(f"PostgreSQL service check failed: {result.stderr}")
        except subprocess.TimeoutExpired:
            issues.append("PostgreSQL service check timed out")
            logger.error("PostgreSQL service check timed out")
        except Exception as e:
            issues.append(f"Error checking PostgreSQL service: {str(e)}")
            logger.error(f"Error checking PostgreSQL service: {str(e)}")
        
        # Check database connections
        try:
            # Use pg_isready to check if PostgreSQL is accepting connections
            pg_isready_cmd = ['pg_isready']
            if self.pg_connection_method == 'tcp':
                pg_isready_cmd.extend(['-h', 'localhost'])
            pg_isready_cmd.extend(['-U', self.pg_user])
            result = subprocess.run(
                pg_isready_cmd,
                capture_output=True,
                text=True,
                timeout=10,
                env={**os.environ, 'PGUSER': self.pg_user}
            )
            if result.returncode != 0:
                issues.append(f"PostgreSQL is not accepting connections: {result.stderr}")
                logger.error(f"PostgreSQL connection check failed: {result.stderr}")
        except Exception as e:
            issues.append(f"Error checking PostgreSQL connections: {str(e)}")
            logger.error(f"Error checking PostgreSQL connections: {str(e)}")
        
        # Check for long-running queries (optional - can be configured)
        if self.config.get('check_long_queries', False):
            try:
                query = """
                    SELECT pid, now() - pg_stat_activity.query_start AS duration, query
                    FROM pg_stat_activity
                    WHERE (now() - pg_stat_activity.query_start) > interval '1 hour'
                    AND state = 'active';
                """
                cmd = ['psql'] + self._build_pg_connection_args(self.pg_user, 'postgres') + ['-t', '-c', query]
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=30,
                    env=self._build_pg_env(self.pg_user)
                )
                if result.stdout.strip():
                    issues.append(f"Long-running queries detected:\n{result.stdout}")
                    logger.warning("Long-running queries detected")
            except Exception as e:
                logger.warning(f"Could not check for long-running queries: {str(e)}")
        
        is_healthy = len(issues) == 0
        if is_healthy:
            logger.info("PostgreSQL health checks passed")
        else:
            logger.warning(f"PostgreSQL health checks found {len(issues)} issues")
        
        return is_healthy, issues
    
    def perform_pre_backup_maintenance(self, db_name: str) -> bool:
        """Perform maintenance operations BEFORE backup (VACUUM ANALYZE, CHECKPOINT)"""
        logger.info(f"Performing pre-backup maintenance for database: {db_name}")
        
        if not self.config.get('run_maintenance', True):
            logger.info("Maintenance operations disabled in config")
            return True
        
        # Use maintenance user if configured, otherwise use backup user
        # Note: VACUUM and CHECKPOINT require superuser privileges
        maintenance_user = self.config.get('pg_maintenance_user', self.pg_user)
        # Validate maintenance user to prevent injection
        if not self._validate_identifier(maintenance_user, "maintenance username"):
            logger.error(f"Invalid maintenance username in config: {maintenance_user}")
            maintenance_user = self.pg_user  # Fallback to backup user
            logger.warning(f"Using backup user '{maintenance_user}' for maintenance operations")
        env = self._build_pg_env(maintenance_user)
        
        maintenance_errors = []
        
        # 1. VACUUM ANALYZE - Clean up dead tuples and update statistics
        # This should be done BEFORE backup for optimal backup consistency
        try:
            logger.info(f"Running VACUUM ANALYZE on {db_name} as {maintenance_user}...")
            cmd = ['psql'] + self._build_pg_connection_args(maintenance_user, db_name) + ['-c', 'VACUUM ANALYZE;']
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour timeout for large databases
                env=env
            )
            if result.returncode != 0:
                error_msg = f"VACUUM ANALYZE failed for {db_name}"
                if "password" not in result.stderr.lower() and "authentication" not in result.stderr.lower():
                    error_msg += f": {result.stderr[:200]}"
                logger.error(error_msg)
                maintenance_errors.append(error_msg)
                # Don't fail backup if VACUUM fails, but log it
            else:
                logger.info(f"VACUUM ANALYZE completed for {db_name}")
        except subprocess.TimeoutExpired:
            logger.warning(f"VACUUM ANALYZE timed out for {db_name} - continuing with backup")
            maintenance_errors.append(f"VACUUM ANALYZE timeout for {db_name}")
        except Exception as e:
            logger.warning(f"Error running VACUUM ANALYZE for {db_name}: {str(e)}")
            maintenance_errors.append(f"VACUUM ANALYZE error for {db_name}: {str(e)}")
        
        # 2. CHECKPOINT - Ensure all data is written to disk
        # This should be done BEFORE backup to ensure consistency
        try:
            logger.info(f"Running CHECKPOINT for {db_name} as {maintenance_user}...")
            cmd = ['psql'] + self._build_pg_connection_args(maintenance_user, db_name) + ['-c', 'CHECKPOINT;']
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout
                env=env
            )
            if result.returncode != 0:
                error_msg = f"CHECKPOINT failed for {db_name}"
                if "password" not in result.stderr.lower() and "authentication" not in result.stderr.lower():
                    error_msg += f": {result.stderr[:200]}"
                logger.warning(error_msg)
                # CHECKPOINT failure is less critical, just warn
            else:
                logger.info(f"CHECKPOINT completed for {db_name}")
        except Exception as e:
            logger.warning(f"Error running CHECKPOINT for {db_name}: {str(e)}")
        
        if maintenance_errors:
            self.warnings.extend(maintenance_errors)
            return False
        
        return True
    
    def perform_post_backup_maintenance(self, db_name: str) -> bool:
        """Perform optional maintenance operations AFTER backup"""
        logger.info(f"Performing post-backup maintenance for database: {db_name}")
        
        if not self.config.get('run_post_backup_maintenance', False):
            logger.debug("Post-backup maintenance disabled in config")
            return True
        
        # Use maintenance user if configured, otherwise use backup user
        # Note: REINDEX requires superuser or table owner privileges
        maintenance_user = self.config.get('pg_maintenance_user', self.pg_user)
        # Validate maintenance user to prevent injection
        if not self._validate_identifier(maintenance_user, "maintenance username"):
            logger.error(f"Invalid maintenance username in config: {maintenance_user}")
            maintenance_user = self.pg_user  # Fallback to backup user
            logger.warning(f"Using backup user '{maintenance_user}' for maintenance operations")
        env = self._build_pg_env(maintenance_user)
        
        # Optional: REINDEX if configured
        # This is typically done AFTER backup to avoid blocking backup
        if self.config.get('reindex_after_backup', False):
            # Validate database name to prevent SQL injection
            if not self._validate_identifier(db_name, "database name"):
                error_msg = f"Invalid database name for REINDEX: {db_name}"
                logger.error(error_msg)
                self.warnings.append(error_msg)
                return False
            
            try:
                logger.info(f"Running REINDEX for {db_name} as {maintenance_user}...")
                # Use psql with -c and proper quoting to prevent SQL injection
                # PostgreSQL identifiers can be safely quoted with double quotes
                # We validate the identifier first, then quote it for safety
                quoted_db_name = f'"{db_name}"'  # Quote identifier
                cmd = ['psql'] + self._build_pg_connection_args(maintenance_user, db_name) + ['-c', f'REINDEX DATABASE {quoted_db_name};']
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=7200,  # 2 hour timeout for large databases
                    env=env
                )
                if result.returncode != 0:
                    error_msg = f"REINDEX failed for {db_name}"
                    if "password" not in result.stderr.lower() and "authentication" not in result.stderr.lower():
                        error_msg += f": {result.stderr[:200]}"
                    logger.warning(error_msg)
                    self.warnings.append(error_msg)
                else:
                    logger.info(f"REINDEX completed for {db_name}")
            except Exception as e:
                logger.warning(f"Error running REINDEX for {db_name}: {str(e)}")
                self.warnings.append(f"REINDEX error for {db_name}: {str(e)}")
        
        return True
    
    def get_databases(self) -> List[str]:
        """Get list of all databases"""
        try:
            result = subprocess.run(
                ['psql'] + self._build_pg_connection_args(self.pg_user) + ['-l', '-t'],
                capture_output=True,
                text=True,
                timeout=30,
                env=self._build_pg_env(self.pg_user)
            )
            
            if result.returncode != 0:
                # Sanitize error message to avoid password leakage
                error_msg = "Failed to list databases"
                stderr_lower = result.stderr.lower()
                if "password" in stderr_lower or "authentication" in stderr_lower:
                    error_msg += " (authentication failed)"
                    error_msg += "\n  → Check PostgreSQL authentication setup:"
                    if self.pg_connection_method == 'tcp':
                        error_msg += "\n    - Verify .pgpass file exists: /root/.pgpass"
                        error_msg += f"\n    - Format: localhost:5432:*:{self.pg_user}:PASSWORD"
                        error_msg += "\n    - Run: bash setup_pg_auth.sh (or see CREATE_BACKUP_USER.md)"
                    else:
                        error_msg += "\n    - For Unix socket: script must run as PostgreSQL user"
                        error_msg += "\n    - Or configure peer authentication in pg_hba.conf"
                else:
                    error_msg += f": {result.stderr}"
                logger.error(error_msg)
                self.errors.append(error_msg)
                return []
            
            # Parse database names (skip template and system databases if configured)
            databases = []
            skip_dbs = self.config.get('skip_databases', ['template0', 'template1'])
            
            for line in result.stdout.split('\n'):
                line = line.strip()
                if line and '|' in line:
                    db_name = line.split('|')[0].strip()
                    # Validate database name to prevent injection attacks
                    if db_name and db_name not in skip_dbs:
                        if self._validate_identifier(db_name, "database name"):
                            databases.append(db_name)
                        else:
                            logger.warning(f"Skipping invalid database name: {db_name}")
            
            logger.info(f"Found {len(databases)} databases to backup: {', '.join(databases)}")
            return databases
            
        except subprocess.TimeoutExpired:
            logger.error("Timeout while listing databases")
            self.errors.append("Timeout while listing databases")
            return []
        except Exception as e:
            logger.error(f"Error listing databases: {str(e)}")
            self.errors.append(f"Error listing databases: {str(e)}")
            return []
    
    def backup_database(self, db_name: str) -> Optional[Path]:
        """Backup a single database"""
        # Check disk space before starting backup (warn only, don't fail)
        # This allows us to catch space issues early but still try the backup
        self._check_disk_space(required_mb=MIN_DISK_SPACE_MB * 2, warn_only=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')  # Add microseconds to prevent collisions
        sanitized_db_name = self._sanitize_filename(db_name)
        backup_file = self.backup_dir / f"{sanitized_db_name}_{timestamp}.sql.gz"
        
        logger.info(f"Starting backup of database: {db_name}")
        
        tmp_path = None
        try:
            # Create temporary file for uncompressed backup
            # Use backup directory temp folder instead of /tmp to avoid disk space issues
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.sql', dir=str(TEMP_DIR)) as tmp_file:
                tmp_path = tmp_file.name
                
                # Run pg_dump
                cmd = [
                    'pg_dump'
                ] + self._build_pg_connection_args(self.pg_user, db_name) + [
                    '-F', 'p',  # plain format
                    '-f', tmp_path
                ]
                
                env = self._build_pg_env(self.pg_user)
                
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=3600,  # 1 hour timeout
                    env=env
                )
                
                if result.returncode != 0:
                    # Sanitize error message
                    error_msg = f"pg_dump failed for {db_name}"
                    stderr_lower = result.stderr.lower()
                    if "password" not in stderr_lower and "authentication" not in stderr_lower:
                        error_msg += f": {result.stderr[:200]}"  # Limit length
                    
                    # Check for disk space errors
                    if "no space left" in stderr_lower or "disk full" in stderr_lower:
                        error_msg += "\n  → Disk space exhausted during backup"
                        error_msg += "\n  → Free up disk space and try again"
                        error_msg += "\n  → Consider cleaning old backups or increasing disk space"
                        # Check current disk space
                        try:
                            stat = shutil.disk_usage(self.backup_dir)
                            free_mb = stat.free / (1024 * 1024)
                            error_msg += f"\n  → Current free space: {free_mb:.2f} MB"
                        except Exception:
                            pass
                    
                    logger.error(error_msg)
                    self.errors.append(error_msg)
                    if tmp_path and os.path.exists(tmp_path):
                        os.unlink(tmp_path)
                    return None
                
                # Compress the backup
                logger.info(f"Compressing backup for {db_name}...")
                try:
                    with open(tmp_path, 'rb') as f_in:
                        with gzip.open(str(backup_file), 'wb', compresslevel=6) as f_out:
                            shutil.copyfileobj(f_in, f_out)
                except Exception as e:
                    error_msg = f"Compression failed for {db_name}: {str(e)}"
                    logger.error(error_msg)
                    self.errors.append(error_msg)
                    if tmp_path and os.path.exists(tmp_path):
                        os.unlink(tmp_path)
                    if backup_file.exists():
                        backup_file.unlink()
                    return None
                finally:
                    # Always clean up temp file after compression (success or failure)
                    if tmp_path and os.path.exists(tmp_path):
                        try:
                            os.unlink(tmp_path)
                        except Exception:
                            pass  # Ignore cleanup errors
                
                # Set proper permissions
                backup_file.chmod(0o640)
                # Ensure proper ownership (root:root or postgres:postgres)
                try:
                    root_uid = pwd.getpwnam('root').pw_uid
                    root_gid = pwd.getpwnam('root').pw_gid
                    os.chown(backup_file, root_uid, root_gid)
                except Exception:
                    pass  # Ignore if chown fails
                
                file_size = backup_file.stat().st_size / (1024 * 1024)  # MB
                logger.info(f"Backup completed for {db_name}: {backup_file.name} ({file_size:.2f} MB)")
                
                return backup_file
                
        except subprocess.TimeoutExpired:
            error_msg = f"Backup timeout for database {db_name}"
            logger.error(error_msg)
            self.errors.append(error_msg)
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)
            return None
        except Exception as e:
            error_msg = f"Error backing up {db_name}: {str(e)}"
            logger.error(error_msg)
            self.errors.append(error_msg)
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass
            return None
    
    def upload_to_azure(self, backup_file: Path, is_retry: bool = False) -> bool:
        """Upload backup file to Azure Blob Storage"""
        if is_retry:
            logger.info(f"Retrying upload of {backup_file.name} to Azure Blob Storage...")
        else:
            logger.info(f"Uploading {backup_file.name} to Azure Blob Storage...")
        
        try:
            # Get Azure credentials from config
            account_name = self.config.get('azure_account_name')
            account_key = self.config.get('azure_account_key')
            container_name = self.config.get('azure_container_name', 'pgbackups')
            
            if not account_name or not account_key:
                error_msg = "Azure credentials not configured"
                logger.error(error_msg)
                self.errors.append(error_msg)
                if not is_retry:
                    self._mark_upload_failed(backup_file)
                return False
            
            # Validate Azure account key format (should be base64)
            try:
                # Try to decode to validate it's proper base64
                base64.b64decode(account_key, validate=True)
            except Exception as e:
                error_msg = f"Azure storage account key appears to be invalid (not base64): {str(e)}"
                logger.error(error_msg)
                logger.error("Please verify the Azure storage account key in the configuration")
                logger.error("The key should be a base64-encoded string from Azure Portal")
                logger.error("Get it from: Azure Portal > Storage Account > Access Keys")
                self.errors.append("Azure account key validation failed")
                if not is_retry:
                    self._mark_upload_failed(backup_file)
                return False
            
            # Create blob service client
            connection_string = (
                f"DefaultEndpointsProtocol=https;"
                f"AccountName={account_name};"
                f"AccountKey={account_key};"
                f"EndpointSuffix=core.windows.net"
            )
            
            try:
                blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            except Exception as e:
                error_msg = f"Failed to create Azure client: {str(e)}"
                error_detail = str(e).lower()
                if "incorrect padding" in error_detail or "base64" in error_detail or "invalid" in error_detail:
                    error_msg += "\n  → The Azure storage account key appears to be malformed"
                    error_msg += "\n  → Please verify the key from Azure Portal (Storage Account > Access Keys)"
                    error_msg += "\n  → Make sure you copied the entire key without extra spaces or newlines"
                    error_msg += "\n  → The key should be a base64-encoded string (typically 88 characters)"
                logger.error(error_msg)
                self.errors.append("Azure client creation failed")
                if not is_retry:
                    self._mark_upload_failed(backup_file)
                return False
            
            # Ensure container exists
            container_client = blob_service_client.get_container_client(container_name)
            try:
                container_client.create_container()
                logger.info(f"Created Azure container: {container_name}")
            except AzureError as e:
                # Check for specific error code instead of string matching
                from azure.core.exceptions import ResourceExistsError
                if not isinstance(e, ResourceExistsError):
                    # Check error code if available
                    if not (hasattr(e, 'error_code') and e.error_code == 'ContainerAlreadyExists'):
                        raise
            
            # Upload blob
            blob_name = f"{backup_file.name}"
            blob_client = blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_name
            )
            
            with open(backup_file, 'rb') as data:
                blob_client.upload_blob(data, overwrite=True)
            
            logger.info(f"Successfully uploaded {backup_file.name} to Azure")
            # Mark as successful (remove from failed list if it was there)
            self._mark_upload_success(backup_file)
            return True
            
        except AzureError as e:
            error_msg = f"Azure upload error: {str(e)}"
            error_detail = str(e).lower()
            if "incorrect padding" in error_detail or "base64" in error_detail:
                error_msg += "\n  → The Azure storage account key appears to be malformed"
                error_msg += "\n  → Please verify the key from Azure Portal (Storage Account > Access Keys)"
                error_msg += "\n  → Make sure you copied the entire key without extra spaces or newlines"
                error_msg += "\n  → Re-run setup: python3 pg_backup_setup.py"
            logger.error(error_msg)
            if not is_retry:
                self.errors.append(error_msg)
                self._mark_upload_failed(backup_file)
            return False
        except Exception as e:
            error_msg = f"Error uploading to Azure: {str(e)}"
            error_detail = str(e).lower()
            if "incorrect padding" in error_detail or "base64" in error_detail or "invalid" in error_detail:
                error_msg += "\n  → The Azure storage account key appears to be malformed"
                error_msg += "\n  → Please verify the key from Azure Portal (Storage Account > Access Keys)"
                error_msg += "\n  → Make sure you copied the entire key without extra spaces or newlines"
                error_msg += "\n  → Re-run setup: python3 pg_backup_setup.py"
            logger.error(error_msg)
            if not is_retry:
                self.errors.append(error_msg)
                self._mark_upload_failed(backup_file)
            return False
    
    def cleanup_old_backups_azure(self) -> int:
        """Delete backups older than retention period from Azure"""
        logger.info("Cleaning up old backups from Azure...")
        
        deleted_count = 0
        cutoff_date = datetime.now() - timedelta(days=RETENTION_DAYS)
        
        try:
            account_name = self.config.get('azure_account_name')
            account_key = self.config.get('azure_account_key')
            container_name = self.config.get('azure_container_name', 'pgbackups')
            
            if not account_name or not account_key:
                logger.warning("Azure credentials not configured, skipping cleanup")
                return 0
            
            # Validate Azure account key format
            try:
                base64.b64decode(account_key, validate=True)
            except Exception:
                logger.warning("Azure account key validation failed during cleanup - skipping")
                return 0
            
            connection_string = (
                f"DefaultEndpointsProtocol=https;"
                f"AccountName={account_name};"
                f"AccountKey={account_key};"
                f"EndpointSuffix=core.windows.net"
            )
            
            try:
                blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            except Exception as e:
                error_detail = str(e).lower()
                if "incorrect padding" in error_detail or "base64" in error_detail:
                    logger.warning(f"Azure client creation failed during cleanup (key issue): {str(e)}")
                    logger.warning("Fix Azure key in configuration and retry")
                else:
                    logger.warning(f"Azure client creation failed during cleanup: {str(e)}")
                return 0
            
            container_client = blob_service_client.get_container_client(container_name)
            
            # List all blobs
            blobs = container_client.list_blobs()
            
            for blob in blobs:
                # Parse date from blob name (format: dbname_YYYYMMDD_HHMMSS_microseconds.sql.gz)
                # Note: dbname may contain underscores, so we can't rely on split position
                try:
                    # Use regex to find the YYYYMMDD pattern (8 consecutive digits)
                    # The timestamp format is always: YYYYMMDD_HHMMSS_microseconds
                    # Pattern: 8 digits, underscore, 6 digits, underscore, 6 digits
                    date_match = re.search(r'_(\d{8})_\d{6}_\d+\.sql\.gz$', blob.name)
                    if date_match:
                        date_str = date_match.group(1)  # YYYYMMDD
                        blob_date = datetime.strptime(date_str, '%Y%m%d')
                        
                        if blob_date < cutoff_date:
                            blob_client = blob_service_client.get_blob_client(
                                container=container_name,
                                blob=blob.name
                            )
                            blob_client.delete_blob()
                            deleted_count += 1
                            logger.info(f"Deleted old backup: {blob.name} (from {blob_date.date()})")
                    else:
                        logger.warning(f"Could not parse date from blob name {blob.name}: expected format dbname_YYYYMMDD_HHMMSS_microseconds.sql.gz")
                except (ValueError, AttributeError) as e:
                    logger.warning(f"Could not parse date from blob name {blob.name}: {str(e)}")
                    continue
            
            logger.info(f"Cleanup completed: deleted {deleted_count} old backups")
            return deleted_count
            
        except Exception as e:
            error_msg = f"Error during Azure cleanup: {str(e)}"
            logger.error(error_msg)
            self.errors.append(error_msg)
            return deleted_count
    
    def cleanup_local_backups(self) -> int:
        """Clean up local backup files older than retention period
        Only deletes files that have been successfully uploaded to Azure
        Failed uploads are kept for retry
        Also cleans up any leftover temp files"""
        logger.info("Cleaning up old local backups...")
        
        deleted_count = 0
        cutoff_date = datetime.now() - timedelta(days=LOCAL_RETENTION_DAYS)
        
        # Clean up any leftover temp files first
        try:
            if TEMP_DIR.exists():
                for tmp_file in TEMP_DIR.glob("*.sql"):
                    try:
                        tmp_file.unlink()
                        logger.info(f"Cleaned up leftover temp file: {tmp_file.name}")
                    except Exception as e:
                        logger.warning(f"Error deleting temp file {tmp_file.name}: {str(e)}")
        except Exception as e:
            logger.warning(f"Error cleaning temp directory: {str(e)}")
        
        try:
            for backup_file in self.backup_dir.glob("*.sql.gz"):
                try:
                    # Skip the failed uploads tracking file
                    if backup_file.name == ".failed_uploads.json":
                        continue
                    
                    file_time = datetime.fromtimestamp(backup_file.stat().st_mtime)
                    
                    # Only delete if:
                    # 1. File is older than retention period AND
                    # 2. File is NOT in failed uploads list (meaning it was successfully uploaded)
                    file_str = str(backup_file)
                    is_failed_upload = file_str in self.failed_uploads
                    
                    if file_time < cutoff_date and not is_failed_upload:
                        backup_file.unlink()
                        deleted_count += 1
                        logger.info(f"Deleted old local backup: {backup_file.name} (successfully uploaded)")
                    elif is_failed_upload:
                        # Check if retry limit exceeded
                        upload_info = self.failed_uploads[file_str]
                        retry_count = upload_info.get('retry_count', 0)
                        if retry_count >= MAX_UPLOAD_RETRIES:
                            # Max retries exceeded, delete file and remove from failed list
                            logger.warning(f"Deleting backup {backup_file.name} after {retry_count} failed upload attempts")
                            backup_file.unlink()
                            del self.failed_uploads[file_str]
                            deleted_count += 1
                            self._save_failed_uploads()
                except Exception as e:
                    logger.warning(f"Error deleting {backup_file.name}: {str(e)}")
            
            logger.info(f"Local cleanup completed: deleted {deleted_count} files")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error during local cleanup: {str(e)}")
            return deleted_count
    
    def retry_failed_uploads(self) -> Tuple[int, int]:
        """Retry uploading previously failed backup files"""
        logger.info("Checking for failed uploads to retry...")
        
        successful_retries = 0
        failed_retries = 0
        
        if not self.failed_uploads:
            logger.info("No failed uploads to retry")
            return (0, 0)
        
        logger.info(f"Found {len(self.failed_uploads)} failed upload(s) to retry")
        
        # Create a copy of the dict to iterate over (since we might modify it)
        failed_files = list(self.failed_uploads.keys())
        
        for file_str in failed_files:
            backup_file = Path(file_str)
            
            # Check if should retry
            if not self._should_retry_upload(backup_file):
                continue
            
            # Check if file still exists
            if not backup_file.exists():
                logger.warning(f"Failed upload file no longer exists: {backup_file.name}")
                if file_str in self.failed_uploads:
                    del self.failed_uploads[file_str]
                    self._save_failed_uploads()
                continue
            
            # Try to upload
            logger.info(f"Retrying upload of {backup_file.name}...")
            if self.upload_to_azure(backup_file, is_retry=True):
                successful_retries += 1
                logger.info(f"Successfully retried upload of {backup_file.name}")
            else:
                failed_retries += 1
                upload_info = self.failed_uploads.get(file_str, {})
                retry_count = upload_info.get('retry_count', 0)
                logger.warning(f"Retry failed for {backup_file.name} (attempt {retry_count + 1}/{MAX_UPLOAD_RETRIES})")
        
        if successful_retries > 0:
            logger.info(f"Successfully retried {successful_retries} upload(s)")
        if failed_retries > 0:
            logger.warning(f"Failed to retry {failed_retries} upload(s)")
        
        return (successful_retries, failed_retries)
    
    def send_email_notification(self, subject: str, body: str, is_error: bool = False):
        """Send email notification"""
        email_config = self.config.get('email', {})
        
        if not email_config.get('enabled', False):
            logger.info("Email notifications are disabled")
            return
        
        try:
            # Use SMTP relay service (SendGrid, Mailgun, etc.) or local SMTP
            smtp_server = email_config.get('smtp_server', 'localhost')
            smtp_port = email_config.get('smtp_port', 587)
            smtp_user = email_config.get('smtp_user')
            smtp_password = email_config.get('smtp_password')
            from_email = email_config.get('from_email')
            to_email = email_config.get('to_email')
            
            if not to_email or '@' not in to_email:
                logger.warning("Email recipient not configured or invalid")
                return
            
            msg = MIMEMultipart()
            msg['From'] = from_email or f"pgbackup@{os.uname().nodename}"
            msg['To'] = to_email
            msg['Subject'] = subject
            
            msg.attach(MIMEText(body, 'plain'))
            
            # Send email with timeout
            context = ssl.create_default_context()
            
            # Set socket timeout (30 seconds)
            socket.setdefaulttimeout(30)
            
            try:
                with smtplib.SMTP(smtp_server, smtp_port, timeout=30) as server:
                    if smtp_port == 587:
                        server.starttls(context=context)
                    if smtp_user and smtp_password:
                        server.login(smtp_user, smtp_password)
                    server.send_message(msg)
            finally:
                # Reset timeout
                socket.setdefaulttimeout(None)
            
            logger.info(f"Email notification sent to {to_email}")
            
        except Exception as e:
            logger.error(f"Failed to send email notification: {str(e)}")
            # Don't add to errors list to avoid infinite loop
    
    def run_backup_procedure(self):
        """Main backup procedure"""
        # Acquire lock to prevent concurrent execution
        if not self._acquire_lock():
            logger.error("Could not acquire lock - another backup may be running")
            return
        
        try:
            logger.info("=" * 60)
            logger.info("Starting PostgreSQL backup procedure")
            logger.info("=" * 60)
            
            start_time = datetime.now()
            
            # Log configuration summary
            logger.info("Backup Configuration:")
            logger.info(f"  PostgreSQL User: {self.pg_user}")
            logger.info(f"  Connection Method: {self.pg_connection_method}")
            run_maintenance = self.config.get('run_maintenance', True)
            logger.info(f"  Pre-backup Maintenance (VACUUM/CHECKPOINT): {'ENABLED' if run_maintenance else 'DISABLED'}")
            if run_maintenance:
                maintenance_user = self.config.get('pg_maintenance_user', self.pg_user)
                logger.info(f"  Maintenance User: {maintenance_user}")
            run_post_maintenance = self.config.get('run_post_backup_maintenance', False)
            logger.info(f"  Post-backup Maintenance (REINDEX): {'ENABLED' if run_post_maintenance else 'DISABLED'}")
            check_queries = self.config.get('check_long_queries', False)
            logger.info(f"  Long-running Query Check: {'ENABLED' if check_queries else 'DISABLED'}")
            logger.info("=" * 60)
            
            # Check disk space before starting
            if not self._check_disk_space():
                error_msg = "Insufficient disk space - backup aborted"
                logger.error(error_msg)
                self.errors.append(error_msg)
                self._send_final_notification(start_time)
                return
            
            # Health checks
            logger.info("")
            is_healthy, health_issues = self.check_postgresql_health()
            if health_issues:
                self.warnings.extend(health_issues)
            
            # STEP 1: Retry any previously failed uploads first
            retry_success, retry_failed = self.retry_failed_uploads()
            if retry_success > 0:
                logger.info(f"Successfully retried {retry_success} previously failed upload(s)")
            
            # Get databases
            logger.info("")
            logger.info("Getting list of databases to backup...")
            databases = self.get_databases()
            if not databases:
                error_msg = "No databases found or error listing databases"
                logger.error(error_msg)
                logger.error("Cannot proceed with backup - exiting")
                self.errors.append(error_msg)
                self._send_final_notification(start_time)
                return
            
            logger.info(f"Found {len(databases)} database(s) to backup: {', '.join(databases)}")
            logger.info("")
            
            # Backup each database
            successful_backups = 0
            failed_backups = 0
            
            for db_name in databases:
                logger.info("-" * 60)
                logger.info(f"Processing database: {db_name}")
                logger.info("-" * 60)
                # STEP 1: Perform pre-backup maintenance (BEFORE backup)
                # VACUUM ANALYZE and CHECKPOINT should be done before backup
                self.perform_pre_backup_maintenance(db_name)
                
                # STEP 2: Perform the backup
                backup_file = self.backup_database(db_name)
                if backup_file:
                    # STEP 3: Upload to Azure
                    if self.upload_to_azure(backup_file):
                        successful_backups += 1
                        
                        # STEP 4: Perform post-backup maintenance (AFTER successful backup)
                        # Optional operations like REINDEX can be done after backup
                        self.perform_post_backup_maintenance(db_name)
                    else:
                        failed_backups += 1
                else:
                    failed_backups += 1
            
            # Cleanup old backups
            self.cleanup_old_backups_azure()
            self.cleanup_local_backups()
            
            # Summary
            duration = datetime.now() - start_time
            logger.info("=" * 60)
            logger.info(f"Backup procedure completed in {duration}")
            logger.info(f"Successful backups: {successful_backups}")
            logger.info(f"Failed backups: {failed_backups}")
            logger.info(f"Errors: {len(self.errors)}")
            logger.info(f"Warnings: {len(self.warnings)}")
            logger.info("=" * 60)
                
            # Send notification if there are errors (always) or warnings (if configured)
            # Always notify on errors as per requirements
            if self.errors:
                self._send_final_notification(start_time, successful_backups, failed_backups)
            elif self.warnings and self.config.get('email', {}).get('notify_on_warnings', False):
                self._send_final_notification(start_time, successful_backups, failed_backups)
        finally:
            # Always release lock
            self._release_lock()
    
    def _send_final_notification(self, start_time: datetime, successful: int = 0, failed: int = 0):
        """Send final notification email"""
        duration = datetime.now() - start_time
        
        subject = "PostgreSQL Backup "
        if self.errors:
            subject += "FAILED"
        elif self.warnings:
            subject += "COMPLETED WITH WARNINGS"
        else:
            subject += "SUCCESS"
        
        body = f"""PostgreSQL Backup Report
{'=' * 60}
Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Duration: {duration}
{'=' * 60}

Results:
- Successful backups: {successful}
- Failed backups: {failed}
- Errors: {len(self.errors)}
- Warnings: {len(self.warnings)}

"""
        
        if self.errors:
            body += "ERRORS:\n"
            for error in self.errors:
                body += f"  - {error}\n"
            body += "\n"
        
        if self.warnings:
            body += "WARNINGS:\n"
            for warning in self.warnings:
                body += f"  - {warning}\n"
            body += "\n"
        
        body += f"\nLog file: {LOG_FILE}\n"
        
        self.send_email_notification(subject, body, is_error=len(self.errors) > 0)


def main():
    """Main entry point"""
    try:
        backup_manager = PostgreSQLBackup()
        backup_manager.run_backup_procedure()
        
        if backup_manager.errors:
            sys.exit(1)
        else:
            sys.exit(0)
            
    except KeyboardInterrupt:
        logger.error("Backup interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()


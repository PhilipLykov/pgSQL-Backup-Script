#!/usr/bin/env python3
"""
PostgreSQL Backup Configuration
Secure configuration management for backup script
"""

import os
import json
from cryptography.fernet import Fernet
from pathlib import Path

class SecureConfig:
    """Secure configuration manager using encryption"""
    
    def __init__(self, config_dir="/etc/pgbackup"):
        self.config_dir = Path(config_dir)
        self.key_file = self.config_dir / ".encryption_key"
        self.config_file = self.config_dir / "config.enc"
        self._ensure_config_dir()
    
    def _ensure_config_dir(self):
        """Create config directory with secure permissions"""
        # Create directory with restrictive permissions
        # If running as postgres user, we'll adjust permissions in setup_permissions.sh
        try:
            self.config_dir.mkdir(mode=0o700, exist_ok=True)
        except PermissionError:
            # Directory exists but we can't create it - that's okay if we can read it
            pass
        
        # Try to set permissions on existing files (may fail if not owner)
        try:
            if self.key_file.exists():
                self.key_file.chmod(0o600)
            if self.config_file.exists():
                self.config_file.chmod(0o600)
        except (PermissionError, OSError):
            # Not the owner, that's okay - permissions should be set by setup script
            pass
    
    def _get_or_create_key(self):
        """Get encryption key or create new one"""
        if self.key_file.exists():
            with open(self.key_file, 'rb') as f:
                return f.read()
        else:
            key = Fernet.generate_key()
            with open(self.key_file, 'wb') as f:
                f.write(key)
            self.key_file.chmod(0o600)
            return key
    
    def save_config(self, config_dict):
        """Encrypt and save configuration"""
        key = self._get_or_create_key()
        fernet = Fernet(key)
        
        config_json = json.dumps(config_dict).encode()
        encrypted = fernet.encrypt(config_json)
        
        with open(self.config_file, 'wb') as f:
            f.write(encrypted)
        self.config_file.chmod(0o600)
    
    def load_config(self):
        """Load and decrypt configuration"""
        if not self.config_file.exists():
            return None
        
        key = self._get_or_create_key()
        fernet = Fernet(key)
        
        with open(self.config_file, 'rb') as f:
            encrypted = f.read()
        
        decrypted = fernet.decrypt(encrypted)
        return json.loads(decrypted.decode())


#!/usr/bin/env python3
"""
PostgreSQL Backup Setup Script
Interactive setup for configuring backup system
"""

import os
import sys
import getpass
import shutil
from pathlib import Path

# Add script directory to Python path for imports
SCRIPT_DIR = Path(__file__).parent.absolute()
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from pg_backup_config import SecureConfig

def get_input(prompt: str, default: str = None, password: bool = False, required: bool = False) -> str:
    """Get user input with optional default and password masking"""
    if default:
        prompt_text = f"{prompt} [{default}]: "
    else:
        prompt_text = f"{prompt}: "
    
    while True:
        if password:
            value = getpass.getpass(prompt_text)
        else:
            value = input(prompt_text)
        
        value = value.strip()
        
        if value:
            return value
        elif default:
            return default
        elif not required:
            return ""
        else:
            print("This field is required. Please enter a value.")

def main():
    """Interactive setup"""
    print("=" * 60)
    print("PostgreSQL Backup System Setup")
    print("=" * 60)
    print()
    
    config_manager = SecureConfig()
    
    # Check if config exists
    existing_config = config_manager.load_config()
    if existing_config:
        overwrite = input("Configuration already exists. Overwrite? (y/N): ").strip().lower()
        if overwrite != 'y':
            print("Setup cancelled.")
            return
    
    config = {}
    
    print("\n--- PostgreSQL Configuration ---")
    print("IMPORTANT: PostgreSQL authentication must be configured for the backup script to work.")
    print()
    print("You have two options:")
    print("  1. Unix Socket (Peer Authentication) - No password needed, but script must run as postgres user")
    print("  2. TCP/IP Connection - Requires password or .pgpass file")
    print()
    
    # Connection method
    connection_method = input("Connection method (unix_socket/tcp) [tcp]: ").strip().lower()
    if not connection_method:
        connection_method = 'tcp'
    if connection_method not in ['unix_socket', 'tcp']:
        print("Invalid option, defaulting to 'tcp'")
        connection_method = 'tcp'
    config['pg_connection_method'] = connection_method
    
    if connection_method == 'unix_socket':
        print("\nUsing Unix socket (peer authentication)")
        print("NOTE: The backup script must run as the PostgreSQL user (typically 'postgres')")
        print("      or you must configure peer authentication in pg_hba.conf")
        pg_user = get_input("PostgreSQL username (must match system user for peer auth)", "postgres", required=True)
        config['pg_user'] = pg_user
        print(f"\n✓ Unix socket authentication configured for user '{pg_user}'")
        print("  No password needed - using peer authentication")
    else:
        print("\nUsing TCP/IP connection (requires password authentication)")
        print("RECOMMENDED: Create a dedicated backup user in PostgreSQL for security.")
        print("See CREATE_BACKUP_USER.md for instructions.")
        print()
        
        # PostgreSQL backup user
        while True:
            pg_user = get_input("PostgreSQL backup username", "pgbackup", required=True)
            if pg_user:
                config['pg_user'] = pg_user
                break
        
        # PostgreSQL password authentication
        print("\nPassword authentication options:")
        print("  A) Use .pgpass file (Recommended - password stored securely)")
        print("  B) Store password in encrypted config (Less secure)")
        auth_method = input("Choose authentication method (A/B) [A]: ").strip().upper()
        if not auth_method:
            auth_method = 'A'
        
        if auth_method == 'A':
            print(f"\n✓ Using .pgpass file for authentication")
            print(f"\nIMPORTANT: You must create /root/.pgpass file with the following format:")
            print(f"  localhost:5432:*:{pg_user}:YOUR_PASSWORD")
            print(f"\nTo create it, run:")
            print(f"  echo 'localhost:5432:*:{pg_user}:YOUR_PASSWORD' | sudo tee /root/.pgpass")
            print(f"  sudo chmod 600 /root/.pgpass")
            print(f"\nSee CREATE_BACKUP_USER.md for detailed instructions.")
            create_now = input("\nCreate .pgpass file now? (y/N): ").strip().lower()
            if create_now == 'y':
                password = getpass.getpass(f"Enter password for PostgreSQL user '{pg_user}': ")
                if password:
                    pgpass_file = Path("/root/.pgpass")
                    pgpass_entry = f"localhost:5432:*:{pg_user}:{password}\n"
                    try:
                        # Check if .pgpass exists as directory (common mistake)
                        if pgpass_file.is_dir():
                            print(f"WARNING: {pgpass_file} exists as a directory! Removing it...")
                            shutil.rmtree(pgpass_file)
                        
                        # Remove existing entry for this user if it exists
                        existing_entries = []
                        if pgpass_file.exists() and pgpass_file.is_file():
                            with open(pgpass_file, 'r') as f:
                                for line in f:
                                    line = line.strip()
                                    # Skip entries for this user (format: host:port:db:user:password)
                                    if line and not line.startswith('#') and ':' in line:
                                        parts = line.split(':')
                                        if len(parts) >= 4 and parts[3] != pg_user:
                                            existing_entries.append(line)
                        
                        # Write all entries (existing + new)
                        with open(pgpass_file, 'w') as f:
                            for entry in existing_entries:
                                f.write(entry + '\n')
                            f.write(pgpass_entry)
                        pgpass_file.chmod(0o600)
                        print(f"✓ Created/updated {pgpass_file}")
                    except Exception as e:
                        print(f"ERROR: Failed to create .pgpass file: {e}")
                        print("Please create it manually using the instructions above.")
        else:
            password = getpass.getpass(f"PostgreSQL password for user '{pg_user}': ")
            if password:
                config['PGPASSWORD'] = password
                print("✓ Password will be stored in encrypted config")
    
    # Skip databases
    skip_dbs_input = input("Databases to skip (comma-separated, default: template0,template1): ").strip()
    if skip_dbs_input:
        config['skip_databases'] = [db.strip() for db in skip_dbs_input.split(',')]
    else:
        config['skip_databases'] = ['template0', 'template1']
    
    print("\n--- Azure Blob Storage Configuration ---")
    while True:
        account_name = get_input("Azure Storage Account Name", required=True)
        if account_name:
            config['azure_account_name'] = account_name
            break
    
    while True:
        account_key = getpass.getpass("Azure Storage Account Key: ")
        if account_key:
            config['azure_account_key'] = account_key
            break
        print("Account key is required.")
    
    config['azure_container_name'] = get_input("Azure Container Name", "pgbackups")
    
    print("\n--- Email Notification Configuration ---")
    enable_email = input("Enable email notifications? (Y/n): ").strip().lower()
    if enable_email != 'n':
        while True:
            to_email = get_input("To Email Address (your Gmail)", required=True)
            if to_email and '@' in to_email:
                break
            print("Please enter a valid email address.")
        
        smtp_port_input = get_input("SMTP Port", "587")
        try:
            smtp_port = int(smtp_port_input) if smtp_port_input else 587
        except ValueError:
            print("Invalid port number, using default 587")
            smtp_port = 587
        
        config['email'] = {
            'enabled': True,
            'smtp_server': get_input("SMTP Server", "smtp.gmail.com"),
            'smtp_port': smtp_port,
            'smtp_user': get_input("SMTP Username (Gmail: your-email@gmail.com)"),
            'smtp_password': getpass.getpass("SMTP Password (Gmail: use App Password)"),
            'from_email': get_input("From Email Address"),
            'to_email': to_email,
            'notify_on_warnings': input("Notify on warnings? (y/N): ").strip().lower() == 'y'
        }
    else:
        config['email'] = {'enabled': False}
    
    print("\n--- Additional Options ---")
    config['check_long_queries'] = input("Check for long-running queries? (y/N): ").strip().lower() == 'y'
    
    # Maintenance options
    print("\n--- Database Maintenance Options ---")
    print("Note: VACUUM ANALYZE and CHECKPOINT require superuser privileges.")
    print("If using a non-superuser backup account, you can:")
    print("  1. Use a separate maintenance user (postgres superuser)")
    print("  2. Disable maintenance operations")
    print()
    
    run_maintenance = input("Run VACUUM ANALYZE and CHECKPOINT before backup? (Y/n): ").strip().lower()
    config['run_maintenance'] = run_maintenance != 'n'
    
    if config['run_maintenance']:
        use_separate_maintenance = input("Use separate user for maintenance (postgres superuser)? (Y/n): ").strip().lower()
        if use_separate_maintenance != 'n':
            config['pg_maintenance_user'] = 'postgres'
            print("Maintenance will run as 'postgres' superuser")
            print("Backups will run as configured backup user")
        else:
            config['pg_maintenance_user'] = pg_user
            print(f"Warning: Maintenance will run as '{pg_user}' - ensure user has superuser privileges")
        print("Note: VACUUM ANALYZE and CHECKPOINT will run before each database backup")
    
    run_post_maintenance = input("Run REINDEX after backup? (y/N): ").strip().lower()
    config['run_post_backup_maintenance'] = run_post_maintenance == 'y'
    if run_post_maintenance == 'y':
        config['reindex_after_backup'] = True
        print("Note: REINDEX will run after successful backup (can be time-consuming)")
    else:
        config['reindex_after_backup'] = False
    
    # Save configuration
    try:
        config_manager.save_config(config)
        print("\n" + "=" * 60)
        print("Configuration saved successfully!")
        print("=" * 60)
        print(f"\nConfiguration stored in: {config_manager.config_file}")
        print("Make sure to secure this directory and files.")
        print("\nNext steps:")
        print("1. Install Python dependencies:")
        print("   bash install_dependencies.sh")
        print("   (Or see INSTALL.md for alternative methods)")
        
        # Provide different instructions based on connection method
        if config.get('pg_connection_method') == 'unix_socket':
            pg_user = config.get('pg_user', 'postgres')
            print(f"\n2. Test the backup script (MUST run as {pg_user} user):")
            print(f"   sudo -u {pg_user} python3 pg_backup_main.py")
            print(f"   OR: sudo su - {pg_user} -c 'python3 /opt/pgSQL-bck-script/pg_backup_main.py'")
            print(f"\n   IMPORTANT: For cron/systemd, ensure the script runs as {pg_user} user")
        else:
            print("\n2. Test the backup script:")
            print("   python3 pg_backup_main.py")
            print("\n   IMPORTANT: Make sure .pgpass file is set up: /root/.pgpass")
            print("   If you see password prompts, run: bash setup_pg_auth.sh")
        
        print("\n3. Set up cron job or systemd timer")
        print("   See INSTALL.md for detailed instructions")
        if config.get('pg_connection_method') == 'unix_socket':
            pg_user = config.get('pg_user', 'postgres')
            print(f"   NOTE: Cron/systemd must run as {pg_user} user for Unix socket authentication")
    except Exception as e:
        print(f"\nERROR: Failed to save configuration: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    if os.geteuid() != 0:
        print("ERROR: This script must be run as root")
        sys.exit(1)
    main()


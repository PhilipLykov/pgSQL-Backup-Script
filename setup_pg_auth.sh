#!/bin/bash
# PostgreSQL Authentication Setup Helper
# This script helps set up authentication for the PostgreSQL backup script

set -e

echo "=========================================="
echo "PostgreSQL Backup Authentication Setup"
echo "=========================================="
echo ""

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo "ERROR: This script must be run as root (use sudo)"
    exit 1
fi

# Check if PostgreSQL is running
if ! systemctl is-active --quiet postgrespro-1c-17.service 2>/dev/null && \
   ! systemctl is-active --quiet postgresql.service 2>/dev/null && \
   ! systemctl is-active --quiet postgresql@*.service 2>/dev/null; then
    echo "WARNING: PostgreSQL service does not appear to be running."
    echo "Please start PostgreSQL before running this script."
    exit 1
fi

echo "This script will help you set up PostgreSQL authentication for backups."
echo ""
echo "You have two options:"
echo "  1. Unix Socket (Peer Authentication) - No password, but script must run as postgres user"
echo "  2. TCP/IP with Dedicated Backup User - More secure, requires password"
echo ""

read -p "Choose option (1 or 2) [2]: " option
option=${option:-2}

if [ "$option" = "1" ]; then
    echo ""
    echo "=== Unix Socket (Peer Authentication) Setup ==="
    echo ""
    echo "For Unix socket authentication:"
    echo "  - The backup script will use peer authentication"
    echo "  - No password is needed"
    echo "  - The script must run as the PostgreSQL user (typically 'postgres')"
    echo ""
    echo "To use this method:"
    echo "  1. In the setup script (pg_backup_setup.py), choose 'unix_socket'"
    echo "  2. Ensure the backup script runs as the postgres user"
    echo "     (You may need to modify the cron job or systemd service)"
    echo ""
    echo "✓ Unix socket authentication setup complete"
    echo "  Continue with: python3 pg_backup_setup.py"
    
elif [ "$option" = "2" ]; then
    echo ""
    echo "=== TCP/IP with Dedicated Backup User Setup ==="
    echo ""
    
    # Get backup username
    read -p "PostgreSQL backup username [pgbackup]: " pg_user
    pg_user=${pg_user:-pgbackup}
    
    # Validate username - only allow alphanumeric and underscore
    if [[ ! "$pg_user" =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]]; then
        echo "ERROR: Invalid username. Username must start with letter or underscore and contain only letters, numbers, and underscores."
        exit 1
    fi
    
    # Check if user already exists (using parameterized query via -v)
    user_exists=$(sudo -u postgres psql -v username="$pg_user" -tAc "SELECT 1 FROM pg_roles WHERE rolname=:'username'" 2>/dev/null || echo "0")
    
    if [ "$user_exists" = "1" ]; then
        echo "User '$pg_user' already exists in PostgreSQL."
        read -p "Do you want to change the password? (y/N): " change_pass
        if [ "${change_pass,,}" != "y" ]; then
            echo "Keeping existing password."
            skip_user_creation=true
        fi
    fi
    
    if [ "$skip_user_creation" != "true" ]; then
        # Get password
        read -sp "Enter password for PostgreSQL user '$pg_user': " pg_password
        echo ""
        read -sp "Confirm password: " pg_password_confirm
        echo ""
        
        if [ "$pg_password" != "$pg_password_confirm" ]; then
            echo "ERROR: Passwords do not match!"
            exit 1
        fi
        
        if [ -z "$pg_password" ]; then
            echo "ERROR: Password cannot be empty!"
            exit 1
        fi
        
        # Create or update user (using parameterized queries to prevent SQL injection)
        if [ "$user_exists" = "1" ]; then
            echo "Updating password for existing user '$pg_user'..."
            # Use psql -v for safe variable substitution
            sudo -u postgres psql -v username="$pg_user" -v password="$pg_password" <<EOF
ALTER USER :username WITH PASSWORD :'password';
EOF
            if [ $? -ne 0 ]; then
                echo "ERROR: Failed to update password. Make sure you're running as root."
                exit 1
            fi
        else
            echo "Creating PostgreSQL user '$pg_user'..."
            # Use psql -v for safe variable substitution
            sudo -u postgres psql -v username="$pg_user" -v password="$pg_password" <<EOF
CREATE USER :username WITH PASSWORD :'password';
ALTER USER :username WITH REPLICATION;
EOF
            if [ $? -ne 0 ]; then
                echo "ERROR: Failed to create user. Make sure PostgreSQL is running and accessible."
                exit 1
            fi
            echo "✓ User '$pg_user' created"
        fi
        
        # Grant privileges on all databases
        echo ""
        echo "Granting privileges on databases..."
        databases=$(sudo -u postgres psql -tAc "SELECT datname FROM pg_database WHERE datistemplate = false AND datname != 'postgres'" 2>/dev/null)
        
        if [ -z "$databases" ]; then
            echo "WARNING: No user databases found. You may need to grant privileges manually."
        else
            for db in $databases; do
                # Validate database name (should be safe from psql output, but validate anyway)
                if [[ ! "$db" =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]]; then
                    echo "  WARNING: Skipping invalid database name: $db"
                    continue
                fi
                echo "  Granting privileges on database: $db"
                # Use psql -v for safe variable substitution
                sudo -u postgres psql -d "$db" -v username="$pg_user" -v dbname="$db" <<EOF 2>/dev/null || true
GRANT CONNECT ON DATABASE :dbname TO :username;
GRANT USAGE ON SCHEMA public TO :username;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO :username;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO :username;
EOF
            done
            echo "✓ Privileges granted"
        fi
        
        # Also grant on postgres database (using parameterized query)
        sudo -u postgres psql -d postgres -v username="$pg_user" <<EOF 2>/dev/null || true
GRANT CONNECT ON DATABASE postgres TO :username;
GRANT USAGE ON SCHEMA public TO :username;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO :username;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO :username;
EOF
    fi
    
    # Create .pgpass file
    echo ""
    echo "Creating .pgpass file..."
    pgpass_file="/root/.pgpass"
    
    # Check if .pgpass exists as directory (common mistake)
    if [ -d "$pgpass_file" ]; then
        echo "WARNING: $pgpass_file exists as a directory! Removing it..."
        rm -rf "$pgpass_file"
    fi
    
    # Get password if we didn't create user
    if [ "$skip_user_creation" = "true" ] && [ -z "$pg_password" ]; then
        read -sp "Enter password for PostgreSQL user '$pg_user': " pg_password
        echo ""
    fi
    
    # Add entry to .pgpass (remove existing entry for this user first)
    if [ -f "$pgpass_file" ]; then
        # Remove existing entries for this user
        grep -v "localhost:5432:\*:$pg_user:" "$pgpass_file" > "${pgpass_file}.tmp" 2>/dev/null || true
        mv "${pgpass_file}.tmp" "$pgpass_file" 2>/dev/null || true
    fi
    
    # Add new entry
    echo "localhost:5432:*:$pg_user:$pg_password" >> "$pgpass_file"
    chmod 600 "$pgpass_file"
    
    echo "✓ Created/updated $pgpass_file"
    echo ""
    echo "Verifying .pgpass file:"
    ls -l "$pgpass_file"
    echo ""
    
    # Test connection (using .pgpass, not environment variable for security)
    echo "Testing PostgreSQL connection..."
    # Clear PGPASSWORD from environment to force use of .pgpass
    unset PGPASSWORD
    if sudo -u postgres psql -h localhost -U "$pg_user" -d postgres -c "SELECT 1;" >/dev/null 2>&1; then
        echo "✓ Connection test successful!"
    else
        echo "WARNING: Connection test failed. Please verify:"
        echo "  - PostgreSQL is running"
        echo "  - User '$pg_user' has correct password"
        echo "  - .pgpass file is correctly formatted"
    fi
    
    echo ""
    echo "=========================================="
    echo "✓ PostgreSQL authentication setup complete!"
    echo "=========================================="
    echo ""
    echo "Next steps:"
    echo "  1. Run the setup script: python3 pg_backup_setup.py"
    echo "  2. Choose 'tcp' as connection method"
    echo "  3. Enter '$pg_user' as the PostgreSQL username"
    echo "  4. Choose option A (use .pgpass file) when prompted"
    echo ""
    
else
    echo "ERROR: Invalid option. Please choose 1 or 2."
    exit 1
fi


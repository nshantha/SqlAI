# Comprehensive database connection test script
import psycopg2
import os
import sys
import getpass
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get database configuration from environment variables
DB_JDBC_URL = os.getenv("DB_JDBC_URL", "")
DB_USER = os.getenv("DB_USER", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")  # Default to empty string instead of hardcoded password

# Parse JDBC URL to extract components
jdbc_parts = DB_JDBC_URL.replace("jdbc:", "").split("://")
if len(jdbc_parts) == 2:
    host_port_db = jdbc_parts[1].split("/")
    host_port = host_port_db[0].split(":")
    
    DB_HOST = host_port[0]
    DB_PORT = int(host_port[1]) if len(host_port) > 1 else 5432
    DB_NAME = host_port_db[1] if len(host_port_db) > 1 else ""
else:
    # Default values if parsing fails
    DB_HOST = "localhost"
    DB_PORT = 5432
    DB_NAME = ""

def print_connection_info():
    """Print current connection information"""
    print("\n=== Database Connection Information ===")
    print(f"Host: {DB_HOST}")
    print(f"Port: {DB_PORT}")
    print(f"Database: {DB_NAME}")
    print(f"User: {DB_USER}")
    print(f"Password: {'*' * len(DB_PASSWORD) if DB_PASSWORD else 'None'}")
    print("=====================================\n")

def test_password_auth(host, port, database, user, password):
    """Test password authentication"""
    print("\n--- Testing Password Authentication ---")
    try:
        print(f"Connecting to {host}:{port}/{database} as {user}...")
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            connect_timeout=10
        )
        print("✅ Password authentication successful!")
        
        # Test a simple query
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        print(f"Query result: {result}")
        
        # Get server version
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        print(f"Server version: {version}")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Password authentication failed: {e}")
        return False

def test_kerberos_auth(host, port, database, user):
    """Test Kerberos authentication"""
    print("\n--- Testing Kerberos Authentication ---")
    try:
        print(f"Connecting to {host}:{port}/{database} as {user} using Kerberos...")
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            krbsrvname="postgres",
            gsslib="gssapi",
            connect_timeout=10
        )
        print("✅ Kerberos authentication successful!")
        
        # Test a simple query
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        print(f"Query result: {result}")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Kerberos authentication failed: {e}")
        return False

def interactive_connection_test():
    """Interactive connection test with user input"""
    global DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    
    print("\n=== Interactive Database Connection Test ===")
    print("Enter new connection details (press Enter to keep current value)")
    
    new_host = input(f"Host [{DB_HOST}]: ").strip()
    if new_host:
        DB_HOST = new_host
    
    new_port = input(f"Port [{DB_PORT}]: ").strip()
    if new_port:
        try:
            DB_PORT = int(new_port)
        except ValueError:
            print("Invalid port number, keeping current value")
    
    new_db = input(f"Database [{DB_NAME}]: ").strip()
    if new_db:
        DB_NAME = new_db
    
    new_user = input(f"User [{DB_USER}]: ").strip()
    if new_user:
        DB_USER = new_user
    
    new_password = getpass.getpass(f"Password (hidden): ")
    if new_password:
        DB_PASSWORD = new_password
    
    print_connection_info()
    
    # Test with new credentials
    password_success = test_password_auth(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)
    
    if not password_success:
        print("\nWould you like to try Kerberos authentication? (y/n)")
        try_kerberos = input().strip().lower() == 'y'
        if try_kerberos:
            test_kerberos_auth(DB_HOST, DB_PORT, DB_NAME, DB_USER)
    
    # Ask if user wants to update .env file
    print("\nWould you like to update the .env file with these credentials? (y/n)")
    update_env = input().strip().lower() == 'y'
    if update_env:
        update_env_file(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)

def update_env_file(host, port, database, user, password):
    """Update the .env file with new credentials"""
    try:
        # Read current .env file
        with open(".env", "r") as f:
            lines = f.readlines()
        
        # Update relevant lines
        new_lines = []
        jdbc_updated = False
        user_updated = False
        password_updated = False
        
        for line in lines:
            if line.startswith("DB_JDBC_URL="):
                new_lines.append(f"DB_JDBC_URL=jdbc:postgresql://{host}:{port}/{database}\n")
                jdbc_updated = True
            elif line.startswith("DB_USER="):
                new_lines.append(f"DB_USER={user}\n")
                user_updated = True
            elif line.startswith("DB_PASSWORD="):
                new_lines.append(f"DB_PASSWORD={password}\n")
                password_updated = True
            else:
                new_lines.append(line)
        
        # Add any missing lines
        if not jdbc_updated:
            new_lines.append(f"DB_JDBC_URL=jdbc:postgresql://{host}:{port}/{database}\n")
        if not user_updated:
            new_lines.append(f"DB_USER={user}\n")
        if not password_updated:
            new_lines.append(f"DB_PASSWORD={password}\n")
        
        # Write updated .env file
        with open(".env", "w") as f:
            f.writelines(new_lines)
        
        print("✅ .env file updated successfully!")
    except Exception as e:
        print(f"❌ Failed to update .env file: {e}")

def main():
    """Main function"""
    print("=== Database Connection Test ===")
    print("This script will help diagnose and fix database connection issues.")
    
    # Print current connection info
    print_connection_info()
    
    # Test current credentials
    password_success = test_password_auth(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)
    kerberos_success = False
    
    if not password_success:
        print("\nTrying Kerberos authentication as fallback...")
        kerberos_success = test_kerberos_auth(DB_HOST, DB_PORT, DB_NAME, DB_USER)
    
    # If both authentication methods fail, offer interactive mode
    if not password_success and not kerberos_success:
        print("\n❌ Both authentication methods failed.")
        print("Would you like to enter new connection details? (y/n)")
        try_interactive = input().strip().lower() == 'y'
        if try_interactive:
            interactive_connection_test()
    else:
        print("\n✅ Connection test completed successfully!")

if __name__ == "__main__":
    main()
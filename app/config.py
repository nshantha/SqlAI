import os
import urllib.parse
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# API Keys
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

# Database configuration
DB_JDBC_URL = os.getenv("DB_JDBC_URL", "")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Parse JDBC URL to extract components
# Format: jdbc:postgresql://hostname:port/database
jdbc_parts = DB_JDBC_URL.replace("jdbc:", "").split("://")
if len(jdbc_parts) == 2:
    host_port_db = jdbc_parts[1].split("/")
    host_port = host_port_db[0].split(":")
    
    DB_HOST = host_port[0]
    DB_PORT = host_port[1] if len(host_port) > 1 else "5432"
    DB_NAME = host_port_db[1] if len(host_port_db) > 1 else ""
else:
    # Default values if parsing fails
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "")

# MCP PostgreSQL server configuration
# Default to using npx which is more reliable
MCP_PG_SERVER_PATH = os.getenv("MCP_PG_SERVER_PATH", "npx")


# Encode username and password for URL safety
encoded_password = urllib.parse.quote_plus(DB_PASSWORD) if DB_PASSWORD else ""
MCP_PG_CONNECTION_STRING = f"postgres://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Application settings
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = int(os.getenv("APP_PORT", "8000"))
DEBUG = os.getenv("DEBUG", "False").lower() == "true"

# Database identification patterns
# Maps patterns found in schema to database names
DB_IDENTIFICATION_PATTERNS = {
    # Pattern: Database name
    r"promo_tracker": "promo_tracker_db",
    # Add more patterns as needed
}

# Default regex pattern to extract database name from schema header
DB_NAME_HEADER_PATTERN = r"Database Schema (?:for|of)?\s+([a-zA-Z0-9_]+)"
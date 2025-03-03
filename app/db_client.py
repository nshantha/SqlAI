import logging
import psycopg2
import psycopg2.extras
import urllib.parse
from typing import Dict, List, Any, Optional, Tuple

logger = logging.getLogger(__name__)

class DatabaseClient:
    """Client for interacting directly with PostgreSQL database"""
    
    def __init__(self, jdbc_url: str, username: str, password: str):
        """
        Initialize database client
        
        Args:
            jdbc_url: JDBC URL in format jdbc:postgresql://host:port/database
            username: Database username
            password: Database password
        """
        self.jdbc_url = jdbc_url
        self.username = username
        self.password = password
        self.conn = None
        
        # Parse JDBC URL to extract components
        self.host, self.port, self.database = self._parse_jdbc_url(jdbc_url)
    
    def _parse_jdbc_url(self, jdbc_url: str) -> Tuple[str, int, str]:
        """
        Parse JDBC URL to extract host, port, and database name
        
        Args:
            jdbc_url: JDBC URL in format jdbc:postgresql://host:port/database
            
        Returns:
            Tuple of (host, port, database)
        """
        # Remove jdbc: prefix
        if jdbc_url.startswith("jdbc:"):
            jdbc_url = jdbc_url[5:]
        
        # Parse URL
        parsed = urllib.parse.urlparse(jdbc_url)
        
        # Extract components
        host = parsed.hostname or "localhost"
        port = parsed.port or 5432
        
        # Extract database name (remove leading slash)
        database = parsed.path
        if database.startswith("/"):
            database = database[1:]
        
        return host, port, database
    
    async def connect(self, use_kerberos: bool = False) -> bool:
        """
        Connect to the database
        
        Args:
            use_kerberos: Whether to use Kerberos authentication
            
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Close existing connection if any
            if self.conn and not self.conn.closed:
                self.conn.close()
            
            # Build connection parameters
            conn_params = {
                "host": self.host,
                "port": self.port,
                "database": self.database,
                "user": self.username,
            }
            
            # Add password if not using Kerberos
            if not use_kerberos:
                if not self.password:
                    logger.error("Password authentication requested but no password provided")
                    return False
                conn_params["password"] = self.password
                logger.info(f"Attempting to connect with password authentication to {self.host}:{self.port}/{self.database}")
            
            # Add Kerberos options if using Kerberos
            if use_kerberos:
                conn_params["krbsrvname"] = "postgres"
                conn_params["gsslib"] = "gssapi"
                logger.info(f"Attempting to connect with Kerberos authentication to {self.host}:{self.port}/{self.database}")
            
            # Connect to the database with a timeout
            self.conn = psycopg2.connect(**conn_params, connect_timeout=10)
            self.conn.autocommit = True
            
            # Test the connection with a simple query
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            
            logger.info(f"Connected to database {self.database} at {self.host}")
            return True
            
        except psycopg2.OperationalError as e:
            if "password authentication failed" in str(e):
                logger.error(f"Password authentication failed for user '{self.username}'. Please check your credentials.")
            elif "could not initiate GSSAPI security context" in str(e):
                logger.error(f"Kerberos authentication failed. Please check your Kerberos configuration.")
            else:
                logger.error(f"Database connection error: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    async def disconnect(self):
        """Disconnect from the database"""
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.info("Disconnected from database")
    
    async def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            List of dictionaries with query results
        """
        if not self.conn or self.conn.closed:
            raise RuntimeError("Not connected to database")
        
        try:
            # Create cursor
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                # Execute query
                if params:
                    # Convert dict params to tuple if needed
                    if isinstance(params, dict):
                        # For named parameters
                        cursor.execute(query, params)
                    else:
                        # For positional parameters
                        cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                # Fetch results for SELECT queries
                if query.strip().upper().startswith("SELECT") or "RETURNING" in query.upper():
                    results = cursor.fetchall()
                    # RealDictCursor already returns dict-like objects, no need to convert
                    return list(results)
                
                # For other queries, return affected row count
                return [{"affected_rows": cursor.rowcount}]
        
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
    
    async def get_database_schema(self) -> Dict[str, Any]:
        """
        Get database schema information
        
        Returns:
            Dictionary with database schema information
        """
        if not self.conn or self.conn.closed:
            raise RuntimeError("Not connected to database")
        
        try:
            schema = {}
            
            # Get schemas
            schema_query = """
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
            ORDER BY schema_name
            """
            schemas = await self.execute_query(schema_query)
            
            # For each schema, get tables
            for schema_row in schemas:
                schema_name = schema_row["schema_name"]
                schema[schema_name] = {}
                
                # Get tables
                table_query = """
                SELECT table_name, table_type
                FROM information_schema.tables
                WHERE table_schema = %s
                ORDER BY table_name
                """
                # Pass parameters as a list for positional parameters
                tables = await self.execute_query(table_query, [schema_name])
                
                # For each table, get columns
                for table_row in tables:
                    table_name = table_row["table_name"]
                    table_type = table_row["table_type"]
                    
                    # Store table info
                    schema[schema_name][table_name] = {
                        "type": table_type,
                        "columns": {}
                    }
                    
                    # Get columns
                    column_query = """
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                    """
                    # Pass parameters as a list for positional parameters
                    columns = await self.execute_query(column_query, [schema_name, table_name])
                    
                    # Store column info
                    for column_row in columns:
                        column_name = column_row["column_name"]
                        schema[schema_name][table_name]["columns"][column_name] = {
                            "data_type": column_row["data_type"],
                            "is_nullable": column_row["is_nullable"],
                            "default": column_row["column_default"]
                        }
            
            return schema
            
        except Exception as e:
            logger.error(f"Error getting database schema: {e}")
            raise
    
    async def format_schema_for_llm(self) -> str:
        """
        Format database schema in a way that's helpful for LLMs
        
        Returns:
            Formatted schema string
        """
        try:
            schema = await self.get_database_schema()
            
            if not schema:
                return "No schema information available."
            
            formatted = ["# Database Schema\n"]
            
            for schema_name, tables in schema.items():
                formatted.append(f"## Schema: {schema_name}\n")
                
                for table_name, table_info in tables.items():
                    table_type = table_info.get("type", "TABLE")
                    formatted.append(f"### {table_type}: {table_name}\n")
                    
                    # Add column information
                    formatted.append("| Column | Type | Nullable | Default |")
                    formatted.append("|--------|------|----------|---------|")
                    
                    columns = table_info.get("columns", {})
                    for column_name, column_info in columns.items():
                        nullable = "YES" if column_info.get("is_nullable") == "YES" else "NO"
                        default = column_info.get("default", "") or ""
                        data_type = column_info.get("data_type", "unknown")
                        formatted.append(
                            f"| {column_name} | {data_type} | {nullable} | {default} |"
                        )
                    
                    formatted.append("\n")
            
            return "\n".join(formatted)
            
        except Exception as e:
            logger.error(f"Error formatting schema for LLM: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return f"Error getting schema information: {str(e)}"
    
    async def test_connection(self) -> Tuple[bool, str]:
        """
        Test database connection
        
        Returns:
            Tuple of (success, message)
        """
        try:
            # Try standard password auth first
            if await self.connect(use_kerberos=False):
                return True, "Connected successfully using password authentication."
            
            # Try Kerberos if password auth fails
            if await self.connect(use_kerberos=True):
                return True, "Connected successfully using Kerberos authentication."
            
            return False, "Failed to connect using both password and Kerberos authentication."
            
        except Exception as e:
            return False, f"Connection test failed: {str(e)}"
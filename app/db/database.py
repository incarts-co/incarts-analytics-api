import logging
import asyncio
from supabase import create_client, Client
from app.core.config import settings
import httpx
import os
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

supabase_client = None
direct_db_connection = None

def get_supabase_client():
    global supabase_client
    if supabase_client is None:
        try:
            supabase_client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
            logger.info("Supabase client created successfully.")
        except Exception as e:
            logger.error(f"Error creating Supabase client: {e}")
            raise
    return supabase_client

def get_direct_db_connection():
    """
    Creates a direct connection to the Supabase PostgreSQL database
    using connection information extracted from the DATABASE_URL or
    using service role credentials.
    
    This bypasses Supabase client limitations and allows direct SQL execution.
    """
    global direct_db_connection
    if direct_db_connection is None:
        try:
            # If the environment has a direct DATABASE_URL, use it
            if os.environ.get("DATABASE_URL"):
                conn_str = os.environ.get("DATABASE_URL")
                logger.info(f"Attempting to connect with DATABASE_URL: {conn_str[:20]}...")
                direct_db_connection = psycopg2.connect(conn_str)
                logger.info("Direct PostgreSQL connection created from DATABASE_URL.")
            # Otherwise construct one from Supabase service key if available 
            elif settings.SUPABASE_SERVICE_KEY:
                # Extract project ID from Supabase URL
                url = settings.SUPABASE_URL
                # Remove https:// or http:// prefix
                if url.startswith("https://"):
                    url = url[8:]
                elif url.startswith("http://"):
                    url = url[7:]
                
                # Extract the project ID
                project_id = url.split('.')[0]
                logger.info(f"Extracted project ID: {project_id}")
                
                # Construct the correct database host
                db_host = f"db.{project_id}.supabase.co"
                logger.info(f"Attempting to connect to PostgreSQL at {db_host} with service key")
                
                # Standard Supabase connection string format
                try:
                    direct_db_connection = psycopg2.connect(
                        host=db_host,
                        port="5432",
                        database="postgres", 
                        user="postgres",
                        password=settings.SUPABASE_SERVICE_KEY,
                        sslmode="require"
                    )
                    logger.info("Direct PostgreSQL connection created using service key.")
                except Exception as e:
                    logger.warning(f"Failed to connect with standard format: {e}")
                    
                    # Try without sslmode as fallback
                    try:
                        logger.info("Trying connection without SSL requirement")
                        direct_db_connection = psycopg2.connect(
                            host=db_host,
                            port="5432",
                            database="postgres", 
                            user="postgres",
                            password=settings.SUPABASE_SERVICE_KEY
                        )
                        logger.info("Direct PostgreSQL connection created without SSL requirement.")
                    except Exception as e2:
                        logger.error(f"All connection attempts failed: {e2}")
            else:
                logger.warning("No DATABASE_URL or SUPABASE_SERVICE_KEY available for direct DB connection.")
                
            # Test the connection
            if direct_db_connection:
                cursor = direct_db_connection.cursor()
                cursor.execute("SELECT 1")
                test_result = cursor.fetchone()
                cursor.close()
                logger.info(f"Connection test result: {test_result}")
                
        except Exception as e:
            logger.error(f"Error creating direct PostgreSQL connection: {e}")
            direct_db_connection = None
    return direct_db_connection

# For compatibility with existing code that expects a database connection
# Creates a wrapper that simulates asyncpg methods but uses direct psycopg2 connection
async def get_connection():
    try:
        # Log connection attempt details
        logger.info("Attempting to establish database connection")
        
        # Always try direct connection if environment variables are set
        if os.environ.get("DATABASE_URL") or settings.SUPABASE_SERVICE_KEY:
            logger.info("Trying direct PostgreSQL connection")
            conn = get_direct_db_connection()
            if conn:
                logger.info("Direct PostgreSQL connection successful")
                connection = DirectPgConnection(conn)
                yield connection
                return
            else:
                logger.warning("Direct PostgreSQL connection failed")
                
        # Fallback to Supabase client
        logger.info("Trying Supabase client connection")
        client = get_supabase_client()
        if not client:
            logger.error("Failed to initialize any database connection")
            raise ConnectionError("Failed to initialize database connection.")
        
        logger.info("Using Supabase client connection")
        connection = SupabaseConnection(client)
        yield connection
    except Exception as e:
        logger.error(f"Error getting database connection: {e}")
        raise

class DirectPgConnection:
    """
    Wrapper class to provide asyncpg-like interface using direct psycopg2 connection.
    This allows direct SQL execution with full permissions.
    """
    def __init__(self, pg_connection):
        self.conn = pg_connection
    
    async def fetchval(self, query, *args):
        """Fetch a single value from the query result."""
        cursor = self.conn.cursor()
        try:
            logger.info(f"Executing SQL query: {query}")
            logger.info(f"With parameters: {args}")
            # For psycopg2, we need to convert tuple of tuples to a flat tuple
            flat_args = args[0] if args and isinstance(args[0], tuple) else args
            cursor.execute(query, flat_args)
            result = cursor.fetchone()
            logger.info(f"Query result: {result}")
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
        finally:
            cursor.close()
    
    async def fetch(self, query, *args):
        """Fetch multiple rows from the query result."""
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        try:
            logger.info(f"Executing SQL query: {query}")
            logger.info(f"With parameters: {args}")
            # For psycopg2, we need to convert tuple of tuples to a flat tuple
            flat_args = args[0] if args and isinstance(args[0], tuple) else args
            cursor.execute(query, flat_args)
            results = cursor.fetchall()
            logger.info(f"Query returned {len(results)} rows")
            return results
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
        finally:
            cursor.close()
    
    async def execute(self, query, *args):
        """Execute a query without returning results."""
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, args)
            self.conn.commit()
        finally:
            cursor.close()

class SupabaseConnection:
    """
    Wrapper class to provide asyncpg-like interface using Supabase client.
    This is a fallback if direct connection is not available.
    """
    def __init__(self, supabase_client):
        self.client = supabase_client
    
    async def fetchval(self, query, *args):
        """Fetch a single value from the query result."""
        result = await self._execute_sql(query, args)
        if not result or not isinstance(result, list) or len(result) == 0:
            return None
        # Get the first row and the first column value
        first_row = result[0]
        first_col = list(first_row.values())[0] if first_row and len(first_row) > 0 else None
        return first_col
    
    async def fetch(self, query, *args):
        """Fetch multiple rows from the query result."""
        result = await self._execute_sql(query, args)
        return result if result and isinstance(result, list) else []
    
    async def _execute_sql(self, query, args):
        """
        Attempt to execute SQL through Supabase REST API.
        This is a limited fallback approach with restricted capabilities.
        """
        logger.warning("Using limited Supabase REST API for query execution. Some queries may not work.")
        try:
            # For simple queries, try to use table/view access
            if query.strip().upper().startswith("SELECT") and "FROM" in query.upper():
                # Very basic attempt to extract table name
                from_parts = query.upper().split("FROM")[1].strip().split()
                if from_parts:
                    table_name = from_parts[0].strip().lower()
                    # Try to use table select
                    result = self.client.table(table_name).select("*").execute()
                    return result.data
            
            # Otherwise, attempt RPC call - assumes functions already exist
            # This will likely fail without proper setup
            return []
        except Exception as e:
            logger.error(f"Error executing SQL through Supabase: {e}")
            return []
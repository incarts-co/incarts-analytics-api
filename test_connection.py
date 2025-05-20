import asyncio
import asyncpg
from app.core.config import settings

async def test_db_connection():
    print(f"Attempting to connect to database with connection string...")
    try:
        # Attempt to create a connection
        conn = await asyncpg.connect(settings.DATABASE_URL)
        
        # Run a simple query to verify connection
        version = await conn.fetchval('SELECT version();')
        print(f"Connection successful!")
        print(f"Database version: {version}")
        
        # Close the connection
        await conn.close()
        return True
    except Exception as e:
        print(f"Connection failed: {e}")
        return False

if __name__ == "__main__":
    result = asyncio.run(test_db_connection())
    exit(0 if result else 1)
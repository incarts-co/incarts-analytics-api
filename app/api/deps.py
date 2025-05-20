from app.db.database import get_connection

# This file can be used for other common dependencies as your app grows.
# For now, it just re-exports get_connection for easier import paths.
__all__ = ["get_connection"]
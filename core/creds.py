from enum import Enum
import os
import keyring


def _get_secret(env_var: str, keyring_name: str):
    """Retrieve secrets from environment variables or keyring."""
    env_val = os.getenv(env_var)
    if env_val is not None:
        return env_val
    try:
        return keyring.get_password('system', keyring_name)
    except Exception:
        return None

class Creds(Enum):
    """Secure storage for database credentials."""

    db_username = _get_secret('DB_USERNAME', 'db_username')
    db_password = _get_secret('DB_PASSWORD', 'db_password')
    snowflake_user = _get_secret('SNOWFLAKE_USER', 'snowflake_user')
    snowflake_password = _get_secret('SNOWFLAKE_PASSWORD', 'snowflake_password')

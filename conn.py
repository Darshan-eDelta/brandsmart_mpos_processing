# conn.py
import os
import re
import sys
from sqlalchemy import create_engine

# Import project-specific modules
import config
from log import log

# --- Global Environment Configuration ---
ENVIRONMENT = config.use_env

def get_db_connection_string() -> str:
    """
    Constructs and returns the appropriate PostgreSQL connection string (URL)
    based on the environment configuration.
    """
    db_config = None
    if ENVIRONMENT == 'prod':
        log.info("Using 'prod' environment configuration.")
        db_config = config.PG_PROD_DB_CONFIG
    elif ENVIRONMENT == 'dev':
        log.info("Using 'dev' environment configuration.")
        db_config = config.PG_DEV_DB_CONFIG
    else:
        log.error(f"Unknown environment: '{ENVIRONMENT}'. Halting.")
        sys.exit(1)

    if not db_config:
        log.error(f"Database configuration not found for environment: '{ENVIRONMENT}'.")
        sys.exit(1)

    # Construct the database URL from the config dictionary
    try:
        return (
            f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
    except KeyError as e:
        log.error(f"Database configuration dictionary is missing a required key: {e}")
        sys.exit(1)

def mask_password_in_url(url: str) -> str:
    """Replaces the password in a database URL with '***' for safe logging."""
    return re.sub(r':([^@]+)@', r':***@', url)

if __name__ == "__main__":
    # This block now tests if a valid connection string can be generated
    # and if SQLAlchemy can connect using it.
    print("Attempting to get DB connection string...")
    connection_string = get_db_connection_string()
    
    if connection_string:
        print(f"Successfully generated connection string: {mask_password_in_url(connection_string)}")
        
        print("\nTesting connection with SQLAlchemy engine...")
        try:
            engine = create_engine(connection_string)
            with engine.connect() as conn:
                print("SQLAlchemy engine connected successfully!")
            print("Connection closed.")
        except Exception as e:
            print(f"Failed to connect using SQLAlchemy engine. Error: {e}")
    else:
        print("Failed to generate a connection string.")
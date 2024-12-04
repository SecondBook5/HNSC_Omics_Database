# File: postgres_config.py
# This file configures the PostgreSQL connection for the HNSC Omics Database project,
# using SQLAlchemy with connection pooling, session management, and enhanced error handling.

import os  # Import os for accessing environment variables
from sqlalchemy import create_engine  # Import create_engine to establish a PostgreSQL connection with SQLAlchemy
from sqlalchemy.orm import sessionmaker  # Import sessionmaker for session management
from sqlalchemy.engine import Engine  # Import Engine type for type hinting
from sqlalchemy.exc import OperationalError, SQLAlchemyError  # Import specific exceptions for error handling
from dotenv import load_dotenv  # Import load_dotenv to load environment variables from a .env file
from pathlib import Path  # Import Path for managing filesystem paths
import logging  # Import logging to track database connection information and errors

# Configure logging to capture database connection information and errors
logging.basicConfig(level=logging.INFO)  # Set log level to INFO for general logs
logger = logging.getLogger(__name__)  # Create a logger specific to this module

# Load environment variables from the .env file in the config directory
env_path = Path(__file__).resolve().parent.parent / 'config' / '.env'  # Define the path to the .env file
try:
    load_dotenv(dotenv_path=env_path)  # Load environment variables from the .env file
    logger.info(".env file loaded successfully.")  # Log successful loading of .env
except Exception as e:  # Catch unexpected errors during .env loading
    logger.error(f"Failed to load .env file: {e}")  # Log the error details
    raise RuntimeError("Unable to load .env file. Ensure the file exists and is correctly formatted.") from e  # Raise descriptive error

# Construct PostgreSQL connection URL from environment variables
try:
    # Build the connection URL using environment variables
    POSTGRES_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB_NAME')}"
    # Validate that all necessary environment variables are present
    if not all([os.getenv('DB_USER'), os.getenv('DB_PASSWORD'), os.getenv('PG_HOST'), os.getenv('PG_PORT'), os.getenv('PG_DB_NAME')]):
        raise KeyError("One or more required PostgreSQL environment variables are missing.")
    logger.info("PostgreSQL connection URL constructed successfully.")  # Log successful URL construction
except KeyError as e:  # Catch missing environment variable errors
    logger.error(f"Missing environment variable: {e}")  # Log missing variable error
    raise RuntimeError(
        f"Environment variable {e} is required for PostgreSQL configuration.") from e  # Raise clear error for missing variables
except Exception as e:  # Catch any other unexpected errors during URL construction
    logger.error(f"Unexpected error during PostgreSQL URL construction: {e}")  # Log the error
    raise RuntimeError("Unexpected error occurred while constructing PostgreSQL connection URL.") from e  # Raise descriptive error

# Initialize SQLAlchemy Engine with connection pooling and error handling
try:
    engine: Engine = create_engine(
        POSTGRES_URL,  # Use the constructed PostgreSQL URL for the connection
        pool_size = int(os.getenv("PG_POOL_SIZE", 20)),  # Set max number of connections in the pool
        max_overflow = int(os.getenv("PG_MAX_OVERFLOW", 10)),  # Allow additional connections if the pool is full
        pool_timeout=int(os.getenv("PG_POOL_TIMEOUT", 30)),  # Wait timeout in seconds for a connection from the pool
        pool_recycle=1800,  # Recycle connections every 30 minutes to prevent stale connections
        echo=os.getenv("DEBUG", "False").lower() == "true"  # Enable SQL query logging if DEBUG mode is enabled
    )
    logger.info("PostgreSQL Engine created successfully.")  # Log successful engine creation
except OperationalError as op_err:  # Handle operational errors during engine creation
    logger.error(f"OperationalError during PostgreSQL engine creation: {op_err}")  # Log the error details
    raise RuntimeError(
        "Unable to connect to PostgreSQL - check database credentials, server status, and network connection.") from op_err  # Raise descriptive error
except SQLAlchemyError as sql_err:  # Handle general SQLAlchemy errors
    logger.error(f"SQLAlchemyError during PostgreSQL engine creation: {sql_err}")  # Log the error details
    raise RuntimeError(
        "Unexpected SQLAlchemy error occurred during engine creation. Verify SQLAlchemy setup.") from sql_err  # Raise descriptive error
except Exception as e:  # Catch unexpected errors during engine creation
    logger.error(f"Unexpected error during PostgreSQL engine initialization: {e}")  # Log the error details
    raise RuntimeError("Unexpected error occurred while initializing PostgreSQL engine.") from e  # Raise descriptive error

# Create a session factory for PostgreSQL using SQLAlchemy
try:
    SessionLocal = sessionmaker(
        autocommit=False,  # Disable autocommit for transactions
        autoflush=False,  # Disable autoflush to avoid unnecessary database writes
        bind=engine  # Bind the session to the initialized engine
    )
    logger.info("PostgreSQL session factory created successfully.")  # Log successful session factory creation
except Exception as e:  # Catch any unexpected errors during session factory creation
    logger.error(f"Unexpected error during PostgreSQL session factory initialization: {e}")  # Log the error details
    raise RuntimeError("Failed to create PostgreSQL session factory.") from e  # Raise descriptive error


def get_postgres_session():
    """
    Dependency function that provides a PostgreSQL session for ORM operations.
    Ensures the session is closed after use and logs errors if any occur during session creation.

    Yields:
        SessionLocal: A session connected to the PostgreSQL database.

    Raises:
        RuntimeError: If unable to create a session due to connection or configuration issues.
    """
    db = None  # Initialize db to None to prevent reference before assignment
    try:
        db = SessionLocal()  # Create a new session from the session factory
        logger.info("PostgreSQL session created successfully.")  # Log successful session creation
        yield db  # Yield the session to the calling context
    except OperationalError as op_err:  # Handle operational errors during session creation
        logger.error(f"OperationalError during PostgreSQL session creation: {op_err}")  # Log the error details
        raise RuntimeError(
            "Operational error encountered when connecting to the PostgreSQL session. Check server status.") from op_err  # Raise descriptive error
    except SQLAlchemyError as sql_err:  # Handle general SQLAlchemy errors during session handling
        logger.error(f"SQLAlchemyError during PostgreSQL session handling: {sql_err}")  # Log the error details
        raise RuntimeError(
            "SQLAlchemy error encountered during session operations.") from sql_err  # Raise descriptive error
    except Exception as e:  # Handle any unexpected errors during session handling
        logger.error(f"Unexpected error during PostgreSQL session creation: {e}")  # Log the error details
        raise RuntimeError(
            "Unexpected error occurred during PostgreSQL session creation.") from e  # Raise generic error
    finally:
        if db:  # Ensure the session is only closed if it was successfully created
            db.close()  # Close the session to release resources
            logger.info("PostgreSQL session closed.")  # Log successful session closure

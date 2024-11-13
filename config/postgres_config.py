# postgres_config.py
# This file configures the PostgreSQL connection for the HNSC Omics Database project,
# using SQLAlchemy with connection pooling, session management, and enhanced error handling.

import os  # Import os for accessing environment variables
from sqlalchemy import create_engine  # Import create_engine to establish a PostgreSQL connection with SQLAlchemy
from sqlalchemy.orm import sessionmaker  # Import sessionmaker for session management
from sqlalchemy.engine import Engine  # Import Engine type for type hinting
from sqlalchemy.exc import OperationalError, SQLAlchemyError  # Import specific exceptions for error handling
from dotenv import load_dotenv  # Import load_dotenv to load environment variables from .env file
from pathlib import Path  # Import Path for managing filesystem paths
import logging  # Import logging to track database connection information and errors

# Configure logging to capture database connection information and errors
logging.basicConfig(level=logging.INFO)  # Set log level to INFO
logger = logging.getLogger(__name__)  # Get a logger specific to this module

# Load environment variables from the .env file in the config directory
env_path = Path(__file__).resolve().parent.parent / 'config' / '.env'  # Define the path to the .env file
load_dotenv(dotenv_path=env_path)  # Load environment variables from .env file

# Construct PostgreSQL connection URL from environment variables
try:
    POSTGRES_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB_NAME')}"
except KeyError as e:  # Catch any missing environment variable errors
    logger.error(f"Missing environment variable: {e}")  # Log missing variable error
    raise RuntimeError(
        f"Environment variable {e} is required for the PostgreSQL configuration.") from e  # Raise a clear error for missing variables

# Initialize SQLAlchemy Engine with connection pooling and error handling
try:
    engine: Engine = create_engine(
        POSTGRES_URL,  # Use the constructed PostgreSQL URL for the connection
        pool_size=20,  # Set max number of connections in the pool
        max_overflow=10,  # Allow additional connections if pool is full
        pool_timeout=30,  # Wait timeout in seconds for a connection from the pool
        pool_recycle=1800,  # Recycle connections every 30 minutes to prevent stale connections
        echo=os.getenv("DEBUG", "False").lower() == "true"  # Enable SQL query logging if DEBUG mode is set to True
    )
    logger.info("PostgreSQL Engine created successfully.")  # Log successful engine creation
except OperationalError as op_err:  # Catch specific errors related to connectivity and resource issues
    logger.error(f"Failed to create PostgreSQL Engine: {op_err}")  # Log the operational error
    raise RuntimeError(
        "Unable to connect to PostgreSQL - check database credentials and network connection.") from op_err  # Raise an error for debugging
except SQLAlchemyError as sql_err:  # Catch general SQLAlchemy errors during engine creation
    logger.error(f"SQLAlchemy encountered an error: {sql_err}")  # Log SQLAlchemy-specific error
    raise RuntimeError(
        "Unexpected SQLAlchemy error occurred during engine creation.") from sql_err  # Raise error to indicate unexpected issue

# Create a session factory for PostgreSQL using SQLAlchemy
SessionLocal = sessionmaker(autocommit=False, autoflush=False,
                            bind=engine)  # Set autocommit and autoflush to False for ORM operations


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
        yield db  # Yield the session to the calling context
    except OperationalError as op_err:  # Catch connection-related issues during session creation
        logger.error(f"OperationalError during session creation: {op_err}")  # Log the operational error
        raise RuntimeError(
            "Operational error encountered when connecting to the PostgreSQL session.") from op_err  # Raise a descriptive error
    except SQLAlchemyError as sql_err:  # Catch general SQLAlchemy errors during session operations
        logger.error(f"SQLAlchemyError during session handling: {sql_err}")  # Log the SQLAlchemy error
        raise RuntimeError(
            "SQLAlchemy error encountered during session operations.") from sql_err  # Raise a descriptive error
    except Exception as e:  # Catch any unexpected errors that may occur
        logger.error(f"Unexpected error during session creation: {e}")  # Log the unexpected error
        raise RuntimeError(
            "Unexpected error during PostgreSQL session creation.") from e  # Raise a generic error for debugging
    finally:
        if db:  # Check if db is not None, ensuring db.close() is called only if the session was created successfully
            db.close()  # Close the session to release resources
            logger.info("PostgreSQL session closed.")  # Log that the session has been closed

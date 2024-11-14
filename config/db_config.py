# db_config.py
# This file serves as a centralized controller for database configurations in the HNSC Omics Database project.
# It provides unified access to PostgreSQL and MongoDB connections, handling session management, error handling,
# and defensive programming for reliable database operations across the project.

from config.postgres_config import get_postgres_session, engine as postgres_engine  # Import PostgreSQL session and engine from postgres_config
from config.mongo_config import get_mongo_client  # Import MongoDB client function from mongo_config
from sqlalchemy.ext.declarative import declarative_base  # Import declarative_base to define SQLAlchemy ORM models
from sqlalchemy.exc import SQLAlchemyError  # Import SQLAlchemyError for error handling in database operations
from pymongo.errors import PyMongoError  # Import PyMongoError for error handling with MongoDB
import logging  # Import logging to capture information and error messages

# Configure logging for database operations
logging.basicConfig(level=logging.INFO)  # Set logging level to INFO
logger = logging.getLogger(__name__)  # Create a logger specific to this module

# Define the SQLAlchemy Base class for ORM models
Base = declarative_base()  # Base class for all ORM models, allows table definitions for PostgreSQL


def get_postgres_engine():
    """
    Provides the SQLAlchemy engine for PostgreSQL.

    This function returns the PostgreSQL engine created in postgres_config, which
    is configured with pooling and error handling. Used for direct database
    connections where ORM sessions are not needed.

    Returns:
        Engine: The SQLAlchemy engine for PostgreSQL.
    """
    try:
        logger.info("Accessing PostgreSQL engine.")  # Log access attempt to PostgreSQL engine
        return postgres_engine  # Return the PostgreSQL engine instance
    except SQLAlchemyError as e:  # Catch SQLAlchemy errors if engine is unavailable
        logger.error(f"Error accessing PostgreSQL engine: {e}")  # Log specific SQLAlchemy error
        raise RuntimeError(
            "Failed to access PostgreSQL engine. Check configuration and connection.") from e  # Raise descriptive error


def get_db_session():
    """
    Provides a PostgreSQL session for ORM operations.

    This function yields a session from the session factory, allowing ORM operations
    with automatic session closure and error handling for PostgreSQL interactions.

    Yields:
        SessionLocal: A SQLAlchemy session bound to the PostgreSQL engine.

    Raises:
        RuntimeError: If unable to create a session due to connection or configuration issues.
    """
    session = None  # Initialize session to None for defensive programming
    try:
        session = get_postgres_session()  # Retrieve a session from the session factory
        logger.info("PostgreSQL session created successfully.")  # Log successful session creation
        yield session  # Yield session to the calling context
    except SQLAlchemyError as e:  # Catch SQLAlchemy errors during session creation or use
        logger.error(f"Error during PostgreSQL session operation: {e}")  # Log SQLAlchemy error
        raise RuntimeError(
            "Failed to create or use PostgreSQL session. Check database configuration.") from e  # Raise descriptive error
    finally:
        if session:  # Ensure session is only closed if it was created successfully
            session.close()  # Close the session to release resources
            logger.info("PostgreSQL session closed.")  # Log session closure


def get_mongo_connection():
    """
    Provides the MongoDB client for document-based operations.

    This function returns the MongoDB client created in mongo_config, configured with
    connection pooling, retry logic, and defensive error handling. It verifies the
    connection with a server ping before use.

    Returns:
        MongoClient: The MongoDB client for performing document-based operations.

    Raises:
        RuntimeError: If unable to establish a MongoDB client connection.
    """
    try:
        mongo_client = get_mongo_client()  # Retrieve the MongoDB client instance
        logger.info("MongoDB client accessed successfully.")  # Log successful access to MongoDB client
        return mongo_client  # Return the MongoDB client instance
    except PyMongoError as e:  # Catch general PyMongo errors for MongoDB operations
        logger.error(f"Error accessing MongoDB client: {e}")  # Log PyMongo error
        raise RuntimeError(
            "Failed to access MongoDB client. Check configuration and connection.") from e  # Raise descriptive error

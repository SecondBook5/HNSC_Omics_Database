# File: config/db_config.py
# This file serves as a centralized controller for database configurations in the HNSC Omics Database project.
# It provides unified access to PostgreSQL and MongoDB connections, handling session management, error handling,
# and defensive programming for reliable database operations across the project.

from contextlib import contextmanager  # For context-managed database sessions
from sqlalchemy.ext.declarative import declarative_base  # For SQLAlchemy ORM models
from sqlalchemy.orm import sessionmaker  # For SQLAlchemy session creation
from sqlalchemy.exc import SQLAlchemyError  # For SQLAlchemy error handling
from pymongo.errors import PyMongoError  # For MongoDB error handling
from config.postgres_config import get_postgres_session, engine as postgres_engine  # PostgreSQL session and engine
from config.mongo_config import get_mongo_client  # MongoDB client configuration
import logging  # For logging messages

# Configure logging for database operations
logging.basicConfig(level=logging.INFO)  # Set logging level to INFO
logger = logging.getLogger(__name__)  # Create a logger specific to this module

# Define the SQLAlchemy Base class for ORM models
Base = declarative_base()  # Base class for all ORM models

# Session factory for PostgreSQL
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=postgres_engine)


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
        logger.info("Accessing PostgreSQL engine.")  # Log access attempt
        return postgres_engine  # Return the PostgreSQL engine instance
    except SQLAlchemyError as e:  # Catch SQLAlchemy errors if engine is unavailable
        logger.error(f"Error accessing PostgreSQL engine: {e}")  # Log specific error
        raise RuntimeError(
            "Failed to access PostgreSQL engine. Check configuration and connection."
        ) from e  # Raise descriptive error


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
        logger.info("PostgreSQL session created successfully.")  # Log successful creation
        yield session  # Yield session to the calling context
    except SQLAlchemyError as e:  # Catch SQLAlchemy errors during session creation or use
        logger.error(f"Error during PostgreSQL session operation: {e}")  # Log error
        raise RuntimeError(
            "Failed to create or use PostgreSQL session. Check database configuration."
        ) from e  # Raise descriptive error
    finally:
        if session:  # Ensure session is only closed if it was created successfully
            session.close()  # Close the session to release resources
            logger.info("PostgreSQL session closed.")  # Log session closure


@contextmanager
def get_session_context():
    """
    Provides a PostgreSQL session as a context manager.

    This method allows safe usage in `with` statements, ensuring the session
    is properly closed after operations.

    Yields:
        Session: A SQLAlchemy session for ORM operations.

    Raises:
        RuntimeError: If unable to create or use the session.
    """
    session = SessionLocal()  # Initialize session
    try:
        yield session  # Yield the session to the context
    except SQLAlchemyError as e:  # Handle SQLAlchemy errors
        session.rollback()  # Rollback the transaction on error
        logger.error(f"Error during session operation: {e}")  # Log error
        raise
    finally:
        session.close()  # Ensure session is closed
        logger.info("Session closed.")  # Log closure


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
        logger.info("MongoDB client accessed successfully.")  # Log successful access
        return mongo_client  # Return the MongoDB client instance
    except PyMongoError as e:  # Catch general PyMongo errors
        logger.error(f"Error accessing MongoDB client: {e}")  # Log error
        raise RuntimeError(
            "Failed to access MongoDB client. Check configuration and connection."
        ) from e  # Raise descriptive error


@contextmanager
def get_mongo_context():
    """
    Provides a MongoDB client as a context manager.

    This method ensures the MongoDB client is properly closed after usage,
    allowing it to be used in `with` statements.

    Yields:
        MongoClient: A MongoDB client for performing document-based operations.

    Raises:
        RuntimeError: If unable to establish a MongoDB client connection.
    """
    mongo_client = None
    try:
        mongo_client = get_mongo_client()  # Retrieve the MongoDB client instance
        logger.info("MongoDB client accessed successfully.")  # Log successful access
        yield mongo_client  # Yield the client to the context
    except PyMongoError as e:  # Handle MongoDB errors
        logger.error(f"Error accessing MongoDB client: {e}")  # Log error
        raise RuntimeError(
            "Failed to access MongoDB client. Check configuration and connection."
        ) from e  # Raise descriptive error
    finally:
        if mongo_client:
            mongo_client.close()  # Close the MongoDB connection
            logger.info("MongoDB client connection closed.")  # Log closure

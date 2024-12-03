# File: mongo_config.py
# This file configures the MongoDB connection for the HNSC Omics Database project,
# using pymongo with connection pooling, retry logic, and enhanced error handling.

import os  # Import os for accessing environment variables
from pymongo import MongoClient, ReadPreference  # Import MongoClient and ReadPreference for MongoDB connection
from pymongo.errors import ConnectionFailure, ConfigurationError, PyMongoError  # Import specific exceptions for error handling
from dotenv import load_dotenv  # Import load_dotenv to load environment variables from a .env file
from pathlib import Path  # Import Path for managing filesystem paths
import logging  # Import logging to track MongoDB connection information and errors

# Configure logging to capture MongoDB connection information and errors
logging.basicConfig(level=logging.INFO)  # Set log level to INFO
logger = logging.getLogger(__name__)  # Get a logger specific to this module

# Load environment variables from the .env file in the config directory
env_path = Path(__file__).resolve().parent.parent / 'config' / '.env'  # Define the path to the .env file
try:
    load_dotenv(dotenv_path=env_path)  # Load environment variables from the .env file
    logger.info(".env file loaded successfully.")  # Log successful loading of .env
except Exception as e:  # Catch any unexpected error during loading
    logger.error(f"Failed to load .env file: {e}")  # Log the error details
    raise RuntimeError("Unable to load .env file. Check the file path and format.") from e  # Raise descriptive error

# Construct MongoDB connection URL from environment variables
try:
    # Retrieve MongoDB connection details from environment variables
    MONGO_URL = f"mongodb://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('MONGO_HOST')}:{os.getenv('MONGO_PORT')}/{os.getenv('MONGO_DB_NAME')}"
    if not all([os.getenv('DB_USER'), os.getenv('DB_PASSWORD'), os.getenv('MONGO_HOST'), os.getenv('MONGO_PORT'), os.getenv('MONGO_DB_NAME')]):
        raise KeyError("One or more required MongoDB environment variables are missing.")  # Ensure all variables are present
    logger.info("MongoDB connection URL constructed successfully.")  # Log successful URL creation
except KeyError as e:  # Catch any missing environment variable errors
    logger.error(f"Missing environment variable: {e}")  # Log missing variable error
    raise RuntimeError(
        f"Environment variable {e} is required for MongoDB configuration.") from e  # Raise a clear error for missing variables
except Exception as e:  # Catch unexpected errors during URL construction
    logger.error(f"Unexpected error during MongoDB URL construction: {e}")  # Log the error
    raise RuntimeError("Error occurred while constructing MongoDB connection URL.") from e  # Raise descriptive error

# Initialize MongoDB Client with pooling and retry settings
try:
    client = MongoClient(
        MONGO_URL,  # Use the constructed MongoDB URL for the connection
        maxPoolSize=50,  # Set max number of connections in the pool
        minPoolSize=10,  # Set minimum number of open connections in the pool
        serverSelectionTimeoutMS=5000,  # Timeout for selecting a MongoDB server (5 seconds)
        socketTimeoutMS=60000,  # Timeout for socket operations (1 minute)
        connectTimeoutMS=30000,  # Timeout for initial connection to MongoDB (30 seconds)
        retryWrites=True,  # Enable retry logic for write operations
        read_preference=ReadPreference.PRIMARY  # Read from the primary node in a replica set
    )
    # Log successful client creation
    logger.info("MongoDB Client created successfully with the provided configurations.")
except ConnectionFailure as conn_err:  # Catch connection-related errors
    logger.error(f"Failed to connect to MongoDB: {conn_err}")  # Log the connection failure
    raise RuntimeError(
        "Unable to connect to MongoDB - check server status and network connection.") from conn_err  # Raise a descriptive error
except ConfigurationError as conf_err:  # Catch configuration-related errors
    logger.error(f"MongoDB configuration error: {conf_err}")  # Log configuration error
    raise RuntimeError(
        "MongoDB configuration error - check environment variables and settings.") from conf_err  # Raise error for incorrect configuration
except PyMongoError as pymongo_err:  # Catch general PyMongo errors
    logger.error(f"PyMongo encountered an error: {pymongo_err}")  # Log PyMongo-specific error
    raise RuntimeError(
        "Unexpected PyMongo error during MongoDB client creation.") from pymongo_err  # Raise an error to indicate unexpected issue
except Exception as e:  # Catch any other unexpected errors
    logger.error(f"Unexpected error during MongoDB client initialization: {e}")  # Log the error
    raise RuntimeError("Unexpected error occurred while initializing MongoDB client.") from e  # Raise a descriptive error


def get_mongo_client() -> MongoClient:
    """
    Provides a MongoDB client instance for database operations.

    This function yields the MongoClient, allowing other modules to interact
    with MongoDB in a safe and efficient way. It logs connection issues and
    handles errors gracefully.

    Returns:
        MongoClient: A MongoDB client instance for database operations.

    Raises:
        RuntimeError: If unable to connect to MongoDB due to configuration
                      or connection issues.
    """
    try:
        # Attempt to access a collection to trigger server selection and verify connection
        client.admin.command('ping')  # Ping the MongoDB server to confirm connectivity
        logger.info("Successfully connected to MongoDB.")  # Log successful connection check
        return client  # Return the MongoClient instance
    except ConnectionFailure as conn_err:  # Catch connection-related errors
        logger.error(f"ConnectionFailure during MongoDB ping: {conn_err}")  # Log connection failure error
        raise RuntimeError(
            "Connection failure during MongoDB client ping - server may be unreachable.") from conn_err  # Raise descriptive error
    except ConfigurationError as conf_err:  # Catch configuration-related errors
        logger.error(f"ConfigurationError during MongoDB client ping: {conf_err}")  # Log configuration error
        raise RuntimeError(
            "Configuration error during MongoDB client ping - check settings.") from conf_err  # Raise descriptive error
    except PyMongoError as pymongo_err:  # Catch general PyMongo errors
        logger.error(f"PyMongoError during MongoDB client ping: {pymongo_err}")  # Log generic PyMongo error
        raise RuntimeError("Unexpected error during MongoDB client ping.") from pymongo_err  # Raise descriptive error
    except Exception as e:  # Catch any unexpected errors during ping
        logger.error(f"Unexpected error during MongoDB client ping: {e}")  # Log the error
        raise RuntimeError("Unexpected error occurred during MongoDB client ping.") from e  # Raise descriptive error

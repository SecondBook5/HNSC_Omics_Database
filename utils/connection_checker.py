# HNSC_Omics_Database/utils/connection_checker.py
import time  # Import time module for retry delay
import os  # Import os module to handle environment variables
from sqlalchemy import create_engine, text  # SQLAlchemy for PostgreSQL connection
from sqlalchemy.exc import OperationalError  # Exception for PostgreSQL connection errors
from pymongo import MongoClient  # MongoClient for MongoDB connection
from pymongo.errors import ConnectionFailure  # Exception for MongoDB connection errors
from dotenv import load_dotenv  # Load environment variables from .env file
from pathlib import Path

# Define the path to the .env file in the config directory
# Move up one directory level from the utils folder and point to config/.env
env_path = Path(__file__).resolve().parent.parent / 'config' / '.env'

# Load the .env file from the specified path
load_dotenv(dotenv_path=env_path)

class DatabaseConnectionChecker:
    """
    Checks the connection to PostgreSQL and MongoDB databases with retry logic.
    This class provides methods to verify connections to PostgreSQL and MongoDB,
    including retry mechanisms to handle transient connection failures.
    """

    def __init__(self, postgres_url: str, mongo_url: str, retries: int = 3, delay: int = 2) -> None:
        """
        Initializes the DatabaseConnectionChecker with database URLs and retry parameters.

        Args:
            postgres_url (str): PostgreSQL connection string.
            mongo_url (str): MongoDB connection string.
            retries (int): Maximum number of retry attempts.
            delay (int): Delay (in seconds) between retry attempts.
        """
        # PostgreSQL URL for connecting to the database
        self.postgres_url: str = postgres_url

        # MongoDB URL for connecting to the database
        self.mongo_url: str = mongo_url

        # Number of retries to attempt on failure
        self.retries: int = retries

        # Delay in seconds between each retry attempt
        self.delay: int = delay

    def check_postgresql_connection(self) -> bool:
        """
        Attempts to connect to PostgreSQL with retry logic.

        Returns:
            bool: True if the connection is successful, False otherwise.
        """
        # Loop over the number of retries
        for attempt in range(1, self.retries + 1):
            try:
                # Create a SQLAlchemy engine to connect to PostgreSQL
                engine = create_engine(self.postgres_url)

                # Connect to PostgreSQL and execute a test query
                with engine.connect() as conn:
                    # Execute a simple query to validate the connection
                    conn.execute(text("SELECT 1"))

                # Print success message if connection is successful
                print("PostgreSQL connection successful.")
                return True

            except OperationalError:  # Catch connection errors
                # Print retry message and delay
                print(
                    f"Attempt {attempt}: PostgreSQL connection failed. Retrying in {self.delay} seconds...")
                time.sleep(self.delay)

        # Print failure message after max retries
        print("PostgreSQL connection failed after maximum retries.")
        return False

    def check_mongodb_connection(self) -> bool:
        """
        Attempts to connect to MongoDB with retry logic.

        Returns:
            bool: True if the connection is successful, False otherwise.
        """
        # Loop over the number of retries
        for attempt in range(1, self.retries + 1):
            try:
                # Create a MongoClient instance with a timeout
                client = MongoClient(self.mongo_url, serverSelectionTimeoutMS=2000)

                # Ping the MongoDB server to validate the connection
                client.admin.command('ping')

                # Print success message if connection is successful
                print("MongoDB connection successful.")
                return True

            except ConnectionFailure:  # Catch connection errors
                # Print retry message and delay
                print(
                    f"Attempt {attempt}: MongoDB connection failed. Retrying in {self.delay} seconds...")
                time.sleep(self.delay)

        # Print failure message after max retries
        print("MongoDB connection failed after maximum retries.")
        return False


# Retrieve connection details from environment variables for PostgreSQL and MongoDB
postgres_url: str = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB_NAME')}"
mongo_url: str = f"mongodb://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('MONGO_HOST')}:{os.getenv('MONGO_PORT')}/{os.getenv('MONGO_DB_NAME')}"

# Initialize the connection checker with URLs created from .env variables
checker = DatabaseConnectionChecker(postgres_url=postgres_url, mongo_url=mongo_url)

# Attempt to check PostgreSQL connection and print the result
checker.check_postgresql_connection()

# Attempt to check MongoDB connection and print the result
checker.check_mongodb_connection()

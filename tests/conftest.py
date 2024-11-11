# tests/conftest.py
import pytest
import sys
import os

# Add the parent directory to the system path to ensure correct module import
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.connection_checker import DatabaseConnectionChecker

@pytest.fixture(scope="session", autouse=True)
def check_database_connections():
    """
    Fixture that checks database connections before running tests.
    This ensures PostgreSQL and MongoDB are accessible.
    """
    checker = DatabaseConnectionChecker()

    # Check PostgreSQL connection
    postgres_connected = checker.check_postgresql_connection()
    assert postgres_connected, "Failed to connect to PostgreSQL database."

    # Check MongoDB connection
    mongo_connected = checker.check_mongodb_connection()
    assert mongo_connected, "Failed to connect to MongoDB database."

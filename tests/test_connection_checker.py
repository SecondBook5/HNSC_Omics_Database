# File: tests/test_connection_checker.py
# Simplified test cases for utils/connection_checker.py using pytest.

import pytest
from unittest.mock import patch, MagicMock
from utils.connection_checker import DatabaseConnectionChecker


@pytest.fixture
def mock_postgres_engine():
    """
    Mock fixture for the PostgreSQL engine connection.
    """
    with patch("utils.connection_checker.get_postgres_engine") as mock_engine:
        yield mock_engine


@pytest.fixture
def mock_mongo_client():
    """
    Mock fixture for the MongoDB client connection.
    """
    with patch("utils.connection_checker.get_mongo_client") as mock_client:
        yield mock_client


def test_postgresql_connection_success(mock_postgres_engine):
    """
    Test PostgreSQL connection success.
    """
    # Mock the engine's connect method to simulate a successful connection
    mock_engine_instance = MagicMock()
    mock_postgres_engine.return_value = mock_engine_instance
    mock_engine_instance.connect.return_value.__enter__.return_value.execute.return_value = True

    checker = DatabaseConnectionChecker(retries=1, delay=0)
    assert checker.check_postgresql_connection() is True
    mock_engine_instance.connect.assert_called_once()


def test_mongodb_connection_success(mock_mongo_client):
    """
    Test MongoDB connection success.
    """
    # Mock the MongoDB client's admin command to simulate success
    mock_client_instance = MagicMock()
    mock_mongo_client.return_value = mock_client_instance
    mock_client_instance.admin.command.return_value = {"ok": 1}

    checker = DatabaseConnectionChecker(retries=1, delay=0)
    assert checker.check_mongodb_connection() is True
    mock_client_instance.admin.command.assert_called_once_with('ping')


def test_all_connections_success(mock_postgres_engine, mock_mongo_client):
    """
    Test successful connections for all databases.
    """
    # Mock successful behaviors for both PostgreSQL and MongoDB
    mock_engine_instance = MagicMock()
    mock_postgres_engine.return_value = mock_engine_instance
    mock_engine_instance.connect.return_value.__enter__.return_value.execute.return_value = True

    mock_client_instance = MagicMock()
    mock_mongo_client.return_value = mock_client_instance
    mock_client_instance.admin.command.return_value = {"ok": 1}

    checker = DatabaseConnectionChecker(retries=1, delay=0)
    assert checker.check_all_connections() is True
    mock_engine_instance.connect.assert_called_once()
    mock_client_instance.admin.command.assert_called_once_with('ping')

# db_config.py
# This module provides reusable functions to create database connections for PostgreSQL and MongoDB.
# It reads connection settings from environment variables, allowing secure and flexible access
# to both databases across the project. The functions return connection objects without performing
# retries or connection testing, leaving that to a dedicated checker. This file also defines the
# Base class and session factory for SQLAlchemy ORM models.

import os
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
from pymongo import MongoClient

# Load environment variables from the .env file located in the config directory
# This approach keeps sensitive information out of the codebase itself
env_path = Path(__file__).resolve().parent.parent / 'config' / '.env'
load_dotenv(dotenv_path=env_path)

# Debugging print statements to verify each environment variable
print("DB_USER:", os.getenv("DB_USER"))
print("DB_PASSWORD:", os.getenv("DB_PASSWORD"))
print("PG_HOST:", os.getenv("PG_HOST"))
print("PG_PORT:", os.getenv("PG_PORT"))
print("PG_DB_NAME:", os.getenv("PG_DB_NAME"))
print("MONGO_HOST:", os.getenv("MONGO_HOST"))
print("MONGO_PORT:", os.getenv("MONGO_PORT"))
print("MONGO_DB_NAME:", os.getenv("MONGO_DB_NAME"))

# Define the SQLAlchemy Base for ORM models
Base = declarative_base()

# PostgreSQL connection URL constructed from environment variables
postgres_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB_NAME')}"

# Create an engine for PostgreSQL connection
engine: Engine = create_engine(postgres_url)

# Define the session factory, binding sessions to the PostgreSQL engine
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_postgres_engine() -> Engine:
    """
    Creates and returns a SQLAlchemy engine instance for PostgreSQL.

    This function reads database credentials and connection settings from
    environment variables, providing a reusable engine that can be used to
    interact with the PostgreSQL database.

    Returns:
        Engine: A connection engine to PostgreSQL.
    """
    return engine


def get_mongo_client() -> MongoClient:
    """
    Creates and returns a pymongo MongoClient instance for MongoDB.

    This function reads MongoDB credentials and connection settings from
    environment variables, providing a reusable client for MongoDB operations.

    Returns:
        MongoClient: A MongoDB client.
    """
    # Build the MongoDB connection URL from environment variables
    mongo_url = f"mongodb://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('MONGO_HOST')}:{os.getenv('MONGO_PORT')}/{os.getenv('MONGO_DB_NAME')}"

    # Return a MongoClient instance, enabling connections to MongoDB
    return MongoClient(mongo_url)

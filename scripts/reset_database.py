from config.db_config import get_postgres_engine
from db.schema.metadata_schema import Base

def reset_database():
    """
    Drops and recreates all tables in the database to match the current schema.
    WARNING: This will delete all existing data in the tables!
    """
    # Get the database engine
    engine = get_postgres_engine()

    # Drop all tables
    print("Dropping existing tables...")
    Base.metadata.drop_all(engine)

    # Recreate all tables
    print("Creating new tables...")
    Base.metadata.create_all(engine)

    print("Database schema updated successfully.")

if __name__ == "__main__":
    reset_database()

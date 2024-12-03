import argparse
from config.db_config import get_postgres_engine, Base
from sqlalchemy import MetaData
from sqlalchemy.exc import ProgrammingError


def reset_database():
    """
    Drops and recreates all tables and associated indexes in the database to match the current schema.
    WARNING: This will delete all existing data in the tables!
    """
    # Get the database engine
    engine = get_postgres_engine()

    # Use a metadata object to introspect the database
    meta = MetaData()

    # Reflect current tables in the database
    meta.reflect(bind=engine)

    # Drop all tables explicitly using the metadata
    print("Dropping existing tables...")
    try:
        meta.drop_all(engine)
        print("Existing tables dropped successfully.")
    except ProgrammingError as e:
        print(f"Error dropping tables: {e}")

    # Recreate all tables
    print("Creating new tables...")
    Base.metadata.create_all(engine)

    print("Database schema updated successfully.")


def confirm_reset():
    """
    Prompt the user to confirm if they want to proceed with resetting the database.
    Returns:
        bool: True if the user confirms, False otherwise.
    """
    while True:
        user_input = input(
            "WARNING: This will remove all data from your database.\n"
            "Are you sure you want to proceed? (yes/no): "
        ).strip().lower()
        if user_input in ['yes', 'no']:
            return user_input == 'yes'
        else:
            print("Invalid input. Please type 'yes' or 'no'.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Reset the database schema by dropping all tables and recreating them."
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Skip the confirmation prompt and reset the database immediately."
    )
    args = parser.parse_args()

    if args.confirm or confirm_reset():
        reset_database()
    else:
        print("Operation aborted. The database was not modified.")

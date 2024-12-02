import re
from sqlalchemy.exc import SQLAlchemyError
from config.db_config import get_session_context
from db.schema.cptac_metadata_schema import CptacColumns


def parse_source_from_description(description: str) -> str:
    """
    Parse the source information from the description string.

    Args:
        description (str): The description string.

    Returns:
        str: The extracted source or an empty string if not found.
    """
    # Regex to match the pattern "from [source]"
    match = re.search(r'from (\w+)', description)
    return match.group(1) if match else ""


def update_missing_sources_in_columns():
    """
    Update missing sources in the `CptacColumns` table by parsing the `description` column.
    """
    with get_session_context() as session:
        try:
            # Query all entries with missing source
            missing_source_entries = session.query(CptacColumns).filter(CptacColumns.source == None).all()

            if not missing_source_entries:
                print("No entries with missing source found in CptacColumns.")
                return

            for entry in missing_source_entries:
                # Extract the source from the description
                if entry.description:
                    source = parse_source_from_description(entry.description)
                    if source:
                        # Update the source field
                        entry.source = source
                        print(f"Updated entry ID {entry.id}: Source set to '{source}'")
                    else:
                        print(f"Could not parse source for entry ID {entry.id} with description '{entry.description}'")
                else:
                    print(f"No description found for entry ID {entry.id}")

            # Commit the updates to the database
            session.commit()
            print("All missing sources updated successfully in CptacColumns.")

        except SQLAlchemyError as e:
            session.rollback()
            print(f"Database error: {e}")
        except Exception as e:
            session.rollback()
            print(f"Unexpected error: {e}")


if __name__ == "__main__":
    update_missing_sources_in_columns()

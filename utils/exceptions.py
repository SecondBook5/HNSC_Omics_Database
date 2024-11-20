class MissingForeignKeyError(Exception):
    """
    Custom exception raised when a referenced foreign key value does not exist in the database.
    """
    def __init__(self, missing_keys, foreign_key_name):
        self.missing_keys = missing_keys
        self.foreign_key_name = foreign_key_name
        message = f"Missing foreign key constraint for {foreign_key_name}: {missing_keys}"
        super().__init__(message)

from abc import ABC, abstractmethod
from typing import Dict, Any


class DataMapper(ABC):
    """
    Abstract base class for mapping extracted data to a target schema.

    This class provides a flexible interface for child classes to handle
    various types of data (e.g., metadata, raw data, hierarchical data).
    """

    def __init__(self, schema: Dict[str, Any]) -> None:
        """
        Initializes the DataMapper with a target schema.

        Args:
            schema (Dict[str, Any]): The schema for the target structure (e.g., tables, collections).
        """
        self.schema = schema

    @abstractmethod
    def map_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Maps raw data to the target schema.

        Args:
            raw_data (Dict[str, Any]): Extracted raw data to be mapped.

        Returns:
            Dict[str, Any]: Data mapped to the target schema.

        Raises:
            NotImplementedError: If not implemented in child classes.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @staticmethod
    def normalize_field(field: str) -> str:
        """
        Normalizes field names (e.g., removing extra spaces or special characters).

        Args:
            field (str): Raw field name.

        Returns:
            str: Normalized field name.
        """
        return field.strip().lower().replace(" ", "_")

    def validate_mapped_data(self, mapped_data: Dict[str, Any]) -> bool:
        """
        Validates the mapped data against the target schema.

        Args:
            mapped_data (Dict[str, Any]): Mapped data to validate.

        Returns:
            bool: True if valid, False otherwise.

        Raises:
            ValueError: If required structures or fields are missing in the mapped data.
        """
        for target, fields in self.schema.items():
            if target not in mapped_data:
                raise ValueError(f"Missing required structure: {target}")
            for field in fields:
                if field not in mapped_data[target]:
                    raise ValueError(f"Missing required field '{field}' in structure '{target}'")
        return True

# File: pipeline/geo_pipeline/geo_metadata_mapper.py

# Import the base class for data mapping
from pipeline.abstract_etl.data_mapper import DataMapper
# Import typing utilities to define types for function arguments and returns
from typing import Dict, List, Union
import logging


class GeoMetadataMapper(DataMapper):
    """
    Maps GEO metadata to the relational database schema.

    This class transforms extracted metadata (Series, Samples, Characteristics)
    into the format required for database insertion while preserving relationships.
    """

    def __init__(self, schema: Dict[str, List[str]], debug: bool = False) -> None:
        """
        Initializes the GeoMetadataMapper.

        Args:
            schema (Dict[str, List[str]]): A dictionary defining the database schema.
            debug (bool): Enables detailed debug logging if True.
        """
        super().__init__(schema, debug)
        self.logger = logging.getLogger("GeoMetadataMapper")
        if debug:
            logging.basicConfig(level=logging.DEBUG)

    def map_data(self, raw_data: Dict[str, Union[Dict, List]]) -> Dict[str, List[Dict[str, Union[str, None]]]]:
        """
        Maps raw GEO metadata to database-ready format.
        Args:
            raw_data (Dict[str, Union[Dict, List]]): Extracted GEO metadata.
        Returns:
            Dict[str, List[Dict[str, Union[str, None]]]]: Mapped data for database insertion.
        Raises:
            ValueError: If raw_data contains invalid structure or unexpected keys.
        """
        if not raw_data or not isinstance(raw_data, dict):
            raise ValueError("Invalid raw_data provided. Must be a non-empty dictionary.")
        # Initialize a dictionary to hold mapped data for each database table
        mapped_data = {table: [] for table in self.schema.keys()}
        # Validate raw_data keys
        allowed_keys = {"Series", "Samples"}
        if not allowed_keys.issuperset(raw_data.keys()):
            raise ValueError(f"Unexpected keys in raw_data: {set(raw_data.keys()) - allowed_keys}")
        # Check if Series data is present in the raw data and map it
        if "Series" in raw_data:
            try:
                mapped_data["Series"].append(self._map_series(raw_data["Series"]))
            except Exception as e:
                self.logger.error(f"Error mapping Series data: {e}")
                raise
        # Check if Sample data is present in the raw data and map it
        if "Samples" in raw_data:
            for sample in raw_data["Samples"]:
                try:
                    mapped_data["Samples"].append(self._map_sample(sample))

                    # Map Characteristics if available
                    characteristics = sample.get("Characteristics", {})
                    if characteristics:
                        mapped_data["Characteristics"].extend(
                            self._map_characteristics(sample["SampleID"], characteristics)
                        )
                except Exception as e:
                    self.logger.error(f"Error mapping Sample data: {e}")
                    raise
        # Validate mapped data against schema
        self._validate_mapped_data(mapped_data)
        # Return the fully mapped data dictionary for database insertion
        return mapped_data

    def _validate_mapped_data(self, mapped_data: Dict[str, List[Dict[str, Union[str, None]]]]) -> None:
        """
        Validates mapped data against the defined schema.

        Args:
            mapped_data (Dict[str, List[Dict[str, Union[str, None]]]]): Mapped data to validate.

        Raises:
            ValueError: If mapped data does not conform to the schema.
        """
        for table, rows in mapped_data.items():
            for row in rows:
                for column in row.keys():
                    if column not in self.schema.get(table, []):
                        self.logger.error(
                            f"Unexpected column '{column}' in table '{table}'. Allowed columns: {self.schema[table]}"
                        )
                        raise ValueError(f"Invalid column '{column}' for table '{table}'.")

    @staticmethod
    def _map_series(series_data: Dict[str, Union[str, None]]) -> Dict[str, Union[str, None]]:
        """
        Maps a single Series metadata entry to the database schema.

        Args:
            series_data (Dict[str, Union[str, None]]): Series metadata.

        Returns:
            Dict[str, Union[str, None]]: Mapped Series entry.
        """
        return {
            "series_id": series_data.get("SeriesID", "Unknown"),  # Default to 'Unknown' if SeriesID is missing
            "title": series_data.get("Title", "NA"),
            "submission_date": series_data.get("SubmissionDate", "NA"),
            "last_update_date": series_data.get("LastUpdateDate", "NA"),
            "pubmed_id": series_data.get("PubMedID"),
            "summary": series_data.get("Summary"),
            "overall_design": series_data.get("OverallDesign")
        }

    @staticmethod
    def _map_sample(sample_data: Dict[str, Union[str, None]]) -> Dict[str, Union[str, None]]:
        """
        Maps a single Sample metadata entry to the database schema.

        Args:
            sample_data (Dict[str, Union[str, None]]): Sample metadata.

        Returns:
            Dict[str, Union[str, None]]: Mapped Sample entry.
        """
        return {
            "sample_id": sample_data.get("SampleID", "Unknown"),  # Default to 'Unknown' if SampleID is missing
            "title": sample_data.get("Title", "NA"),
            "submission_date": sample_data.get("SubmissionDate", "NA"),
            "last_update_date": sample_data.get("LastUpdateDate", "NA"),
            "organism": sample_data.get("Organism", "Unknown")
        }

    @staticmethod
    def _map_characteristics(sample_id: str, characteristics: Dict[str, str]) -> List[Dict[str, Union[str, None]]]:
        """
        Maps Characteristics dictionary to individual rows in the database schema.

        Args:
            sample_id (str): The Sample ID associated with the characteristics.
            characteristics (Dict[str, str]): Dictionary of characteristics.

        Returns:
            List[Dict[str, Union[str, None]]]: List of mapped Characteristics rows.
        """
        mapped_characteristics = []
        for tag, value in characteristics.items():
            mapped_characteristics.append({
                "sample_id": sample_id,
                "characteristic_tag": tag,
                "characteristic_value": value or "NA"  # Default to "NA" if value is missing
            })
        return mapped_characteristics

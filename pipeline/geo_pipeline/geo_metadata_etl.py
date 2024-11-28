# File: pipeline/geo_pipeline/geo_metadata_etl.py
import logging
import os
import json
import re
from lxml import etree
from typing import Dict, Optional
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from config.db_config import get_postgres_engine
from db.schema.metadata_schema import DatasetSeriesMetadata, DatasetSampleMetadata
from config.logger_config import configure_logger
from pipeline.geo_pipeline.geo_file_handler import GeoFileHandler  # Import GeoFileHandler


class GeoMetadataETL:
    """
    Handles the extraction, transformation, and loading (ETL) of GEO metadata
    from XML files into a database, including logging and error handling.

    This class provides functionality to parse GEO metadata from MINiML XML files,
    validate the extracted metadata, and insert it into a PostgreSQL database.
    """
# ------------------ Initialization and Configuration ---------------------------------------------------------------
    def __init__(self, file_path: str, template_path: str, file_handler: GeoFileHandler, debug_mode: bool = False) -> None:
        """
        Initializes the GeoMetadataETL with file paths, logging settings, and a file handler.

        Args:
            file_path (str): Path to the GEO XML file.
            template_path (str): Path to the JSON field mapping template.
            file_handler (GeoFileHandler): Handles logging and file-related operations.
            debug_mode (bool): Enables detailed debug logging.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"XML file not found: {file_path}")
        if not os.path.exists(template_path):
            raise FileNotFoundError(f"Template file not found: {template_path}")

        # Assign the file paths and debug mode
        self.file_path = file_path
        self.template_path = template_path
        self.debug_mode = debug_mode
        self.file_handler = file_handler  # Assign the file handler instance

        # Configure the logger using the centralized configuration
        self.logger = configure_logger(
            name="GeoMetadataETL",  # Unique name for this component
            log_file="geo_metadata_etl.log",  # Log file specific to this ETL process
            level=logging.DEBUG if debug_mode else logging.INFO,  # Set level based on debug flag
            output="both"  # Log to both console and file
        )

        # Load the JSON template for XML field mappings
        self.template = self._load_template()


    def _load_template(self) -> Dict[str, Dict[str, str]]:
        """Loads the JSON template for XML field mappings."""
        try:
            with open(self.template_path, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            self.logger.error(f"Failed to load template: {e}")
            raise

    # ------------------------------- Validation ------------------------------------
    def _validate_xml(self) -> None:
        """Validates that the XML file is well-formed."""
        try:
            etree.parse(self.file_path)
        except etree.XMLSyntaxError as e:
            self.logger.error(f"Invalid XML structure: {e}")
            raise RuntimeError("Invalid XML structure.") from e

#-----------------------------Parsing and Extraction-----------------------------------
    def _extract_fields(self, element: etree._Element, field_paths: Dict[str, str], ns: Dict[str, str]) -> Dict:
        """
        Extracts fields from an XML element based on field paths.

        Args:
            element (etree._Element): XML element to extract data from.
            field_paths (Dict[str, str]): Dictionary mapping field names to XML paths.
            ns (Dict[str, str]): Namespace dictionary for XML.

        Returns:
            Dict: A dictionary of extracted fields.
        """
        data = {}
        for field_name, path in field_paths.items():
            try:
                if "@" in path:  # Handle attribute-based fields
                    attribute_value = element.xpath(path, namespaces=ns)
                    data[field_name] = attribute_value[0] if attribute_value else None
                elif field_name == "Characteristics":  # Handle multiple characteristics
                    characteristics = [
                        {"tag": char.attrib.get("tag", "Unknown"), "value": (char.text or "").strip()}
                        for char in element.findall(path, namespaces=ns)
                    ]
                    data[field_name] = characteristics
                elif field_name == "RelatedDatasets":  # Handle as list of dictionaries for JSON compatibility
                    relations = [
                        {"type": rel.attrib.get("type", "Unknown"), "target": rel.attrib.get("target", "")}
                        for rel in element.findall(path, namespaces=ns)
                    ]
                    data[field_name] = relations
                else:  # Default behavior for other fields
                    sub_elem = element.find(path, namespaces=ns)
                    data[field_name] = sub_elem.text.strip() if sub_elem is not None and sub_elem.text else None
            except Exception as e:
                self.logger.warning(f"Error extracting field '{field_name}' at path '{path}': {e}")
                data[field_name] = None
        return data

    def _process_series_data(self, series_elem: etree._Element, ns: Dict[str, str]) -> Optional[Dict]:
        """
        Processes and extracts metadata for a single Series element.

        This method extracts metadata from a Series XML element using the defined field template.
        It logs any errors encountered during extraction and ensures that the returned data is validated.

        Args:
            series_elem (etree._Element): XML element for the Series.
            ns (Dict[str, str]): XML namespace dictionary.

        Returns:
            Optional[Dict]: Extracted and validated metadata for the Series, or None if extraction fails.
        """
        try:
            # Extract fields using the defined template
            data = self._extract_fields(series_elem, self.template['Series'], ns)

            # Validate the extracted data (if a validation method is available for Series)
            if not data or 'SeriesID' not in data:
                self.logger.error(f"Validation failed for Series data: {data}")
                return None

            # Log the extracted data for debugging purposes
            self.logger.debug(f"Extracted Series data: {data}")
            return data
        except Exception as e:
            # Log detailed error information
            self.logger.error(f"Error processing Series element. Element: {series_elem.tag}, Error: {e}")
            return None

    def _process_sample_data(self, sample_elem: etree._Element, ns: Dict[str, str], series_id: str) -> Optional[Dict]:
        """
        Processes and validates metadata for a single Sample element.

        This method extracts metadata from a Sample XML element and associates it with the parent SeriesID.
        It logs any errors encountered during extraction or validation.

        Args:
            sample_elem (etree._Element): XML element representing the Sample.
            ns (Dict[str, str]): Namespace dictionary for parsing.
            series_id (str): The SeriesID to inherit for this sample.

        Returns:
            Optional[Dict]: A dictionary of validated Sample metadata, or None if invalid.
        """
        try:
            # Extract fields using the defined template for Samples
            data = self._extract_fields(sample_elem, self.template['Sample'], ns)

            # Inherit SeriesID from the parent Series
            data['SeriesID'] = series_id

            # Validate the extracted data
            if not self._validate_sample_data(data):
                self.logger.error(f"Validation failed for Sample data: {data}")
                return None

            # Log the extracted and validated data for debugging purposes
            self.logger.debug(f"Extracted and validated Sample data: {data}")
            return data
        except Exception as e:
            # Log detailed error information
            self.logger.error(f"Error processing Sample element. Element: {sample_elem.tag}, Error: {e}")
            return None

#-----------------------------Database Operations-----------------------------------
    def _pre_insert_series_id(self, session, series_id: str) -> None:
        """
        Pre-inserts the SeriesID into the dataset_series_metadata table to satisfy foreign key constraints.

        Args:
            session: The database session.
            series_id (str): The inferred SeriesID from the file name.
        """
        try:
            insert_query = insert(DatasetSeriesMetadata).values({"SeriesID": series_id}).on_conflict_do_nothing()
            session.execute(insert_query)
            session.commit()
        except SQLAlchemyError as e:
            self.logger.error(f"Error pre-inserting SeriesID {series_id}: {e}")
            raise

    def _update_series_sample_count(self, session, series_id: str, sample_count: int) -> None:
        """
        Updates the SampleCount field for a specific SeriesID in the dataset_series_metadata table.

        Args:
            session: SQLAlchemy session object.
            series_id (str): The SeriesID whose SampleCount needs to be updated.
            sample_count (int): The total number of samples associated with the SeriesID.

        Raises:
            SQLAlchemyError: If an error occurs while updating the database.
        """
        try:
            update_query = (
                insert(DatasetSeriesMetadata)
                .values(SeriesID=series_id, SampleCount=sample_count)
                .on_conflict_do_update(
                    index_elements=['SeriesID'],
                    set_={'SampleCount': sample_count}
                )
            )
            session.execute(update_query)
        except SQLAlchemyError as e:
            self.logger.error(f"Error updating SampleCount for SeriesID {series_id}: {e}")
            raise
#------------------------------ Main ETL Process -------------------------------------
    def parse_and_stream(self) -> int:
        """
        Parses a GEO MINiML file, extracts metadata, and streams it to the database.

        Steps:
            1. Infers the SeriesID from the filename and validates it.
            2. Ensures the SeriesID exists in the database using an upsert operation.
            3. Parses the XML file efficiently, minimizing memory usage with iterparse.
            4. Extracts metadata for Series and Sample elements and streams them to the database.
            5. Prevents duplicate uploads using a set of already uploaded SampleIDs.
            6. Updates the SampleCount field for the SeriesID in the database after all samples are processed.
            7. Logs the processing status using `GeoFileHandler.log_processed()`.
            8. Cleans up files associated with the SeriesID after successful processing.

        Returns:
            int: The number of samples processed and successfully uploaded to the database.

        Raises:
            RuntimeError: If any critical error occurs during the parsing or streaming process.
        """
        # Extract the base name of the file to infer the SeriesID.
        base_name = os.path.basename(self.file_path)
        inferred_series_id = base_name.split("_")[0]  # Extract SeriesID from the filename.

        # Validate inferred SeriesID using a regular expression.
        if not inferred_series_id or not re.match(r'^GSE\d+$', inferred_series_id):
            # Log and raise an error if SeriesID cannot be inferred.
            self.logger.critical(f"Failed to infer a valid SeriesID from filename: {base_name}")
            raise ValueError(f"Invalid SeriesID inferred from filename: {base_name}")

        # Log the inferred SeriesID.
        self.logger.info(f"Inferred SeriesID from filename: {inferred_series_id}")

        # Set up the database engine and session.
        engine = get_postgres_engine()
        Session = sessionmaker(bind=engine)
        session = Session()

        # Define the XML namespace for parsing.
        ns = {'geo': 'http://www.ncbi.nlm.nih.gov/geo/info/MINiML'}

        sample_count = 0  # Initialize sample count
        try:
            # Validate the XML structure to ensure the file is well-formed.
            self._validate_xml()
            self.logger.info("Parsing and streaming metadata in a single pass...")

            # Step 1: Ensure the SeriesID exists in the database.
            try:
                insert_query = insert(DatasetSeriesMetadata).values(
                    SeriesID=inferred_series_id
                ).on_conflict_do_nothing()  # Avoid duplicate insertions on conflict.
                session.execute(insert_query)
                session.commit()
                self.logger.info(f"Ensured SeriesID {inferred_series_id} exists in dataset_series_metadata.")
            except SQLAlchemyError as e:
                self.logger.error(f"Error ensuring SeriesID in database: {e}")
                raise

            # Step 2: Initialize variables for tracking uploaded samples.
            uploaded_samples = set()  # Track uploaded samples to avoid redundancy

            # Step 3: Parse the XML file using iterparse for efficient memory usage.
            context = etree.iterparse(
                self.file_path, events=("start", "end"),
                tag=["{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Series",
                     "{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Sample"]
            )

            for event, elem in context:
                if event == "end" and elem.tag.endswith("Series"):
                    # Process Series metadata
                    series_data = self._process_series_data(elem, ns)
                    if not series_data:
                        # Fallback to inferred SeriesID if no data extracted
                        series_data = {"SeriesID": inferred_series_id}
                    else:
                        # Ensure SeriesID matches inferred SeriesID
                        series_data["SeriesID"] = inferred_series_id

                    # Stream Series metadata to the database.
                    self._stream_series_to_db(session, series_data)
                    self.logger.info(f"Series {series_data['SeriesID']} uploaded successfully.")
                    elem.clear()  # Clear element memory
                    del elem.getparent()[0]  # Remove parent references

                elif event == "end" and elem.tag.endswith("Sample"):
                    # Process Sample metadata
                    sample_data = self._process_sample_data(elem, ns, inferred_series_id)
                    sample_id = sample_data.get("SampleID")
                    if sample_id and sample_id not in uploaded_samples:
                        self._stream_sample_to_db(session, sample_data)  # Stream Sample data.
                        uploaded_samples.add(sample_id)
                        sample_count += 1  # Increment the sample count
                        self.logger.info(f"Sample {sample_id} uploaded successfully.")
                    elem.clear()  # Clear element memory
                    del elem.getparent()[0]  # Remove parent references

            # Step 4: Update the SampleCount field for the SeriesID.
            self._update_series_sample_count(session, inferred_series_id, sample_count)
            self.logger.info(f"Updated SeriesID {inferred_series_id} with SampleCount {sample_count}.")

            # Step 5: Commit all changes made during parsing.
            session.commit()

            # Step 6: Log the processing status for the SeriesID.
            self.file_handler.log_processed(inferred_series_id)

            # Step 7: Clean up files for the processed SeriesID.
            self.file_handler.clean_files(inferred_series_id)

            # Log the successful completion of the ETL process.
            self.logger.info("Metadata streaming completed successfully.")

        except Exception as e:
            # Rollback changes in case of an error.
            session.rollback()
            self.logger.critical(f"Error during streaming: {e}")
            raise RuntimeError("Streaming failed.") from e
        finally:
            # Ensure the database session is closed.
            session.close()

        # Return the number of samples processed.
        return sample_count

    # ------------------- Helper Functions ----------------------------------------------
    def _validate_sample_data(self, sample_data: Dict) -> bool:
        """
        Validates the Sample metadata dictionary before insertion into the database.

        This method ensures that all required fields for a sample are present
        and logs an error if any required field is missing. It is essential
        for maintaining data integrity and preventing database insertion errors.

        Args:
            sample_data (Dict): A dictionary containing the Sample metadata to validate.

        Returns:
            bool: True if all required fields are present, False otherwise.
        """
        # Define the required fields for a valid Sample record.
        required_fields = ['SampleID', 'SeriesID', 'Title']

        # Iterate over each required field to check if it exists in sample_data.
        for field in required_fields:
            # If the field is missing, log an error and return False.
            if not sample_data.get(field):
                self.logger.error(f"Sample validation failed: Missing field '{field}'")
                return False

        # If all required fields are present, return True.
        return True

    def _stream_series_to_db(self, session, series_data: Dict) -> None:
        """
        Inserts or updates Series metadata into the dataset_series_metadata table.

        This method streams Series metadata into the database, ensuring that
        duplicate entries are handled gracefully by using an upsert operation.

        Args:
            session: SQLAlchemy session object for database operations.
            series_data (Dict): A dictionary containing the Series metadata to insert or update.

        Raises:
            SQLAlchemyError: If a database error occurs during the operation.
        """
        try:
            # Prepare an upsert query to insert or update the Series metadata.
            insert_query = insert(DatasetSeriesMetadata).values(series_data).on_conflict_do_update(
                index_elements=['SeriesID'],  # Use SeriesID as the unique key for conflict resolution.
                set_={key: series_data[key] for key in series_data if key != 'SeriesID'}
                # Update all fields except SeriesID.
            )
            # Execute the upsert query within the given database session.
            session.execute(insert_query)
        except SQLAlchemyError as e:
            # Log an error if a database exception occurs and re-raise the exception.
            self.logger.error(f"Database error during Series insertion: {e}")
            raise

    def _stream_sample_to_db(self, session, sample_data: Dict) -> None:
        """
        Inserts Sample metadata into the dataset_sample_metadata table.

        This method streams Sample metadata into the database. If a duplicate
        entry is encountered, the operation will silently skip inserting the record.

        Args:
            session: SQLAlchemy session object for database operations.
            sample_data (Dict): A dictionary containing the Sample metadata to insert.

        Raises:
            SQLAlchemyError: If a database error occurs during the operation.
        """
        try:
            # Prepare an insert query to add the Sample metadata, ignoring duplicates.
            insert_query = insert(DatasetSampleMetadata).values(sample_data).on_conflict_do_nothing()
            # Execute the insert query within the given database session.
            session.execute(insert_query)
        except SQLAlchemyError as e:
            # Log an error if a database exception occurs and re-raise the exception.
            self.logger.error(f"Database error during Sample insertion: {e}")
            raise

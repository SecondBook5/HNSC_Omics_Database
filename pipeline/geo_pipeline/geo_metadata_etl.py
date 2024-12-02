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
from db.schema.geo_metadata_schema import GeoSeriesMetadata, GeoSampleMetadata
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
        # Ensure the XML file exists and is a valid file
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"XML file not found: {file_path}")
        if not os.path.isfile(file_path):
            raise ValueError(f"Provided file path is not a valid file: {file_path}")
        if not os.access(file_path, os.R_OK):
            raise PermissionError(f"XML file is not readable: {file_path}")

        # Ensure the JSON template file exists and is valid
        if not os.path.exists(template_path):
            raise FileNotFoundError(f"Template file not found: {template_path}")
        if not os.path.isfile(template_path):
            raise ValueError(f"Provided template path is not a valid file: {template_path}")
        if not os.access(template_path, os.R_OK):
            raise PermissionError(f"Template file is not readable: {template_path}")

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
        """
        Loads the JSON template for XML field mappings.

        Returns:
            Dict[str, Dict[str, str]]: Parsed JSON template for field mappings.

        Raises:
            KeyError: If required keys are missing in the template.
            json.JSONDecodeError: If the template is not valid JSON.
            OSError: If the template file cannot be read.
        """
        try:
            with open(self.template_path, 'r') as f:
                template = json.load(f)

                # Validate required keys in the template
                required_keys = ['Series', 'Sample']
                missing_keys = [key for key in required_keys if key not in template]
                if missing_keys:
                    raise KeyError(f"Missing required keys in template: {', '.join(missing_keys)}")

                return template
        except (KeyError, json.JSONDecodeError, OSError) as e:
            self.logger.error(f"Failed to load template: {e}")
            raise

    # ------------------------------- Validation ------------------------------------
    def _validate_xml(self) -> None:
        """
        Validates that the XML file is well-formed.

        Raises:
            RuntimeError: If the XML structure is invalid.
        """
        try:
            # Parse the XML to ensure it is well-formed
            etree.parse(self.file_path)
            self.logger.info("XML structure validated successfully.")
        except etree.XMLSyntaxError as e:
            self.logger.error(f"Invalid XML structure: {e}")
            raise RuntimeError("Invalid XML structure.") from e
        except OSError as e:
            self.logger.error(f"Error accessing XML file: {e}")
            raise RuntimeError("Failed to validate XML file.") from e

    # ----------------------------- Parsing and Extraction -----------------------------------
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
        if not isinstance(ns, dict):
            self.logger.error("Invalid namespace dictionary provided.")
            raise ValueError("Namespace dictionary must be a valid dictionary.")

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

                # Validate required fields
                if field_name in ['SeriesID', 'SampleID'] and not data[field_name]:
                    raise ValueError(f"Critical field '{field_name}' is missing.")

            except ValueError as ve:
                self.logger.error(f"Validation failed for field '{field_name}': {ve}")
                data[field_name] = None
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
        if not isinstance(series_elem, etree._Element):
            self.logger.error("Invalid Series element provided for processing.")
            raise ValueError("Series element must be a valid XML element.")

        if not isinstance(ns, dict):
            self.logger.error("Invalid namespace dictionary provided for Series processing.")
            raise ValueError("Namespace dictionary must be a valid dictionary.")

        try:
            # Extract fields using the defined template
            data = self._extract_fields(series_elem, self.template['Series'], ns)

            # Validate the extracted data
            if not data or 'SeriesID' not in data:
                self.logger.error(f"Validation failed for Series data: {data}")
                return None

            # Log the extracted data for debugging purposes
            self.logger.debug(f"Extracted Series data: {data}")
            return data
        except KeyError as e:
            self.logger.error(f"Template key error while processing Series: {e}")
            raise ValueError("Series processing failed due to template issues.") from e
        except Exception as e:
            self.logger.error(f"Unexpected error while processing Series: {e}")
            raise RuntimeError("Series processing encountered an unexpected error.") from e

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
        if not isinstance(sample_elem, etree._Element):
            self.logger.error("Invalid Sample element provided for processing.")
            raise ValueError("Sample element must be a valid XML element.")

        if not isinstance(ns, dict):
            self.logger.error("Invalid namespace dictionary provided for Sample processing.")
            raise ValueError("Namespace dictionary must be a valid dictionary.")

        if not isinstance(series_id, str) or not series_id:
            self.logger.error("Invalid SeriesID provided for Sample processing.")
            raise ValueError("SeriesID must be a non-empty string.")

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
        except KeyError as e:
            self.logger.error(f"Template key error while processing Sample: {e}")
            raise ValueError("Sample processing failed due to template issues.") from e
        except Exception as e:
            self.logger.error(f"Unexpected error while processing Sample: {e}")
            raise RuntimeError("Sample processing encountered an unexpected error.") from e


#-----------------------------Database Operations-----------------------------------
    def _pre_insert_series_id(self, session, series_id: str) -> None:
        """
        Pre-inserts the SeriesID into the dataset_series_metadata table to satisfy foreign key constraints.

        Args:
            session: The database session.
            series_id (str): The inferred SeriesID from the file name.

        Raises:
            ValueError: If the SeriesID is invalid.
            RuntimeError: If a database error occurs during the insertion.
        """
        # Validate session and series_id inputs
        if session is None or not hasattr(session, "execute"):
            self.logger.error("Invalid session object provided for _pre_insert_series_id.")
            raise ValueError("Session must be a valid SQLAlchemy session object.")

        if not isinstance(series_id, str) or not series_id.strip():
            self.logger.error("Invalid SeriesID provided for pre-insertion.")
            raise ValueError("SeriesID must be a non-empty string.")

        try:
            # Log the pre-insertion attempt
            self.logger.debug(f"Attempting to pre-insert SeriesID: {series_id}")

            # Prepare the insert query
            insert_query = insert(GeoSeriesMetadata).values({"SeriesID": series_id}).on_conflict_do_nothing()

            # Execute the query
            session.execute(insert_query)
            session.commit()

            # Log successful insertion
            self.logger.info(f"Successfully pre-inserted SeriesID: {series_id}")
        except SQLAlchemyError as e:
            # Log database errors
            self.logger.error(f"Database error while pre-inserting SeriesID {series_id}: {e}")
            raise RuntimeError(f"Failed to pre-insert SeriesID {series_id} into the database.") from e
        except Exception as e:
            # Handle unexpected exceptions
            self.logger.critical(f"Unexpected error in _pre_insert_series_id for SeriesID {series_id}: {e}")
            raise RuntimeError("Unexpected error during pre-insertion of SeriesID.") from e

    def _update_series_sample_count(self, session, series_id: str, sample_count: int) -> None:
        """
        Updates the SampleCount field for a specific SeriesID in the dataset_series_metadata table.

        Args:
            session: SQLAlchemy session object.
            series_id (str): The SeriesID whose SampleCount needs to be updated.
            sample_count (int): The total number of samples associated with the SeriesID.

        Raises:
            ValueError: If inputs are invalid.
            RuntimeError: If a database error occurs during the update.
        """
        # Validate session, series_id, and sample_count inputs
        if session is None or not hasattr(session, "execute"):
            self.logger.error("Invalid session object provided for _update_series_sample_count.")
            raise ValueError("Session must be a valid SQLAlchemy session object.")

        if not isinstance(series_id, str) or not series_id.strip():
            self.logger.error("Invalid SeriesID provided for sample count update.")
            raise ValueError("SeriesID must be a non-empty string.")

        if not isinstance(sample_count, int) or sample_count < 0:
            self.logger.error("Invalid sample count provided for update.")
            raise ValueError("Sample count must be a non-negative integer.")

        try:
            # Log the update attempt
            self.logger.debug(f"Updating SampleCount for SeriesID {series_id} to {sample_count}")

            # Prepare the upsert query to update the sample count
            update_query = (
                insert(GeoSeriesMetadata)
                .values(SeriesID=series_id, SampleCount=sample_count)
                .on_conflict_do_update(
                    index_elements=['SeriesID'],
                    set_={'SampleCount': sample_count}
                )
            )

            # Execute the update query
            session.execute(update_query)
            session.commit()

            # Log successful update
            self.logger.info(f"Successfully updated SampleCount for SeriesID {series_id} to {sample_count}")
        except SQLAlchemyError as e:
            # Log database errors
            self.logger.error(f"Database error while updating SampleCount for SeriesID {series_id}: {e}")
            raise RuntimeError(f"Failed to update SampleCount for SeriesID {series_id} in the database.") from e
        except Exception as e:
            # Handle unexpected exceptions
            self.logger.critical(f"Unexpected error in _update_series_sample_count for SeriesID {series_id}: {e}")
            raise RuntimeError("Unexpected error during SampleCount update.") from e

#------------------------------ Main ETL Process -------------------------------------
    def parse_and_stream(self) -> int:
        """
        Parses a GEO MINiML file, extracts metadata, and streams it to the database.

        This method performs the ETL process for a GEO MINiML file, ensuring robust error
        handling and efficient memory usage. It validates the input file, processes Series
        and Sample metadata, and updates the database while maintaining data integrity.

        Steps:
            1. Validate Input:
                - Ensure the input file exists, is an XML file, and infer the SeriesID from its name.
                - Log errors or warnings for invalid files.
            2. Initialize Database Session:
                - Set up the PostgreSQL engine and database session for interactions.
            3. Validate XML Structure:
                - Check if the XML file is well-formed and log any syntax errors.
            4. Ensure SeriesID in Database:
                - Insert the SeriesID into the database if it does not already exist.
            5. Fetch Existing Samples:
                - Retrieve existing SampleIDs for the SeriesID to avoid duplicate uploads.
            6. Initialize XML Parsing:
                - Use `iterparse` for efficient, memory-safe processing of XML elements.
            7. Process XML Elements:
                - Parse and insert metadata for Series and Sample elements into the database.
                - Handle missing or invalid metadata with fallbacks or logs.
            8. Update SampleCount:
                - Update the count of associated samples for the SeriesID in the database.
            9. Log Processing Status:
                - Mark the SeriesID as processed in the metadata log.
                - Clean up files associated with the SeriesID.
            10. Return Processed Sample Count:
                - Return the total number of new samples successfully processed.

        Returns:
            int: The number of samples processed and successfully uploaded to the database.

        Raises:
            RuntimeError: If any critical error occurs during the parsing or streaming process.
        """

        # ------------------- Step 1: Validate Input -------------------

        # Validate that the file path exists and is an XML file.
        if not self.file_path or not self.file_path.endswith(".xml"):
            # Log a warning and skip processing for non-XML files.
            self.logger.warning(f"Skipping non-XML or invalid file: {self.file_path}")
            return 0  # Skip processing and return zero new samples.

        # Extract the base name of the file to infer the SeriesID.
        base_name = os.path.basename(self.file_path)  # Extract the file name from the path.
        inferred_series_id = base_name.split("_")[0] if base_name else None  # Get SeriesID from the filename.

        # Validate that the inferred SeriesID matches the expected format (e.g., "GSE12345").
        if not inferred_series_id or not re.match(r'^GSE\d+$', inferred_series_id):
            # Log an error if the SeriesID is invalid.
            self.logger.error(f"Failed to infer a valid SeriesID from filename: {base_name}")
            return 0  # Skip processing and return zero new samples.

        # Log the inferred SeriesID for debugging purposes.
        self.logger.info(f"Processing GEO Series ID: {inferred_series_id}")

        # ------------------- Step 2: Initialize Database Session -------------------

        try:
            # Initialize the PostgreSQL database engine.
            engine = get_postgres_engine()

            # Create a sessionmaker object bound to the engine.
            Session = sessionmaker(bind=engine)

            # Create a new session for database interactions.
            session = Session()
        except Exception as e:
            # Log and raise an error if the database session cannot be initialized.
            self.logger.critical(f"Failed to initialize database session: {e}")
            raise RuntimeError("Database initialization failed.") from e

        # Define the XML namespace for MINiML files.
        ns = {'geo': 'http://www.ncbi.nlm.nih.gov/geo/info/MINiML'}

        # Initialize a counter for new samples processed during this run.
        new_sample_count = 0

        # ------------------- Step 3: Validate XML Structure -------------------

        try:
            # Validate that the XML file is well-formed.
            self._validate_xml()
            self.logger.info("XML structure validated.")  # Log successful validation.

            # ------------------- Step 4: Ensure SeriesID in Database -------------------

            try:
                # Insert the SeriesID into the database if it doesn't already exist.
                insert_query = insert(GeoSeriesMetadata).values(SeriesID=inferred_series_id).on_conflict_do_nothing()
                session.execute(insert_query)  # Execute the query.
                session.commit()  # Commit the transaction.
                self.logger.info(f"Ensured SeriesID {inferred_series_id} in database.")  # Log success.
            except SQLAlchemyError as e:
                # Log any errors related to the SeriesID insertion.
                self.logger.error(f"Error ensuring SeriesID {inferred_series_id}: {e}")
                raise RuntimeError("Failed to ensure SeriesID in database.") from e

            # ------------------- Step 5: Fetch Existing Samples -------------------

            try:
                # Fetch all existing samples for the SeriesID to avoid duplicate uploads.
                existing_samples = session.query(GeoSampleMetadata.SampleID).filter_by(
                    SeriesID=inferred_series_id).all()
                uploaded_samples = set(sample[0] for sample in existing_samples)  # Convert to a set for fast lookups.
            except SQLAlchemyError as e:
                # Log any errors related to fetching existing samples.
                self.logger.error(f"Error fetching existing samples for SeriesID {inferred_series_id}: {e}")
                raise RuntimeError("Failed to fetch existing samples.") from e

            # ------------------- Step 6: Initialize XML Parsing -------------------

            try:
                # Create an XML iterparse context for efficient streaming of elements.
                context = etree.iterparse(
                    self.file_path, events=("start", "end"),
                    tag=["{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Series",
                         "{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Sample"]
                )
            except Exception as e:
                # Log and raise an error if XML parsing cannot be initialized.
                self.logger.error(f"Error initializing XML parsing for {self.file_path}: {e}")
                raise RuntimeError("Failed to initialize XML parsing.") from e

            # ------------------- Step 7: Process XML Elements -------------------

            for event, elem in context:
                if event == "end" and elem.tag.endswith("Series"):
                    # Process the Series element and extract metadata.
                    series_data = self._process_series_data(elem, ns)

                    # If metadata extraction fails, create a fallback dictionary.
                    if not series_data:
                        self.logger.warning(
                            f"Missing or invalid Series metadata for {inferred_series_id}. Using fallback.")
                        series_data = {"SeriesID": inferred_series_id}
                    else:
                        # Ensure the SeriesID in the metadata matches the inferred SeriesID.
                        series_data["SeriesID"] = inferred_series_id

                    # Insert the Series metadata into the database.
                    try:
                        # Stream the Series metadata into the database.
                        self._stream_series_to_db(session, series_data)
                    except SQLAlchemyError as e:
                        # Log database errors for Series insertion.
                        self.logger.error(f"Failed to insert Series {inferred_series_id}: {e}")

                    # Clear memory for the processed element to avoid memory leaks.
                    elem.clear()
                    while elem.getparent() is not None:
                        del elem.getparent()[0]

                elif event == "end" and elem.tag.endswith("Sample"):
                    # Process the Sample element and extract metadata.
                    sample_data = self._process_sample_data(elem, ns, inferred_series_id)

                    # Extract the SampleID from the metadata.
                    sample_id = sample_data.get("SampleID") if sample_data else None

                    # Avoid re-uploading samples that already exist in the database.
                    if sample_id and sample_id not in uploaded_samples:
                        try:
                            # Insert new Sample metadata into the database.
                            result = session.execute(
                                insert(GeoSampleMetadata).values(sample_data).on_conflict_do_nothing()
                            )
                            if result.rowcount > 0:  # If a new row was inserted.
                                uploaded_samples.add(sample_id)  # Add the SampleID to the uploaded set.
                                new_sample_count += 1  # Increment the new sample counter.
                        except SQLAlchemyError as e:
                            # Log database errors for Sample insertion.
                            self.logger.error(f"Failed to insert Sample {sample_id}: {e}")

                    # Clear memory for the processed element to avoid memory leaks.
                    elem.clear()
                    while elem.getparent() is not None:
                        del elem.getparent()[0]

            # ------------------- Step 8: Update SampleCount -------------------

            if new_sample_count > 0:
                try:
                    # Update the SampleCount field for the SeriesID.
                    self._update_series_sample_count(session, inferred_series_id, len(uploaded_samples))
                    self.logger.info(f"Updated SampleCount for SeriesID {inferred_series_id}: {len(uploaded_samples)}")
                except SQLAlchemyError as e:
                    # Log any errors that occur while updating the count.
                    self.logger.error(f"Error updating SampleCount for SeriesID {inferred_series_id}: {e}")

            # Commit all changes to the database.
            session.commit()

            # ------------------- Step 9: Log Processing Status -------------------

            try:
                # Log the SeriesID as processed in the metadata log.
                self.file_handler.log_processed(inferred_series_id)

                # Clean up files associated with the SeriesID.
                self.file_handler.clean_files(inferred_series_id)
            except Exception as e:
                self.logger.error(f"Error during file handling for SeriesID {inferred_series_id}: {e}")

            # Log the successful completion of the ETL process.
            self.logger.info("Metadata streaming completed successfully.")

        except Exception as e:
            # Roll back changes in case of an error during processing.
            session.rollback()
            self.logger.critical(f"Error during streaming: {e}")
            raise RuntimeError("Streaming failed.") from e
        finally:
            # Ensure the database session is closed to avoid resource leaks.
            if session:
                session.close()

        # ------------------- Step 10: Return Processed Sample Count -------------------

        # Return the total number of new samples processed during this run.
        return new_sample_count

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

        # Check if sample_data is None or not a dictionary.
        if not isinstance(sample_data, dict):
            self.logger.error("Invalid sample_data provided: Must be a dictionary.")
            return False

        # Iterate over each required field to check if it exists in sample_data.
        for field in required_fields:
            # If the field is missing or empty, log an error and return False.
            if not sample_data.get(field):
                self.logger.error(f"Sample validation failed: Missing or empty field '{field}'")
                return False

        # If all required fields are present and valid, return True.
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
            # Validate input parameters before proceeding.
            if not session:
                self.logger.error("Invalid session provided: Cannot be None.")
                raise ValueError("Session is required for database operations.")
            if not isinstance(series_data, dict):
                self.logger.error("Invalid series_data provided: Must be a dictionary.")
                raise ValueError("Series data must be a dictionary.")

            # Prepare an upsert query to insert or update the Series metadata.
            insert_query = insert(GeoSeriesMetadata).values(series_data).on_conflict_do_update(
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
        except Exception as e:
            # Log unexpected errors and re-raise them for further handling.
            self.logger.error(f"Unexpected error during Series streaming: {e}")
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
            # Validate input parameters before proceeding.
            if not session:
                self.logger.error("Invalid session provided: Cannot be None.")
                raise ValueError("Session is required for database operations.")
            if not isinstance(sample_data, dict):
                self.logger.error("Invalid sample_data provided: Must be a dictionary.")
                raise ValueError("Sample data must be a dictionary.")

            # Prepare an insert query to add the Sample metadata, ignoring duplicates.
            insert_query = insert(GeoSampleMetadata).values(sample_data).on_conflict_do_nothing()

            # Execute the insert query within the given database session.
            session.execute(insert_query)
        except SQLAlchemyError as e:
            # Log an error if a database exception occurs and re-raise the exception.
            self.logger.error(f"Database error during Sample insertion: {e}")
            raise
        except Exception as e:
            # Log unexpected errors and re-raise them for further handling.
            self.logger.error(f"Unexpected error during Sample streaming: {e}")
            raise


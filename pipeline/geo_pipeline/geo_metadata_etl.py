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

        self.file_path = file_path
        self.template_path = template_path
        self.debug_mode = debug_mode
        self.file_handler = file_handler  # Assign the file handler instance

        self.logger = configure_logger(
            name="GeoMetadataETL",
            log_dir="./logs",
            log_file="geo_metadata_etl.log",
            level=logging.DEBUG if debug_mode else logging.INFO,
            output="both"
        )

        self.template = self._load_template()


    def _load_template(self) -> Dict[str, Dict[str, str]]:
        """Loads the JSON template for XML field mappings."""
        try:
            with open(self.template_path, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            self.logger.error(f"Failed to load template: {e}")
            raise

    def _validate_xml(self) -> None:
        """Validates that the XML file is well-formed."""
        try:
            etree.parse(self.file_path)
        except etree.XMLSyntaxError as e:
            self.logger.error(f"Invalid XML structure: {e}")
            raise RuntimeError("Invalid XML structure.") from e

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

        Args:
            series_elem (etree._Element): XML element for the Series.
            ns (Dict[str, str]): XML namespace dictionary.

        Returns:
            Optional[Dict]: Extracted and validated metadata for the Series, or None if extraction fails.
        """
        try:
            # Extract fields using the defined template
            data = self._extract_fields(series_elem, self.template['Series'], ns)
            # Log the extraction process
            self.logger.debug(f"Extracted Series data: {data}")
            return data
        except Exception as e:
            self.logger.error(f"Error processing Series element: {e}")
            return None

    def _process_sample_data(self, sample_elem: etree._Element, ns: Dict[str, str], series_id: str) -> Optional[Dict]:
        """
        Processes and validates a single Sample element with inherited SeriesID.

        Args:
            sample_elem (etree._Element): XML element representing the Sample.
            ns (Dict[str, str]): Namespace dictionary for parsing.
            series_id (str): The SeriesID to inherit for this sample.

        Returns:
            Optional[Dict]: A dictionary of validated Sample metadata, or None if invalid.
        """
        try:
            # Extract fields from the Sample element
            data = self._extract_fields(sample_elem, self.template['Sample'], ns)
            data['SeriesID'] = series_id  # Inherit SeriesID from the parent Series
            return data
        except Exception as e:
            self.logger.error(f"Error processing Sample element: {e}")
            return None

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

    def parse_and_stream(self) -> None:
        """
        Parses the GEO MINiML file, extracts metadata, and streams it directly to the database.
        Associates all metadata with the inferred SeriesID from the filename.
        """
        # Extract SeriesID from filename
        base_name = os.path.basename(self.file_path)
        inferred_series_id = base_name.split("_")[0]  # e.g., GSE112026 from GSE112026_family.xml

        # Validate inferred SeriesID
        if not inferred_series_id or not re.match(r'^GSE\d+$', inferred_series_id):
            self.logger.critical(f"Failed to infer a valid SeriesID from filename: {base_name}")
            raise ValueError(f"Invalid SeriesID inferred from filename: {base_name}")

        self.logger.info(f"Inferred SeriesID from filename: {inferred_series_id}")

        # Set up database session
        engine = get_postgres_engine()
        Session = sessionmaker(bind=engine)
        session = Session()

        # Namespace for XML parsing
        ns = {'geo': 'http://www.ncbi.nlm.nih.gov/geo/info/MINiML'}

        try:
            self._validate_xml()
            self.logger.info("Parsing and streaming metadata in a single pass...")

            # Step 1: Ensure the SeriesID exists in the database
            try:
                insert_query = insert(DatasetSeriesMetadata).values(
                    SeriesID=inferred_series_id
                ).on_conflict_do_nothing()
                session.execute(insert_query)
                session.commit()
                self.logger.info(f"Ensured SeriesID {inferred_series_id} exists in dataset_series_metadata.")
            except SQLAlchemyError as e:
                self.logger.error(f"Error ensuring SeriesID in database: {e}")
                raise

            # Step 2: Initialize sample count
            sample_count = 0
            uploaded_samples = set()  # Track uploaded samples to avoid redundancy

            # Parse the XML
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

                    self._stream_series_to_db(session, series_data)
                    self.logger.info(f"Series {series_data['SeriesID']} uploaded successfully.")
                    elem.clear()  # Clear element memory
                    del elem.getparent()[0]  # Remove parent references

                elif event == "end" and elem.tag.endswith("Sample"):
                    # Process Sample metadata
                    sample_data = self._process_sample_data(elem, ns, inferred_series_id)
                    sample_id = sample_data.get("SampleID")
                    if sample_id and sample_id not in uploaded_samples:
                        self._stream_sample_to_db(session, sample_data)
                        uploaded_samples.add(sample_id)
                        sample_count += 1  # Increment the sample count
                        self.logger.info(f"Sample {sample_id} uploaded successfully.")
                    elem.clear()  # Clear element memory
                    del elem.getparent()[0]  # Remove parent references

            # Step 3: Update the sample count in dataset_series_metadata
            self._update_series_sample_count(session, inferred_series_id, sample_count)
            self.logger.info(f"Updated SeriesID {inferred_series_id} with SampleCount {sample_count}.")

            # Commit all changes after parsing
            session.commit()

            # Log the processing status
            self.file_handler.log_processed(inferred_series_id)

            # Clean up files after processing
            self.file_handler.clean_files(inferred_series_id)

            self.logger.info("Metadata streaming completed successfully.")

        except Exception as e:
            session.rollback()
            self.logger.critical(f"Error during streaming: {e}")
            raise RuntimeError("Streaming failed.") from e
        finally:
            session.close()

    def _validate_sample_data(self, sample_data: Dict) -> bool:
        """Validates Sample data before insertion."""
        required_fields = ['SampleID', 'SeriesID', 'Title']
        for field in required_fields:
            if not sample_data.get(field):
                self.logger.error(f"Sample validation failed: Missing field '{field}'")
                return False
        return True

    def _stream_series_to_db(self, session, series_data: Dict) -> None:
        """Streams Series metadata to the database."""
        try:
            insert_query = insert(DatasetSeriesMetadata).values(series_data).on_conflict_do_update(
                index_elements=['SeriesID'],
                set_={key: series_data[key] for key in series_data if key != 'SeriesID'}
            )
            session.execute(insert_query)
        except SQLAlchemyError as e:
            self.logger.error(f"Database error during Series insertion: {e}")
            raise

    def _stream_sample_to_db(self, session, sample_data: Dict) -> None:
        """Streams Sample metadata to the database."""
        try:
            insert_query = insert(DatasetSampleMetadata).values(sample_data).on_conflict_do_nothing()
            session.execute(insert_query)
        except SQLAlchemyError as e:
            self.logger.error(f"Database error during Sample insertion: {e}")
            raise

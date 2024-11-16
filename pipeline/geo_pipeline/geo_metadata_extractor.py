# File: pipeline/geo_pipeline/geo_metadata_extractor.py
import json
from lxml import etree
from typing import Dict, Optional, List
from datetime import datetime
import re
import time
from pydantic import BaseModel, ValidationError, Field
import logging

# ---------------- Configuration ----------------
LOG_FILE = "../../logs/geo_metadata_extractor.log"  # Path to the log file

# ---------------- Regular Expressions ----------------
# Regular expressions for validating GEO series and sample IDs
GEO_ACCESSION_PATTERN = re.compile(r'^GSE\d+$')
SAMPLE_ID_PATTERN = re.compile(r'^GSM\d+$')


# ---------------- Pydantic Models for Validation ----------------
class SeriesMetadata(BaseModel):
    """
    Pydantic model for validating Series metadata.

    Attributes:
        SeriesID: The unique identifier for the series.
        Title: The title of the dataset series.
        SubmissionDate: The date the series was submitted.
        LastUpdateDate: The last update date for the series.
        PubMedID: The PubMed ID associated with the series, if available.
        Summary: A summary of the series' objectives and findings.
        OverallDesign: An overview of the experimental design.
        Relations: A list of relations to other datasets or resources.
        SupplementaryData: Supplementary data or files linked to the series.
    """
    SeriesID: str
    Title: Optional[str] = None
    SubmissionDate: Optional[str] = None
    LastUpdateDate: Optional[str] = None
    PubMedID: Optional[str] = None
    Summary: Optional[str] = None
    OverallDesign: Optional[str] = None
    Relations: List[dict] = Field(default_factory=list)
    SupplementaryData: Optional[str] = None


class SampleMetadata(BaseModel):
    """
    Pydantic model for validating Sample metadata.

    Attributes:
        SampleID: The unique identifier for the sample.
        Title: The title of the dataset sample.
        SubmissionDate: The date the sample was submitted.
        LastUpdateDate: The last update date for the sample.
        Organism: The organism associated with the sample.
        Characteristics: A dictionary containing the sample's characteristics.
        Relations: A list of relations to other datasets or resources.
        SupplementaryData: Supplementary data or files linked to the sample.
    """
    SampleID: str
    Title: Optional[str] = None
    SubmissionDate: Optional[str] = None
    LastUpdateDate: Optional[str] = None
    Organism: Optional[str] = None
    Characteristics: dict = Field(default_factory=dict)
    Relations: List[dict] = Field(default_factory=list)
    SupplementaryData: Optional[str] = None


# ---------------- Metadata Extractor Class ----------------
class GeoMetadataExtractor:
    """
    Class for extracting GEO metadata from MINiML files.

    This class provides methods to extract, validate, and log metadata from
    GEO XML files. It supports integration into larger pipelines.

    Attributes:
        file_path: Path to the GEO MINiML file.
        template_path: Path to the JSON template for field mappings.
        debug_mode: Enables detailed debug logging if True.
        verbose_mode: Enables detailed terminal output if True.
    """

    def __init__(self, file_path: str, template_path: str, debug_mode: bool = True, verbose_mode: bool = True) -> None:
        """
        Initializes the GeoMetadataExtractor class.

        Args:
            file_path: Path to the GEO XML file.
            template_path: Path to the JSON field mapping template.
            debug_mode: Enables detailed debug logging.
            verbose_mode: Enables verbose terminal output.
        """
        # Save file paths and flags
        self.file_path = file_path
        self.template_path = template_path
        self.debug_mode = debug_mode
        self.verbose_mode = verbose_mode

        # Configure logging
        log_level = logging.DEBUG if self.debug_mode else logging.INFO
        logging.basicConfig(
            filename=LOG_FILE,
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filemode='w'
        )
        self.logger = logging.getLogger()  # Initialize logger instance

        # Inform the user about logging setup if verbose mode is enabled
        if self.verbose_mode:
            print(f"Logging is set to {logging.getLevelName(log_level)} mode. Log file: {LOG_FILE}")

        # Load the JSON template for field mappings
        self.template = self._load_template()

    def _load_template(self) -> Dict[str, Dict[str, str]]:
        """
        Loads the JSON template for XML field mappings.

        Returns:
            A dictionary representing the field mappings.

        Raises:
            RuntimeError: If the template file cannot be loaded.
        """
        try:
            # Read the JSON template file
            with open(self.template_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.error(f"Template file not found: {self.template_path}")
            raise RuntimeError("Template file not found.")
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing JSON template: {e}")
            raise RuntimeError("Invalid JSON template file.")
        except Exception as e:
            self.logger.error(f"Unexpected error loading template: {e}")
            raise RuntimeError("Failed to load template.") from e

    @staticmethod
    def _parse_date(date_str: str) -> Optional[str]:
        """
        Converts a date string to ISO format or returns None if invalid.

        Args:
            date_str: The date string to convert.

        Returns:
            The ISO-formatted date or None if the date is invalid.
        """
        try:
            # Parse the date if provided
            return datetime.strptime(date_str, "%Y-%m-%d").date().isoformat() if date_str else None
        except ValueError:
            return None  # Return None if the date is invalid

    def _extract_fields(self, element: etree._Element, field_paths: Dict[str, str], ns: Dict[str, str]) -> Dict[str, Optional[str]]:
        """
        Extracts fields from an XML element based on field paths.

        Args:
            element: XML element to extract data from.
            field_paths: Dictionary mapping field names to XML paths.
            ns: Namespace dictionary for XML parsing.

        Returns:
            A dictionary of extracted fields.
        """
        data = {}  # Initialize dictionary to store extracted data
        for field_name, path in field_paths.items():
            try:
                # Handle multi-element fields such as Relations and Supplementary Data
                if path.endswith("Relation") or path.endswith("Supplementary-Data"):
                    if path.endswith("Relation"):
                        # Extract relations data
                        relations = [
                            {"type": relation.attrib.get("type", "Unknown"),
                             "target": relation.attrib.get("target", "")}
                            for relation in element.findall(path, ns)
                        ]
                        data[field_name] = relations
                    elif path.endswith("Supplementary-Data"):
                        # Extract supplementary data
                        supplementary_data = "; ".join([
                            supp_data.text.strip() for supp_data in element.findall(path, ns) if supp_data.text
                        ])
                        data[field_name] = supplementary_data
                elif field_name == "Characteristics":
                    # Handle Characteristics as a dictionary
                    characteristics = {
                        char.attrib.get("tag", "Unknown"): char.text.strip() if char.text else "NA"
                        for char in element.findall(path, ns)
                    }
                    data[field_name] = characteristics
                else:
                    # Extract single-value fields
                    sub_elem = element.find(path, ns)
                    data[field_name] = sub_elem.text.strip() if sub_elem is not None and sub_elem.text else None
            except Exception as e:
                # Log warning for extraction errors
                self.logger.warning(f"Error extracting field {field_name}: {e}")
        return data

    def _process_series_data(self, series_elem: etree._Element, ns: Dict[str, str]) -> Optional[Dict[str, Optional[str]]]:
        """
        Processes and validates a single Series element.

        Args:
            series_elem: The Series XML element to process.
            ns: The namespace dictionary for XML parsing.

        Returns:
            A dictionary of validated series data or None if invalid.
        """
        try:
            # Extract the SeriesID and validate it
            series_id = series_elem.findtext(self.template['Series']['SeriesID'], default="", namespaces=ns)
            if not GEO_ACCESSION_PATTERN.match(series_id):
                self.logger.warning(f"Invalid Series ID: {series_id}")
                return None

            # Extract fields and validate with Pydantic
            data = self._extract_fields(series_elem, self.template['Series'], ns)
            validated_data = SeriesMetadata(**data)
            return validated_data.dict()
        except ValidationError as ve:
            self.logger.error(f"Validation Error in Series: {ve}")
            return None
        except Exception as e:
            self.logger.error(f"Error processing Series element: {e}")
            return None

    def _process_sample_data(self, sample_elem: etree._Element, ns: Dict[str, str]) -> Optional[Dict[str, Optional[str]]]:
        """
        Processes and validates a single Sample element.

        Args:
            sample_elem: The Sample XML element to process.
            ns: The namespace dictionary for XML parsing.

        Returns:
            A dictionary of validated sample data or None if invalid.
        """
        try:
            # Extract the SampleID and validate it
            sample_id = sample_elem.findtext(self.template['Sample']['SampleID'], default="", namespaces=ns)
            if not SAMPLE_ID_PATTERN.match(sample_id):
                self.logger.warning(f"Invalid Sample ID: {sample_id}")
                return None

            # Extract fields and validate with Pydantic
            data = self._extract_fields(sample_elem, self.template['Sample'], ns)
            validated_data = SampleMetadata(**data)
            return validated_data.dict()
        except ValidationError as ve:
            self.logger.error(f"Validation Error in Sample: {ve}")
            return None
        except Exception as e:
            self.logger.error(f"Error processing Sample element: {e}")
            return None

    def parse(self):
        """
        Parses the GEO MINiML file and logs results.

        Processes Series and Sample elements from the XML file, validating and logging data.
        """
        try:
            # Start the timer and initialize counters
            start_time = time.time()
            ns = {'geo': 'http://www.ncbi.nlm.nih.gov/geo/info/MINiML'}
            series_count = 0
            sample_count = 0

            # Log and parse Series elements
            self.logger.info("Parsing Series data...")
            context = etree.iterparse(self.file_path, events=("start", "end"),
                                      tag="{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Series")
            for event, elem in context:
                if event == "end":
                    series_data = self._process_series_data(elem, ns)
                    if series_data:
                        self.logger.debug(f"Extracted Series Data: {series_data}")
                        if self.verbose_mode:
                            print(json.dumps(series_data, indent=2))
                        series_count += 1
                    elem.clear()  # Clear element to free memory

            # Log and parse Sample elements
            self.logger.info("Parsing Sample data...")
            context = etree.iterparse(self.file_path, events=("start", "end"),
                                      tag="{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Sample")
            for event, elem in context:
                if event == "end":
                    sample_data = self._process_sample_data(elem, ns)
                    if sample_data:
                        self.logger.debug(f"Extracted Sample Data: {sample_data}")
                        if self.verbose_mode:
                            print(json.dumps(sample_data, indent=2))
                        sample_count += 1
                    elem.clear()  # Clear element to free memory

            # Calculate elapsed time and log summary
            elapsed_time = time.time() - start_time
            self.logger.info(f"Parsing completed: {series_count} series and {sample_count} samples parsed.")
            self.logger.info(f"Time elapsed: {elapsed_time:.2f} seconds")
            if self.verbose_mode:
                print(f"Parsing completed: {series_count} series and {sample_count} samples parsed.")
                print(f"Time elapsed: {elapsed_time:.2f} seconds")
        except Exception as e:
            # Handle fatal parsing errors
            self.logger.critical(f"Fatal error during parsing: {e}")
            raise


# ---------------- Execution ----------------
if __name__ == "__main__":
    FILE_PATH = "../../resources/data/metadata/geo_metadata/GSE112026_family.xml"
    TEMPLATE_PATH = "../../resources/geo_tag_template.json"

    # Instantiate and run the metadata extractor
    extractor = GeoMetadataExtractor(file_path=FILE_PATH, template_path=TEMPLATE_PATH, debug_mode=True, verbose_mode=True)
    extractor.parse()

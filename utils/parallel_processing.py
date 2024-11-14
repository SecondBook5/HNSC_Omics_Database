# parallel_processing.py

import concurrent.futures  # For parallel execution of tasks
import logging  # For logging process information and errors
from abc import ABC, abstractmethod  # For defining an abstract base class
from typing import List, Optional, Any, Dict, Union  # For type hints
import requests  # For handling HTTP requests to download resources
import os  # For file and directory management

# Set up logging configuration to track process flow and errors
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)  # Initialize logger for module


class ParallelProcessor(ABC):
    """
    Abstract base class for defining a generic parallel processor.
    This class serves as a blueprint for specific parallel download
    and processing tasks on various dataset formats.
    """

    def __init__(self, resource_ids: List[str], output_dir: str):
        # Ensure that the output directory exists; create if necessary
        os.makedirs(output_dir, exist_ok=True)

        # Initialize resource identifiers list for downloading resources
        self.resource_ids = resource_ids
        # Set output directory to store downloaded files
        self.output_dir = output_dir

    @abstractmethod
    def download_resource(self, resource_id: str) -> Optional[str]:
        """
        Abstract method for downloading resources based on a unique identifier.
        This method must be implemented in derived classes.

        Args:
            resource_id (str): The unique identifier for the resource.

        Returns:
            Optional[str]: Path to the downloaded file or None if download fails.
        """
        pass  # Must be implemented by subclass

    @abstractmethod
    def process_resource(self, file_path: str) -> Any:
        """
        Abstract method for processing a downloaded resource.
        This method must be implemented in derived classes.

        Args:
            file_path (str): Path to the file to process.

        Returns:
            Any: Result of processing, defined by subclass implementation.
        """
        pass  # Must be implemented by subclass

    def execute(self) -> None:
        """
        Executes the parallel download and processing of resources.
        Each resource is handled independently, reducing memory consumption.
        """
        # Set up ThreadPoolExecutor for parallel execution
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Map resource ID to its respective download and process task
            future_to_resource_id = {
                executor.submit(self.download_and_process, resource_id): resource_id
                for resource_id in self.resource_ids
            }

            # Handle task completion and log outcomes for each future
            for future in concurrent.futures.as_completed(future_to_resource_id):
                resource_id = future_to_resource_id[future]
                try:
                    # Retrieve the result of the task (if successful)
                    result = future.result()
                    logger.info(f"Successfully processed resource {resource_id} with result: {result}")
                except Exception as e:
                    # Log error if task fails
                    logger.error(f"Error processing resource {resource_id}: {e}")

    def download_and_process(self, resource_id: str) -> Any:
        """
        Combines downloading and processing for a single resource,
        ensuring each task is handled with error checking.

        Args:
            resource_id (str): Unique identifier for the resource.

        Returns:
            Any: Result from the processing step.
        """
        try:
            # Attempt to download the resource and get its file path
            file_path = self.download_resource(resource_id)
            # Ensure that download was successful before processing
            if file_path:
                # Process the downloaded file and return the result
                result = self.process_resource(file_path)
                return result
            else:
                # Raise an error if download fails
                raise ValueError(f"Download failed for resource {resource_id}")
        except Exception as e:
            # Log error if download or processing fails
            logger.error(f"Failed to download and process resource {resource_id}: {e}")
            # Raise the exception to ensure task failure is captured
            raise


class GEODataProcessor(ParallelProcessor):
    """
    Concrete class extending ParallelProcessor for handling GEO datasets.
    Implements methods for downloading GEO XML files and processing them.

    Methods:
        download_resource: Downloads a GEO dataset based on its ID.
        process_resource: Parses and processes the XML file.
    """

    def __init__(self, geo_ids: List[str], output_dir: str, xml_parser: Any):
        # Initialize superclass with list of GEO IDs and output directory
        super().__init__(geo_ids, output_dir)
        # Store XML parser instance for later use
        self.xml_parser = xml_parser

    def download_resource(self, geo_id: str) -> Optional[str]:
        """
        Downloads a GEO dataset XML file based on its ID.

        Args:
            geo_id (str): GEO identifier for the dataset.

        Returns:
            Optional[str]: Path to the downloaded XML file, or None if download fails.
        """
        # Construct URL and file path for download based on GEO ID
        base_url = "https://ftp.ncbi.nlm.nih.gov/geo/series"
        stub = geo_id[:-3] + 'nnn'
        url = f"{base_url}/{stub}/{geo_id}/miniml/{geo_id}_family.xml.tgz"
        file_path = os.path.join(self.output_dir, f"{geo_id}_family.xml.tgz")

        try:
            # Attempt to download the GEO XML file
            response = requests.get(url, timeout=30)
            response.raise_for_status()  # Check for HTTP errors

            # Write downloaded content to the file
            with open(file_path, 'wb') as f:
                f.write(response.content)
            logger.info(f"Downloaded GEO file for {geo_id}")

            # Return the path of the downloaded file
            return file_path
        except requests.RequestException as e:
            # Log any download errors
            logger.error(f"Failed to download GEO file for {geo_id}: {e}")
            return None

    def process_resource(self, file_path: str) -> Dict[str, Union[str, Dict]]:
        """
        Processes the downloaded GEO XML file using the provided XML parser.

        Args:
            file_path (str): Path to the downloaded XML file.

        Returns:
            Dict[str, Union[str, Dict]]: Parsed data from the XML file.
        """
        try:
            # Use the XML parser to parse and extract metadata
            parsed_data = self.xml_parser.parse(file_path)
            logger.info(f"Processed file at {file_path}")

            # Return parsed data for downstream use
            return parsed_data
        except Exception as e:
            # Log any processing errors
            logger.error(f"Failed to process GEO file at {file_path}: {e}")
            raise  # Re-raise to ensure failure is captured

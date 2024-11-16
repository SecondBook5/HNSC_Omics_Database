# File: pipeline/geo_pipeline/geo_metadata_downloader.py

import tarfile  # For handling .tar.gz file extraction
import os  # For file and directory management
import requests  # For making HTTP requests
import logging  # For logging debug and error messages
from pipeline.abstract_etl.data_downloader import DataDownloader  # Base class for data downloading
from typing import Optional, List  # For type hints


class GeoMetadataDownloader(DataDownloader):
    """
    GEO-specific data downloader that implements DataDownloader for GEO XML files.
    Downloads and extracts GEO XML files by constructing URLs based on GEO series ID.

    Attributes:
        base_url (str): Base URL for GEO data repository.
        logger (logging.Logger): Logger instance for debug and info output.
    """

    def __init__(self, output_dir: str, debug: bool = False) -> None:
        """
        Initializes GeoMetadataDownloader with output directory and optional debug flag.

        Args:
            output_dir (str): Directory to save downloaded files.
            debug (bool): Enables debug output if True.
        """
        # Initialize the parent class with the output directory
        super().__init__(output_dir)
        # Base URL for constructing GEO file download links
        self.base_url: str = "https://ftp.ncbi.nlm.nih.gov/geo/series"
        # Initialize a logger for logging download events and errors
        self.logger: logging.Logger = self._initialize_logger(debug)

    @staticmethod
    def _initialize_logger(debug: bool) -> logging.Logger:
        """
        Initializes and returns a logger.

        Args:
            debug (bool): Enables debug output if True.

        Returns:
            logging.Logger: Configured logger instance.
        """
        # Create a logger instance for the downloader
        logger = logging.getLogger("GeoMetadataDownloader")
        # Set the logging level based on the debug flag
        logger.setLevel(logging.DEBUG if debug else logging.INFO)
        # Add a stream handler for logging to console
        if not logger.hasHandlers():  # Avoid duplicate handlers during testing
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
            logger.addHandler(handler)
        return logger

    def download_files(self, file_ids: List[str]) -> None:
        """
        Downloads and extracts multiple GEO XML files based on their IDs.

        Args:
            file_ids (List[str]): List of GEO series IDs to download and extract.
        """
        # Iterate over each GEO series ID in the provided list
        for file_id in file_ids:
            try:
                # Log the start of processing for the current GEO series
                self.logger.debug(f"Preparing to download GEO series: {file_id}")
                # Attempt to download and extract the file
                extracted_path = self.download_file(file_id)
                # If successful, log the success and output the path
                if extracted_path:
                    self.logger.info(f"Successfully processed {file_id}: {extracted_path}")
                    print(f"{file_id} downloaded and extracted to: {extracted_path}")
                else:
                    # Log an error if the process fails for the file
                    self.logger.error(f"Failed to process {file_id}")
            except Exception as e:
                # Log any critical errors encountered during the process
                self.logger.critical(f"Critical error processing {file_id}: {e}")

    def download_file(self, file_id: str) -> Optional[str]:
        """
        Downloads a GEO XML file for a given GEO series ID and extracts it if successful.

        Args:
            file_id (str): GEO series ID (e.g., "GSE112021").

        Returns:
            Optional[str]: Path to the downloaded and extracted XML file, or None if failed.

        Raises:
            ValueError: If the file ID is empty.
        """
        # Raise a ValueError if the file ID is empty
        if not file_id:
            raise ValueError("File ID cannot be empty.")

        try:
            # Construct the stub and URL for the download
            stub = file_id[:-3] + 'nnn'
            url = f"{self.base_url}/{stub}/{file_id}/miniml/{file_id}_family.xml.tgz"
            # Paths for the tar file and extracted XML file
            output_path = os.path.join(self.output_dir, f"{file_id}_family.xml.tgz")
            extracted_path = os.path.join(self.output_dir, f"{file_id}_family.xml")

            # Log constructed paths and URL for debugging
            self.logger.debug(f"URL: {url}")
            self.logger.debug(f"Output Path: {output_path}")
            self.logger.debug(f"Extracted Path: {extracted_path}")

            # Check if the file already exists to avoid re-downloading
            if self.file_exists(extracted_path):
                self.logger.info(f"File already exists: {extracted_path}")
                return extracted_path

            # Download the file from the constructed URL
            downloaded_path = self.download_from_url(url, output_path)
            if not downloaded_path:
                # Log and return None if the download fails
                self.logger.error(f"Failed to download file from {url}")
                return None

            # Extract the downloaded tar file
            extracted_file = self._extract_file(downloaded_path, extracted_path)
            if extracted_file:
                return extracted_file
            else:
                # Log error if extraction fails
                self.logger.error(f"Extraction failed for {downloaded_path}")
                return None

        except ValueError as ve:
            # Log any value errors encountered
            self.logger.error(f"Value Error: {ve}")
        except requests.RequestException as re:
            # Log any request-specific exceptions
            self.logger.error(f"Request Exception while downloading: {re}")
        except Exception as e:
            # Log any other unexpected exceptions
            self.logger.error(f"Unexpected error during download or extraction: {e}")
        return None

    def download_from_url(self, url: str, output_path: str) -> Optional[str]:
        """
        Downloads a file from the specified URL and saves it to the given output path.

        Args:
            url (str): The URL to download from.
            output_path (str): Path to save the downloaded file.

        Returns:
            Optional[str]: Path to the downloaded file, or None if the download failed.
        """
        try:
            # Log the start of the download
            self.logger.info(f"Downloading file from {url}")
            # Make an HTTP GET request to fetch the file
            response = requests.get(url, timeout=30)
            # Raise an exception for HTTP errors
            response.raise_for_status()

            # Save the downloaded content to the specified output path
            with open(output_path, 'wb') as f:
                f.write(response.content)

            # Log success and return the output path
            self.logger.info(f"Download complete: {output_path}")
            return output_path

        except requests.Timeout:
            # Log a timeout error if the request times out
            self.logger.error(f"Timeout while trying to download {url}")
        except requests.HTTPError as http_err:
            # Log an HTTP error if the response status code indicates failure
            self.logger.error(f"HTTP error occurred: {http_err}")
        except requests.RequestException as req_err:
            # Log any other request-related errors
            self.logger.error(f"Request error: {req_err}")
        except Exception as e:
            # Log unexpected errors
            self.logger.error(f"Unexpected error while downloading file: {e}")
        return None

    def _extract_file(self, tar_path: str, expected_path: str) -> Optional[str]:
        """
        Extracts a tar.gz file and validates the extracted file existence.

        Args:
            tar_path (str): Path to the tar.gz file.
            expected_path (str): Path to the expected extracted file.

        Returns:
            Optional[str]: Path to the extracted file, or None if extraction failed.
        """
        try:
            # Log the start of extraction
            self.logger.info(f"Extracting file: {tar_path}")
            # Open and extract the tar file to the output directory
            with tarfile.open(tar_path, 'r:gz') as tar:
                tar.extractall(path=self.output_dir)

            # Remove the tar file after successful extraction
            os.remove(tar_path)
            self.logger.info(f"Extraction complete: {expected_path}")

            # Verify the existence of the extracted file
            if os.path.exists(expected_path):
                return expected_path
            else:
                # Log an error if the extracted file is missing
                self.logger.error(f"Extracted file not found: {expected_path}")
                return None

        except tarfile.TarError as tar_err:
            # Log tar file errors
            self.logger.error(f"Tar file error: {tar_err}")
        except OSError as os_err:
            # Log OS errors
            self.logger.error(f"OS error during extraction: {os_err}")
        except Exception as e:
            # Log unexpected errors during extraction
            self.logger.error(f"Unexpected error during extraction: {e}")
        return None


# ---------------- Execution ----------------
if __name__ == "__main__":
    # Output directory for GEO metadata files
    OUTPUT_DIR = "../../resources/data/metadata/geo_metadata"
    # File containing GEO series IDs to download
    GEO_IDS_FILE = "../../resources/geo_ids.txt"

    try:
        # Read GEO series IDs from the input file
        with open(GEO_IDS_FILE, 'r') as f:
            geo_ids = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        logging.error(f"GEO IDs file not found: {GEO_IDS_FILE}")
        exit(1)
    except Exception as e:
        logging.error(f"Unexpected error reading GEO IDs file: {e}")
        exit(1)

    # Initialize the downloader and start processing
    downloader = GeoMetadataDownloader(output_dir=OUTPUT_DIR, debug=True)
    downloader.download_files(geo_ids)

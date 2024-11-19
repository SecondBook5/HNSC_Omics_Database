# File: pipeline/geo_pipeline/geo_metadata_downloader.py
# This script implements a GEO metadata downloader that integrates defensive programming
# to ensure robust database interactions and metadata logging.

# Import required libraries for handling compressed files, HTTP requests, logging, and type hints
import tarfile  # For handling .tar.gz file extraction
import os  # For file and directory management
import requests  # For making HTTP requests
import logging  # For logging debug and error messages
from typing import Optional, List  # For type hints
# Import base class for data downloading
from pipeline.abstract_etl.data_downloader import DataDownloader
# Import database session handling from the configuration module
from config.db_config import get_postgres_engine
# Import SQLAlchemy exception class for database error handling
from sqlalchemy.exc import SQLAlchemyError
# Import defensive database connection checker
from utils.connection_checker import DatabaseConnectionChecker


class GeoMetadataDownloader(DataDownloader):
    """
    GEO-specific data downloader that extends the base DataDownloader to handle GEO XML files.
    Downloads and extracts GEO XML files, with additional logging of metadata into PostgreSQL.

    Attributes:
        base_url (str): Base URL for GEO data repository.
        logger (logging.Logger): Logger instance for debug and info output.
        db_checker (DatabaseConnectionChecker): Checks database connections with retry logic.
    """

    def __init__(self, output_dir: str, debug: bool = False, logger: Optional[logging.Logger] = None) -> None:
        """
        Initializes GeoMetadataDownloader with output directory and optional debug flag.

        Args:
            output_dir (str): Directory to save downloaded files.
            debug (bool): Enables debug output if True.
            logger (Optional[logging.Logger]): Optional external logger to use.
        """
        # Initialize the parent class with the output directory
        super().__init__(output_dir)
        # Define the base URL for constructing GEO file download links
        self.base_url: str = "https://ftp.ncbi.nlm.nih.gov/geo/series"
        # Use the provided logger or set up a default logger
        self.logger: logging.Logger = logger or self._initialize_default_logger(debug)
        # Initialize the database connection checker for defensive programming
        self.db_checker = DatabaseConnectionChecker()

    @staticmethod
    def _initialize_default_logger(debug: bool) -> logging.Logger:
        """
        Initializes and returns a default logger if none is provided.

        Args:
            debug (bool): Enables debug output if True.

        Returns:
            logging.Logger: Configured logger instance.
        """
        # Create a logger for the GeoMetadataDownloader
        logger = logging.getLogger("GeoMetadataDownloader")
        # Set logging level based on the debug flag
        logger.setLevel(logging.DEBUG if debug else logging.INFO)
        # Add a console handler to the logger if no handlers exist
        if not logger.hasHandlers():
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
            logger.addHandler(handler)
        return logger

    def log_metadata_to_db(self, geo_id: str, status: str, message: Optional[str] = None,
                           file_names: Optional[List[str]] = None) -> None:
        """
        Logs download metadata into the PostgreSQL database after validating the connection.

        Args:
            geo_id (str): GEO series ID.
            status (str): Status of the operation ('Success' or 'Failure').
            message (Optional[str]): Additional details or error messages.
            file_names (Optional[List[str]]): List of associated filenames.
        """
        # Check if the PostgreSQL connection is active
        if not self.db_checker.check_postgresql_connection():
            # Log an error and skip database logging if the connection is not active
            self.logger.error(f"Skipping database logging for GEO ID {geo_id} due to connection issues.")
            return

        try:
            # Log the start of the database logging process for this GEO ID
            self.logger.info(f"Logging download status for GEO ID {geo_id} into the database.")

            # Open a database session to interact with PostgreSQL
            with get_db_session() as session:
                # SQL query to insert or update the metadata log entry
                query = """
                    INSERT INTO geo_metadata_log (geo_id, status, message, file_names, log_time)
                    VALUES (:geo_id, :status, :message, :file_names, NOW())
                    ON CONFLICT (geo_id)
                    DO UPDATE SET 
                        status = EXCLUDED.status,
                        message = EXCLUDED.message,
                        file_names = EXCLUDED.file_names,
                        log_time = NOW();
                """
                # Execute the query with the provided parameters
                session.execute(query, {
                    "geo_id": geo_id,  # GEO ID of the dataset
                    "status": status,  # Status of the operation
                    "message": message,  # Error message or additional details
                    "file_names": file_names if file_names else None,  # List of filenames
                })
                # Commit the transaction to save changes to the database
                session.commit()
                # Log the success of the logging operation
                self.logger.info(f"Logged metadata for GEO ID {geo_id} with files: {file_names}")
        except SQLAlchemyError as e:
            # Log an error if a database-related issue occurs
            self.logger.error(f"Failed to log metadata for GEO ID {geo_id} due to database error: {e}")
        except Exception as e:
            # Log unexpected errors during the logging process
            self.logger.error(f"Unexpected error while logging metadata for GEO ID {geo_id}: {e}")

    def download_files(self, file_ids: List[str]) -> None:
        """
        Downloads and extracts multiple GEO XML files based on their IDs,
        and logs metadata into the database.

        Args:
            file_ids (List[str]): List of GEO series IDs to download and extract.
        """
        # Iterate through each GEO series ID in the provided list
        for file_id in file_ids:
            try:
                # Log the start of the download process for this GEO series ID
                self.logger.debug(f"Preparing to download GEO series: {file_id}")

                # Download and extract files for this GEO ID
                extracted_path = self.download_file(file_id)

                # Get the list of files downloaded or generated for this GEO ID
                downloaded_files = self.get_downloaded_filenames(file_id)

                # If the file was successfully downloaded and extracted
                if extracted_path:
                    # Log success and record in the database with associated filenames
                    self.logger.info(f"Successfully processed {file_id}: {extracted_path}")
                    self.log_metadata_to_db(
                        geo_id=file_id,  # GEO ID
                        status="Success",  # Status indicating success
                        message="File downloaded and extracted.",  # Success message
                        file_names=downloaded_files  # List of related filenames
                    )
                else:
                    # If the file was not successfully processed, log failure
                    self.logger.error(f"Failed to process {file_id}")
                    self.log_metadata_to_db(
                        geo_id=file_id,  # GEO ID
                        status="Failure",  # Status indicating failure
                        message="Failed to download or extract file."  # Failure message
                    )
            except Exception as e:
                # Log critical errors during the download process
                self.logger.critical(f"Critical error processing {file_id}: {e}")
                # Record the failure in the database with the error message
                self.log_metadata_to_db(
                    geo_id=file_id,  # GEO ID
                    status="Failure",  # Status indicating failure
                    message=str(e)  # Error message
                )

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
            # Define paths for the tar file and extracted XML file
            output_path = os.path.join(self.output_dir, f"{file_id}_family.xml.tgz")
            extracted_path = os.path.join(self.output_dir, f"{file_id}_family.xml")

            # Log constructed paths and URL for debugging purposes
            self.logger.debug(f"URL: {url}")
            self.logger.debug(f"Output Path: {output_path}")
            self.logger.debug(f"Extracted Path: {extracted_path}")

            # Check if the file already exists to avoid redundant downloads
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
            # Log any unexpected errors during the download or extraction process
            self.logger.error(f"Unexpected error during download or extraction: {e}")
            raise

    def get_downloaded_filenames(self, geo_id: str) -> List[str]:
        """
        Retrieves filenames related to the given GEO ID from the output directory.

        Args:
            geo_id (str): The GEO series ID.

        Returns:
            List[str]: List of filenames related to the GEO ID.
        """
        try:
            # Initialize an empty list to store filenames
            files = []
            # Construct the directory path for the GEO ID
            geo_dir = os.path.join(self.output_dir, geo_id)

            # Check if the directory exists
            if os.path.exists(geo_dir):
                # Walk through all files in the directory and its subdirectories
                for root, _, filenames in os.walk(geo_dir):
                    # Add each filename to the list, relative to the output directory
                    files.extend([os.path.relpath(os.path.join(root, f), self.output_dir) for f in filenames])
            # Return the list of filenames
            return files
        except Exception as e:
            # Log an error if there is an issue retrieving filenames
            self.logger.error(f"Error retrieving filenames for GEO ID {geo_id}: {e}")
            # Return an empty list in case of an error
            return []

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
    # Define the output directory for downloaded GEO metadata
    OUTPUT_DIR = "../../resources/data/metadata/geo_metadata/raw_metadata"
    # Define the file containing GEO series IDs to download
    GEO_IDS_FILE = "../../resources/geo_ids.txt"

    try:
        # Read GEO series IDs from the input file, skipping empty lines
        with open(GEO_IDS_FILE, 'r') as f:
            geo_ids = [line.strip() for line in f if line.strip()]

        # Initialize the downloader and start the download process
        downloader = GeoMetadataDownloader(output_dir=OUTPUT_DIR, debug=True)
        downloader.download_files(geo_ids)

    except FileNotFoundError:
        logging.error(f"GEO IDs file not found: {GEO_IDS_FILE}")
        exit(1)
    except Exception as e:
        # Log any critical errors during script execution
        logging.critical(f"Failed to execute downloader: {e}")




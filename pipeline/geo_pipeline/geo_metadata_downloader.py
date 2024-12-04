# File: pipeline/geo_pipeline/geo_metadata_downloader.py

import tarfile  # For handling .tar.gz file extraction
import os  # For file and directory management
import requests  # For making HTTP requests
import logging  # For logging information and errors
from typing import Optional, List  # For type hints
from config.logger_config import configure_logger  # Import centralized logger configuration
from pipeline.geo_pipeline.geo_file_handler import GeoFileHandler  # Import GeoFileHandler


class GeoMetadataDownloader:
    """
    GEO-specific data downloader for handling GEO XML files.
    Downloads and extracts GEO XML files by constructing URLs based on GEO series IDs.

    Attributes:
        output_dir (str): Directory to save downloaded files.
        base_url (str): Base URL for GEO data repository.
        logger (logging.Logger): Logger instance for debug and info output.
        file_handler (GeoFileHandler): Handles logging and file-related operations.
    """

    def __init__(self, output_dir: str, file_handler: GeoFileHandler, debug: bool = False) -> None:
        """
        Initializes GeoMetadataDownloader with output directory, file handler, and optional debug flag.

        Args:
            output_dir (str): Directory to save downloaded files.
            file_handler (GeoFileHandler): Handles logging and file-related operations.
            debug (bool): Enables debug output if True.
        """
        # Validate that the output directory is provided
        if not output_dir:
            raise ValueError("Output directory path cannot be empty.")

        # Ensure the output directory exists or create it
        os.makedirs(output_dir, exist_ok=True)
        self.output_dir = output_dir

        # Set the file handler for logging and file cleanup
        self.file_handler = file_handler

        # Base URL for constructing GEO file download links
        self.base_url: str = "https://ftp.ncbi.nlm.nih.gov/geo/series"

        # Configure the logger using the centralized logger configuration
        self.logger = configure_logger(
            name="GeoMetadataDownloader",
            log_file="geo_metadata_downloader.log",
            level=logging.DEBUG if debug else logging.INFO,
            output="both"
        )

    def download_files(self, file_ids: List[str]) -> None:
        """
        Downloads and extracts multiple GEO files based on their IDs.
        Logs success once per GEO ID instead of for each file.

        Args:
            file_ids (List[str]): List of GEO series IDs to download and extract.
        """
        # Iterate over the list of GEO series IDs
        for file_id in file_ids:
            try:
                # Log the start of processing for the current GEO series
                self.logger.debug(f"Starting download for GEO ID: {file_id}")
                # Attempt to download and extract files for the GEO ID
                downloaded_files = self.download_file(file_id)

                # Log success for downloaded files if any are present
                if downloaded_files:
                    # Log success for the entire GEO ID instead of individual files
                    self.logger.info(
                        f"Successfully downloaded and validated files for GEO ID {file_id}: {len(downloaded_files)} files.")
                    # Log the successful download in the file handler
                    self.file_handler.log_download(file_id, downloaded_files)
                else:
                    # Log an error if no files were downloaded successfully
                    self.logger.error(f"No files processed successfully for GEO ID {file_id}.")
            except Exception as e:
                # Log critical errors that occur during processing for a GEO ID
                self.logger.critical(f"Critical error processing GEO ID {file_id}: {e}")

    def download_file(self, file_id: str) -> Optional[List[str]]:
        """
        Downloads a GEO file for a given GEO series ID and extracts it.
        Logs both XML and non-XML files.

        Args:
            file_id (str): GEO series ID (e.g., "GSE112021").

        Returns:
            Optional[List[str]]: List of all extracted files (XML and non-XML), or None if failed.
        """
        # Raise an error if the file ID is empty
        if not file_id:
            raise ValueError("File ID cannot be empty.")
        try:
            # Construct the folder stub based on GEO series ID format
            stub = file_id[:-3] + 'nnn'
            # Construct the full URL for downloading the GEO file
            url = f"{self.base_url}/{stub}/{file_id}/miniml/{file_id}_family.xml.tgz"
            # Define the file paths for the tar.gz file and the extracted directory
            tar_path = os.path.join(self.output_dir, f"{file_id}_family.xml.tgz")
            geo_dir = os.path.join(self.output_dir, file_id)
            # Ensure the extraction directory exists
            os.makedirs(geo_dir, exist_ok=True)

            # Download the file from the URL
            downloaded_tar_path = self._download_from_url(url, tar_path)
            if not downloaded_tar_path:
                self.logger.error(f"Failed to download file for GEO ID {file_id}. URL: {url}")
                return None

            # Extract the downloaded tar file
            extracted_files = self._extract_file(downloaded_tar_path, geo_dir)
            if not extracted_files:
                self.logger.error(f"Extraction failed for GEO ID {file_id}")
                return None

            # Categorize files as XML and non-XML
            xml_files = [f for f in extracted_files if f.endswith(".xml")]
            non_xml_files = [f for f in extracted_files if not f.endswith(".xml")]

            # Log categorized files
            self.logger.info(
                f"XML files for GEO ID {file_id}: {xml_files}" if xml_files else f"No XML files found for GEO ID {file_id}.")
            self.logger.info(
                f"Non-XML files for GEO ID {file_id}: {non_xml_files}" if non_xml_files else f"No non-XML files found for GEO ID {file_id}.")

            return extracted_files
        except Exception as e:
            # Log unexpected errors during download or extraction
            self.logger.error(f"Error during download or extraction for {file_id}: {e}")
            return None

    def _download_from_url(self, url: str, output_path: str) -> Optional[str]:
        """
        Downloads a file from the specified URL and saves it to the output path.

        Args:
            url (str): URL of the file to download.
            output_path (str): Path to save the downloaded file.

        Returns:
            Optional[str]: Path to the saved file if successful, None otherwise.
        """
        try:
            # Log the start of the download process
            self.logger.info(f"Downloading file from {url}")
            # Send an HTTP GET request to fetch the file
            response = requests.get(url, timeout=30)
            # Raise an exception for HTTP errors (e.g., 404, 500)
            response.raise_for_status()

            # Write the response content to the output file
            with open(output_path, 'wb') as file:
                file.write(response.content)

            # Log a success message and return the output file path
            self.logger.info(f"Download complete: {output_path}")
            return output_path
        except requests.Timeout:
            # Log timeout errors if the request exceeds the time limit
            self.logger.error(f"Timeout while trying to download {url}")
        except requests.HTTPError as http_err:
            # Log HTTP-specific errors
            self.logger.error(f"HTTP error occurred: {http_err}")
        except requests.RequestException as req_err:
            # Log any other request-related errors
            self.logger.error(f"Request error while downloading {url}: {req_err}")
        except Exception as e:
            # Log unexpected errors during the download
            self.logger.error(f"Unexpected error during download: {e}")
        return None

    def _extract_file(self, tar_path: str, extract_dir: str) -> Optional[List[str]]:
        """
        Extracts a tar.gz file and validates extracted files.

        Args:
            tar_path (str): Path to the tar.gz file.
            extract_dir (str): Directory to extract the files to.

        Returns:
            Optional[List[str]]: List of paths to the extracted files, or None if extraction failed.
        """
        try:
            # Check if the tar file exists
            if not os.path.exists(tar_path):
                self.logger.error(f"File not found for extraction: {tar_path}")
                return None

            # Log the start of the extraction process
            self.logger.info(f"Extracting file: {tar_path}")

            # Open and extract the tar.gz file
            with tarfile.open(tar_path, 'r:gz') as tar:
                tar.extractall(path=extract_dir)

            # Remove the tar.gz file after successful extraction
            os.remove(tar_path)
            self.logger.info(f"Extraction complete: {extract_dir}")

            # Validate and list extracted files
            extracted_files = [
                os.path.join(extract_dir, f)
                for f in os.listdir(extract_dir)
                if os.path.isfile(os.path.join(extract_dir, f))
            ]
            if not extracted_files:
                self.logger.error(f"No files found in extraction directory: {extract_dir}")
                return None

            # Log the extracted files
            self.logger.info(f"Extracted files: {extracted_files}")
            return extracted_files
        except tarfile.TarError as tar_err:
            # Log errors related to tar file handling
            self.logger.error(f"Tar file error: {tar_err}")
        except Exception as e:
            # Log unexpected errors during extraction
            self.logger.error(f"Unexpected error during extraction: {e}")
        return None

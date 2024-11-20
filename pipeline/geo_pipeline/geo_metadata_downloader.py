# File: pipeline/geo_pipeline/geo_metadata_downloader.py

import tarfile  # For handling .tar.gz file extraction
import os  # For file and directory management
import requests  # For making HTTP requests
import logging
from typing import Optional, List  # For type hints
from config.logger_config import configure_logger  # Import centralized logger configuration


class GeoMetadataDownloader:
    """
    GEO-specific data downloader for handling GEO XML files.
    Downloads and extracts GEO XML files by constructing URLs based on GEO series IDs.

    Attributes:
        output_dir (str): Directory to save downloaded files.
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
        # Validate that the output directory is provided
        if not output_dir:
            raise ValueError("Output directory path cannot be empty.")

        # Ensure the output directory exists or create it
        os.makedirs(output_dir, exist_ok=True)
        self.output_dir = output_dir

        # Base URL for constructing GEO file download links
        self.base_url: str = "https://ftp.ncbi.nlm.nih.gov/geo/series"

        # Configure the logger using the centralized logger configuration
        self.logger = configure_logger(
            name="GeoMetadataDownloader",
            log_dir="./logs",
            log_file="geo_metadata_downloader.log",
            level=logging.DEBUG if debug else logging.INFO,
            output="both"
        )

    def download_files(self, file_ids: List[str]) -> None:
        """
        Downloads and extracts multiple GEO XML files based on their IDs.

        Args:
            file_ids (List[str]): List of GEO series IDs to download and extract.
        """
        # Iterate over the list of GEO series IDs
        for file_id in file_ids:
            try:
                # Log the start of processing for the current GEO series
                self.logger.debug(f"Preparing to download GEO series: {file_id}")
                # Attempt to download and extract the file
                extracted_path = self.download_file(file_id)
                # Log success if the file was downloaded and extracted successfully
                if extracted_path:
                    self.logger.info(f"Successfully processed {file_id}: {extracted_path}")
                else:
                    # Log an error if the process failed for the current file
                    self.logger.error(f"Failed to process {file_id}")
            except Exception as e:
                # Log critical errors that occur during processing
                self.logger.critical(f"Critical error processing {file_id}: {e}")

    def download_file(self, file_id: str) -> Optional[str]:
        """
        Downloads a GEO XML file for a given GEO series ID and extracts it.

        Args:
            file_id (str): GEO series ID (e.g., "GSE112021").

        Returns:
            Optional[str]: Path to the downloaded and extracted XML file, or None if failed.
        """
        # Raise an error if the file ID is empty
        if not file_id:
            raise ValueError("File ID cannot be empty.")

        try:
            # Construct the folder stub based on GEO series ID format
            stub = file_id[:-3] + 'nnn'
            # Construct the full URL for downloading the GEO file
            url = f"{self.base_url}/{stub}/{file_id}/miniml/{file_id}_family.xml.tgz"
            # Define the file paths for the tar.gz file and the extracted XML file
            output_path = os.path.join(self.output_dir, f"{file_id}_family.xml.tgz")
            extracted_path = os.path.join(self.output_dir, f"{file_id}_family.xml")

            # Check if the file has already been extracted
            if os.path.isfile(extracted_path):
                self.logger.info(f"File already exists: {extracted_path}")
                return extracted_path

            # Download the tar.gz file from the URL
            downloaded_path = self._download_from_url(url, output_path)
            if not downloaded_path:
                # Log and return None if the download failed
                self.logger.error(f"Failed to download file from {url}")
                return None

            # Extract the downloaded file and return the path to the extracted file
            return self._extract_file(downloaded_path, extracted_path)
        except Exception as e:
            # Log any unexpected errors during download or extraction
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
            self.logger.error(f"Request error: {req_err}")
        except Exception as e:
            # Log unexpected errors during the download
            self.logger.error(f"Unexpected error during download: {e}")
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
            # Log the start of the extraction process
            self.logger.info(f"Extracting file: {tar_path}")
            # Open and extract the tar.gz file
            with tarfile.open(tar_path, 'r:gz') as tar:
                tar.extractall(path=self.output_dir)

            # Remove the tar.gz file after successful extraction
            os.remove(tar_path)
            self.logger.info(f"Extraction complete: {expected_path}")

            # Check if the extracted file exists
            if os.path.exists(expected_path):
                return expected_path
            else:
                # Log an error if the extracted file is missing
                self.logger.error(f"Extracted file not found: {expected_path}")
                return None
        except tarfile.TarError as tar_err:
            # Log errors related to tar file handling
            self.logger.error(f"Tar file error: {tar_err}")
        except OSError as os_err:
            # Log OS-related errors during extraction
            self.logger.error(f"OS error during extraction: {os_err}")
        except Exception as e:
            # Log unexpected errors during extraction
            self.logger.error(f"Unexpected error during extraction: {e}")
        return None

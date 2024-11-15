import tarfile
import os
import requests
import logging
from pipeline.abstract_etl.data_downloader import DataDownloader
from typing import Optional


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
        super().__init__(output_dir)
        self.base_url = "https://ftp.ncbi.nlm.nih.gov/geo/series"
        self.logger = self._initialize_logger(debug)

    @staticmethod
    def _initialize_logger(debug: bool) -> logging.Logger:
        """
        Initializes and returns a logger.

        Args:
            debug (bool): Enables debug output if True.

        Returns:
            logging.Logger: Configured logger instance.
        """
        logger = logging.getLogger("GeoMetadataDownloader")
        logger.setLevel(logging.DEBUG if debug else logging.INFO)
        if not logger.hasHandlers():  # Prevent adding multiple handlers during testing
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
            logger.addHandler(handler)
        return logger

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
        if not file_id:
            raise ValueError("File ID cannot be empty.")

        # Construct the download URL and local file paths
        stub = file_id[:-3] + 'nnn'
        url = f"{self.base_url}/{stub}/{file_id}/miniml/{file_id}_family.xml.tgz"
        output_path = os.path.join(self.output_dir, f"{file_id}_family.xml.tgz")
        extracted_path = os.path.join(self.output_dir, f"{file_id}_family.xml")

        self.logger.debug(f"URL: {url}")
        self.logger.debug(f"Output Path: {output_path}")
        self.logger.debug(f"Extracted Path: {extracted_path}")

        # Check if the file already exists
        if self.file_exists(extracted_path):
            self.logger.info(f"File already exists: {extracted_path}")
            return extracted_path

        # Download the file
        downloaded_path = self.download_from_url(url, output_path)
        if not downloaded_path:
            self.logger.error(f"Failed to download file from {url}")
            return None

        # Extract the downloaded file
        try:
            self.logger.info(f"Extracting file: {downloaded_path}")
            with tarfile.open(downloaded_path, 'r:gz') as tar:
                tar.extractall(path=self.output_dir)
            os.remove(downloaded_path)  # Remove archive after extraction
            self.logger.info(f"Extraction complete: {extracted_path}")

            # Validate extracted file existence
            if os.path.exists(extracted_path):
                return extracted_path
            else:
                self.logger.error(f"Extracted file not found: {extracted_path}")
                return None

        except (tarfile.TarError, OSError) as e:
            self.logger.error(f"Error during extraction: {e}")
            return None

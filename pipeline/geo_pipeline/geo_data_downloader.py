# File: pipeline/geo_pipeline/geo_data_downloader.py

from pipeline.abstract_etl.data_downloader import DataDownloader
from typing import Optional
import tarfile
import os
import requests


class GeoDataDownloader(DataDownloader):
    """
    GEO-specific data downloader that implements DataDownloader for GEO XML files.
    Downloads and extracts GEO XML files by constructing URLs based on GEO series ID.

    Attributes:
        base_url (str): Base URL for GEO data repository.
        debug (bool): Flag to control debug output.
    """

    def __init__(self, output_dir: str, debug: bool = False) -> None:
        """
        Initializes GeoDataDownloader with output directory and optional debug flag.

        Args:
            output_dir (str): Directory to save downloaded files.
            debug (bool): Enables debug output if True.
        """
        super().__init__(output_dir)
        self.base_url = "https://ftp.ncbi.nlm.nih.gov/geo/series"
        self.debug = debug

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
        # Validate file ID input
        if not file_id:
            raise ValueError("File ID cannot be empty.")

        # Construct the download URL and local file paths
        stub = file_id[:-3] + 'nnn'  # Base series identifier in the URL path
        url = f"{self.base_url}/{stub}/{file_id}/miniml/{file_id}_family.xml.tgz"
        output_path = os.path.join(self.output_dir, f"{file_id}_family.xml.tgz")
        extracted_path = os.path.join(self.output_dir, f"{file_id}_family.xml")

        # Debug output for constructed paths
        if self.debug:
            print(f"[DEBUG] URL: {url}")
            print(f"[DEBUG] Output Path: {output_path}")
            print(f"[DEBUG] Extracted Path: {extracted_path}")

        # Check if the file already exists to avoid redundant downloads
        if self.file_exists(extracted_path):
            if self.debug:
                print(f"[DEBUG] File already exists at {extracted_path}")
            return extracted_path

        # Download the file using the inherited method from DataDownloader
        downloaded_path = self.download_from_url(url, output_path)
        if downloaded_path:
            # Attempt to extract the downloaded .tar.gz file
            try:
                if self.debug:
                    print(f"[DEBUG] Starting extraction of {downloaded_path}")
                with tarfile.open(downloaded_path, 'r:gz') as tar:
                    tar.extractall(path=self.output_dir)

                if self.debug:
                    print(f"[DEBUG] Extraction successful, removing archive {downloaded_path}")
                os.remove(downloaded_path)  # Clean up the archive after extraction

                # Confirm file extraction
                if os.path.exists(extracted_path):
                    return extracted_path
                else:
                    if self.debug:
                        print(f"[DEBUG] Extracted file not found at {extracted_path}")
                    return None
            except (tarfile.TarError, OSError) as e:
                if self.debug:
                    print(f"[DEBUG] Failed to extract {downloaded_path}: {e}")
                return None
        else:
            if self.debug:
                print(f"[DEBUG] Download failed for URL: {url}")
            return None

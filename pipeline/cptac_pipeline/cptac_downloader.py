import os
import shutil
import csv
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
from pipeline.abstract_etl.data_downloader import DataDownloader


class CPTACDownloader(DataDownloader):
    """
    A downloader for handling CPTAC data using file manifests.
    Downloads and organizes data into a structured directory format with parallel processing.
    """

    def __init__(self, output_dir: str, manifest_file: str, max_workers: int = 5) -> None:
        """
        Initialize the CPTACDownloader with the output directory, manifest file, and number of workers.

        Args:
            output_dir (str): Directory to save the downloaded files.
            manifest_file (str): Path to the CPTAC manifest file.
            max_workers (int): Number of threads for parallel downloading.
        """
        super().__init__(output_dir)
        if not os.path.isfile(manifest_file):
            raise ValueError(f"Manifest file does not exist: {manifest_file}")
        self.manifest_file = manifest_file
        self.max_workers = max_workers

    def download_file(self, file_id: str) -> Optional[str]:
        """
        Placeholder for abstract method from the base class.

        Args:
            file_id (str): Identifier of the file to download.

        Returns:
            Optional[str]: Path to the downloaded file or None.
        """
        # Since this script downloads files via manifest, this method isn't used.
        return None

    def download_files(self, delimiter: str = ',') -> None:
        """
        Download files specified in the manifest and organize them into directories.

        Args:
            delimiter (str): Delimiter used in the manifest file (default: ',').

        Raises:
            ValueError: If any required field in the manifest is missing.
        """
        try:
            with open(self.manifest_file, mode='r') as f:
                reader = csv.DictReader(f, delimiter=delimiter)
                file_entries = list(reader)

            # Parallel downloading using ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_file = {
                    executor.submit(self._process_file_entry, row): row for row in file_entries
                }

                for future in as_completed(future_to_file):
                    row = future_to_file[future]
                    try:
                        future.result()
                    except Exception as e:
                        print(f"[ERROR] Failed to process file entry {row.get('File Name', '')}: {e}")

        except FileNotFoundError:
            print(f"[ERROR] Manifest file not found: {self.manifest_file}")
        except Exception as e:
            print(f"[ERROR] Unexpected error occurred while reading manifest: {e}")

    def _process_file_entry(self, row: dict) -> None:
        """
        Process a single row in the manifest to download and organize the file.

        Args:
            row (dict): A dictionary representing a row in the manifest.
        """
        try:
            # Extract necessary fields from the manifest
            file_name = row.get('File Name', '').strip()
            file_url = row.get('File Download Link', '').strip()
            pdc_study_id = row.get('PDC Study ID', '').strip()
            study_version = row.get('PDC Study Version', '').strip()
            data_category = row.get('Data Category', '').strip()
            file_type = row.get('File Type', '').strip()

            # Validate required fields
            if not all([file_name, file_url, pdc_study_id, study_version, data_category, file_type]):
                raise ValueError("Missing one or more required fields in the manifest.")

            # Construct folder structure
            folder_name = os.path.join(
                self.output_dir, pdc_study_id, study_version, data_category, file_type
            )
            file_path = os.path.join(folder_name, file_name)

            # Download and save the file
            self._download_and_save_file(file_url, file_path)

        except Exception as e:
            print(f"[ERROR] Failed to process file entry: {e}")

    def _download_and_save_file(self, url: str, output_path: str) -> None:
        """
        Download a file from a URL and save it to the specified path.

        Args:
            url (str): URL to download the file from.
            output_path (str): Path to save the downloaded file.
        """
        try:
            # Check if the file already exists
            if self.file_exists(output_path):
                print(f"[INFO] File already exists: {output_path}")
                return

            # Create necessary directories
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # Download the file
            response = requests.get(url, stream=True, timeout=15)
            response.raise_for_status()

            # Save the file to the output path
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            print(f"[INFO] Successfully downloaded: {output_path}")

        except requests.exceptions.HTTPError as http_err:
            print(f"[ERROR] HTTP error occurred: {http_err}")
        except requests.exceptions.ConnectionError:
            print("[ERROR] Connection error occurred. Check internet connectivity.")
        except requests.exceptions.Timeout:
            print("[ERROR] Request timed out while downloading the file.")
        except Exception as e:
            print(f"[ERROR] Failed to download file from {url}: {e}")

    def organize_files(self, delimiter: str = ',') -> None:
        """
        Organize files into structured directories based on the manifest.

        Args:
            delimiter (str): Delimiter used in the manifest file (default: ',').
        """
        try:
            with open(self.manifest_file, mode='r') as f:
                reader = csv.DictReader(f, delimiter=delimiter)

                for row in reader:
                    try:
                        # Extract necessary fields from the manifest
                        file_name = row.get('File Name', '').strip()
                        pdc_study_id = row.get('PDC Study ID', '').strip()
                        study_version = row.get('PDC Study Version', '').strip()
                        data_category = row.get('Data Category', '').strip()
                        file_type = row.get('File Type', '').strip()

                        # Validate required fields
                        if not all([file_name, pdc_study_id, study_version, data_category, file_type]):
                            raise ValueError("Missing one or more required fields in the manifest.")

                        # Construct folder structure
                        folder_name = os.path.join(
                            self.output_dir, pdc_study_id, study_version, data_category, file_type
                        )
                        file_path = os.path.join(folder_name, file_name)

                        # Move file to the appropriate folder
                        if self.file_exists(file_name) and not os.path.exists(folder_name):
                            os.makedirs(folder_name)
                            shutil.move(file_name, folder_name)
                            print(f"[INFO] Moved {file_name} to {folder_name}")

                    except Exception as e:
                        print(f"[ERROR] Failed to organize file entry: {e}")

        except FileNotFoundError:
            print(f"[ERROR] Manifest file not found: {self.manifest_file}")
        except Exception as e:
            print(f"[ERROR] Unexpected error occurred while organizing files: {e}")


# ---------------- Execution ----------------
if __name__ == "__main__":
    OUTPUT_DIR = "../../resources/data/raw/CPTAC"
    MANIFEST_FILE = "../../resources/data/metadata/cptac_metadata/PDC_study_manifest_11182024_010526.csv"
    downloader = CPTACDownloader(output_dir=OUTPUT_DIR, manifest_file=MANIFEST_FILE, max_workers=10)
    downloader.download_files()
    downloader.organize_files()

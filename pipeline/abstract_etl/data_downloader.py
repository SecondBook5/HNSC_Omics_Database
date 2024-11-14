# File: pipeline/abstract_etl/data_downloader.py

from abc import ABC, abstractmethod  # Importing abstract base classes from Python's built-in library
import os  # Importing os for file path operations
import requests  # Importing requests for HTTP operations
from typing import Optional  # Importing Optional for type hinting optional return types


class DataDownloader(ABC):
    """
    Abstract base class for downloading data files.
    Provides common methods for downloading and validating file presence.

    Attributes:
        output_dir (str): Directory where the downloaded files will be saved.
    """

    def __init__(self, output_dir: str) -> None:
        # Check if the output directory path is provided
        if not output_dir:
            raise ValueError("Output directory path cannot be empty.")

        # Create the directory if it does not exist to ensure valid file paths
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        self.output_dir = output_dir

    @abstractmethod
    def download_file(self, file_id: str) -> Optional[str]:
        """
        Abstract method for downloading a file.
        Must be implemented by subclasses to handle specific download logic.

        Args:
            file_id (str): Identifier of the file to download.

        Returns:
            Optional[str]: Path to the downloaded file, or None if download failed.
        """
        pass

    @staticmethod
    def download_from_url(url: str, output_path: str) -> Optional[str]:
        """
        Downloads a file from the specified URL and saves it to the output path.

        Args:
            url (str): URL of the file to download.
            output_path (str): Path where the file will be saved.

        Returns:
            Optional[str]: Path to the saved file if successful, None otherwise.

        Raises:
            ValueError: If URL or output path are empty.
        """
        # Validate the URL is non-empty
        if not url:
            raise ValueError("URL cannot be empty.")

        # Validate the output path is non-empty
        if not output_path:
            raise ValueError("Output path cannot be empty.")

        response = None  # Initialize response to ensure it's defined
        try:
            # Attempt to download the file with a timeout and handle various HTTP errors
            response = requests.get(url, stream=True, timeout=15)
            response.raise_for_status()  # Raise error for 4xx/5xx responses

            # Write the file content to disk in chunks to handle large files
            with open(output_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)

            return output_path  # Return path if successful
        except requests.exceptions.HTTPError as http_err:
            status_code = response.status_code if response else "Unknown"
            print(f"HTTP error occurred: {http_err} - Status code: {status_code}")
            return None
        except requests.exceptions.ConnectionError:
            print("Connection error occurred. Check internet connectivity.")
            return None
        except requests.exceptions.Timeout:
            print("The request timed out. Please try again later.")
            return None
        except Exception as e:
            print(f"An error occurred while downloading from {url}: {e}")
            return None

    @staticmethod
    def file_exists(file_path: str) -> bool:
        """
        Check if a file already exists at the given path.

        Args:
            file_path (str): Path to check.

        Returns:
            bool: True if the file exists, False otherwise.

        Raises:
            ValueError: If the file path is empty.
        """
        # Validate the file path input
        if not file_path:
            raise ValueError("File path cannot be empty.")
        return os.path.isfile(file_path)

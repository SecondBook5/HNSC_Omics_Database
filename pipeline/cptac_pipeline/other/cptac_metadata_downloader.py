import os
import requests
import logging
import json
from typing import Optional


class CPTACMetadataDownloader:
    """
    A class to download CPTAC metadata by querying the PDC API directly.
    """

    def __init__(self, output_dir: str, debug: bool = False, logger: Optional[logging.Logger] = None) -> None:
        """
        Initializes CPTACMetadataDownloader with the output directory and optional debug flag.

        Args:
            output_dir (str): Directory to save downloaded files.
            debug (bool): Enables debug output if True.
            logger (Optional[logging.Logger]): Optional external logger to use.
        """
        if not output_dir:
            raise ValueError("Output directory cannot be empty.")

        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        self.metadata_url: str = (
            "https://proteomic.datacommons.cancer.gov/pdc/api/v1.0/metadata?"
            "program_name=Clinical%20Proteomic%20Tumor%20Analysis%20Consortium"
            "&study_name=CPTAC%20HNSCC%20Discovery%20Study%20-%20Phosphoproteome%7CCPTAC%20HNSCC%20Discovery%20Study%20-%20Proteome"
            "&primary_site=Head%20and%20Neck&access=Open"
        )
        self.logger: logging.Logger = logger or self._initialize_default_logger(debug)

    @staticmethod
    def _initialize_default_logger(debug: bool) -> logging.Logger:
        """
        Initializes and returns a default logger if none is provided.

        Args:
            debug (bool): Enables debug output if True.

        Returns:
            logging.Logger: Configured logger instance.
        """
        logger = logging.getLogger("CPTACMetadataDownloader")
        logger.setLevel(logging.DEBUG if debug else logging.INFO)
        if not logger.hasHandlers():
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
            logger.addHandler(handler)
        return logger

    def fetch_metadata(self) -> Optional[dict]:
        """
        Fetches the metadata from the PDC API.

        Returns:
            dict: The metadata response from the API or None if the fetch fails.
        """
        try:
            self.logger.info("Fetching metadata from PDC API...")
            response = requests.get(self.metadata_url)
            response.raise_for_status()

            # Check if metadata is available
            metadata = response.json()
            if 'data' in metadata:
                return metadata['data']  # Return the metadata data section

            self.logger.error("No 'data' key found in the response.")
            return None

        except requests.exceptions.RequestException as err:
            self.logger.error(f"Request failed: {err}")
            return None

    def save_metadata(self, metadata: dict) -> None:
        """
        Saves the fetched metadata into a JSON file in the specified output directory.

        Args:
            metadata (dict): The metadata to save.
        """
        try:
            output_path = os.path.join(self.output_dir, "cptac_metadata.json")
            with open(output_path, 'w') as f:
                json.dump(metadata, f, indent=4)

            self.logger.info(f"Metadata saved to {output_path}")
        except Exception as e:
            self.logger.error(f"Failed to save metadata: {e}")

    def download_and_process_metadata(self) -> None:
        """
        Main method to fetch and process metadata.
        """
        metadata = self.fetch_metadata()
        if metadata:
            self.save_metadata(metadata)
            # Further processing could be done here (e.g., download associated data files)
        else:
            self.logger.error("No metadata fetched, exiting.")


# Example usage
if __name__ == "__main__":
    OUTPUT_DIR = "../../../resources/metadata/cptac_metadata"
    downloader = CPTACMetadataDownloader(output_dir=OUTPUT_DIR, debug=True)
    downloader.download_and_process_metadata()

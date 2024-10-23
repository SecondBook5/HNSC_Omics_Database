import os
import GEOparse
import json
import logging
from tqdm import tqdm
from urllib.error import URLError

# Set up logging to handle different log levels
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


class GEOMetadataExtractor:
    """
    This class handles the extraction of metadata from GEO datasets and stores
    the metadata in JSON format. The class reads a list of GEO IDs, fetches
    the corresponding metadata using GEOparse, and saves the extracted metadata
    in specified directories.
    """

    def __init__(self, geo_id_file: str, output_dir: str):
        """
        Initialize the GEOMetadataExtractor with the location of the GEO ID list
        and the directory where the metadata should be saved.

        Args:
            geo_id_file (str): Path to the text file containing GEO IDs.
            output_dir (str): Path to the directory where metadata should be stored.
        """
        # Store the path to the GEO ID text file
        self.geo_id_file = geo_id_file

        # Store the output directory path
        self.output_dir = output_dir

        # Ensure that the output directory exists or create it
        os.makedirs(self.output_dir, exist_ok=True)

        # Test write permissions for the output directory
        if not os.access(self.output_dir, os.W_OK):
            raise PermissionError(f"No write permissions for directory: {self.output_dir}")

    def extract_all_metadata(self):
        """
        Extract metadata for all GEO IDs listed in the geo_id_file and save them
        as JSON files in the output directory.
        """
        # Attempt to open the GEO ID file
        try:
            with open(self.geo_id_file, 'r') as file:
                # Read the GEO IDs from the file
                geo_ids = [line.strip() for line in file]
        except FileNotFoundError:
            # Handle error if the GEO ID file is not found
            logging.error(f"The GEO ID file '{self.geo_id_file}' was not found.")
            return
        except Exception as e:
            # Handle any other errors that may occur
            logging.error(f"An error occurred while reading the GEO ID file: {e}")
            return

        # Process each GEO ID from the file
        for geo_id in tqdm(geo_ids, desc="Extracting metadata"):
            # Attempt to extract metadata for each GEO ID
            try:
                # Extract metadata for the current GEO ID
                logging.info(f"Extracting metadata for GEO ID: {geo_id}")
                self.extract_metadata(geo_id)
            except URLError as e:
                # Handle specific network-related errors
                logging.error(f"Network error fetching data for {geo_id}: {e}")
            except Exception as e:
                # Handle any errors that occur during metadata extraction
                logging.error(f"Error extracting metadata for GEO ID {geo_id}: {e}")

    def extract_metadata(self, geo_id: str):
        """
        Extract metadata for a single GEO ID and save it as a JSON file.

        Args:
            geo_id (str): The GEO ID to extract metadata for.
        """
        # Check if the metadata file already exists
        output_file = os.path.join(self.output_dir, f"{geo_id}_metadata.json")
        if os.path.exists(output_file):
            # If the file exists, skip extraction
            logging.info(f"Metadata for {geo_id} already exists at {output_file}. Skipping.")
            return

        # Validate the GEO ID format (example: should start with "GSE")
        if not geo_id.startswith("GSE") or not geo_id[3:].isdigit():
            logging.error(f"Invalid GEO ID format: {geo_id}")
            return

        # Attempt to fetch the GEO dataset using GEOparse
        try:
            gse = GEOparse.get_GEO(geo=geo_id, destdir=self.output_dir)
        except Exception as e:
            # Handle any errors during the GEOparse data fetching process
            logging.error(f"Error fetching GEO data for {geo_id}: {e}")
            return

        # Prepare a dictionary to hold the metadata
        metadata = {
            "geo_id": geo_id,
            "platform": gse.metadata.get("platform_id", "N/A"),
            "organism": gse.metadata.get("organism_ch1", "N/A"),
            "experiment_type": gse.metadata.get("experiment_type", "N/A"),
            "samples": len(gse.gsms),  # Example: number of samples in the GEO dataset
            "metadata": gse.metadata
        }

        # Attempt to save the metadata to a JSON file
        try:
            with open(output_file, 'w') as json_file:
                # Save the metadata in a JSON format
                json.dump(metadata, json_file, indent=4)
            logging.info(f"Metadata saved for {geo_id} at {output_file}")
        except Exception as e:
            # Handle errors during the saving process
            logging.error(f"Error saving metadata for {geo_id}: {e}")


# Usage example (assuming the geo_ids.txt is in the correct location):
geo_extractor = GEOMetadataExtractor(geo_id_file="../resources/geo_ids.txt", output_dir="../resources/metadata/geo_metadata")

# Call the method to extract metadata for all GEO IDs
try:
    geo_extractor.extract_all_metadata()
except Exception as e:
    logging.error(f"An error occurred during metadata extraction: {e}")

import os
import json
import logging
from typing import List, Any, Dict, Optional, TextIO, cast
from data_structures.hashmap import HashMap  # Import custom HashMap for caching paths
from data_structures.tree import BinaryTree  # Import custom BinaryTree for directory management

# Configure logging for runtime information and debugging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")



class DirectoryNotWritableError(Exception):
    """Raised when a directory lacks write permissions."""
    pass


class FileWriteError(Exception):
    """Raised when an error occurs while writing to a file."""
    pass


class GEOFileHandler:
    """
    A class to handle file operations related to GEO metadata extraction.
    It manages reading GEO IDs, saving metadata files, and validating directory structures.
    Utilizes custom data structures (HashMap, BinaryTree) to improve efficiency and maintainability.
    """

    def __init__(self, geo_id_file: str, output_dir: str):
        """
        Initialize GEOFileHandler with the GEO ID file path and output directory.

        Args:
            geo_id_file (str): Path to the file containing GEO IDs.
            output_dir (str): Directory to save output metadata files.

        Raises:
            GEOFileNotFoundError: If the GEO ID file does not exist.
            DirectoryNotWritableError: If the output directory is not writable.
        """
        # Check and store GEO ID file path
        self.geo_id_file = geo_id_file
        if not os.path.exists(geo_id_file):
            raise FileNotFoundError(f"The GEO ID file '{geo_id_file}' was not found.")

        # Verify and store output directory
        self.output_dir = output_dir
        if not os.path.isdir(output_dir) or not os.access(output_dir, os.W_OK):
            raise DirectoryNotWritableError(f"No write permissions for directory: '{output_dir}'")

        # Set up caching for directory paths using HashMap
        self.path_cache = HashMap()

        # Use BinaryTree to manage file paths efficiently
        self.dir_tree = BinaryTree()

        # Preload directory structure into tree for efficient file management
        self._initialize_directory_tree(self.output_dir)

    def _initialize_directory_tree(self, directory: str) -> None:
        """
        Populate the BinaryTree with existing directory structure.

        Args:
            directory (str): The root directory to initialize the tree.
        """
        for root, _, files in os.walk(directory):
            for file in files:
                self.dir_tree.insert(os.path.join(root, file))
        logging.info(f"Directory tree initialized for '{directory}'")

    def read_geo_ids(self) -> List[str]:
        """
        Read GEO IDs from the specified file and return them as a list.

        Returns:
            List[str]: A list of GEO IDs.

        Raises:
            FileNotFoundError: If an error occurs while reading the GEO ID file.
        """
        try:
            with open(self.geo_id_file, 'r') as file:
                geo_ids = [line.strip() for line in file]
            logging.info(f"Successfully read GEO IDs from '{self.geo_id_file}'")
            return geo_ids
        except Exception as e:
            raise FileNotFoundError(f"Error reading GEO IDs from '{self.geo_id_file}': {e}")

    def save_metadata(self, geo_id: str, metadata: Dict[str, Any]) -> None:
        """
        Save metadata as a JSON file in the output directory.

        Args:
            geo_id (str): The GEO ID for which metadata is being saved.
            metadata (Dict[str, Any]): The metadata to save in JSON format.

        Raises:
            FileWriteError: If an error occurs while writing the metadata file.
        """
        output_file_path = os.path.join(self.output_dir, f"{geo_id}_metadata.json")

        # Caching file path for quick access
        if not self.path_cache.contains(geo_id):
            self.path_cache.put(geo_id, output_file_path)

        # Attempt to write metadata to a JSON file
        try:
            with open(output_file_path, 'w') as json_file:  #type: ignore
                json_file = cast(TextIO, json_file)  # Type cast to TextIO to avoid type warning
                # noinspection PyTypeChecker
                json.dump(metadata, json_file, indent=4)
            logging.info(f"Metadata for GEO ID '{geo_id}' saved to '{output_file_path}'")
        except Exception as e:
            raise FileWriteError(f"Error saving metadata for GEO ID '{geo_id}': {e}")

    def load_metadata(self, geo_id: str) -> Optional[Dict[str, Any]]:
        """
        Load and return metadata for a specified GEO ID, if it exists in the output directory.

        Args:
            geo_id (str): The GEO ID for which metadata is being loaded.

        Returns:
            Optional[Dict[str, Any]]: Metadata as a dictionary, or None if the file does not exist.

        Raises:
            GEOFileNotFoundError: If an error occurs while reading the metadata file.
        """
        output_file_path = self.path_cache.get(geo_id) or os.path.join(self.output_dir, f"{geo_id}_metadata.json")

        # Attempt to read metadata from the JSON file
        try:
            with open(output_file_path, 'r') as json_file:
                metadata = json.load(json_file)
            logging.info(f"Metadata for GEO ID '{geo_id}' loaded from '{output_file_path}'")
            return metadata
        except FileNotFoundError:
            logging.warning(f"Metadata file for GEO ID '{geo_id}' does not exist.")
            return None
        except Exception as e:
            raise FileNotFoundError(f"Error loading metadata for GEO ID '{geo_id}': {e}")

    def verify_output_structure(self) -> None:
        """
        Verify the existence and permissions of the output directory, creating it if necessary.

        Raises:
            DirectoryNotWritableError: If the output directory is not writable.
        """
        if not os.path.exists(self.output_dir):
            try:
                os.makedirs(self.output_dir)
                logging.info(f"Output directory '{self.output_dir}' created.")
            except Exception as e:
                raise DirectoryNotWritableError(f"Unable to create output directory '{self.output_dir}': {e}")
        elif not os.access(self.output_dir, os.W_OK):
            raise DirectoryNotWritableError(f"No write permissions for directory '{self.output_dir}'")

    def list_saved_geo_files(self) -> List[str]:
        """
        List all GEO metadata files saved in the output directory.

        Returns:
            List[str]: A list of filenames for saved GEO metadata files.
        """
        geo_files = [file for file in os.listdir(self.output_dir) if file.endswith("_metadata.json")]
        logging.info(f"Found {len(geo_files)} GEO metadata files in '{self.output_dir}'")
        return geo_files

    def delete_metadata(self, geo_id: str) -> None:
        """
        Delete a metadata file associated with a given GEO ID.

        Args:
            geo_id (str): The GEO ID whose metadata file should be deleted.

        Raises:
            GEOFileNotFoundError: If the metadata file does not exist.
            FileWriteError: If an error occurs while deleting the file.
        """
        output_file_path = self.path_cache.get(geo_id) or os.path.join(self.output_dir, f"{geo_id}_metadata.json")

        # Attempt to delete the metadata file
        try:
            #
            if os.path.exists(output_file_path):
                # remove
                os.remove(output_file_path)
                self.path_cache.delete(geo_id)  # Remove path from cache
                logging.info(f"Metadata file for GEO ID '{geo_id}' deleted.")
            else:
                raise FileNotFoundError(f"Metadata file for GEO ID '{geo_id}' not found.")
        except Exception as e:
            raise FileWriteError(f"Error deleting metadata file for GEO ID '{geo_id}': {e}")

# File: utils/config_utils.py
# Description: Utility functions for loading, validating, and managing configuration files.

import yaml  # Import PyYAML for reading and parsing YAML files
import os  # Import OS for file and directory handling
from pathlib import Path  # Import Path for OS-independent file paths

class ConfigLoaderError(Exception):
    """
    Custom exception for errors encountered during configuration loading or validation.
    """
    pass


def load_config(config_file_path: str, default_config: dict = None) -> dict:
    """
    Load the configuration from a YAML file.

    Args:
        config_file_path (str): Path to the YAML configuration file.
        default_config (dict, optional): Default configuration to use if loading fails.

    Returns:
        dict: Parsed configuration dictionary.

    Raises:
        ConfigLoaderError: If the configuration file does not exist or fails to parse.
    """
    # Step 1: Check if the YAML file exists at the specified path
    if not os.path.exists(config_file_path):
        # If a default configuration is provided, return it with a warning
        if default_config is not None:
            print(f"Config file '{config_file_path}' not found. Using default configuration.")
            return default_config
        # If no default is provided, raise a custom error
        raise ConfigLoaderError(f"Config file '{config_file_path}' not found.")

    # Step 2: Attempt to load the YAML file
    try:
        # Open the file in read mode
        with open(config_file_path, "r") as config_file:
            # Use safe_load to parse the YAML content into a dictionary
            config = yaml.safe_load(config_file)
        # Return the parsed configuration dictionary
        return config
    # Step 3: Catch YAML parsing errors and handle them
    except yaml.YAMLError as e:
        # If a default configuration is provided, return it with a warning
        if default_config is not None:
            print(f"Error parsing YAML file '{config_file_path}': {e}. Using default configuration.")
            return default_config
        # If no default is provided, raise a custom error
        raise ConfigLoaderError(f"Error parsing YAML file '{config_file_path}': {e}")


def validate_config(config: dict, required_keys: list) -> None:
    """
    Validate that required keys are present in the configuration dictionary.

    Args:
        config (dict): The configuration dictionary to validate.
        required_keys (list): A list of keys that must be present in the configuration.

    Raises:
        ConfigLoaderError: If any required keys are missing.
    """
    # Step 1: Check for missing required keys in the configuration
    missing_keys = [key for key in required_keys if key not in config]
    # Step 2: If missing keys are found, raise a custom error
    if missing_keys:
        raise ConfigLoaderError(f"Missing required keys in configuration: {missing_keys}")


def ensure_directories(config: dict, keys: list) -> None:
    """
    Ensure that all directories specified in the configuration exist.

    Args:
        config (dict): The configuration dictionary containing directory paths.
        keys (list): List of keys in the configuration that correspond to directory paths.

    Raises:
        ConfigLoaderError: If any directory paths are invalid.
    """
    # Step 1: Iterate over the specified keys
    for key in keys:
        # Retrieve the directory path from the configuration
        dir_path = config.get(key)
        if not dir_path:
            raise ConfigLoaderError(f"Missing or invalid directory path for key: {key}")

        # Step 2: Create the directory if it does not exist
        try:
            os.makedirs(dir_path, exist_ok=True)
            print(f"Verified or created directory: {dir_path}")
        except OSError as e:
            raise ConfigLoaderError(f"Error creating directory '{dir_path}': {e}")

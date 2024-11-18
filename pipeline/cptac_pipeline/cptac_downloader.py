# File: pipeline/cptac_pipeline/cptac_downloader.py

import os
from typing import Optional
import cptac
from pipeline.abstract_etl.data_downloader import DataDownloader


class CPTACDownloader(DataDownloader):
    """
    A downloader for CPTAC data focused on a specific cancer type.
    Extends the abstract DataDownloader class to fetch metadata and omics data.
    """

    def __init__(self, output_dir: str, cancer_type: str) -> None:
        """
        Initialize the CPTACDownloader with a target output directory and cancer type.

        Args:
            output_dir (str): Directory to save downloaded files.
            cancer_type (str): Cancer type for which data will be downloaded.
        """
        super().__init__(output_dir)
        self.cancer_type = cancer_type.lower()
        self.cancer_data = None

        # Check if the specified cancer type is available
        self._validate_cancer_type()

    def _validate_cancer_type(self) -> None:
        """
        Validate that the specified cancer type is supported by the CPTAC package.

        Raises:
            ValueError: If the cancer type is not supported.
        """
        available_cancers = cptac.get_cancer_info()
        if self.cancer_type not in available_cancers:
            raise ValueError(
                f"Cancer type '{self.cancer_type}' is not supported. "
                f"Available types: {', '.join(available_cancers.keys())}"
            )

    def _load_cancer_data(self) -> None:
        """
        Load the cancer-specific dataset using the CPTAC package.
        """
        self.cancer_data = getattr(cptac, self.cancer_type.capitalize())()

    def download_file(self, file_id: str) -> Optional[str]:
        """
        Implementation of the abstract download_file method.
        Currently serves as a placeholder for downloading files.

        Args:
            file_id (str): Identifier of the file to download.

        Returns:
            Optional[str]: Placeholder path or None.
        """
        # In CPTAC context, we download data programmatically via API calls.
        # This method can be customized to handle specific download scenarios.
        print(f"Downloading file with ID: {file_id}")
        return None

    def list_data_sources(self) -> dict:
        """
        List available data sources for the specified cancer type.

        Returns:
            dict: A dictionary of data types and their respective sources.
        """
        if not self.cancer_data:
            self._load_cancer_data()

        sources = self.cancer_data.list_data_sources()
        print(sources)  # Display the available data sources
        return sources

    def get_clinical_data(self) -> Optional[str]:
        """
        Download and save clinical data for the specified cancer type.

        Returns:
            Optional[str]: Path to the saved clinical data CSV file.
        """
        if not self.cancer_data:
            self._load_cancer_data()

        clinical_data = self.cancer_data.get_clinical("mssm")
        output_path = os.path.join(self.output_dir, f"{self.cancer_type}_clinical.csv")

        # Save clinical data to a CSV file
        clinical_data.to_csv(output_path, index=True)
        print(f"Downloaded and saved clinical data to: {output_path}")
        return output_path


if __name__ == "__main__":
    # Define the output directory
    output_dir = "../../resources/data/raw/CPTAC"

    # Initialize the downloader for HNSCC
    downloader = CPTACDownloader(output_dir=output_dir, cancer_type="hnscc")

    # Dry run: Fetch and display metadata without downloading any files
    print("---- Available Data Sources ----")
    downloader.list_data_sources()  # Lists available data types and sources

    print("\n---- Clinical Metadata Preview ----")
    downloader.get_clinical_data()  # Dry run to ensure this method works, outputs metadata to console

    print("\n---- Proteomics Data Sources ----")
    # Directly list proteomics sources
    proteomics_sources = downloader.list_data_sources()
    print("Proteomics data sources:", proteomics_sources.get("proteomics", []))

    # Indicate end of dry run
    print("\nDry run completed. Metadata displayed.")

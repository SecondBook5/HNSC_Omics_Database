"""
geo_metadata_pipeline.py

This script handles the extraction, transformation, and loading (ETL) of GEO metadata from XML files
for the HNSC Omics Database project. It parses XML files to retrieve relevant metadata fields and structures
them for ingestion into DatasetSeriesMetadata and DatasetSampleMetadata tables in the database.

Functions:
    - load_geo_ids: Loads GEO series IDs from a text file.
    - download_miniml_file: Downloads and extracts a MINiML XML file for a given GEO series ID.
    - parse_geo_xml: Parses XML files to extract GEO metadata.
    - get_namespace: Extracts the XML namespace for handling namespaced tags.
    - process_series_metadata: Extracts series-level metadata from XML.
    - process_sample_metadata: Extracts sample-level metadata and replaces "NA" with None for certain fields.
    - ingest_metadata_to_db: Loads the validated metadata into the database using SQLAlchemy ORM models.
"""

import os
import requests
import tarfile
import xml.etree.ElementTree as ET
from sqlalchemy.orm import Session
from db.db_config import engine  # Adjust this import path as needed
from db.schema.metadata_schema import DatasetSeriesMetadata, DatasetSampleMetadata  # ORM models for database schema

# Directory to save downloaded MINiML metadata files
METADATA_DIR = "../resources/data/metadata/geo_metadata"
GEO_ID_FILE = "../resources/geo_ids.txt"
os.makedirs(METADATA_DIR, exist_ok=True)

def load_geo_ids(geo_id_file: str) -> list:
    """Load GEO IDs from a text file."""
    with open(geo_id_file, 'r') as file:
        geo_ids = [line.strip() for line in file if line.strip()]
    return geo_ids


def download_miniml_file(gse_id: str) -> str:
    """
    Download and extract a MINiML XML file for a specified GEO series (GSE) ID.

    Args:
        gse_id (str): The GEO Series ID for the file to download.

    Returns:
        str: Path to the extracted XML file if successful, None otherwise.
    """
    # Generate URL and file paths based on the GEO ID
    stub = gse_id[:-3] + 'nnn'
    url = f"https://ftp.ncbi.nlm.nih.gov/geo/series/{stub}/{gse_id}/miniml/{gse_id}_family.xml.tgz"
    output_archive = os.path.join(METADATA_DIR, f"{gse_id}_family.xml.tgz")
    output_xml_file = os.path.join(METADATA_DIR, f"{gse_id}_family.xml")

    print(f"Attempting to download MINiML file for accession {gse_id}...")

    try:
        # Download the tar.gz archive containing the MINiML XML file
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # Write the downloaded content to the archive file
        with open(output_archive, 'wb') as file:
            file.write(response.content)
        print(f"Downloaded MINiML file for {gse_id} successfully.")

        # Extract the XML file from the archive
        with tarfile.open(output_archive, 'r:gz') as tar:
            tar.extractall(path=METADATA_DIR)
        print(f"Extracted MINiML file: {output_xml_file}")

    except requests.exceptions.RequestException as e:
        print(f"Failed to download MINiML file for {gse_id} due to network error: {e}")
        return None
    finally:
        # Clean up by removing the archive file
        if os.path.exists(output_archive):
            os.remove(output_archive)

    return output_xml_file


def parse_geo_xml(xml_path: str) -> ET.ElementTree:
    """
    Parse GEO XML file to extract metadata elements.

    Args:
        xml_path (str): Path to the XML file to parse.

    Returns:
        ET.ElementTree: Parsed XML tree.
    """
    try:
        # Parse the XML file and return the ElementTree object
        tree = ET.parse(xml_path)
        return tree
    except ET.ParseError as e:
        raise ValueError(f"Error parsing XML file at {xml_path}: {e}")


def get_namespace(element: ET.Element) -> str:
    """
    Extract XML namespace from an element.

    Args:
        element (ET.Element): XML element to get the namespace from.

    Returns:
        str: The namespace string.
    """
    return element.tag[element.tag.find("{"):element.tag.rfind("}") + 1]


def process_series_metadata(tree: ET.ElementTree) -> dict:
    """
    Processes series-level metadata from an XML tree.

    Args:
        tree (ET.ElementTree): Parsed XML tree.


    Returns:
        dict: Dictionary of series metadata.
    """
    root = tree.getroot()
    ns = get_namespace(root)  # Extract namespace

    # Extract series metadata fields
    series_metadata = {
        'SeriesID': root.findtext(f'.//{ns}Series iid'),
        'Title': root.findtext(f'.//{ns}Title'),
        'GEOAccession': root.findtext(f'.//{ns}Accession'),
        'Status': root.findtext(f'.//{ns}Status'),
        'SubmissionDate': root.findtext(f'.//{ns}Submission-Date'),
        'LastUpdateDate': root.findtext(f'.//{ns}Last-Update-Date'),
        'PubMedID': root.findtext(f'.//{ns}Pubmed-ID'),
        'Summary': root.findtext(f'.//{ns}Summary'),
        'OverallDesign': root.findtext(f'.//{ns}Overall-Design'),
        'SeriesType': root.findtext(f'.//{ns}Type'),
        'PlatformID': root.findtext(f'.//{ns}Platform-Ref'),
        'Organism': root.findtext(f'.//{ns}Organism'),
        'Contributors': root.findtext(f'.//{ns}Contributor'),
        'DatabaseName': root.findtext(f'.//{ns}DatabaseName'),
        'DatabasePublicID': root.findtext(f'.//{ns}DatabasePublicID'),
        'DatabaseOrganization': root.findtext(f'.//{ns}DatabaseOrganization'),
        'DatabaseWebLink': root.findtext(f'.//{ns}DatabaseWebLink'),
        'DatabaseEmail': root.findtext(f'.//{ns}DatabaseEmail')
    }
    print("Parsed Series Metadata:", series_metadata)
    return series_metadata


def process_sample_metadata(tree: ET.ElementTree, series_id: str) -> list:
    """
    Processes sample-level metadata from an XML tree.

    Args:
        tree (ET.ElementTree): Parsed XML tree.
        series_id (str): Series ID for each sample.

    Returns:
        list: List of dictionaries, each containing metadata for a sample.
    """
    root = tree.getroot()
    ns = get_namespace(root)  # Extract namespace
    sample_metadata_list = []

    for sample in root.findall(f'.//{ns}Sample'):
        sample_metadata = {
            'SeriesID': series_id,  # Assign SeriesID
            'SampleID': sample.findtext(f'.//{ns}Accession'),
            'GEOAccession': sample.findtext(f'.//{ns}Accession'),
            'Title': sample.findtext(f'.//{ns}Title'),
            # Additional fields extracted here...
        }

        # Replace 'NA' values and strip whitespace
        sample_metadata = {k: (v.strip() if v and v.strip() != "NA" else None) for k, v in sample_metadata.items()}

        sample_metadata_list.append(sample_metadata)
    return sample_metadata_list


def ingest_metadata_to_db(series_metadata: dict, sample_metadata_list: list):
    """
    Inserts series and sample metadata into the database.

    Args:
        series_metadata (dict): Series metadata dictionary.
        sample_metadata_list (list): List of sample metadata dictionaries.
    """
    # Use SQLAlchemy session for database interaction
    with Session(engine) as session:
        if not series_metadata['SeriesID']:
            print("Essential series metadata missing; skipping insertion.")
            return

        series_record = DatasetSeriesMetadata(**series_metadata)
        session.add(series_record)

        for sample_metadata in sample_metadata_list:
            sample_record = DatasetSampleMetadata(**sample_metadata)
            session.add(sample_record)

        session.commit()
        print("Metadata ingested successfully into the database.")


def main():
    """
    Main execution function that downloads, parses, processes, and ingests metadata.
    """
    geo_ids = load_geo_ids(GEO_ID_FILE)
    for gse_id in geo_ids:
        xml_path = download_miniml_file(gse_id)
        if xml_path:
            tree = parse_geo_xml(xml_path)
            filename = os.path.basename(xml_path)



if __name__ == "__main__":
    main()

from lxml import etree
from typing import Dict, Optional
from datetime import datetime
import re
import json
import time

# Regular expressions for GEO accession and sample ID validation
GEO_ACCESSION_PATTERN = re.compile(r'^GSE\d+$')
SAMPLE_ID_PATTERN = re.compile(r'^GSM\d+$')


def parse_date(date_str: str) -> Optional[str]:
    """Converts a date string to ISO format or returns None if invalid."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date().isoformat() if date_str else None
    except ValueError:
        return None


def process_series_data(series_elem: etree._Element, ns: Dict[str, str]) -> Optional[Dict[str, Optional[str]]]:
    """Parses a single Series element."""
    try:
        series_id = series_elem.findtext('geo:Accession', default="", namespaces=ns)
        if not GEO_ACCESSION_PATTERN.match(series_id):
            print(f"Invalid Series ID: {series_id}")
            return None

        status = series_elem.find('geo:Status', ns)
        relations = [
            {
                "type": relation.attrib.get("type", "Unknown"),
                "target": relation.attrib.get("target", "")
            }
            for relation in series_elem.findall('geo:Relation', ns)
        ]
        supplementary_data = "; ".join([
            supp_data.text.strip()
            for supp_data in series_elem.findall('geo:Supplementary-Data', ns) if supp_data.text
        ])

        return {
            "SeriesID": series_id,
            "Title": series_elem.findtext('geo:Title', default="", namespaces=ns),
            "SubmissionDate": parse_date(
                status.findtext('geo:Submission-Date', default="", namespaces=ns)) if status else None,
            "LastUpdateDate": parse_date(
                status.findtext('geo:Last-Update-Date', default="", namespaces=ns)) if status else None,
            "PubMedID": series_elem.findtext('geo:Pubmed-ID', default="", namespaces=ns),
            "Summary": series_elem.findtext('geo:Summary', default="", namespaces=ns),
            "OverallDesign": series_elem.findtext('geo:Overall-Design', default="", namespaces=ns),
            "Relations": json.dumps(relations),
            "SupplementaryData": supplementary_data
        }
    except Exception as e:
        print(f"Error parsing Series element: {e}")
        return None


def process_sample_data(sample_elem: etree._Element, ns: Dict[str, str]) -> Optional[Dict[str, Optional[str]]]:
    """Parses a single Sample element."""
    try:
        sample_id = sample_elem.findtext('geo:Accession', default="", namespaces=ns)
        if not SAMPLE_ID_PATTERN.match(sample_id):
            print(f"Invalid Sample ID: {sample_id}")
            return None

        status = sample_elem.find('geo:Status', ns)
        characteristics = [
            {
                "tag": char.attrib.get("tag", "Unknown"),
                "value": char.text.strip() if char.text else ""
            }
            for char in sample_elem.findall('geo:Channel/geo:Characteristics', ns)
        ]
        supplementary_data = "; ".join([
            supp_data.text.strip()
            for supp_data in sample_elem.findall('geo:Supplementary-Data', ns) if supp_data.text
        ])
        relations = [
            {
                "type": relation.attrib.get("type", "Unknown"),
                "target": relation.attrib.get("target", "")
            }
            for relation in sample_elem.findall('geo:Relation', ns)
        ]

        return {
            "SampleID": sample_id,
            "Title": sample_elem.findtext('geo:Title', default="", namespaces=ns),
            "SubmissionDate": parse_date(
                status.findtext('geo:Submission-Date', default="", namespaces=ns)) if status else None,
            "LastUpdateDate": parse_date(
                status.findtext('geo:Last-Update-Date', default="", namespaces=ns)) if status else None,
            "Organism": sample_elem.findtext('geo:Channel/geo:Organism', default="", namespaces=ns),
            "Characteristics": json.dumps(characteristics),
            "Relations": json.dumps(relations),
            "SupplementaryData": supplementary_data
        }
    except Exception as e:
        print(f"Error parsing Sample element: {e}")
        return None


def parse_geo_metadata(file_path: str):
    """Parses the GEO MINiML file using lxml and prints statistics."""
    start_time = time.time()
    ns = {'geo': 'http://www.ncbi.nlm.nih.gov/geo/info/MINiML'}

    series_count = 0
    sample_count = 0

    print("Parsing Series data...")
    context = etree.iterparse(file_path, events=("start", "end"), tag="{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Series")
    for event, elem in context:
        if event == "end":
            series_data = process_series_data(elem, ns)
            if series_data:
                print(json.dumps(series_data, indent=2))
                series_count += 1
            elem.clear()

    print("Parsing Sample data...")
    context = etree.iterparse(file_path, events=("start", "end"), tag="{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Sample")
    for event, elem in context:
        if event == "end":
            sample_data = process_sample_data(elem, ns)
            if sample_data:
                print(json.dumps(sample_data, indent=2))
                sample_count += 1
            elem.clear()

    elapsed_time = time.time() - start_time
    print("\nParsing completed.")
    print(f"Series parsed: {series_count}")
    print(f"Samples parsed: {sample_count}")
    print(f"Time elapsed: {elapsed_time:.2f} seconds")


if __name__ == "__main__":
    file_path = "../../resources/data/metadata/geo_metadata/GSE112026_family.xml"
    parse_geo_metadata(file_path)

import os
import requests
import ftplib
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import JSONB
from config.db_config import get_session_context
from config.logger_config import configure_logger
from sqlalchemy import String
from db.schema.geo_metadata_schema import GeoSeriesMetadata, GeoSampleMetadata
from urllib.parse import urlparse

class GeoDataDownloader:
    """
    Handles downloading of GEO data based on metadata stored in the database.

    This class fetches series and sample metadata, identifies supplementary data files,
    and downloads them to a specified directory.
    """

    def __init__(self, download_root: str = "../resources/data/raw/GEO"):
        """
        Initializes the GeoDataDownloader with configuration.

        Args:
            download_root (str): Root directory for storing downloaded GEO data.
        """
        self.download_root = download_root
        self.logger = configure_logger(name="geo_data_downloader")

    def get_series_ids(self, session: Session, data_type: str) -> list:
        """
        Fetches series IDs from the database for a specified data type.

        Args:
            session (Session): Active SQLAlchemy session.
            data_type (str): Data type to filter by (e.g., "ATAC-Seq").

        Returns:
            list: Series IDs matching the data type.
        """
        try:
            # Use JSONB functions to search for data type in JSONB array or handle it as a string
            series = (
                session.query(GeoSeriesMetadata)
                .filter(
                    (GeoSeriesMetadata.DataTypes.op("@>")([data_type]))  # JSONB array element check
                    | (GeoSeriesMetadata.DataTypes.cast(String).like(f"%{data_type}%"))  # Handle as string
                )
                .all()
            )
            self.logger.info(f"Found {len(series)} series for data type: {data_type}")
            return [s.SeriesID for s in series]
        except SQLAlchemyError as e:
            self.logger.error(f"Error fetching series for data type {data_type}: {e}")
            return []

    def get_sample_supplementary_data(self, session: Session, series_id: str) -> list:
        """
        Fetches supplementary data URLs for all samples related to a series.

        Args:
            session (Session): Active SQLAlchemy session.
            series_id (str): Series ID to fetch sample data for.

        Returns:
            list: URLs of supplementary data files.
        """
        try:
            samples = (
                session.query(GeoSampleMetadata)
                .filter(GeoSampleMetadata.SeriesID == series_id)
                .all()
            )
            urls = [
                sample.SupplementaryData
                for sample in samples
                if sample.SupplementaryData is not None
            ]
            self.logger.info(
                f"Found {len(urls)} supplementary files for series: {series_id}"
            )
            return urls
        except SQLAlchemyError as e:
            self.logger.error(
                f"Error fetching supplementary data for series {series_id}: {e}"
            )
            return []

    def download_files(self, urls: list, series_id: str):
        """
        Downloads files from a list of URLs to a directory named after the series ID.

        Args:
            urls (list): List of file URLs to download.
            series_id (str): Series ID to name the target directory.
        """
        series_dir = os.path.join(self.download_root, series_id)
        os.makedirs(series_dir, exist_ok=True)

        for url in urls:
            try:
                # Get the file name from the URL
                file_name = os.path.basename(url)
                file_path = os.path.join(series_dir, file_name)

                # Skip download if the file already exists
                if os.path.exists(file_path):
                    self.logger.info(f"File already exists, skipping: {file_path}")
                    continue

                # Determine if the URL is FTP or HTTP/HTTPS
                parsed_url = urlparse(url)
                if parsed_url.scheme == 'ftp':
                    # FTP download using ftplib
                    self.download_ftp(url, file_path)
                else:
                    # HTTP/HTTPS download using requests
                    self.download_http(url, file_path)

            except Exception as e:
                self.logger.error(f"Error downloading file {url}: {e}")

    def download_ftp(self, ftp_url: str, local_file_path: str):
        """
        Downloads a file using FTP protocol.

        Args:
            ftp_url (str): The FTP URL to download the file from.
            local_file_path (str): The local path to save the downloaded file.
        """
        try:
            # Parse the FTP URL
            parsed_url = urlparse(ftp_url)
            ftp_host = parsed_url.hostname
            ftp_path = parsed_url.path.lstrip('/')
            ftp_user = parsed_url.username or 'anonymous'
            ftp_pass = parsed_url.password or ''

            # Connect to the FTP server
            with ftplib.FTP(ftp_host) as ftp:
                ftp.login(user=ftp_user, passwd=ftp_pass)
                with open(local_file_path, 'wb') as local_file:
                    ftp.retrbinary(f'RETR {ftp_path}', local_file.write)
                    self.logger.info(f"Downloaded successfully: {local_file_path}")
        except ftplib.all_errors as e:
            self.logger.error(f"Error downloading via FTP: {ftp_url} - {e}")

    def download_http(self, http_url: str, local_file_path: str):
        """
        Downloads a file using HTTP/HTTPS protocol.

        Args:
            http_url (str): The HTTP/HTTPS URL to download the file from.
            local_file_path (str): The local path to save the downloaded file.
        """
        try:
            self.logger.info(f"Downloading {http_url} to {local_file_path}")
            response = requests.get(http_url, stream=True)
            response.raise_for_status()
            with open(local_file_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            self.logger.info(f"Downloaded successfully: {local_file_path}")
        except requests.RequestException as e:
            self.logger.error(f"Error downloading file {http_url}: {e}")

    def run(self, data_type: str):
        """
        Orchestrates the downloading process for a specified data type.

        Args:
            data_type (str): Data type to process (e.g., "ATAC-Seq").
        """
        with get_session_context() as session:
            self.logger.info(f"Starting GEO data download for data type: {data_type}")

            # Step 1: Fetch series IDs for the data type
            series_ids = self.get_series_ids(session, data_type)

            if not series_ids:
                self.logger.warning(f"No series found for data type: {data_type}")
                return

            for series_id in series_ids:
                # Step 2: Fetch supplementary data URLs for the series
                urls = self.get_sample_supplementary_data(session, series_id)

                # Step 3: Download files to the appropriate directory
                if urls:
                    self.download_files(urls, series_id)
                else:
                    self.logger.warning(f"No supplementary data URLs found for series: {series_id}")

            self.logger.info("GEO data download completed.")

# Example Usage
if __name__ == "__main__":
    downloader = GeoDataDownloader()
    downloader.run(data_type="ATAC-Seq")

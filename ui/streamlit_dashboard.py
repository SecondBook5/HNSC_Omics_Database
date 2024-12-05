import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
import sys
import os

# Add the absolute path to the project root
sys.path.insert(0, "/mnt/c/Users/ajboo/BookAbraham/BiologicalDatabases/HNSC_Omics_Database")

# Import pipeline modules
from scripts.geo_metadata_pipeline import GeoMetadataPipeline
from pipeline.geo_pipeline.geo_classifier import DataTypeDeterminer
from pipeline.geo_pipeline.fetch_pubmed_ids import fetch_pubmed_ids, fetch_citation
from pipeline.geo_pipeline.geo_data_downloader import GeoDataDownloader

# Load environment variables
load_dotenv()

# Database configuration from environment
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
PG_DB_NAME = os.getenv("PG_DB_NAME")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB_NAME}"

# Create SQLAlchemy engine and session factory
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

# Streamlit App Title
st.title("GEO Metadata Pipeline and Query Interface")

# Sidebar
st.sidebar.header("Options")
app_mode = st.sidebar.selectbox(
    "Choose Mode",
    [
        "Run GEO Metadata Pipeline",
        "SQL Query Interface",
        "Fetch PubMed Citations",
        "Download GEO Data by Type"
    ]
)

# Run GEO Metadata Pipeline
if app_mode == "Run GEO Metadata Pipeline":
    st.header("Run GEO Metadata Pipeline")

    geo_ids_input = st.text_area(
        "Enter GEO IDs (comma-separated):",
        placeholder="GSE12345, GSE67890"
    )

    if st.button("Run Pipeline"):
        geo_ids = [geo_id.strip() for geo_id in geo_ids_input.split(",") if geo_id.strip()]
        if not geo_ids:
            st.error("Please enter valid GEO IDs.")
        else:
            try:
                # Run pipeline
                st.write(f"Processing GEO IDs: {', '.join(geo_ids)}")
                pipeline = GeoMetadataPipeline(geo_ids=geo_ids)
                pipeline.execute_pipeline()
                st.success("Pipeline completed successfully!")

                # Run classifier
                st.write("Running classifier...")
                for geo_id in geo_ids:
                    determiner = DataTypeDeterminer(geo_id)
                    determiner.process()
                st.success("Classifier completed successfully!")

                # Query and display GEO Series and Data Types
                with engine.connect() as conn:
                    query = f"""
                        SELECT "SeriesID", "DataTypes" FROM geo_series_metadata
                        WHERE "SeriesID" IN ({','.join([f"'{geo_id}'" for geo_id in geo_ids])});
                    """
                    result = conn.execute(text(query))
                    data = [{"SeriesID": row["SeriesID"], "DataTypes": row["DataTypes"]} for row in result.mappings()]

                    if data:
                        st.write("GEO Series and Data Types:")
                        for record in data:
                            st.write(f"GEO Series: {record['SeriesID']}, Data_Type = {record['DataTypes']}")
                    else:
                        st.warning("No results found for the entered GEO IDs.")

                # Display GEO Series Metadata Table
                st.write("GEO Series Metadata Table:")
                with engine.connect() as conn:
                    series_query = f"""
                        SELECT * FROM geo_series_metadata
                        WHERE "SeriesID" IN ({','.join([f"'{geo_id}'" for geo_id in geo_ids])});
                    """
                    result = conn.execute(text(series_query))
                    series_data = [dict(row) for row in result.mappings()]

                    if series_data:
                        df = pd.json_normalize(series_data, max_level=1)
                        st.dataframe(df)
                    else:
                        st.warning("No GEO Series Metadata found for the entered GEO IDs.")

                # Option to display GEO Sample Metadata
                if st.checkbox("Show GEO Sample Metadata"):
                    st.write("GEO Sample Metadata Table:")
                    with engine.connect() as conn:
                        sample_query = f"""
                            SELECT * FROM geo_sample_metadata
                            WHERE "SeriesID" IN ({','.join([f"'{geo_id}'" for geo_id in geo_ids])});
                        """
                        result = conn.execute(text(sample_query))
                        sample_data = [dict(row) for row in result.mappings()]

                        if sample_data:
                            sample_df = pd.json_normalize(sample_data, max_level=1)
                            st.dataframe(sample_df)
                        else:
                            st.warning("No GEO Sample Metadata found for the entered GEO IDs.")
            except Exception as e:
                st.error(f"An error occurred: {e}")

# SQL Query Interface
elif app_mode == "SQL Query Interface":
    st.header("SQL Query Interface")

    sql_query = st.text_area("Enter your SQL query:", placeholder="SELECT * FROM geo_series_metadata LIMIT 10;")
    visualize = st.checkbox("Visualize results (table and charts)")

    if st.button("Run Query"):
        if sql_query.strip():
            try:
                with engine.connect() as conn:
                    result = conn.execute(text(sql_query))
                    results = [dict(row) for row in result.mappings()]

                    if results:
                        st.write("Query Results:")
                        df = pd.DataFrame(results)
                        st.dataframe(df)

                        if visualize:
                            numeric_columns = df.select_dtypes(include=["number"]).columns
                            if numeric_columns.any():
                                plot_column = st.selectbox("Select a column to visualize:", numeric_columns)
                                st.bar_chart(df[plot_column])
                            else:
                                st.warning("No numeric columns available for visualization.")
                    else:
                        st.warning("Query executed successfully but returned no results.")
            except Exception as e:
                st.error(f"Error: {e}")
        else:
            st.warning("Please enter a SQL query.")

# Fetch PubMed Citations
elif app_mode == "Fetch PubMed Citations":
    st.header("Fetch PubMed Citations")

    if st.button("Fetch Citations"):
        try:
            with Session() as session:
                pubmed_ids = fetch_pubmed_ids(session)
                if not pubmed_ids:
                    st.warning("No PubMed IDs found in the database.")
                else:
                    citations = [{"PubMedID": pubmed_id, "Citation": fetch_citation(pubmed_id)} for pubmed_id in pubmed_ids]
                    st.write("Citations:")
                    st.dataframe(pd.DataFrame(citations))
        except Exception as e:
            st.error(f"Error: {e}")

# Download GEO Data by Type
elif app_mode == "Download GEO Data by Type":
    st.header("Download GEO Data by Type")

    data_type = st.text_input("Enter Data Type (e.g., ATAC-Seq):", placeholder="ATAC-Seq")

    if st.button("Download Data"):
        if not data_type.strip():
            st.error("Please enter a valid data type.")
        else:
            try:
                downloader = GeoDataDownloader()
                downloader.run(data_type=data_type)
                st.success(f"Data download for {data_type} completed successfully!")
            except Exception as e:
                st.error(f"Error: {e}")

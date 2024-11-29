import cptac
import pandas as pd


def extract_metadata_for_cancer(cancer_name: str):
    """
    Extract metadata for the given cancer dataset.

    Args:
        cancer_name (str): The name of the cancer dataset (e.g., 'Hnscc', 'Brca').

    Returns:
        pd.DataFrame: A DataFrame containing metadata about available data types and sources.
    """
    print(f"Loading {cancer_name} dataset...")
    try:
        # Dynamically load the cancer dataset using its name
        cancer_data = getattr(cptac, cancer_name)()
        print(f"{cancer_name} dataset loaded successfully!")
    except Exception as e:
        print(f"Error loading {cancer_name} dataset: {e}")
        return pd.DataFrame()

    # Get available data types and sources
    print("\nListing available data types and sources:")
    try:
        data_sources = cancer_data.list_data_sources()
        print(data_sources)
    except Exception as e:
        print(f"Error listing data sources: {e}")
        return pd.DataFrame()

    # Process each data type and its sources
    metadata_entries = []
    for _, row in data_sources.iterrows():
        data_type = row["Data type"]
        available_sources = row["Available sources"]

        # Ensure sources are iterable
        if not isinstance(available_sources, list):
            print(f"Skipping invalid sources for data type '{data_type}': {available_sources}")
            continue

        for source in available_sources:
            print(f"\nProcessing Data Type: '{data_type}', Source: '{source}'")
            try:
                # Fetch the dataframe
                df = cancer_data.get_dataframe(data_type, source)
                # Extract metadata
                entry = {
                    "Data Type": data_type,
                    "Source": source,
                    "Number of Samples": len(df),
                    "Number of Features": len(df.columns),
                    "Sample IDs (First 5)": df.index[:5].tolist(),
                    "Feature Names (First 5)": df.columns[:5].tolist(),
                }
                metadata_entries.append(entry)
                print(f"Metadata collected for {data_type} from {source}: {entry}")
            except Exception as e:
                print(f"Error processing '{data_type}' from '{source}': {e}")

    # Return as a DataFrame
    metadata_df = pd.DataFrame(metadata_entries)
    return metadata_df


def save_metadata(metadata_df: pd.DataFrame, output_file: str):
    """
    Save metadata to a CSV file.

    Args:
        metadata_df (pd.DataFrame): The DataFrame containing metadata.
        output_file (str): Path to the output CSV file.
    """
    if metadata_df.empty:
        print("No metadata to save.")
        return

    try:
        metadata_df.to_csv(output_file, index=False)
        print(f"Metadata saved to {output_file}")
    except Exception as e:
        print(f"Error saving metadata to {output_file}: {e}")


if __name__ == "__main__":
    # Specify the cancer dataset
    cancer_dataset_name = "Hnscc"

    # Extract metadata
    metadata = extract_metadata_for_cancer(cancer_dataset_name)

    # Save metadata to a CSV file
    save_metadata(metadata, f"{cancer_dataset_name.lower()}_metadata.csv")

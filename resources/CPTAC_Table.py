import cptac
import pandas as pd


# Load phosphoproteomics data
hn = cptac.Hnscc()
df = hn.get_transcriptomics("broad")
print(df.head())

# Step 1: Reset index and flatten MultiIndex columns
print("Index before reset_index:")
print(df.index)

df.reset_index(inplace=True)  # Reset index to make Patient_ID explicit

if isinstance(df.columns, pd.MultiIndex):
    # Flatten MultiIndex columns
    df.columns = [
        "|".join(filter(None, map(str, col))).strip()  # Join levels with "|"
        for col in df.columns
    ]

print("Flattened columns:")
print(df.columns)

# Step 2: Extract metadata rows
metadata = df.iloc[:4, :]  # Extract first 4 rows as metadata
metadata = metadata.T.reset_index()  # Transpose and reset index
metadata.columns = ["feature", "Name", "Site", "Peptide", "Database_ID"]  # Rename columns

print("Metadata (transposed) structure:")
print(metadata.head())

# Step 3: Extract quantification data
quantification_data = df.iloc[4:, :]  # Extract rows with actual data
quantification_data.reset_index(drop=True, inplace=True)  # Ensure clean indices
print("Quantification data structure:")
print(quantification_data.head())

# Step 4: Melt the quantification data
try:
    melted_df = quantification_data.melt(
        id_vars=["Patient_ID"],  # Ensure this matches the column name
        var_name="feature",
        value_name="quantification"
    )
    print("Melted DataFrame structure:")
    print(melted_df.head())
except KeyError as e:
    print(f"Error during melting: {e}")
    print("Available columns in quantification_data:", quantification_data.columns)

# Step 5: Merge melted data with metadata
final_df = melted_df.merge(metadata, on="feature", how="left")

# Step 6: Validate merged DataFrame
missing_metadata = final_df[final_df["Name"].isna()]
if not missing_metadata.empty:
    print("Features missing metadata:")
    print(missing_metadata["feature"].unique())

# Drop rows with missing metadata or quantification
final_df.dropna(subset=["Name", "quantification"], inplace=True)

# Validate results
print("Final DataFrame structure (after cleaning):")
print(final_df.head())

# Logging
print(f"Final DataFrame shape: {final_df.shape}")


# Extract metadata rows (first 4 rows are metadata, adjust indexing as needed)
metadata = df.iloc[:4, :].copy()  # Extract the first 4 rows
metadata = metadata.transpose().reset_index()  # Transpose and reset index
metadata.columns = ["feature", "Name", "Site", "Peptide", "Database_ID"]  # Rename columns

# Validate metadata
print("Metadata structure (after transpose and column renaming):")
print(metadata.head())


# Reset index for quantification data to include 'Patient_ID'
quantification_data = df.iloc[4:, :].copy()  # Skip metadata rows
quantification_data.reset_index(inplace=True)  # Reset index to include 'Patient_ID'

# Validate quantification data
print("Quantification data structure (after resetting index):")
print(quantification_data.head())

# Melt quantification data
melted_df = quantification_data.melt(
    id_vars=["Patient_ID"],  # Ensure 'Patient_ID' is present
    var_name="feature",
    value_name="quantification"
)

# Validate melted DataFrame
print("Melted DataFrame structure:")
print(melted_df.head())

# Merge melted data with metadata
final_df = melted_df.merge(metadata, on="feature", how="left")

# Drop rows with missing metadata or quantification
final_df.dropna(subset=["Name", "quantification"], inplace=True)

# Validate final DataFrame
print("Final DataFrame structure:")
print(final_df.head())

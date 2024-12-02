import cptac
import pandas as pd

# Load the Hnscc dataset
hnscc = cptac.Hnscc()

# Fetch the proteomics dataframe for the 'bcm' source
try:
    proteomics_df = hnscc.get_proteomics("bcm")
    print("Proteomics data successfully loaded.")
except Exception as e:
    print(f"Error fetching proteomics data: {e}")
    exit()

# Display basic information about the dataframe
print(f"Dataframe Shape: {proteomics_df.shape}")
print(f"Dataframe Head:\n{proteomics_df.head()}")

# Count NaN and non-NaN values
na_counts = proteomics_df.iloc[:1].isna().sum().sum()
non_na_counts = proteomics_df.iloc[:1].notna().sum().sum()

# Output the counts
print(f"Total NaN values: {na_counts}")
print(f"Total non-NaN values: {non_na_counts}")

# Save the dataframe for review if needed
proteomics_df.to_csv("Hnscc_bcm_proteomics_debug.csv", index=True)
print("Proteomics dataframe saved to 'Hnscc_bcm_proteomics_debug.csv'.")

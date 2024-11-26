# Generalized ChIP-Seq Processing Script with Relative Paths

# Load necessary libraries
library(GEOquery)    # For downloading metadata
library(data.table)  # For parsing and processing .narrowPeak files
library(tools)       # For file manipulation
library(tidyverse)   # For general data wrangling

# Function: Process a ChIP-Seq Dataset
process_chip_seq <- function(geo_id) {
  # Set current working directory
  base_dir <- "C:/Users/ajboo/BookAbraham/BiologicalDatabases/HNSC_Omics_Database"
  setwd(base_dir)
  cat("Current working directory set to:", base_dir, "\n")
  
  # Define dataset-specific directories
  raw_data_dir <- file.path(base_dir, "resources", "data", "raw", "GEO", geo_id)
  metadata_dir <- file.path(base_dir, "resources", "data", "metadata", geo_id)
  processed_data_dir <- file.path(base_dir, "resources", "data", "processed","ChIP_Seq", geo_id)
  
  # Ensure directories exist
  if (!dir.exists(raw_data_dir)) dir.create(raw_data_dir, recursive = TRUE)
  if (!dir.exists(metadata_dir)) dir.create(metadata_dir, recursive = TRUE)
  if (!dir.exists(processed_data_dir)) dir.create(processed_data_dir, recursive = TRUE)
  
  # Step 1: Download Metadata
  cat("Downloading metadata for", geo_id, "...\n")
  gse_chip <- tryCatch(
    getGEO(geo_id, GSEMatrix = TRUE),
    error = function(e) {
      cat("Error downloading metadata:", e$message, "\n")
      stop("Cannot proceed without metadata.")
    }
  )
  
  # Extract metadata
  chip_metadata <- tryCatch(
    pData(phenoData(gse_chip[[1]])),
    error = function(e) {
      cat("Error extracting metadata:", e$message, "\n")
      stop("Cannot extract metadata.")
    }
  )
  
  # Save metadata to a CSV
  metadata_csv <- file.path(metadata_dir, paste0(geo_id, "_metadata.csv"))
  write.csv(chip_metadata, file = metadata_csv, row.names = FALSE)
  cat("Metadata saved to:", metadata_csv, "\n")
  
  # Step 2: Download Supplementary Files
  cat("Downloading supplementary files...\n")
  supplementary_links <- chip_metadata$supplementary_file_1
  supplementary_links <- supplementary_links[!is.na(supplementary_links) & supplementary_links != "NONE"]
  
  for (i in seq_along(supplementary_links)) {
    file_url <- supplementary_links[i]
    file_name <- file.path(raw_data_dir, basename(file_url))
    
    if (!file.exists(file_name)) {
      tryCatch({
        download.file(file_url, destfile = file_name, mode = "wb")
        cat("Downloaded:", file_name, "\n")
      }, error = function(e) {
        cat("Error downloading file:", file_url, "\nError message:", e$message, "\n")
      })
    } else {
      cat("File already exists:", file_name, "\n")
    }
  }
  
  cat("All supplementary files are downloaded or already exist.\n")
  
  # Step 3: Process .narrowPeak Files
  cat("Processing .narrowPeak files...\n")
  narrowPeak_files <- list.files(raw_data_dir, full.names = TRUE, pattern = "*.narrowPeak.gz")
  narrowPeak_columns <- c("chr", "start", "end", "name", "score", "strand", "signalValue", "pValue", "qValue", "peak")
  
  for (file in narrowPeak_files) {
    tryCatch({
      # Load the file
      peak_data <- fread(file, header = FALSE)
      colnames(peak_data) <- narrowPeak_columns  # Assign column names
      
      # Save the processed data to the processed directory
      output_file <- file.path(processed_data_dir, paste0(file_path_sans_ext(basename(file)), "_parsed.csv"))
      write.csv(peak_data, file = output_file, row.names = FALSE)
      
      cat("Processed and saved:", output_file, "\n")
    }, error = function(e) {
      cat("Error processing file:", file, "\nError message:", e$message, "\n")
    })
  }
  
  cat("All .narrowPeak files have been processed for", geo_id, ".\n")
}

# Example Usage: Process GSE112021
geo_id <- "GSE112021"  # Specific to your current ChIP-Seq dataset
process_chip_seq(geo_id)

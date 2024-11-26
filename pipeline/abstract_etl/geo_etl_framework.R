# Load necessary libraries
library(data.table)
library(tools)
library(GEOquery)

# Function 1: Set Up Directories
setup_directories <- function(base_dir, geo_id, data_type) {
  raw_data_dir <- file.path(base_dir, "resources", "data", "raw", "GEO", geo_id)
  extracted_dir <- file.path(raw_data_dir, "extracted")
  metadata_dir <- file.path(base_dir, "resources", "data", "metadata", geo_id)
  processed_data_dir <- file.path(base_dir, "resources", "data", "processed", data_type, geo_id)
  
  # Ensure directories exist
  if (!dir.exists(raw_data_dir)) dir.create(raw_data_dir, recursive = TRUE)
  if (!dir.exists(extracted_dir)) dir.create(extracted_dir, recursive = TRUE)
  if (!dir.exists(metadata_dir)) dir.create(metadata_dir, recursive = TRUE)
  if (!dir.exists(processed_data_dir)) dir.create(processed_data_dir, recursive = TRUE)
  
  return(list(raw = raw_data_dir, extracted = extracted_dir, 
              metadata = metadata_dir, processed = processed_data_dir))
}

# Function 2: Download or Verify Metadata
download_metadata <- function(geo_id, metadata_dir) {
  metadata_file <- file.path(metadata_dir, paste0(geo_id, "_metadata.csv"))
  
  if (!file.exists(metadata_file)) {
    cat("Downloading metadata for", geo_id, "...\n")
    tryCatch({
      gse <- getGEO(geo_id, GSEMatrix = TRUE)
      metadata <- pData(phenoData(gse[[1]]))
      write.csv(metadata, metadata_file, row.names = FALSE)
      cat("Metadata saved to:", metadata_file, "\n")
    }, error = function(e) {
      stop("Error retrieving metadata for ", geo_id, ": ", e$message)
    })
  } else {
    cat("Metadata file already exists:", metadata_file, "\n")
  }
}

# Function 3: Handle Supplementary Files
handle_files <- function(geo_id, raw_data_dir) {
  tar_file <- file.path(raw_data_dir, paste0(geo_id, "_RAW.tar"))
  
  # If a tar file exists, process it
  if (file.exists(tar_file)) {
    cat("Tar file found for", geo_id, ". Preparing to extract...\n")
    return(tar_file)  # Return the tar file for extraction
  }
  
  # Otherwise, look for .gz files
  gz_files <- list.files(raw_data_dir, pattern = "\\.gz$", full.names = TRUE)
  if (length(gz_files) > 0) {
    cat("Compressed (.gz) files found. Skipping tar extraction.\n")
    return(NULL)  # Signal that no tar extraction is needed
  }
  
  # Try downloading supplementary files if neither exists
  cat("No files found for", geo_id, ". Attempting to download supplementary files...\n")
  tryCatch({
    gse <- getGEO(geo_id, GSEMatrix = FALSE)
    meta <- Meta(gse)
    supplementary_links <- meta$supplementary_file
    supplementary_links <- supplementary_links[!is.na(supplementary_links) & supplementary_links != "NONE"]
    
    if (length(supplementary_links) == 0) {
      stop("No supplementary files found in metadata.")
    }
    
    for (file_url in supplementary_links) {
      file_name <- file.path(raw_data_dir, basename(file_url))
      if (!file.exists(file_name)) {
        download.file(file_url, destfile = file_name, mode = "wb")
        cat("Downloaded:", file_name, "\n")
      } else {
        cat("File already exists:", file_name, "\n")
      }
    }
  }, error = function(e) {
    stop("Error retrieving or downloading supplementary files for ", geo_id, ": ", e$message)
  })
  
  return(NULL)
}

# Function 4: Extract Tar File
extract_tar_file <- function(tar_file, extracted_dir) {
  if (is.null(tar_file)) {
    cat("No tar file to extract.\n")
    return()
  }
  
  cat("Extracting tar file:", tar_file, "to", extracted_dir, "\n")
  tryCatch({
    untar(tar_file, exdir = extracted_dir)
    cat("Extraction complete.\n")
  }, error = function(e) {
    stop("Error extracting tar file: ", e$message)
  })
}

# Function 5: Inspect Files
inspect_files <- function(extracted_dir, raw_data_dir, log_file) {
  files <- c(
    list.files(extracted_dir, full.names = TRUE),
    list.files(raw_data_dir, pattern = "\\.gz$", full.names = TRUE)
  )
  
  if (length(files) == 0) {
    stop("No files found for inspection in extracted or raw directories.")
  }
  
  log_con <- file(log_file, open = "wt")
  writeLines("File Inspection Log\n====================\n", log_con)
  
  for (file in files) {
    tryCatch({
      writeLines(paste0("\nInspecting file: ", basename(file), "\n"), log_con)
      data <- fread(file, nrows = 10)
      dimensions <- paste("Dimensions:", paste(dim(data), collapse = " x "))
      writeLines(dimensions, log_con)
      writeLines("\nPreview:\n", log_con)
      writeLines(capture.output(print(head(data))), log_con)
      writeLines("\nStructure:\n", log_con)
      writeLines(capture.output(str(data)), log_con)
      writeLines("\n-------------------------------------------------------------\n", log_con)
    }, error = function(e) {
      writeLines(paste("Error inspecting file:", file, "\nError message:", e$message, "\n"), log_con)
    })
  }
  
  close(log_con)
  cat("Inspection log saved to:", log_file, "\n")
}

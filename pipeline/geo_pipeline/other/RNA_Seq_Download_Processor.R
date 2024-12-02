library(GEOquery)
library(data.table)


setwd("C:/Users/ajboo/BookAbraham/BiologicalDatabases/HNSC_Omics_Database")

# Load the abstract ETL framework
source("pipeline/abstract_etl/geo_etl_framework.R")

# Function: Inspect RNA-Seq Data
inspect_rna_seq <- function(geo_id) {
  base_dir <- "C:/Users/ajboo/BookAbraham/BiologicalDatabases/HNSC_Omics_Database"
  setwd(base_dir)
  cat("Current working directory set to:", base_dir, "\n")
  
  # Step 1: Set up directories
  dirs <- setup_directories(base_dir, geo_id, data_type = "RNA-Seq")
  
  # Step 2: Download metadata and inspect files
  download_metadata(geo_id, dirs$metadata)
  tar_file <- handle_files(geo_id, dirs$raw)
  extract_tar_file(tar_file, dirs$extracted)
  
  # Step 3: Log file inspection
  log_file <- file.path(dirs$processed, paste0(geo_id, "_file_inspection_log.txt"))
  inspect_files(dirs$extracted, dirs$raw, log_file)
}

# Example Usage: Inspect RNA-Seq GEO IDs
geo_ids <- c("GSE112026", "GSE114446", "GSE202036")  # List of RNA-Seq GEO IDs
for (geo_id in geo_ids) {
  cat("\nInspecting RNA-Seq GEO ID:", geo_id, "\n")
  tryCatch({
    inspect_rna_seq(geo_id)
  }, error = function(e) {
    cat("Error inspecting RNA-Seq GEO ID:", geo_id, "\nError message:", e$message, "\n")
  })
}


#_______________________________________________________________________________
# Phase 2
#____________________________________________________________________________

# Load Required Libraries
library(data.table)
library(DESeq2)

# Function: Process RNA-Seq Data for GSE114446
process_rna_seq_deseq2 <- function(geo_id) {
  base_dir <- "C:/Users/ajboo/BookAbraham/BiologicalDatabases/HNSC_Omics_Database"
  setwd(base_dir)
  cat("Current working directory set to:", base_dir, "\n")
  
  # Step 1: Set up directories
  dirs <- setup_directories(base_dir, geo_id, data_type = "RNA-Seq")
  
  # Step 2: Define paths for raw and processed data
  raw_data_path <- "C:/Users/ajboo/BookAbraham/BiologicalDatabases/HNSC_Omics_Database/resources/data/raw/GEO/GSE114446/GSE114446_STCCountsCG.txt.gz"
  if (!file.exists(raw_data_path)) {
    stop("Raw counts file not found:", raw_data_path)
  }
  
  processed_data_path <- file.path(dirs$processed, paste0(geo_id, "_processed_counts.csv"))
  
  # Step 3: Load Raw Counts
  cat("Loading raw counts file:", raw_data_path, "\n")
  counts_data <- fread(raw_data_path, header = TRUE)
  
  # Validate and process data
  if (!"gene" %in% colnames(counts_data)) {
    stop("Raw counts file is missing required 'gene' column.")
  }
  cat("Preview of raw counts data:\n")
  print(head(counts_data, 5))
  
  sample_columns <- colnames(counts_data)[-1]
  if (length(sample_columns) == 0) {
    stop("Raw counts file has no sample columns.")
  }
  
  # Convert to DESeq2 format
  count_matrix <- as.matrix(counts_data[, ..sample_columns])
  rownames(count_matrix) <- counts_data$gene
  
  # Ensure counts are integers
  if (!all(count_matrix == round(count_matrix))) {
    cat("Warning: Non-integer values detected in count data. Rounding to integers...\n")
    count_matrix <- round(count_matrix)
  }
  
  # Step 4: Count and Filter Lowly Expressed Genes
  total_genes <- nrow(count_matrix)
  cat("Total number of genes before filtering:", total_genes, "\n")
  
  cat("Filtering lowly expressed genes...\n")
  keep <- rowSums(count_matrix >= 10) >= 2  # Retain genes with at least 10 counts in 2 samples
  filtered_counts <- count_matrix[keep, ]
  
  filtered_out_genes <- total_genes - nrow(filtered_counts)
  cat("Number of genes filtered out:", filtered_out_genes, "\n")
  cat("Number of genes after filtering:", nrow(filtered_counts), "\n")
  
  # Step 5: Normalize Data Using DESeq2
  cat("Normalizing data using DESeq2...\n")
  coldata <- data.frame(row.names = colnames(filtered_counts), condition = rep("condition", ncol(filtered_counts)))
  dds <- DESeqDataSetFromMatrix(countData = filtered_counts, colData = coldata, design = ~1)
  
  # Perform DESeq2 normalization
  dds <- estimateSizeFactors(dds)
  normalized_counts <- counts(dds, normalized = TRUE)
  
  # Apply Variance Stabilizing Transformation (VST)
  vst_data <- vst(dds, blind = TRUE)
  vst_matrix <- assay(vst_data)
  
  # Step 6: Save Processed Data
  cat("Saving processed counts data...\n")
  processed_data <- data.table(gene = rownames(vst_matrix), vst_matrix)
  fwrite(processed_data, file = processed_data_path)
  cat("Processed data saved to:", processed_data_path, "\n")
}

# Example Usage: Process RNA-Seq GEO ID GSE114446
tryCatch({
  process_rna_seq_deseq2("GSE114446")
}, error = function(e) {
  cat("Error processing RNA-Seq GEO ID GSE114446\nError message:", e$message, "\n")
})

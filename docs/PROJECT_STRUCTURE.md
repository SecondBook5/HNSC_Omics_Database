# HNSC Omics Database Project Structure and Workflow Documentation

## Table of Contents

1. [Introduction](#introduction)
2. [Directory Structure Overview](#directory-structure-overview)
3. [Detailed Directory Description](#detailed-directory-description)
    - [Root Level Files](#root-level-files)
    - [`alembic/`](#alembic)
    - [`config/`](#config)
    - [`db/`](#db)
    - [`docs/`](#docs)
    - [`logs/`](#logs)
    - [`notebooks/`](#notebooks)
    - [`pipeline/`](#pipeline)
    - [`resources/`](#resources)
    - [`scripts/`](#scripts)
    - [`setup/`](#setup)
    - [`tests/`](#tests)
    - [`utils/`](#utils)
4. [Workflow and Component Interaction](#workflow-and-component-interaction)
    - [Pipeline Workflows](#pipeline-workflows)
    - [Database Integration](#database-integration)
    - [Utilities and Data Structures](#utilities-and-data-structures)
    - [Scripts and Auxiliary Tasks](#scripts-and-auxiliary-tasks)
5. [Workflow Diagrams](#workflow-diagrams)
6. [Snakemake Workflow Management](#snakemake-workflow-management)
7. [Usage Guidelines](#usage-guidelines)
8. [Future Expansion](#future-expansion)
9. [Conclusion](#conclusion)

---

## Introduction

The HNSC Omics Database project aims to build a comprehensive database integrating various omics data sources, such as GEO, TCGA, and CPTAC. To manage the complexity and scale of this project, a well-organized directory structure and clear documentation are essential. This document outlines the directory structure, describes the purpose of each component, and explains how the different parts of the project work together.

## Directory Structure Overview

Below is the high-level directory structure of the project:

```plaintext
HNSC_Omics_Database/
├── README.md
├── requirements.txt
├── alembic/
├── config/
├── db/
├── docs/
├── logs/
├── notebooks/
├── pipeline/
├── resources/
├── scripts/
├── setup/
├── tests/
└── utils/
```

## Detailed Directory Description

### Root Level Files

- **`README.md`**: Provides an overview of the project, setup instructions, and general information.
- **`requirements.txt`**: Lists all Python dependencies required to run the project.

### `alembic/`

- **Purpose**: Manages database schema migrations using Alembic.
- **Components**:
  - **`alembic.ini`**: Configuration file for Alembic.
  - **`versions/`**: Contains migration scripts that track changes to the database schema.

### `config/`

- **Purpose**: Stores configuration files and settings for the project.
- **Components**:
  - **`.env`**: Environment variables for sensitive information (e.g., database credentials).
  - **`db_config.py`**: Central database configuration, including connection details.
  - **`logrotate_hnsc_omics.conf`**: Configuration for log rotation to manage log file sizes.

### `db/`

- **Purpose**: Contains all database-related code, including models, ETL scripts, queries, and utilities.
- **Components**:

#### `db/config/`

- **Purpose**: Database-specific configuration files.
- **Files**:
  - **`db_config.py`**: Main database configurations.
  - **`postgres_config.py`**: PostgreSQL-specific settings.
  - **`mongo_config.py`**: MongoDB-specific settings.
  - **`sqlalchemy_settings.py`**: SQLAlchemy ORM configurations.

#### `db/etl/`

- **Purpose**: ETL scripts for data extraction, transformation, and loading.
- **Files**:
  - **`geo_etl.py`**: ETL functions for GEO data.
  - **`tcga_etl.py`**: ETL functions for TCGA data.
  - **`cptac_etl.py`**: ETL functions for CPTAC data.
  - **`data_cleaning.py`**: Shared data cleaning utilities.

#### `db/integration/`

- **Purpose**: Scripts for data integration and harmonization across data sources.
- **Files**:
  - **`integration_config.py`**: Integration settings and strategies.
  - **`harmonization.py`**: Functions for data field alignment and normalization.
  - **`mapping.py`**: Utilities for cross-referencing IDs and entities.
  - **`index_optimization.py`**: Indexing strategies for efficient queries.

#### `db/models/`

- **Purpose**: ORM models defining the database schema.
- **Files**:
  - **`base.py`**: Base SQLAlchemy model that others inherit from.
  - **`geo_models.py`**: Models specific to GEO data.
  - **`tcga_models.py`**: Models specific to TCGA data.
  - **`cptac_models.py`**: Models specific to CPTAC data.
  - **`clinical_models.py`**: Models for clinical data shared across sources.
  - **`integration_models.py`**: Models for integrated data.

#### `db/migration/`

- **Purpose**: Manages database schema migrations.
- **Components**:
  - **`alembic/`**: Contains migration scripts.
  - **`alembic.ini`**: Configuration file for Alembic.
  - **`migrate.py`**: Script to automate migrations.

#### `db/queries/`

- **Purpose**: Stores SQL and ORM query functions.
- **Files**:
  - **`geo_queries.py`**: GEO-specific queries.
  - **`tcga_queries.py`**: TCGA-specific queries.
  - **`cptac_queries.py`**: CPTAC-specific queries.
  - **`integration_queries.py`**: Queries for integrated data.
  - **`common_queries.py`**: Common queries across data sources.

#### `db/utils/`

- **Purpose**: Utility functions for database operations.
- **Files**:
  - **`db_utils.py`**: General database utilities.
  - **`mongo_db_integration.py`**: MongoDB-specific utilities.
  - **`postgres_integration.py`**: PostgreSQL-specific utilities.
  - **`schema_helpers.py`**: Helpers for schema management.
  - **`connection_manager.py`**: Manages database connections.
  - **`transaction_manager.py`**: Manages database transactions.

### `docs/`

- **Purpose**: Contains project documentation and diagrams.
- **Components**:
  - **`diagrams/`**: Visual representations, such as ER diagrams.
  - **`metadata_extraction.md`**: Documentation for metadata extraction processes.
  - **`PROJECT_STRUCTURE.md`**: (This document) Detailed project structure and workflow documentation.

### `logs/`

- **Purpose**: Stores log files generated by the pipelines and database operations.
- **Files**:
  - **`setup_mongodb.log`**: Log for MongoDB setup.
  - **`setup_postgresql.log`**: Log for PostgreSQL setup.
  - **`<pipeline_logs>.log`**: Logs generated by various pipelines.

### `notebooks/`

- **Purpose**: Contains Jupyter notebooks for data analysis and exploration.
- **Files**:
  - **`ATACSeq_analysis.ipynb`**: Example analysis notebook.
  - **`<additional_notebooks>.ipynb`**: Other analysis notebooks.

### `pipeline/`

- **Purpose**: Houses the main data processing pipelines for each data source and the integration process.
- **Components**:

#### `pipeline/geo_pipeline/`

- **Files**:
  - **`geo_metadata_pipeline.py`**: Main orchestrator for GEO data processing.
  - **`tag_validation.py`**: Validates GEO XML tags.
  - **`metadata_parsing.py`**: Parses GEO XML data.
  - **`metadata_insertion.py`**: Inserts GEO data into the database.
  - **`geo_tag_template.json`**: JSON template for GEO XML tags.

#### `pipeline/tcga_pipeline/`

- **Files**:
  - **`tcga_metadata_pipeline.py`**: Main orchestrator for TCGA data processing.
  - **`metadata_parsing.py`**: Parses TCGA data.
  - **`metadata_insertion.py`**: Inserts TCGA data into the database.
  - **`tcga_tag_template.json`**: JSON template for TCGA data.

#### `pipeline/cptac_pipeline/`

- **Files**:
  - **`cptac_metadata_pipeline.py`**: Main orchestrator for CPTAC data processing.
  - **`metadata_parsing.py`**: Parses CPTAC data.
  - **`metadata_insertion.py`**: Inserts CPTAC data into the database.
  - **`cptac_tag_template.json`**: JSON template for CPTAC data.

#### `pipeline/integration/`

- **Files**:
  - **`data_integration_pipeline.py`**: Orchestrator for integrating data from all sources.
  - **`integration_helpers.py`**: Helper functions for data harmonization.

### `resources/`

- **Purpose**: Stores external data, templates, and resource files.
- **Components**:

#### `resources/data/`

- **Purpose**: Organized storage of data files by source.
- **Subdirectories**:
  - **`metadata/`**: Metadata files for each data source.
    - **`cbioportal_metadata/`**: Placeholder for future data.
    - **`firehose_metadata/`**: Placeholder for future data.
    - **`geo_metadata/`**: GEO metadata files.
    - **`tcga_metadata/`**: TCGA metadata files.
    - **`cptac_metadata/`**: CPTAC metadata files.
  - **`raw/`**: Raw data files organized by source.
    - **`CPTAC/`**, **`GEO/`**, **`TCGA/`**

#### `resources/geo_ids.txt`

- **Purpose**: A text file containing GEO IDs to process.

#### `resources/results/`

- **Purpose**: Stores processed results organized by data type.

### `scripts/`

- **Purpose**: Contains auxiliary scripts for tasks not part of the main pipelines.
- **Components**:

#### `scripts/etl/`

- **Purpose**: ETL scripts for ad-hoc data extraction and preprocessing.

#### `scripts/auxiliary_scripts.py`

- **Purpose**: Miscellaneous helper scripts for various tasks.

### `setup/`

- **Purpose**: Contains setup scripts for initializing the environment and databases.
- **Files**:
  - **`setup_mongodb.sh`**: Script to set up MongoDB.
  - **`setup_postgresql.sh`**: Script to set up PostgreSQL.

### `tests/`

- **Purpose**: Contains unit tests for pipelines and components.
- **Subdirectories**:
  - **`geo_pipeline_tests/`**: Tests for GEO pipeline.
  - **`tcga_pipeline_tests/`**: Tests for TCGA pipeline.
  - **`cptac_pipeline_tests/`**: Tests for CPTAC pipeline.
  - **`integration_tests/`**: Tests for data integration.

### `utils/`

- **Purpose**: Contains shared utility functions and data structures.
- **Components**:

#### `utils/`

- **Files**:
  - **`connection_checker.py`**: Utility to check database connections.
  - **`parallel_processing.py`**: Utilities for parallel execution.
  - **`xml_tree_parser.py`**: Helper functions for parsing XML trees.

#### `utils/data_structures/`

- **Purpose**: Custom data structures used across the project.
- **Files**:
  - **`__init__.py`**: Initialization file for module imports.
  - **`graph.py`**: Graph data structure implementation.
  - **`tree.py`**: Tree data structure implementation.
  - **`hashmap.py`**: HashMap data structure implementation.
  - **`xml_tree.py`**: XML tree data structure implementation.

## Workflow and Component Interaction

### Pipeline Workflows

Each data source has a dedicated pipeline that orchestrates the data processing workflow:

1. **Data Extraction**: Raw data is retrieved from the source and stored in `resources/data/raw/`.
2. **Tag Validation**: Using `tag_validation.py`, the pipeline validates that the XML tags or data fields match those expected, based on the tag templates (e.g., `geo_tag_template.json`).
3. **Data Parsing**: The raw data is parsed into a structured format using `metadata_parsing.py`.
4. **Data Transformation**: The parsed data is transformed as necessary, using ETL functions from `db/etl/`.
5. **Data Insertion**: Transformed data is inserted into the database using models from `db/models/` and insertion scripts (`metadata_insertion.py`).
6. **Integration (Optional)**: After insertion, data can be integrated across sources using the integration pipeline.

### Database Integration

- **Models and Schemas**: Defined in `db/models/`, ensuring consistent data structures.
- **ETL Processes**: Located in `db/etl/`, these are utilized by pipelines for data transformation.
- **Queries**: `db/queries/` provides reusable query functions for data retrieval and manipulation.
- **Integration Utilities**: `db/integration/` contains scripts for harmonizing data across different sources.

### Utilities and Data Structures

- **Shared Utilities**: `utils/` provides common functions like database connection checks and parallel processing tools.
- **Data Structures**: Custom structures in `utils/data_structures/` are used for efficient data handling within pipelines and scripts.

### Scripts and Auxiliary Tasks

- **Supplemental Scripts**: Located in `scripts/`, these perform tasks such as ad-hoc data extraction or maintenance not covered by the main pipelines.
- **ETL Scripts**: `scripts/etl/` contains scripts for data preprocessing that supports the main pipelines.

---

## Workflow and Component Interaction

Each data source has a dedicated pipeline that orchestrates the data processing workflow:

1. **Data Extraction**: Raw data is retrieved from the source and stored in `resources/data/raw/`.
2. **Tag Validation**: Using `tag_validation.py`, the pipeline validates that the XML tags or data fields match those expected, based on the tag templates (e.g., `geo_tag_template.json`).
3. **Data Parsing**: The raw data is parsed into a structured format using `metadata_parsing.py`.
4. **Data Transformation**: The parsed data is transformed as necessary, using ETL functions from `db/etl/`.
5. **Data Insertion**: Transformed data is inserted into the database using models from `db/models/` and insertion scripts (`metadata_insertion.py`).
6. **Integration (Optional)**: After insertion, data can be integrated across sources using the integration pipeline.

---

## Workflow Diagrams

To visualize the workflow and interactions across different components, refer to the diagrams below.

### Pipeline Workflow Diagram

The pipeline workflow diagram illustrates the end-to-end process for each data source, covering extraction, validation, parsing, transformation, insertion, and integration. 

- **Location**: `docs/diagrams/pipeline_workflow_diagram.png`

![Pipeline Workflow Diagram](./diagrams/pipeline_workflow_diagram.png)

### Component Interaction Diagram

The component interaction diagram highlights how the primary directories—`pipeline/`, `db/`, `scripts/`, and `utils/`—interact, showing dependencies and data flow across the project structure.

- **Location**: `docs/diagrams/component_interaction_diagram.png`

![Component Interaction Diagram](./diagrams/component_interaction_diagram.png)

These diagrams are stored in `docs/diagrams/` for easy reference and can be updated as the project evolves.

---

## Snakemake Workflow Management

The project uses Snakemake for workflow management, enabling automation and efficient task sequencing across data sources.

- **`Snakefile` Location**: `pipeline/Snakefile`

### Description

The `Snakefile` in the `pipeline/` directory orchestrates the workflows for each data source (GEO, TCGA, CPTAC) as well as the integration process. It sequences tasks such as validation, parsing, transformation, and database insertion for each data source, making the data ingestion pipeline both automated and streamlined.

The `Snakefile` manages:
- **Dependency Tracking**: Ensures that tasks are run in the correct order.
- **Parallel Execution**: Allows for parallelization to increase efficiency.
- **Automated Error Handling**: Manages errors and logs during execution.

Snakemake’s workflow management enhances scalability and maintains organized, efficient data processing across all stages. 

---

## Usage Guidelines

1. **Setting Up the Environment**:
   - Install dependencies using `requirements.txt`.
   - Configure environment variables in `config/.env`.
   - Run setup scripts in `setup/` to initialize databases.

2. **Running Pipelines**:
   - Navigate to the appropriate pipeline directory (e.g., `pipeline/geo_pipeline/`).
   - Execute the main pipeline script (e.g., `python geo_metadata_pipeline.py`).

3. **Adding a New Data Source**:
   - Create a new pipeline directory under `pipeline/`.
   - Define ETL functions in `db/etl/` for the new source.
   - Create models in `db/models/`.
   - Update integration scripts if cross-source integration is needed.

4. **Testing**:
   - Run tests located in the `tests/` directory using a test runner like pytest.
   - Ensure new code is covered by appropriate unit tests.

5. **Data Management**:
   - Store raw data in `resources/data/raw/`.
   - Processed data and results should be stored in `resources/data/processed/` and `resources/results/`.

6. **Documentation**:
   - Update documentation in `docs/` when changes are made to the project structure or workflows.
   - Use clear and descriptive commit messages to aid in project tracking.


## Workflow Diagrams

- **Pipeline Workflow**: ![Pipeline Workflow Diagram](./diagrams/pipeline_workflow_diagram.png)
- **Component Interactions**: ![Component Interaction Diagram](./diagrams/component_interaction_diagram.png)


## Future Expansion

- **Adding New Data Sources**: The structure supports scalability. For new sources, replicate the existing pipeline and db module patterns.
- **Integration Enhancements**: As more data sources are added, the integration module can be expanded to handle more complex harmonization.
- **Performance Optimization**: Utilize the `utils/parallel_processing.py` for improving pipeline execution times.
- **Automation**: Consider implementing workflow management tools like Snakemake to automate and parallelize pipeline execution.

## Conclusion

This documentation serves as a guide to understanding the organization and interaction of components within the HNSC Omics Database project. By following the outlined structure and usage guidelines, contributors can effectively navigate, maintain, and expand the project, ensuring it remains scalable and maintainable as it grows.

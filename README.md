# HNSC Multi-Omics Database Project

## Project Overview

This repository is dedicated to the development of a **Head and Neck Squamous Cell Carcinoma (HNSC) Multi-Omics Database**. The project integrates multi-omics data, including RNA-Seq, ChIP-Seq, ATAC-Seq, and other datasets, to facilitate the analysis of tumor biology and therapeutic targets. The database supports personalized therapy development, such as Antibody-Drug Conjugates (ADCs), by providing researchers access to cross-omics data, helping identify biomarkers and gene-protein interactions.

## Key Objectives
- **Metadata Collection**: Acquire metadata from sources like GEO, TCGA, and CPTAC.
- **Schema Design**: Design a flexible schema for structured and semi-structured data.
- **Database Integration**: Use PostgreSQL and MongoDB for structured and semi-structured data management.
- **Future Expansion**: Prepare for machine learning models and advanced visualizations for systems biology insights.

## Current Status
We are currently in the **Data Integration Phase** with initial sources including datasets from GEO, TCGA, and CPTAC.

## Features
- Integration of multi-omics data.
- Data querying and visualization for gene expression, protein abundance, and mutations.
- Initial analysis pipelines for ChIP-Seq, RNA-Seq, and other data types.

---

## Getting Started

### Prerequisites

Ensure you have the following tools installed:
- **Python 3.x**
- **R**
- **Jupyter Notebook** or **RStudio**
- **PostgreSQL** and **MongoDB**

### Setting Up Environment Variables

1. **Create a `.env` file in the `config/` directory** with the following structure:
   ```plaintext
   # Shared database credentials
   DB_USER="omics_user"
   DB_PASSWORD="your_secure_password"  # Replace with a secure password

   # PostgreSQL configuration
   PG_DB_NAME="hnsc_omics_pg"
   PG_HOST="localhost"
   PG_PORT="5432"

   # MongoDB configuration
   MONGO_DB_NAME="hnsc_omics_mongo"
   MONGO_HOST="localhost"
   MONGO_PORT="27017"

   # Environment
   ENV="development"
   LOG_LEVEL="debug"
   ```

### Installing Dependencies

#### Python Dependencies
To install the required Python packages:
```bash
pip install -r requirements.txt
```

#### R Dependencies
For R dependencies, they will be managed within each relevant script.

---

## Database Setup

### PostgreSQL Setup

1. **Run the PostgreSQL setup script**:
   ```bash
   chmod +x setup/setup_postgresql.sh
   ./setup/setup_postgresql.sh
   ```
   - This script will:
     - Start the PostgreSQL service.
     - Create the `hnsc_omics_pg` database.
     - Create the `omics_user` user with specified password and privileges.
     - Grant necessary privileges to the user on the database.

2. **Verify the Database and User**:
   - Connect to the PostgreSQL database:
     ```bash
     psql -U omics_user -d hnsc_omics_pg -h localhost -W
     ```
   - Enter the password from your `.env` file when prompted.

### MongoDB Setup

1. **Run the MongoDB setup script**:
   ```bash
    chmod +x setup/setup_mongodb.sh
   ./setup/setup_mongodb.sh
   ```
   - This script will:
     - Connect to MongoDB and create the `hnsc_omics_mongo` database.
     - Create the `omics_user` user with specified password and privileges.

2. **Verify MongoDB Setup**:
   - Connect to MongoDB and confirm the `hnsc_omics_mongo` database and `omics_user` exist:
     ```bash
     mongosh -u omics_user -p your_secure_password --authenticationDatabase hnsc_omics_mongo
     show collections
     ```

---

## Directory Structure

The directory layout includes folders for scripts, schema, and data storage, structured as follows:

```plaintext
.
├── config/                     # Configuration files, including .env
├── data/                       # Raw datasets
├── metadata/                   # Metadata files from GEO, TCGA, and CPTAC
├── db/                         # Database schema and integration scripts
│   ├── postgresql_schema.sql   # SQL file for creating PostgreSQL tables
│   ├── mongo_integration.py    # MongoDB integration script
│   └── postgres_integration.py # PostgreSQL integration code
├── setup/                      # Setup scripts for databases
│   ├── setup_postgresql.sh     # Script to set up PostgreSQL database and user
│   └── setup_mongodb.sh        # Script to set up MongoDB database and user
├── scripts/                    # Data acquisition and analysis scripts
│   ├── python/                 # Python-specific scripts
│   └── r/                      # R-specific scripts
├── notebooks/                  # Jupyter Notebooks for exploratory data analysis
├── results/                    # Analysis results and processed data
├── logs/                       # Log files for database setup and other processes
├── .gitignore                  # Files to ignore in version control
└── README.md                   # Project README file
```

## Database Schema

The `postgresql_schema.sql` file in `db/` defines the following tables for structured data in PostgreSQL:

- **GENE**: Stores gene-related information (e.g., Gene ID, symbol, name, chromosomal position).
- **CLINICAL_METADATA**: Contains patient metadata, such as age, gender, survival time, and treatment.
- **SAMPLE**: Links sample-specific data to clinical metadata and provides information on tissue type and platform.
- **RNA_SEQ**: Holds RNA sequencing data, linked to both genes and samples.

Example table definitions can be found in `db/postgresql_schema.sql`.

---

## Testing

### Verifying Database Setup

1. **PostgreSQL**:
   - Log in to PostgreSQL as `omics_user` and confirm you can connect to `hnsc_omics_pg`.
   - List tables in the database:
     ```sql
     \dt
     ```
   - This will confirm the tables were created successfully.

2. **MongoDB**:
   - Connect to MongoDB as `omics_user` and confirm you can connect to `hnsc_omics_mongo`.
   - List collections to verify they were created:
     ```javascript
     show collections
     ```

---

## Contributing

If you'd like to contribute, please open a pull request or issue. Ensure your code follows best practices and is well-documented.

---

## License

This project is currently private. License details will be added upon release.

---

# Data Engineering School 2024 Final Work: Top-10 Most Similar Molecules

## Overview

This project sets up a data pipeline using Apache Airflow to process monthly CSV files containing compound structures, compute Tanimoto similarity scores, and handle the results. The pipeline is orchestrated using Airflow and deployed using Docker Compose.

## Project Goals

1. Ingest ChemBL data into a Data Warehouse (DWH).
2. Compute Morgan fingerprints for compound structures and store them in an S3 bucket.
3. Compute Tanimoto similarity scores between a set of target molecules and source molecules in the ChemBL database.
4. Store similarity results in an S3 bucket.
5. Identify top-10 most similar molecules for each target molecule.
6. Design a data mart and create specific database views.

## Project Structure

- **dags/**: Directory containing Airflow DAG files.
  - `monthly_similarity_processing_dag.py`: Airflow DAG for orchestrating the monthly similarity processing pipeline.
    
- **dags/ddl/**: Contains SQL scripts for setting up the Data Warehouse and staging schemas.
  - `dwh_ddl.sql`: SQL script for setting up the Data Warehouse schema.
  - `stg_ddl.sql`: SQL script for setting up the staging schema.
 
- **dags/scripts/**: Contains Python scripts for data ingestion, processing, and similarity calculations.
  - `aws.py`: Contains functions for interacting with AWS S3.
  - `chembl_data_ingestor.py`: Script for ingesting ChemBL data from the web service.
  - `config.py`: Configuration file with database and S3 settings.
  - `config.yaml`: Configuration settings in YAML format.
  - `db.py`: Contains functions for database interactions.
  - `exceptions.py`: Defines custom exceptions used in the project.
  - `main.py`: Main script to run the ChemBL data ingestion.
  - `models.py`: Defines the database models using SQLModel.
  - `morgan_fingerprint_calculator.py`: Functions to calculate Morgan fingerprints.
  - `morgan_fingerprint_processor.py`: Processor for handling Morgan fingerprint data.
  - `run_ingestor.py`: Script to run the ChemBL data ingestion process (manually).
  - `run_morgan_fingerprint.py`: Script to run the Morgan fingerprint processing (manually).
  - `run_tanimoto_similarity.py`: Script to run the Tanimoto similarity calculations (manually).
  - `tanimoto_similarity_calculator.py`: Functions to calculate Tanimoto similarity scores.
  - `tanimoto_similarity_processor.py`: Processor for handling Tanimoto similarity data.

## Prerequisites

- Docker and Docker Compose installed on your machine.
- An AWS account with access credentials.
- A configured S3 bucket with necessary files.
- PostgreSQL installed and running.

## Setup and Launch

### Step 1: Clone the Repository

Clone the repository to your local machine.

### Step 2: Set Up PostgreSQL Database

Ensure PostgreSQL is installed and running. Create the necessary databases and tables using the SQL scripts provided in the ddl directory:

```sh
psql -U postgres -f ddl/dwh_ddl.sql
psql -U postgres -f ddl/stg_ddl.sql
```

### Step 3: Configure Scripts

Ensure your AWS and database credentials are correctly configured in scripts/config.yaml.

### Step 4: Run Data Ingestion Script

Run the data ingestion script(_run_ingestor.py_) to fetch ChemBL data and insert it into the PostgreSQL database.

### Step 5: Run Morgan fingerprints calculations

Run the Morgan fingerprints script(_run_morgan_fingerprint.py_) to compute Morgan fingerprints for all compound structures.

### Step 6: Initialize Airflow

Run the following command to initialize the Airflow database:

```sh
docker-compose up airflow-init
```

### Step 7: Start Airflow Services

Start the Airflow services with Docker Compose:

```sh
docker-compose up -d
```

### Step 8:  Access Airflow UI

Open your browser and navigate to http://localhost:8080 to access the Airflow web interface.

### Step 9:  Setup AWS sonnection

Add a new connection "aws_default" with your AWS credentials (required for the S3KeySensor).

NB! The "tanimoto_similarity_processor.py" script currently uses credentials from config.yaml. This should be updated to take credentials from Airflow itself.

### Step 10:  Run the Pipeline

The pipeline will automatically run on the first day of each month. If any task fails, an email notification will be sent to the configured email address. Ensure that you provide all the needed credentials for that feature.

## Examples

#### Example of DAG Execution
_Description: Screenshot of Airflow successfully completing the DAG execution for the monthly similarity processing._

<img src="https://github.com/smanitas/final-project-de24/assets/101178295/33bbbab9-1def-4f2a-9afc-0897e72678ae" alt="airflow" width="500"/>

#### Examples of Fact table with molecule similarities and Dimension table with molecules and their properties

**Fact Table:**
<img src="https://github.com/smanitas/final-project-de24/assets/101178295/b4661668-67b9-4732-925b-9a8d170b63e1" alt="fact_table" width="500"/>

**Dimension Table:**
<img src="https://github.com/smanitas/final-project-de24/assets/101178295/e113a0a6-7062-4278-891b-382442d90dc9" alt="dim_table" width="500"/>

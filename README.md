# ETL Process for Employee Data

This project implements an ETL (Extract, Transform, Load) pipeline for managing employee data using Apache Airflow. The pipeline extracts data from a MySQL database, transforms it, and loads it into a PostgreSQL database. Below is a summary of the components and their functionalities.

## ETL workflow using airflow with mysql as source and postgresql as destination and transformation with python

<img src="https://github.com/Sendo-A/etl_workflow_using_airflow/blob/main/Photos/Etl_workflow.gif" alt="workflow" width="800">

## Project Structure

1. **Extract**
   - **Source:** MySQL database (`employees` schema)
   - **Tables Extracted:** `departments`, `dept_emp`, `dept_manager`, `employees`, `salaries`, `titles`
   - **Function:** `get_src_table()`
     - Connects to the MySQL database.
     - Extracts data from the specified tables.
     - Returns a dictionary with table names as keys and data as values.

2. **Load**
   - **Destination:** PostgreSQL database
   - **Function:** `load_src_data(tbl_dict)`
     - Connects to the PostgreSQL database.
     - Loads the extracted data into new tables with a `src_` prefix.
     - Provides feedback on the progress and completion of the data load.

3. **Transform**
   - **Tables Transformed:**
     - `departments`: Adds new department and renames 'Development' to 'Information Technology'.
     - `dept_emp`: Adds a new employee record.
     - `dept_manager`: Adds a new department manager record.
     - `employees`: Adds a new employee record.
     - `salaries`: Adds a new salary record.
     - `titles`: Adds a new title record.
   - **Functions:** 
     - `transform_src_departements()`
     - `transform_src_dept_emp()`
     - `transform_src_dept_manager()`
     - `transform_src_employees()`
     - `transform_src_salaries()`
     - `transform_src_titles()`
   - **Operation:** Each function connects to PostgreSQL, reads the data, applies transformations, and writes the updated data back.

4. **Load Final Model**
   - **Function:** `prd_employees_model()`
     - Connects to PostgreSQL.
     - Reads transformed tables.
     - Merges them into a comprehensive `revised_employees` table.
     - Cleans up and renames columns for consistency.

## Airflow DAG

- **DAG ID:** `employees_etl_dag`
- **Start Date:** August 1, 2024
- **Schedule Interval:** `@once` (runs once)
- **Task Groups:**
  - **`extract_employees_load`**: Handles extraction and loading of source data.
  - **`transform_employees_tables`**: Manages data transformations.
  - **`load_employees_model`**: Finalizes the data by creating the model table.

### Task Dependencies

1. **Extract and Load Source Data**:
   - `get_src_table()` → `load_src_data()`

2. **Transform Data**:
   - `transform_src_departements()`
   - `transform_src_dept_emp()`
   - `transform_src_dept_manager()`
   - `transform_src_employees()`
   - `transform_src_salaries()`
   - `transform_src_titles()`

3. **Load Final Model**:
   - `prd_employees_model()`

  ## Mysql source and Postgresql destination and result data

<img src="https://github.com/Sendo-A/etl_workflow_using_airflow/blob/main/Mysql_employees.PNG" alt="Mysql" width="500"> <img src="https://github.com/Sendo-A/etl_workflow_using_airflow/blob/main/Postgresql_employees.PNG" alt="Postgresql" width="500">  


## Prerequisites

- Apache Airflow
- MySQL Database
- PostgreSQL Database
- Required Python Libraries: `pandas`, `sqlalchemy`, `apache-airflow`, `apache-airflow-providers-mysql`, `apache-airflow-providers-postgres`

## Setup and Execution

1. **Install Dependencies**:
   ```bash
   pip install apache-airflow pandas sqlalchemy apache-airflow-providers-mysql apache-airflow-providers-postgres


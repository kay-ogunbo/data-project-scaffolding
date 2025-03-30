# Code Documentation: Project Scaffolding Tool

This Python script is a command-line tool designed to scaffold a new project directory with a predefined structure, including options for database setup (MySQL, PostgreSQL, MSSQL), Docker integration, Python virtual environment creation, and Git initialization. It uses a CSV file to define the database schema and an SQL mapping file to translate generic data types to database-specific types.

## 1. Code Analysis

### 1.1. Imports

The script begins by importing necessary Python modules:

* `os`: Provides a way of using operating system dependent functionality. (Although imported, it's not directly used in the current version of the code.)
* `csv`: Enables working with CSV files for reading configuration and mapping data.
* `subprocess`: Allows running external commands like `git init`.
* `sys`: Provides access to system-specific parameters and functions, used here for exiting the script on error.
* `pathlib.Path`: Offers an object-oriented way to interact with files and directories.
* `datetime`, `timedelta`: Used for handling date and time related operations. (Although imported, they are not directly used in the current version of the code.)

### 1.2. Configuration Constants

* `REQUIRED_CSV_COLUMNS`: A list defining the mandatory column headers that must be present in the CSV file used to describe the database schema. These columns are 'Table Name', 'Field', 'Datatype', 'Length', 'Decimals', 'Key', 'Enforce', and 'Partition Column'.

### 1.3. Functions

#### 1.3.1. `prompt_user()`

This function interacts with the user through the command line to collect various configuration parameters for the project. It prompts for:

* **Project basics:** Project name and the location where the project directory should be created.
* **Database configuration:** Database type (MySQL, PostgreSQL, MSSQL, or none), database name, paths to the CSV mapping file, and the SQL type mapping file.
* **Project type and architecture:** Whether it's a 'data' or 'normal' project. If it's a 'data' project, it further asks for the data architecture (medallion, data\_mesh, data\_vault) and, if 'medallion' is chosen, the desired layers (e.g., bronze, silver, gold).
* **Additional configurations:** Whether to generate Docker files, the preferred Python environment manager (conda/pip), whether to initialize a Git repository, and which operating system scripts to generate (mac/win/both).

The function returns a dictionary (`config`) containing all the user-provided configurations.

#### 1.3.2. `validate_csv_structure(file_path)`

This function takes the path to the CSV mapping file as input and checks if it meets the following criteria:

* The file is not empty and has headers.
* All the column names listed in `REQUIRED_CSV_COLUMNS` are present in the CSV file's header row.

If the CSV structure is invalid, it raises a `ValueError` with a descriptive message.

#### 1.3.3. `read_sql_mapping(mapping_path)`

This function reads the CSV file specified by `mapping_path`. This file is expected to have at least two columns: 'Mapping' (containing generic data types in lowercase) and 'SQL Datatype' (containing the corresponding SQL data type). It returns a dictionary (`type_map`) where keys are the generic data types and values are their SQL equivalents. It also validates the header row of the mapping file.

#### 1.3.4. `process_table_data(csv_path, sql_mapping)`

This function reads the database schema definition from the CSV file specified by `csv_path`. It uses the `sql_mapping` dictionary to convert the generic data types into specific SQL data types, handling parameters like length and decimals for appropriate types (e.g., VARCHAR, DECIMAL).

The function iterates through each row of the CSV, extracting information about tables, fields, data types, constraints (key, enforce - not null), and partitioning. It performs the following actions:

* Organizes the data into a dictionary (`tables`) where keys are table names.
* For each table, it stores a list of column definitions, a list of primary key fields, the partition column (if any), and its data type.
* It validates if the specified data type exists in the `sql_mapping`.
* It constructs the SQL data type string, including length and decimal parameters where necessary.
* It adds "NOT NULL" constraint to columns where 'Enforce' is marked with 'X'.
* It identifies primary key columns and the partition column.
* Finally, it adds a `PRIMARY KEY` constraint definition to the list of columns for each table that has keys defined.

The function returns the `tables` dictionary containing the processed table schema information.

#### 1.3.5. `generate_sql_commands(config, tables)`

This function takes the user configuration (`config`) and the processed table schema (`tables`) as input and generates SQL commands to create the database and the tables. The generated SQL varies based on the database type specified in the configuration ('mysql', 'postgresql', 'mssql').

Key aspects of the generated SQL:

* **Database Creation:** It includes commands to drop and create the specified database.
* **Schema Creation (for Medallion):** If the project type is 'data' and the architecture is 'medallion', it creates schemas corresponding to the defined medallion layers (e.g., bronze, silver, gold).
* **Table Creation:** For each table in the `tables` dictionary and for each defined medallion layer, it generates `CREATE TABLE` statements with the columns and constraints defined in `process_table_data`. It also includes `DROP TABLE IF EXISTS` statements for each table.
* **Partitioning (for MSSQL):** If the database is MSSQL and a partition column is defined for a table (and the layer is not 'bronze'), it generates SQL to create a partition function and a partition scheme based on the partition column and then creates the table using this scheme. The partitioning is currently hardcoded for a date range ('2023-01-01').
* **MSSQL `GO` Commands:** If the database is MSSQL, it appends `GO` after each command as required by SQL Server tools.

The function returns a single string containing all the generated SQL commands separated by newlines.

#### 1.3.6. `generate_docker_config(project_path, config)`

This function generates two Docker-related files in the specified `project_path`:

* **`Dockerfile`:** A simple Dockerfile that uses a Python 3.9 slim image, sets the working directory to `/app`, copies the `requirements.txt` file, installs dependencies, copies the project files, and sets the default command to run `src/main.py`.
* **`docker-compose.yml`:** A Docker Compose file that defines a service named `app` built from the `Dockerfile`. It also optionally defines a database service (postgres, mysql, or mssql) based on the user's database choice in the `config`. It sets up environment variables and port mappings for the database if chosen. It also defines volumes for database persistence.

#### 1.3.7. `create_project_structure(project_path, config)`

This function creates the basic directory structure for the project within the specified `project_path`. The base directories created are `src`, `tests`, `docs`, and `scripts`. If the project type is 'data', it also creates a `data` directory. If the data architecture is 'medallion', it further creates subdirectories within the `data` directory for each medallion layer (e.g., `data/bronze`, `data/silver`, `data/gold`).

#### 1.3.8. `setup_environment(project_path, config)`

This function generates shell scripts to set up a Python virtual environment based on the user's choice of environment manager (`conda` or `pip`) and the specified operating systems ('mac', 'win', or both).

* For macOS/Linux ('mac' in `config['os_scripts']`), it creates a `setup.sh` script with commands to either create and activate a Conda environment or a Pip virtual environment. The script is made executable.
* For Windows ('win' in `config['os_scripts']`), it creates a `setup.bat` script with the corresponding commands for Conda or Pip environment activation.

#### 1.3.9. `initialize_git(project_path, config)`

If the user chose to initialize Git (`config['git_init']` is True), this function runs the `git init` command within the `project_path` to create a new Git repository. It also creates a basic `.gitignore` file with common Python and environment-related exclusions.

#### 1.3.10. `main()`

This is the main function that orchestrates the entire process. It performs the following steps:

1.  Calls `prompt_user()` to get the project configuration.
2.  Constructs the full project path.
3.  Creates the main project directory.
4.  If a database is configured, it calls `validate_csv_structure()` to ensure the CSV mapping file is valid.
5.  Calls `create_project_structure()` to set up the directory structure.
6.  If a database is configured, it calls `process_table_data()` to process the CSV and `read_sql_mapping()` to get the SQL type mappings. Then, it calls `generate_sql_commands()` to create the SQL scripts and saves this content into separate `.sql` files for each medallion layer (if applicable).
7.  If Docker is enabled, it calls `generate_docker_config()` to create the Dockerfile and docker-compose.yml.
8.  Calls `setup_environment()` to generate environment setup scripts.
9.  Calls `initialize_git()` to initialize a Git repository.
10. Prints a success message with instructions on the next steps.

It also includes a `try...except` block to catch any exceptions that might occur during the process and print an error message before exiting.

### 1.4. Execution Block

The `if __name__ == "__main__":` block ensures that the `main()` function is called only when the script is executed directly.

## 2. How to Set Up

Before running the script, ensure you have the following prerequisites installed on your system:

* **Python 3.6 or higher:** The script is written in Python 3.
* **pip:** Python package installer (usually comes with Python).
* **(Optional) conda:** If you plan to use conda as your environment manager.
* **(Optional) Docker:** If you want to generate Docker files and use containerization.
* **(Optional) Git:** If you want to initialize a Git repository for your project.

**Steps for Setup:**

1.  **Save the script:** Save the provided Python code as a `.py` file (e.g., `project_scaffolder.py`).
2.  **Prepare CSV mapping file:** If you plan to set up a database, create a CSV file that defines your database schema. This file must have the following columns in the header: `Table Name`, `Field`, `Datatype`, `Length`, `Decimals`, `Key`, `Enforce`, and `Partition Column`.
    * `Table Name`: The name of the database table.
    * `Field`: The name of the column in the table.
    * `Datatype`: A generic data type (e.g., string, integer, decimal, date). You will map these to specific SQL types in the next step.
    * `Length`: The length of the field (if applicable).
    * `Decimals`: The number of decimal places (if applicable).
    * `Key`: Mark with 'X' if this field is part of the primary key.
    * `Enforce`: Mark with 'X' if this field should have a 'NOT NULL' constraint.
    * `Partition Column`: Mark with 'X' if this field should be used for partitioning (currently only implemented for MSSQL).
3.  **Prepare SQL type mapping file:** Create another CSV file that maps the generic data types used in your schema CSV to specific SQL data types for your chosen database (MySQL, PostgreSQL, or MSSQL). This file should have at least two columns with headers (case-insensitive): `Mapping` and `SQL Datatype`. For example:
    ```csv
    Mapping,SQL Datatype
    string,VARCHAR(255)
    integer,INT
    decimal,DECIMAL(10, 2)
    date,DATE
    boolean,BOOLEAN
    ```
4.  **Navigate to the directory:** Open your terminal or command prompt and navigate to the directory where you saved the `project_scaffolder.py` file.

## 3. How to Run

To run the project scaffolding tool, execute the Python script from your terminal or command prompt:

```bash
python project_scaffolder.py

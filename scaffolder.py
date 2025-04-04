import os
import csv
import subprocess
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Configuration constants
REQUIRED_CSV_COLUMNS = [
    'Table Name', 'Field Name', 'Datatype', 'Length',
    'Decimal Places', 'Key', 'Enforce', 'Partition Column'
]

# Escaped identifier for special characters in column names
def quote_identifier(db_type, identifier):
    """Properly quote identifiers for different databases"""
    cleaned = identifier.strip()
    if db_type == 'postgresql':
        return f'"{cleaned}"'
    elif db_type == 'mysql':
        return f'`{cleaned}`'
    elif db_type == 'mssql':
        return f'[{cleaned}]'
    return cleaned

def prompt_user():
    """Collect user configuration through interactive prompts"""
    config = {
        'project_name': None,
        'project_location': Path.cwd(),
        'database': None,
        'database_name': None,
        'csv_path': None,
        'sql_mapping_path': None,
        'docker': False,
        'project_type': 'normal',
        'data_arch': None,
        'medallion_layers': [],
        'python_env': 'pip',
        'git_init': False,
        'os_scripts': [],
        'mssql_go': False
    }

    # Project basics
    while not config['project_name']:
        config['project_name'] = input("Project name: ").strip()
        if not config['project_name']:
            print("Project name is required!")

    location = input(
        f"Where to save the project? [default: {str(config['project_location'])}]: ").strip()
    if location:
        config['project_location'] = Path(location).expanduser().resolve()

    # Database configuration
    db_choice = input("Database (mysql/postgresql/mssql/n): ").lower()
    if db_choice in {'mysql', 'postgresql', 'mssql'}:
        config['database'] = db_choice
        config['database_name'] = input("Database name: ").strip()

        while True:
            config['csv_path'] = input("CSV mapping file path: ").strip()
            if Path(config['csv_path']).exists():
                break
            print("File not found!")

        while True:
            config['sql_mapping_path'] = input(
                "SQL type mapping file: ").strip()
            if Path(config['sql_mapping_path']).exists():
                break
            print("File not found!")

        config['mssql_go'] = (db_choice == 'mssql')

    # Project type and architecture
    project_type = input("Project type (data/normal): ").lower()
    if project_type == 'data':
        config['project_type'] = 'data'
        data_arch = input(
            "Architecture (medallion/data_mesh/data_vault): ").lower()
        if data_arch == 'medallion':
            config['data_arch'] = 'medallion'
            layers = input("Medallion layers [bronze silver gold]: ").lower()
            config['medallion_layers'] = layers.split() or [
                'bronze', 'silver', 'gold']

    # Additional configurations
    config['docker'] = input("Generate Docker files? (y/n): ").lower() == 'y'
    config['python_env'] = input("Environment manager (conda/pip): ").lower()
    config['git_init'] = input("Initialize Git? (y/n): ").lower() == 'y'

    os_choice = input("OS scripts (mac/win/both): ").lower()
    if 'mac' in os_choice:
        config['os_scripts'].append('mac')
    if 'win' in os_choice:
        config['os_scripts'].append('win')

    return config


def validate_csv_structure(file_path):
    """Ensure CSV has required columns and valid structure"""
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV file is empty or missing headers")

        missing = [
            col for col in REQUIRED_CSV_COLUMNS if col not in reader.fieldnames]
        if missing:
            raise ValueError(f"Missing columns: {', '.join(missing)}")


def read_sql_mapping(mapping_path):
    """Read and validate SQL type mapping file"""
    type_map = {}
    with open(mapping_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        headers = next(reader)

        if len(headers) < 2 or headers[0].lower() != 'mapping' or headers[1].lower() != 'sql datatype':
            raise ValueError("Invalid SQL mapping file format")

        for row in reader:
            if len(row) >= 2:
                type_map[row[0].strip().lower()] = row[1].strip()
    return type_map


def process_table_data(csv_path, sql_mapping):
    """Process CSV data with special character handling"""
    PARAMETERIZED_TYPES = {
        'NVARCHAR': ['length'],
        'VARCHAR': ['length'],
        'CHAR': ['length'],
        'DECIMAL': ['length', 'decimals'],
        'NUMERIC': ['length', 'decimals']
    }

    tables = {}
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)

        for row in reader:
            table_name = row['Table Name'].strip()
            if not table_name:
                continue

            if table_name not in tables:
                tables[table_name] = {
                    'columns': [],
                    'keys': [],
                    'partition': None,
                    'partition_type': None
                }

            field = row['Field Name'].strip()
            dtype = row['Datatype'].strip().lower()
            length = row['Length'].strip()
            decimals = row['Decimal Places'].strip()
            enforce = row['Enforce'].strip().upper() == 'X'
            is_partition = row['Partition Column'].strip().upper() == 'X'

            if dtype not in sql_mapping:
                raise ValueError(
                    f"Undefined data type '{dtype}' for field '{field}'")

            base_type = sql_mapping[dtype].upper().split('(')[0].strip()

            if base_type in PARAMETERIZED_TYPES:
                required_params = PARAMETERIZED_TYPES[base_type]

                if 'length' in required_params and not length:
                    raise ValueError(
                        f"Missing length for {field} ({base_type})")
                if 'decimals' in required_params and not decimals:
                    raise ValueError(
                        f"Missing decimals for {field} ({base_type})")

                params = []
                if 'length' in required_params:
                    params.append(length)
                if 'decimals' in required_params:
                    params.append(decimals)

                sql_type = f"{base_type}({','.join(params)})" if params else base_type
            else:
                sql_type = base_type

            # Store as dictionary instead of string
            col_def = {
                'field': field,
                'type': sql_type,
                'enforce': enforce
            }

            tables[table_name]['columns'].append(col_def)

            if row['Key'].strip().upper() == 'X':
                tables[table_name]['keys'].append(field)

            if is_partition:
                if tables[table_name]['partition']:
                    raise ValueError(
                        f"Multiple partition columns in {table_name}")
                tables[table_name]['partition'] = field
                tables[table_name]['partition_type'] = base_type

    return tables


def generate_sql_commands(config, tables):
    """Generate database-specific SQL commands with proper quoting"""
    db_type = config['database']
    db_name = config['database_name']
    commands = []

    if db_type == 'mssql':
        commands.extend([
            f"USE master",
            f"DROP DATABASE IF EXISTS {db_name}",
            f"CREATE DATABASE {db_name}",
            f"USE {db_name}"
        ])
    elif db_type == 'mysql':
        commands.extend([
            f"DROP DATABASE IF EXISTS `{db_name}`",
            f"CREATE DATABASE `{db_name}`",
            f"USE `{db_name}`"
        ])
    elif db_type == 'postgresql':
        commands.extend([
            f"DROP DATABASE IF EXISTS {db_name}",
            f"CREATE DATABASE {db_name}"
        ])

    for layer in config.get('medallion_layers', []):
        if db_type == 'postgresql':
            commands.append(f"CREATE SCHEMA IF NOT EXISTS {layer};")
        elif db_type == 'mssql':
            commands.append(f"CREATE SCHEMA {layer};")

        for table, data in tables.items():
            columns = []
            for col in data['columns']:
                quoted_field = quote_identifier(db_type, col['field'])
                col_def = f"{quoted_field} {col['type']}"
                if col['enforce']:
                    col_def += " NOT NULL"
                columns.append(col_def)

            if data['keys']:
                quoted_keys = [quote_identifier(
                    db_type, k) for k in data['keys']]
                columns.append(f"PRIMARY KEY ({', '.join(quoted_keys)})")

            columns_str = ",\n    ".join(columns)
            drop_stmt = (f"IF OBJECT_ID('{layer}.{table}', 'U') IS NOT NULL DROP TABLE {layer}.{table};"
                         if db_type == 'mssql' else f"DROP TABLE IF EXISTS {layer}.{table};")

            create_stmt = f"CREATE TABLE {layer}.{table} (\n    {columns_str}\n)"

            if db_type == 'mssql' and data['partition'] and layer != 'bronze':
                partition_field = quote_identifier(db_type, data['partition'])
                func_name = f"pf_{layer}_{table}"
                scheme_name = f"ps_{layer}_{table}"
                commands.extend([
                    f"CREATE PARTITION FUNCTION {func_name} ({data['partition_type']}) AS RANGE RIGHT FOR VALUES ('2023-01-01')",
                    f"CREATE PARTITION SCHEME {scheme_name} AS PARTITION {func_name} ALL TO ([PRIMARY])",
                    f"DROP TABLE {layer}.{table}",
                    f"CREATE TABLE {layer}.{table} (\n    {columns_str}\n) ON {scheme_name}({partition_field})"
                ])
            else:
                commands.extend([drop_stmt, create_stmt])

    if config['mssql_go']:
        commands = [f"{cmd}\nGO" for cmd in commands]

    return "\n".join(commands)

def generate_docker_config(project_path, config):
    """Generate Docker-related files in project directory"""
    # Dockerfile
    dockerfile_path = project_path / "Dockerfile"
    with open(dockerfile_path, 'w') as f:
        f.write("""FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "src/main.py"]""")

    # docker-compose.yml
    compose_path = project_path / "docker-compose.yml"
    db_service = ""
    if config['database'] == 'postgresql':
        db_service = f"""
  postgres:
    image: postgres:13
    environment:
      POSTGRES_DB: {config['database_name']}
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data"""
    elif config['database'] == 'mysql':
        db_service = f"""
  mysql:
    image: mysql:8
    environment:
      MYSQL_DATABASE: {config['database_name']}
      MYSQL_ROOT_PASSWORD: root
    ports:
      - "3306:3306"
    volumes:
      - mysql_data:/var/lib/mysql"""
    elif config['database'] == 'mssql':
        db_service = f"""
  mssql:
    image: mcr.microsoft.com/mssql/server:2019-latest
    environment:
      SA_PASSWORD: YourStrong!Passw0rd
      ACCEPT_EULA: Y
    ports:
      - "1433:1433"
    volumes:
      - mssql_data:/var/opt/mssql"""

    with open(compose_path, 'w') as f:
        f.write(f"""version: '3.8'
services:
  app:
    build: .
    ports:
      - "5000:5000"{db_service}
volumes:
  postgres_data:
  mysql_data:
  mssql_data:""")


def create_project_structure(project_path, config):
    """Create directory structure in specified project path"""
    base_dirs = ['src', 'tests', 'docs', 'scripts']
    if config['project_type'] == 'data':
        base_dirs.append('data')
        if 'medallion' in config.get('data_arch', ''):
            base_dirs.extend(
                [f"data/{layer}" for layer in config['medallion_layers']])

    for directory in base_dirs:
        (project_path / directory).mkdir(parents=True, exist_ok=True)


def setup_environment(project_path, config):
    """Create environment setup scripts in project directory"""
    env_cmds = {
        'conda': [
            f"conda create --name {config['project_name']}_env python=3.9 -y",
            f"conda activate {config['project_name']}_env"
        ],
        'pip': [
            f"python -m venv {config['project_name']}_env",
            f"source {config['project_name']}_env/bin/activate" if 'mac' in config['os_scripts'] else f"{config['project_name']}_env\Scripts\activate"
        ]
    }

    for os_type in config['os_scripts']:
        if os_type == 'mac':
            setup_path = project_path / "setup.sh"
            with open(setup_path, 'w') as f:
                f.write("#!/bin/bash\n" +
                        "\n".join(env_cmds[config['python_env']]))
            os.chmod(setup_path, 0o755)
        elif os_type == 'win':
            setup_path = project_path / "setup.bat"
            with open(setup_path, 'w') as f:
                f.write("@echo off\n" +
                        "\r\n".join(env_cmds[config['python_env']]))


def initialize_git(project_path, config):
    """Initialize Git repository in project directory"""
    if config['git_init']:
        subprocess.run(['git', 'init'], cwd=project_path)
        gitignore_path = project_path / ".gitignore"
        with open(gitignore_path, 'w') as f:
            f.write("""# Python
venv/
__pycache__/
*.pyc
*.pyo
*.pyd

# Environment
.env
.env.local

# Database
*.db
*.sqlite
*.bak

# IDE
.idea/
.vscode/
.DS_Store""")


def main():
    try:
        config = prompt_user()
        project_path = config['project_location'] / config['project_name']

        # Create project directory
        project_path.mkdir(parents=True, exist_ok=True)

        # Validate inputs
        if config['database']:
            validate_csv_structure(config['csv_path'])

        # Create directory structure
        create_project_structure(project_path, config)

        # Generate SQL files
        if config['database']:
            tables = process_table_data(
                config['csv_path'], read_sql_mapping(config['sql_mapping_path']))
            sql_content = generate_sql_commands(config, tables)

            for layer in config.get('medallion_layers', []):
                sql_path = project_path / f"{layer}.sql"
                with open(sql_path, 'w') as f:
                    f.write(sql_content)

        # Generate Docker files
        if config['docker']:
            generate_docker_config(project_path, config)

        # Generate environment setup
        setup_environment(project_path, config)

        # Initialize Git
        initialize_git(project_path, config)

        print(f"\nProject created successfully at: {project_path}")
        print("Next steps:")
        print(f"1. Review files in {project_path}")
        print("2. Run the setup script for your OS")
        if config['database']:
            print("3. Execute the generated SQL files against your database")
        print("4. Start developing in the 'src' directory")

    except Exception as e:
        print(f"\nError: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

"""
Project Builder Script

A secure, database-agnostic project scaffolding tool with medallion architecture support.
Generates SQL schemas, Docker configurations, and environment setup scripts while enforcing
security best practices.

Key Features:
- Secure input validation and sanitization
- Cross-database support (MySQL, PostgreSQL, SQL Server)
- Medallion architecture implementation
- Automated Docker environment setup
- Git repository initialization
- Security-hardened file operations

Usage:
1. Run the script and follow interactive prompts
2. Generated project structure includes:
   - SQL schema files
   - Docker configurations
   - Environment setup scripts
   - Documentation directory
3. Customize generated files as needed for specific use cases

Security Measures:
- Path traversal prevention
- Input sanitization
- File size limits (10MB max)
- Secure file permissions
- Identifier quoting for SQL injection protection
"""

import os
import csv
import re
import sys
import subprocess
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set


# Security Constants
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB maximum file size limit
# Allowed characters for names
# SAFE_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9_-]+$')
SAFE_NAME_PATTERN = re.compile(r'^[\w/.-]+$')  # Allows word chars + / . -
ALLOWED_DB_TYPES = {'mysql', 'postgresql', 'mssql', 'n'}  # Supported databases
ALLOWED_ARCH = {'medallion', 'data_mesh',
                'data_vault', 'n'}  # Data architectures
ALLOWED_ENVS = {'pip', 'conda'}  # Supported environment managers

# Database Configuration Constants
DB_CONFIG = {
    'mssql': {
        'surrogate_key': "{quoted} INT IDENTITY(1,1) NOT NULL",
        'ingested_at': "DWH_INGESTED_AT DATETIME2 DEFAULT SYSDATETIME()",
        'create_db': [
            "USE master",
            "DROP DATABASE IF EXISTS {db}",
            "CREATE DATABASE {db}",
            "USE {db}"
        ],
        'schema': {
            'create': "CREATE SCHEMA {schema};",
            'drop': "IF EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}') "
                    "DROP SCHEMA {schema};"
        },
        'max_identifier': 128,
        'param_style': 'named'
    },
    'mysql': {
        'surrogate_key': "{quoted} INT AUTO_INCREMENT NOT NULL",
        'ingested_at': "DWH_INGESTED_AT TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6)",
        'create_db': [
            "DROP DATABASE IF EXISTS {db}",
            "CREATE DATABASE {db}",
            "USE {db}"
        ],
        'schema': {
            'create': "CREATE SCHEMA IF NOT EXISTS {schema};",
            'drop': "DROP SCHEMA IF EXISTS {schema};"
        },
        'max_identifier': 64,
        'param_style': 'format'
    },
    'postgresql': {
        'surrogate_key': "{quoted} SERIAL NOT NULL",
        'ingested_at': "DWH_INGESTED_AT TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP",
        'create_db': [
            "DROP DATABASE IF EXISTS {db}",
            "CREATE DATABASE {db}"
        ],
        'schema': {
            'create': "CREATE SCHEMA IF NOT EXISTS {schema};",
            'drop': "DROP SCHEMA IF EXISTS {schema} CASCADE;"
        },
        'max_identifier': 63,
        'param_style': 'numbered'
    }
}

PARAMETERIZED_TYPES = {
    'nvarchar': ['length'],
    'varchar': ['length'],
    'char': ['length'],
    'decimal': ['length', 'decimals'],
    'numeric': ['length', 'decimals']
}


class ProjectConfig:
    """
    Configuration container for project settings

    Attributes:
        project_name (str): Name of the project
        base_location (Path): Base directory for projects
        project_root (Path): Full path to project directory
        database (str|None): Selected database type
        database_name (str|None): Name of the database
        source_system (str|None): Source system name
        csv_path (Path|None): Path to CSV mapping file
        sql_mapping_path (Path|None): Path to SQL type mapping file
        medallion_layers (list): List of medallion layers
        python_env (str): Python environment manager
        git_init (bool): Initialize git repository flag
        os_scripts (set): OS-specific scripts to generate
        docker (bool): Generate Docker files flag
        mssql_go (bool): MSSQL-specific flag
        data_arch (str|None): Data architecture type
    """

    def __init__(self):
        self.project_name: str = ""
        self.base_location: Path = Path.home() / "projects"
        self.project_root: Path = Path()  # Will be set during configuration
        self.database: Optional[str] = None
        self.database_name: Optional[str] = None
        self.source_system: Optional[str] = None
        self.csv_path: Optional[Path] = None
        self.sql_mapping_path: Optional[Path] = None
        self.medallion_layers: List[str] = []
        self.python_env: str = "pip"
        self.git_init: bool = False
        self.os_scripts: Set[str] = set()
        self.docker: bool = False
        self.mssql_go: bool = False
        self.data_arch: Optional[str] = None


def validate_safe_name(name: str) -> Tuple[bool, str]:
    """
    Validate names against security patterns

    Args:
        name (str): Input name to validate

    Returns:
        Tuple[bool, str]: (True, "") if valid, (False, error message) otherwise
    """
    if not SAFE_NAME_PATTERN.match(name):
        return False, ("Invalid characters detected (only letters, numbers, "
                       "hyphens and underscores allowed)")
    if len(name) > 128:
        return False, "Name exceeds maximum length (128 characters)"
    return True, ""


def secure_path(input_path: Path) -> Path:
    """
    Validate and resolve paths securely

    Ensures paths stay within user's home directory to prevent traversal attacks

    Args:
        input_path (Path): Path to validate

    Returns:
        Path: Resolved and validated path

    Raises:
        ValueError: On path traversal attempts
    """
    try:
        home = Path.home().resolve()
        expanded = input_path.expanduser().resolve()

        if home not in expanded.parents and expanded != home:
            raise ValueError(f"Path must be within home directory ({home})")

        return expanded
    except (ValueError, FileNotFoundError) as e:
        raise ValueError(f"Invalid path: {str(e)}") from e


# def sanitize_identifier(identifier: str, db_type: str) -> str:
#     max_length = DB_CONFIG.get(db_type, {}).get('max_identifier', 64)
#     clean = re.sub(r'[^\w]', '', identifier.strip('"`[]'))
#     return clean[:max_length]

def sanitize_identifier(identifier: str, db_type: str) -> str:
    """
    Sanitize identifiers while preserving allowed special characters

    Args:
        identifier: Original identifier from input
        db_type: Database type for length constraints

    Returns:
        Sanitized identifier safe for database use
    """
    # Remove surrounding quotes/brackets and whitespace
    stripped = identifier.strip('"`[] \t\n\r')

    # Allow specific safe characters: letters, numbers, _, /, ., -
    # Remove any other special characters
    clean = re.sub(r'[^\w/.-]', '', stripped)

    # Truncate to database's max identifier length
    max_length = DB_CONFIG.get(db_type, {}).get('max_identifier', 64)
    return clean[:max_length]

class SecureInputHandler:
    """Secure input handling with validation"""

    @staticmethod
    def get_input(prompt: str, validator: callable = None) -> str:
        """
        Get validated user input

        Args:
            prompt (str): Display prompt
            validator (callable): Validation function

        Returns:
            str: Validated input
        """
        while True:
            value = input(prompt).strip()
            if not value:
                continue
            if validator:
                valid, msg = validator(value)
                if valid:
                    return value
                print(f"Invalid input: {msg}")
            else:
                return value

    @staticmethod
    def get_file(prompt: str) -> Path:
        """
        Get validated file path with security checks

        Args:
            prompt (str): Display prompt

        Returns:
            Path: Validated file path
        """
        while True:
            path_str = SecureInputHandler.get_input(prompt)
            path = Path(path_str)
            try:
                validated_path = secure_path(path)
                if not validated_path.exists():
                    print("File not found")
                    continue
                if validated_path.stat().st_size > MAX_FILE_SIZE:
                    print("File exceeds 10MB limit")
                    continue
                return validated_path
            except ValueError as e:
                print(f"Path error: {str(e)}")


class CSVProcessor:
    """Secure CSV processing with schema validation"""

    @staticmethod
    def validate_structure(file_path: Path) -> None:
        """
        Validate CSV file structure

        Args:
            file_path (Path): Path to CSV file

        Raises:
            ValueError: On missing required columns
        """
        required_columns = {'table name', 'field name', 'datatype', 'length', 'decimal places'}
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise ValueError("Empty CSV file")

            columns = {col.strip().lower() for col in reader.fieldnames}
            missing = required_columns - columns
            if missing:
                raise ValueError(f"Missing columns: {', '.join(missing)}")

    @staticmethod
    def read_mapping(file_path: Path) -> Dict[str, str]:
        """
        Read and validate SQL type mapping file

        Args:
            file_path (Path): Path to mapping file

        Returns:
            Dict[str, str]: Type mapping dictionary

        Raises:
            ValueError: On invalid file format
        """
        mapping = {}
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            headers = next(reader, None)

            if not headers or len(headers) < 2:
                raise ValueError("Invalid mapping file format")

            for row in reader:
                if len(row) < 2:
                    continue

                source = sanitize_identifier(row[0].strip().lower(), 'generic')
                target = sanitize_identifier(row[1].strip(), 'generic')

                if source and target:
                    mapping[source] = target

        if not mapping:
            raise ValueError("No valid mappings found in file")
        return mapping

    @staticmethod
    def process_tables(file_path: Path, type_map: Dict[str, str]) -> Dict:
        """
        Process CSV data into table definitions

        Args:
            file_path (Path): Path to CSV file
            type_map (Dict): SQL type mapping

        Returns:
            Dict: Processed table definitions

        Raises:
            ValueError: On missing columns or invalid data
        """
        tables = {}
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    table_name = sanitize_identifier(
                        row['Table Name'].strip().lower(), 'generic'
                    )
                    field_name = sanitize_identifier(
                        row['Field Name'].strip(), 'generic'
                    )
                    dtype = row['Datatype'].strip().lower()
                    length = row['Length'].strip()
                    decimals = row['Decimal Places'].strip()
                    is_key = row['Key'].strip().upper() == 'X'
                    is_partition = row['Partition Column'].strip(
                    ).upper() == 'X'

                    if not table_name:
                        continue

                    if table_name not in tables:
                        tables[table_name] = {
                            'columns': [],
                            'keys': [],
                            'partition': None,
                            'partition_type': None
                        }

                    base_type = type_map.get(dtype, '').split(
                        '(')[0].strip().lower()
                    if not base_type:
                        raise ValueError(f"Undefined type: {dtype}")

                    if base_type in PARAMETERIZED_TYPES:
                        required_params = PARAMETERIZED_TYPES[base_type]
                        params = []
                        # Validate required parameters
                        if 'length' in required_params:
                            if not length:
                                raise ValueError(f"Missing length for {field_name}")
                            params.append(length)

                        if 'decimals' in required_params:
                            if not decimals:
                                raise ValueError(f"Missing decimals for {field_name}")
                            params.append(decimals)

                        sql_type = f"{base_type}({','.join(params)})"


                    else:
                        sql_type = base_type
                    #     if ('length' in PARAMETERIZED_TYPES[base_type]
                    #             and not length):
                    #         raise ValueError(
                    #             f"Missing length for {field_name}")
                    #     if ('decimals' in PARAMETERIZED_TYPES[base_type]
                    #             and not decimals):
                    #         raise ValueError(
                    #             f"Missing decimals for {field_name}")

                    #     if length:
                    #         params.append(length)
                    #     if decimals:
                    #         params.append(decimals)

                    #     sql_type = (f"{base_type}({','.join(params)})"
                    #                 if params else base_type)
                    # else:
                    #     sql_type = base_type

                    tables[table_name]['columns'].append({
                        'field': field_name,
                        'type': sql_type,
                        'enforce': row['Enforce'].strip().upper() == 'X'
                    })

                    if is_key:
                        tables[table_name]['keys'].append(field_name)
                    if is_partition:
                        if tables[table_name]['partition']:
                            raise ValueError(
                                f"Multiple partitions in {table_name}"
                            )
                        tables[table_name]['partition'] = field_name
                        tables[table_name]['partition_type'] = base_type

                except KeyError as e:
                    raise ValueError(f"Missing column in CSV: {e}")

        return tables


class SQLGenerator:
    """SQL DDL generator with database-specific formatting"""

    def __init__(self, config: ProjectConfig):
        """
        Initialize SQL generator

        Args:
            config (ProjectConfig): Project configuration
        """
        self.config = config
        self._validate_config()

    def _escape_string(self, value: str) -> str:
        """Properly escape string literals for different databases"""
        # First sanitize the input
        clean_value = re.sub(r"[^\w/.-]", "", value.strip())

        # Then escape single quotes
        escaped = clean_value.replace("'", "''")

        # Database-specific handling if needed
        if self.config.database == 'postgresql':
            escaped = escaped.replace("%", "%%")

        return f"'{escaped}'"

    def _validate_config(self):
        if not SAFE_NAME_PATTERN.match(self.config.project_name):
            raise ValueError("Invalid project name")
        if self.config.database and self.config.database not in DB_CONFIG:
            raise ValueError("Unsupported database type")

    def _quote(self, identifier: str) -> str:
        clean = sanitize_identifier(identifier, self.config.database)
        return {
            'mssql': f'[{clean}]',
            'mysql': f'`{clean}`',
            'postgresql': f'"{clean}"'
        }.get(self.config.database, clean)

    def generate_ddl(self, tables: Dict) -> Dict[str, str]:
        """
        Generate database DDL commands

        Args:
            tables (Dict): Table definitions

        Returns:
            Dict[str, str]: Layer-specific SQL files
        """
        if not self.config.database:
            return {}

        ddl = {}
        for layer in self.config.medallion_layers:
            commands = []
            commands.extend(self._database_commands())
            commands.extend(self._schema_commands(layer))
            commands.extend(self._table_commands(layer, tables))
            ddl[f"{layer}.sql"] = self._format_commands(commands)

        return ddl

    def _database_commands(self) -> List[str]:
        quoted_db = self._quote(self.config.database_name)
        return [
            cmd.format(db=quoted_db)
            for cmd in DB_CONFIG[self.config.database]['create_db']
        ]

    def _schema_commands(self, layer: str) -> List[str]:
        quoted_layer = self._quote(layer)
        return [
            DB_CONFIG[self.config.database]['schema']['drop'].format(
                schema=quoted_layer
            ),
            DB_CONFIG[self.config.database]['schema']['create'].format(
                schema=quoted_layer
            )
        ]

    def _table_commands(self, layer: str, tables: Dict) -> List[str]:
        commands = []
        for table, data in tables.items():
            quoted_table = self._quote(table)
            full_name = f"{self._quote(layer)}.{quoted_table}"

            columns = [
                DB_CONFIG[self.config.database]['surrogate_key'].format(
                    quoted=self._quote(f"{table}_id")
                )
            ]

            for col in data['columns']:
                quoted_col = self._quote(col['field'])
                col_def = f"{quoted_col} {col['type']}"
                if col['enforce']:
                    col_def += " NOT NULL"
                columns.append(col_def)

            columns.extend([
                "DWH_RECORD_ID VARCHAR(255) NOT NULL",
                "DWH_JOB_RECORD_ID VARCHAR(255) NOT NULL",
                f"DWH_SOURCE_SYSTEM VARCHAR(255) DEFAULT {self._escape_string(self.config.source_system)} NOT NULL",
                f"DWH_SOURCE_TABLE VARCHAR(255) DEFAULT {self._escape_string(table)} NOT NULL",
                DB_CONFIG[self.config.database]['ingested_at']
            ])

            if data['keys']:
                quoted_keys = [self._quote(k) for k in data['keys']]
                columns.append(f"PRIMARY KEY ({', '.join(quoted_keys)})")

            drop_sql = (
                f"IF OBJECT_ID('{full_name}', 'U') IS NOT NULL DROP TABLE {full_name};"
                if self.config.database == 'mssql' else
                f"DROP TABLE IF EXISTS {full_name};"
            )
            create_sql = (
                f"CREATE TABLE {full_name} (\n    "
                + ",\n    ".join(columns)
                + "\n)"
            )

            commands.extend([drop_sql, create_sql])

        return commands

    def _format_commands(self, commands: List[str]) -> str:
        if self.config.mssql_go:
            return "\nGO\n".join(commands) + "\nGO"
        return ";\n".join(commands) + ";"


class ProjectBuilder:
    """Main project builder orchestrator"""

    def __init__(self):
        self.config = ProjectConfig()
        self.input = SecureInputHandler()

    def setup(self):
        """Main entry point for project setup"""
        self._get_basic_info()
        self._get_database_config()
        self._get_architecture()
        self._get_additional_config()
        self._build_project_structure()
        self._generate_project_files()
        self._finalize_project()

    def _get_basic_info(self):
        """Collect basic project information"""
        self.config.project_name = self.input.get_input(
            "Project name: ",
            lambda x: validate_safe_name(x)
        )

        default_base = str(self.config.base_location)
        base_loc_str = self.input.get_input(
            f"Base directory for projects [{default_base}]: ",
            lambda x: (True, "")
        ) or default_base

        try:
            self.config.base_location = secure_path(Path(base_loc_str))
            self.config.project_root = (
                self.config.base_location / self.config.project_name
            )
        except ValueError as e:
            print(f"Error: {str(e)}")
            sys.exit(1)

    def _build_project_structure(self):
        """Create directory structure with security checks"""
        try:
            self.config.project_root.mkdir(parents=True, exist_ok=False)
            (self.config.project_root / "sql").mkdir()
            (self.config.project_root / "src").mkdir()
            (self.config.project_root / "docs").mkdir()
            (self.config.project_root / "scripts").mkdir()

            if self.config.docker:
                (self.config.project_root / "docker").mkdir()

        except FileExistsError:
            print(
                f"Project directory already exists: {self.config.project_root}")
            sys.exit(1)

    def _generate_project_files(self):
        """Generate all project files in appropriate locations"""
        if self.config.database:
            CSVProcessor.validate_structure(self.config.csv_path)
            type_map = CSVProcessor.read_mapping(self.config.sql_mapping_path)
            tables = CSVProcessor.process_tables(
                self.config.csv_path, type_map)

            generator = SQLGenerator(self.config)
            ddl_files = generator.generate_ddl(tables)

            sql_dir = self.config.project_root / "sql"
            for name, content in ddl_files.items():
                path = sql_dir / name
                path.write_text(content)
                path.chmod(0o644)

        if self.config.docker:
            self._generate_docker_files()

        self._generate_environment_scripts()

    def _generate_compose_content(self) -> str:
        """Generate docker-compose.yml content based on project configuration"""
        compose_content = """# WARNING: Change default credentials in production!
version: '3.8'

services:
  app:
    build: .
    ports:
      - "5000:5000"
"""

        if self.config.database:
            db_service = self._generate_db_service()
            compose_content += db_service

        compose_content += "\nvolumes:"
        if self.config.database == 'mssql':
            compose_content += "\n  mssql_data:"
        elif self.config.database == 'mysql':
            compose_content += "\n  mysql_data:"
        elif self.config.database == 'postgresql':
            compose_content += "\n  postgres_data:"

        return compose_content

    def _generate_db_service(self) -> str:
        """Generate database service configuration for docker-compose.yml"""
        db = self.config.database
        name = self.config.database_name

        services = {
            'mssql': f"""
  db:
    image: mcr.microsoft.com/mssql/server:2019-latest
    environment:
      SA_PASSWORD: YourStrong!Passw0rd  # CHANGE IN PRODUCTION
      ACCEPT_EULA: Y
    ports:
      - "1433:1433"
    volumes:
      - mssql_data:/var/opt/mssql
""",
            'mysql': f"""
  db:
    image: mysql:8
    environment:
      MYSQL_ROOT_PASSWORD: root  # CHANGE IN PRODUCTION
      MYSQL_DATABASE: {name}
    ports:
      - "3306:3306"
    volumes:
      - mysql_data:/var/lib/mysql
""",
            'postgresql': f"""
  db:
    image: postgres:13
    environment:
      POSTGRES_DB: {name}
      POSTGRES_PASSWORD: postgres  # CHANGE IN PRODUCTION
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
"""
        }
        return services.get(db, "")

    def _generate_docker_files(self):
        """Generate Docker configuration files"""
        docker_dir = self.config.project_root / "docker"
        docker_dir.mkdir(exist_ok=True)

        # Dockerfile
        dockerfile_path = docker_dir / "Dockerfile"
        dockerfile_path.write_text("""# WARNING: Change default credentials in production!
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "src/main.py"]
""")
        dockerfile_path.chmod(0o644)

        # docker-compose.yml
        compose_path = docker_dir / "docker-compose.yml"
        compose_content = self._generate_compose_content()
        compose_path.write_text(compose_content)
        compose_path.chmod(0o644)

        # .dockerignore
        dockerignore_path = docker_dir / ".dockerignore"
        dockerignore_content = """# Security
.env
secrets/
*.key
*.pem
*.crt

# Python
__pycache__/
*.pyc
*.pyo
*.pyd
venv/

# Development
.idea/
.vscode/
.DS_Store

# Database
*.db
*.dump
*.bak
"""
        dockerignore_path.write_text(dockerignore_content)
        dockerignore_path.chmod(0o644)

    def _generate_environment_scripts(self):
        """Generate setup scripts in scripts/ directory"""
        scripts_dir = self.config.project_root / "scripts"
        env_name = sanitize_identifier(
            f"{self.config.project_name}_env", 'generic')

        for os_type in self.config.os_scripts:
            if os_type == 'mac':
                script_path = scripts_dir / "setup_mac.sh"
                content = f"""#!/bin/bash
python -m venv {env_name}
source {env_name}/bin/activate
pip install --upgrade pip
[ -f requirements.txt ] && pip install -r requirements.txt
echo "Activate with: source {env_name}/bin/activate"
"""
                script_path.write_text(content)
                script_path.chmod(0o755)

            elif os_type == 'win':
                script_path = scripts_dir / "setup_win.bat"
                content = f"""@echo off
python -m venv {env_name}
call {env_name}\\Scripts\\activate.bat
python -m pip install --upgrade pip
if exist requirements.txt pip install -r requirements.txt
echo Activate with: {env_name}\\Scripts\\activate.bat
pause
"""
                script_path.write_text(content)

    def _finalize_project(self):
        """Final steps including git initialization"""
        if self.config.git_init:
            self._init_git_repo()

        print(f"\nProject successfully created at: {self.config.project_root}")
        print("Directory structure:")
        for path in self.config.project_root.glob('**/*'):
            if path.is_dir():
                print(f"  {path.relative_to(self.config.project_root)}/")

    def _init_git_repo(self):
        """Initialize git repository in project root"""
        subprocess.run(
            ['git', 'init', '-q'],
            cwd=self.config.project_root,
            check=True,
            shell=False
        )
        gitignore = self.config.project_root / ".gitignore"
        gitignore.write_text("""# Security
.env
secrets/

# Python
__pycache__/
*.pyc
*.pyo
*.pyd
venv/

# Database
*.db
*.dump
*.bak
""")

    def _get_database_config(self):
        """Collect and validate database configuration from user input"""
        db_choice = self.input.get_input(
            "Database (mysql/postgresql/mssql/n): ",
            # Now returns (bool, message)
            lambda x: (x in ALLOWED_DB_TYPES, "Invalid choice")
        )
        if db_choice == 'n':
            return

        self.config.database = db_choice
        self.config.database_name = self.input.get_input(
            "Database name: ",
            validate_safe_name
        )
        self.config.source_system = self.input.get_input(
            "Source system name: ",
            validator=lambda x: validate_safe_name(x) if x else (True, "")
        )
        self.config.csv_path = self.input.get_file("CSV mapping path: ")
        self.config.sql_mapping_path = self.input.get_file(
            "SQL type mapping: ")
        self.config.mssql_go = (db_choice == 'mssql')

    def _get_architecture(self):
        """Collect and validate architecture configuration"""
        arch = self.input.get_input(
            "Architecture (medallion/data_mesh/data_vault/n): ",
            # Corrected validation
            lambda x: (x in ALLOWED_ARCH, "Invalid architecture")
        )
        if arch == 'n':
            return

        self.config.data_arch = arch
        if arch == 'medallion':
            layers_input = self.input.get_input(
                "Medallion layers (space-separated) [bronze silver gold]: ",
                lambda x: (x, "") if x else ('bronze silver gold', "")
            )
            layers = layers_input.split() if layers_input else [
                'bronze', 'silver', 'gold']
            self.config.medallion_layers = [l.strip() for l in layers]

            for layer in self.config.medallion_layers:
                valid, msg = validate_safe_name(layer)
                if not valid:
                    print(f"Invalid layer name '{layer}': {msg}")
                    sys.exit(1)

    def _get_additional_config(self):
        """Collect additional configuration options"""
        self.config.docker = self.input.get_input(
            "Generate Docker files? (y/n): ").lower() == 'y'
        self.config.python_env = self.input.get_input(
            "Environment manager (pip/conda): ",
            # Fixed validation
            lambda x: (x in ALLOWED_ENVS, "Invalid manager")
        )
        self.config.git_init = self.input.get_input(
            "Initialize Git? (y/n): ").lower() == 'y'

        if self.input.get_input("Generate macOS scripts? (y/n): ").lower() == 'y':
            self.config.os_scripts.add('mac')
        if self.input.get_input("Generate Windows scripts? (y/n): ").lower() == 'y':
            self.config.os_scripts.add('win')


if __name__ == "__main__":
    try:
        sys.tracebacklimit = 0
        builder = ProjectBuilder()
        builder.setup()
    except Exception as e:
        print(f"Setup failed: {str(e)}")
        sys.exit(1)

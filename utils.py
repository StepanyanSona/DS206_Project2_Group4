"""
Utility functions for the dimensional data pipeline.
Flow-agnostic utility functions for reading SQL scripts, parsing configs, etc.
"""
import os
import uuid
import configparser
from typing import Dict, Optional


def generate_uuid() -> str:
    """
    Generate a unique UUID string.

    Returns:
        str: A unique UUID string
    """
    return str(uuid.uuid4())


def read_sql_script(file_path: str) -> str:
    """
    Read an SQL script from a file.

    Args:
        file_path: Path to the SQL file

    Returns:
        str: Contents of the SQL file

    Raises:
        FileNotFoundError: If the file doesn't exist
        IOError: If there's an error reading the file
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"SQL file not found: {file_path}")

    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()


def parse_database_config(config_file_path: str = "sql_server_config.cfg") -> Dict[str, str]:
    """
    Parse database configuration from a config file.

    Expected config file format:
    [DATABASE]
    server = your_server
    database = your_database
    username = your_username
    password = your_password
    driver = ODBC Driver 17 for SQL Server

    Args:
        config_file_path: Path to the configuration file

    Returns:
        dict: Dictionary containing database connection parameters

    Raises:
        FileNotFoundError: If the config file doesn't exist
        configparser.Error: If there's an error parsing the config
    """
    if not os.path.exists(config_file_path):
        raise FileNotFoundError(f"Config file not found: {config_file_path}")

    config = configparser.ConfigParser()
    config.read(config_file_path)

    if 'DATABASE' not in config:
        raise ValueError("Config file must contain a [DATABASE] section")

    db_config = {
        'server': config.get('DATABASE', 'server', fallback='localhost'),
        'database': config.get('DATABASE', 'database', fallback='ORDER_DDS'),
        'username': config.get('DATABASE', 'username', fallback=''),
        'password': config.get('DATABASE', 'password', fallback=''),
        'driver': config.get('DATABASE', 'driver', fallback='ODBC Driver 17 for SQL Server')
    }

    return db_config


def get_connection_string(config_file_path: str = "sql_server_config.cfg") -> str:
    """
    Build a SQL Server connection string from config file.

    Args:
        config_file_path: Path to the configuration file

    Returns:
        str: Connection string for SQL Server
    """
    config = parse_database_config(config_file_path)

    if config['username'] and config['password']:
        # SQL Server Authentication
        conn_str = (
            f"DRIVER={{{config['driver']}}};"
            f"SERVER={config['server']};"
            f"DATABASE={config['database']};"
            f"UID={config['username']};"
            f"PWD={config['password']}"
        )
    else:
        # Windows Authentication
        conn_str = (
            f"DRIVER={{{config['driver']}}};"
            f"SERVER={config['server']};"
            f"DATABASE={config['database']};"
            f"Trusted_Connection=yes"
        )

    return conn_str

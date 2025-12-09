"""
ETL tasks for dimensional data pipeline.
Flow-specific functions for executing SQL scripts with proper parameterization.
"""
import os
import pymssql
from typing import Dict, Optional
from pipeline_dimensional_data.config import *
from utils import read_sql_script, parse_database_config


def execute_sql_script(sql_script: str, config_file_path: str = "sql_server_config.cfg") -> Dict[str, bool]:
    """
    Execute a SQL script using pymssql.
    
    Args:
        sql_script: SQL script to execute
        config_file_path: Path to database configuration file
        
    Returns:
        dict: {'success': True} if successful, {'success': False} otherwise
    """
    try:
        config = parse_database_config(config_file_path)
        # Use pymssql instead of pyodbc (no ODBC driver needed)
        if config['username'] and config['password']:
            conn = pymssql.connect(
                server=config['server'],
                user=config['username'],
                password=config['password'],
                database=config['database'],
                port=1433,
                autocommit=True
            )
        else:
            import getpass
            conn = pymssql.connect(
                server=config['server'],
                user=getpass.getuser(),
                password='',
                database=config['database'],
                port=1433,
                autocommit=True
            )
        cursor = conn.cursor()
        
        # Split script by GO statements and execute each batch
        batches = [batch.strip() for batch in sql_script.split('GO') if batch.strip()]
        
        for batch in batches:
            if batch.strip():  # Only execute non-empty batches
                cursor.execute(batch)
        
        cursor.close()
        conn.close()
        
        return {'success': True}
    except Exception as e:
        print(f"Error executing SQL script: {str(e)}")
        return {'success': False, 'error': str(e)}


def update_dimension_table(
    dimension_name: str,
    staging_table_name: str,
    database_name: str = DATABASE_NAME,
    schema_name: str = SCHEMA_NAME,
    config_file_path: str = "sql_server_config.cfg"
) -> Dict[str, bool]:
    """
    Update a dimension table from its staging table.
    
    Args:
        dimension_name: Name of the dimension table (e.g., 'DimCategories')
        staging_table_name: Name of the staging table (e.g., 'stg_Categories_raw')
        database_name: Name of the database
        schema_name: Name of the schema
        config_file_path: Path to database configuration file
        
    Returns:
        dict: {'success': True} if successful
    """
    try:
        # Read the SQL script
        script_path = os.path.join(
            os.path.dirname(__file__),
            '../../DS206_Project2_Group4 3/pipeline_dimensional_data/queries',
            f'update_dim_{dimension_name.lower().replace("dim", "")}.sql'
        )
        
        sql_script = read_sql_script(script_path)
        
        # Replace parameters
        sql_script = sql_script.replace('{database_name}', database_name)
        sql_script = sql_script.replace('{schema_name}', schema_name)
        sql_script = sql_script.replace('{dim_table_name}', dimension_name)
        sql_script = sql_script.replace('{staging_table_name}', staging_table_name)
        
        # Execute the script
        return execute_sql_script(sql_script, config_file_path)
    except Exception as e:
        print(f"Error updating dimension {dimension_name}: {str(e)}")
        return {'success': False, 'error': str(e)}


def update_dim_categories(prerequisite_result: Optional[Dict] = None) -> Dict[str, bool]:
    """Update DimCategories dimension table."""
    return update_dimension_table(DIM_CATEGORIES, STG_CATEGORIES_RAW)


def update_dim_customers(prerequisite_result: Optional[Dict] = None) -> Dict[str, bool]:
    """Update DimCustomers dimension table."""
    return update_dimension_table(DIM_CUSTOMERS, STG_CUSTOMERS_RAW)


def update_dim_employees(prerequisite_result: Optional[Dict] = None) -> Dict[str, bool]:
    """Update DimEmployees dimension table."""
    return update_dimension_table(DIM_EMPLOYEES, STG_EMPLOYEES_RAW)


def update_dim_products(prerequisite_result: Optional[Dict] = None) -> Dict[str, bool]:
    """Update DimProducts dimension table."""
    return update_dimension_table(DIM_PRODUCTS, STG_PRODUCTS_RAW)


def update_dim_region(prerequisite_result: Optional[Dict] = None) -> Dict[str, bool]:
    """Update DimRegion dimension table."""
    return update_dimension_table(DIM_REGION, STG_REGION_RAW)


def update_dim_shippers(prerequisite_result: Optional[Dict] = None) -> Dict[str, bool]:
    """Update DimShippers dimension table."""
    return update_dimension_table(DIM_SHIPPERS, STG_SHIPPERS_RAW)


def update_dim_suppliers(prerequisite_result: Optional[Dict] = None) -> Dict[str, bool]:
    """Update DimSuppliers dimension table."""
    return update_dimension_table(DIM_SUPPLIERS, STG_SUPPLIERS_RAW)


def update_dim_territories(prerequisite_result: Optional[Dict] = None) -> Dict[str, bool]:
    """Update DimTerritories dimension table."""
    return update_dimension_table(DIM_TERRITORIES, STG_TERRITORIES_RAW)


def update_fact_orders(
    start_date: str,
    end_date: str,
    prerequisite_result: Optional[Dict] = None,
    database_name: str = DATABASE_NAME,
    schema_name: str = SCHEMA_NAME,
    config_file_path: str = "sql_server_config.cfg"
) -> Dict[str, bool]:
    """
    Update FactOrders fact table.
    
    Args:
        start_date: Start date for filtering orders (YYYY-MM-DD)
        end_date: End date for filtering orders (YYYY-MM-DD)
        prerequisite_result: Result from prerequisite task
        database_name: Name of the database
        schema_name: Name of the schema
        config_file_path: Path to database configuration file
        
    Returns:
        dict: {'success': True} if successful
    """
    try:
        # Read the SQL script
        script_path = os.path.join(
            os.path.dirname(__file__),
            '../../DS206_Project2_Group4 3/pipeline_dimensional_data/queries',
            'update_fact.sql'
        )
        
        sql_script = read_sql_script(script_path)
        
        # Replace parameters
        sql_script = sql_script.replace('{database_name}', database_name)
        sql_script = sql_script.replace('{schema_name}', schema_name)
        sql_script = sql_script.replace('{fact_table_name}', FACT_ORDERS)
        sql_script = sql_script.replace('{start_date}', start_date)
        sql_script = sql_script.replace('{end_date}', end_date)
        
        # Execute the script
        return execute_sql_script(sql_script, config_file_path)
    except Exception as e:
        print(f"Error updating fact table: {str(e)}")
        return {'success': False, 'error': str(e)}


def update_fact_orders_error(
    start_date: str,
    end_date: str,
    prerequisite_result: Optional[Dict] = None,
    database_name: str = DATABASE_NAME,
    schema_name: str = SCHEMA_NAME,
    config_file_path: str = "sql_server_config.cfg"
) -> Dict[str, bool]:
    """
    Update FactOrders_Error table with faulty rows.
    
    Args:
        start_date: Start date for filtering orders (YYYY-MM-DD)
        end_date: End date for filtering orders (YYYY-MM-DD)
        prerequisite_result: Result from prerequisite task
        database_name: Name of the database
        schema_name: Name of the schema
        config_file_path: Path to database configuration file
        
    Returns:
        dict: {'success': True} if successful
    """
    try:
        # Read the SQL script
        script_path = os.path.join(
            os.path.dirname(__file__),
            '../../DS206_Project2_Group4 3/pipeline_dimensional_data/queries',
            'update_fact_error.sql'
        )
        
        sql_script = read_sql_script(script_path)
        
        # Replace parameters
        sql_script = sql_script.replace('{database_name}', database_name)
        sql_script = sql_script.replace('{schema_name}', schema_name)
        sql_script = sql_script.replace('{fact_error_table_name}', FACT_ORDERS_ERROR)
        sql_script = sql_script.replace('{start_date}', start_date)
        sql_script = sql_script.replace('{end_date}', end_date)
        
        # Execute the script
        return execute_sql_script(sql_script, config_file_path)
    except Exception as e:
        print(f"Error updating fact error table: {str(e)}")
        return {'success': False, 'error': str(e)}

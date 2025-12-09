"""
Dimensional data flow orchestration.
Sequentially executes all ETL tasks for the dimensional data pipeline.
"""
import sys
import os
from typing import Dict

# Add parent directory to path for imports
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from utils import generate_uuid
# Import pipeline logging module
import importlib.util
logging_spec = importlib.util.spec_from_file_location("pipeline_logging", os.path.join(parent_dir, "pipeline_logging.py"))
pipeline_logging = importlib.util.module_from_spec(logging_spec)
logging_spec.loader.exec_module(pipeline_logging)

from pipeline_dimensional_data import tasks


class DimensionalDataFlow:
    """
    Class for orchestrating the dimensional data pipeline.
    Generates a unique execution_id upon instantiation and sequentially executes all tasks.
    """
    
    def __init__(self, log_file_path: str = "logs/logs_dimensional_data_pipeline.txt"):
        """
        Initialize the dimensional data flow.
        
        Args:
            log_file_path: Path to the log file
        """
        self.execution_id = generate_uuid()
        self.logger = pipeline_logging.setup_logger(self.execution_id, log_file_path)
        self.logger.info(f"DimensionalDataFlow initialized with execution_id: {self.execution_id}")
    
    def exec(self, start_date: str, end_date: str) -> Dict[str, bool]:
        """
        Execute the dimensional data pipeline sequentially.
        
        Args:
            start_date: Start date for fact table ingestion (YYYY-MM-DD)
            end_date: End date for fact table ingestion (YYYY-MM-DD)
            
        Returns:
            dict: {'success': True} if all tasks completed successfully
        """
        self.logger.info(f"Starting dimensional data pipeline execution. Date range: {start_date} to {end_date}")
        
        # Track results
        results = {}
        
        try:
            # Step 1: Update dimension tables (can run in parallel logically, but executing sequentially for simplicity)
            self.logger.info("Step 1: Updating dimension tables...")
            
            # Update all dimensions
            self.logger.info("Updating DimCategories...")
            results['dim_categories'] = tasks.update_dim_categories()
            if not results['dim_categories'].get('success', False):
                raise Exception("Failed to update DimCategories")
            
            self.logger.info("Updating DimCustomers...")
            results['dim_customers'] = tasks.update_dim_customers(results['dim_categories'])
            if not results['dim_customers'].get('success', False):
                raise Exception("Failed to update DimCustomers")
            
            self.logger.info("Updating DimEmployees...")
            results['dim_employees'] = tasks.update_dim_employees(results['dim_customers'])
            if not results['dim_employees'].get('success', False):
                raise Exception("Failed to update DimEmployees")
            
            self.logger.info("Updating DimProducts...")
            results['dim_products'] = tasks.update_dim_products(results['dim_employees'])
            if not results['dim_products'].get('success', False):
                raise Exception("Failed to update DimProducts")
            
            self.logger.info("Updating DimRegion...")
            results['dim_region'] = tasks.update_dim_region(results['dim_products'])
            if not results['dim_region'].get('success', False):
                raise Exception("Failed to update DimRegion")
            
            self.logger.info("Updating DimShippers...")
            results['dim_shippers'] = tasks.update_dim_shippers(results['dim_region'])
            if not results['dim_shippers'].get('success', False):
                raise Exception("Failed to update DimShippers")
            
            self.logger.info("Updating DimSuppliers...")
            results['dim_suppliers'] = tasks.update_dim_suppliers(results['dim_shippers'])
            if not results['dim_suppliers'].get('success', False):
                raise Exception("Failed to update DimSuppliers")
            
            self.logger.info("Updating DimTerritories...")
            results['dim_territories'] = tasks.update_dim_territories(results['dim_suppliers'])
            if not results['dim_territories'].get('success', False):
                raise Exception("Failed to update DimTerritories")
            
            # Step 2: Update fact table (depends on all dimensions)
            self.logger.info("Step 2: Updating fact table...")
            results['fact_orders'] = tasks.update_fact_orders(
                start_date=start_date,
                end_date=end_date,
                prerequisite_result=results['dim_territories']
            )
            if not results['fact_orders'].get('success', False):
                raise Exception("Failed to update FactOrders")
            
            # Step 3: Update fact error table (depends on fact table update)
            self.logger.info("Step 3: Updating fact error table...")
            results['fact_orders_error'] = tasks.update_fact_orders_error(
                start_date=start_date,
                end_date=end_date,
                prerequisite_result=results['fact_orders']
            )
            if not results['fact_orders_error'].get('success', False):
                raise Exception("Failed to update FactOrders_Error")
            
            self.logger.info("Dimensional data pipeline execution completed successfully!")
            return {'success': True, 'execution_id': self.execution_id, 'results': results}
            
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {str(e)}")
            return {'success': False, 'execution_id': self.execution_id, 'error': str(e), 'results': results}

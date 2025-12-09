"""
Configuration file for dimensional data pipeline.
Contains database and table names used throughout the pipeline.
"""

# Database configuration
DATABASE_NAME = "ORDER_DDS"
SCHEMA_NAME = "dbo"

# Dimension table names
DIM_CATEGORIES = "DimCategories"
DIM_CUSTOMERS = "DimCustomers"
DIM_EMPLOYEES = "DimEmployees"
DIM_PRODUCTS = "DimProducts"
DIM_REGION = "DimRegion"
DIM_SHIPPERS = "DimShippers"
DIM_SUPPLIERS = "DimSuppliers"
DIM_TERRITORIES = "DimTerritories"
DIM_SOR = "Dim_SOR"

# Fact table names
FACT_ORDERS = "FactOrders"
FACT_ORDERS_ERROR = "FactOrders_Error"

# Staging table names
STG_CATEGORIES_RAW = "stg_Categories_raw"
STG_CUSTOMERS_RAW = "stg_Customers_raw"
STG_EMPLOYEES_RAW = "stg_Employees_raw"
STG_PRODUCTS_RAW = "stg_Products_raw"
STG_REGION_RAW = "stg_Region_raw"
STG_SHIPPERS_RAW = "stg_Shippers_raw"
STG_SUPPLIERS_RAW = "stg_Suppliers_raw"
STG_TERRITORIES_RAW = "stg_Territories_raw"
STG_ORDERS_RAW = "stg_Orders_raw"
STG_ORDER_DETAILS_RAW = "stg_OrderDetails_raw"

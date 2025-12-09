# DS206 Project 2 - Group 4

**Course:** DS206  
**Project:** Project 2 - Dimensional Data Store (DDS) Creation and Population  
**Group:** Group 4  
**Submission Date:** December 2025

## Project Overview

We have implemented a complete dimensional data warehouse pipeline for order management data according to the project specifications. Our solution includes the database infrastructure setup, ETL pipeline with proper SCD handling, logging system, and a Power BI dashboard. All requirements for Group 4 have been completed and tested.

## Project Structure

The project follows the required structure:

```
DS206_Project2_Group4/
├── infrastructure_initiation/
│   ├── dimensional_database_creation.sql
│   ├── dimensional_db_table_creation.sql
│   └── staging_raw_table_creation.sql
├── pipeline_dimensional_data/
│   ├── __init__.py
│   ├── config.py
│   ├── flow.py
│   ├── tasks.py
│   └── queries/
│       ├── update_dim_*.sql (8 dimension scripts)
│       ├── update_fact.sql
│       └── update_fact_error.sql
├── logs/
│   └── logs_dimensional_data_pipeline.txt
├── dashboard/
│   └── group4_dashboard.pbix
├── main.py
├── utils.py
├── pipeline_logging.py
├── load_staging_data.py
├── requirements.txt
├── sql_server_config.cfg
└── raw_data_source.xlsx
```

## Database Implementation

### Dimension Tables

We implemented all 8 dimension tables with the SCD types specified for Group 4:

- **DimCategories**: SCD1 - Simple overwrite when data changes
- **DimCustomers**: SCD2 - Historical tracking with EffectiveStartDate and EffectiveEndDate
- **DimEmployees**: SCD1 with delete - Overwrites existing records and marks deleted employees with IsDeleted flag
- **DimProducts**: SCD2 with delete closing - Historical tracking that closes records when products are deleted
- **DimRegion**: SCD4 - Main table with separate DimRegion_Hist table for change tracking
- **DimShippers**: SCD1 with delete - Overwrites and tracks deletions
- **DimSuppliers**: SCD3 - Tracks CompanyName changes with CompanyName_Current and CompanyName_Prior columns
- **DimTerritories**: SCD4 - Main table with DimTerritories_Hist for history tracking

All dimension tables include surrogate keys, SOR_SK references to Dim_SOR, and staging_raw_id_nk for data lineage tracking.

### Fact Tables

- **FactOrders**: INSERT-based fact table (as required for Group 4). Includes all dimension foreign keys and measures (Quantity, UnitPrice, Discount). Supports date range filtering via start_date and end_date parameters.

- **FactOrders_Error**: Captures rows that fail to load into the fact table due to missing or invalid natural keys. Includes ErrorReason field to identify which dimension key was missing.

### Staging Tables

We created 10 staging tables (stg_Categories_raw, stg_Customers_raw, stg_Employees_raw, stg_Products_raw, stg_Region_raw, stg_Shippers_raw, stg_Suppliers_raw, stg_Territories_raw, stg_Orders_raw, stg_OrderDetails_raw), each with a staging_raw_id_sk IDENTITY column as required.

## ETL Pipeline Implementation

### SQL Scripts

All SQL scripts are parametrized using placeholder syntax ({database_name}, {schema_name}, {start_date}, etc.) that get replaced at runtime. This allows the scripts to be flexible and reusable.

The dimension update scripts implement the appropriate SCD logic:
- SCD1 scripts use MERGE with simple UPDATE/INSERT
- SCD2 scripts use MERGE with EffectiveStartDate/EndDate handling
- SCD3 script updates CompanyName_Current and moves old value to CompanyName_Prior
- SCD4 scripts update the main table and insert change records into history tables

The fact table script (update_fact.sql) uses INSERT-based approach with date filtering, joining staging tables with dimension tables to resolve surrogate keys. The error script (update_fact_error.sql) captures rows where dimension lookups fail.

### Python Implementation

**utils.py**: Contains flow-agnostic utility functions including read_sql_script() for reading SQL files, parse_database_config() for reading configuration, and generate_uuid() for creating execution IDs.

**pipeline_dimensional_data/tasks.py**: Contains all ETL task functions. Each task function returns a dictionary with {'success': True/False} to ensure atomicity. Tasks check prerequisite results before executing to maintain sequential flow.

**pipeline_dimensional_data/flow.py**: Contains the DimensionalDataFlow class. Upon instantiation, it generates a unique execution_id using UUID. The exec() method sequentially executes all dimension updates, then fact table updates, with proper error handling.

**pipeline_logging.py**: Sets up a logger that includes the execution_id in every log message. Logs are written to logs/logs_dimensional_data_pipeline.txt with timestamps and execution details.

**main.py**: Implements command-line argument parsing for --start_date and --end_date parameters. Validates date formats and date ranges before executing the pipeline.

**load_staging_data.py**: Utility script to load data from raw_data_source.xlsx into staging tables. Handles all 10 source tables and their respective column mappings.

## Pipeline Execution

The pipeline can be executed from the command line:

```bash
python main.py --start_date=1996-01-01 --end_date=1998-12-31
```

Execution flow:
1. All dimension tables are updated sequentially (DimCategories → DimCustomers → ... → DimTerritories)
2. Each dimension update must succeed before the next one runs
3. After all dimensions are updated, FactOrders is populated
4. Finally, FactOrders_Error is populated with any invalid rows

All executions are logged with a unique execution_id, making it easy to track and debug pipeline runs.

## Power BI Dashboard

We created a Power BI dashboard (group4_dashboard.pbix) that connects to the ORDER_DDS database. The dashboard includes:

- Two main pages: "Sales Overview" and "Performance Analysis"
- Multiple visualizations per page (line charts, bar charts, pie charts, tables)
- Slicers for filtering (date range, category, year, region)
- DAX measures including 8 Date Intelligence measures (YTD, MTD, QTD, Previous Year/Month/Quarter, YoY/MoM Growth) and 5 CALCULATE() + FILTER() combinations
- Tooltip page for additional context
- Page navigation for drill-through functionality
- Reset buttons on each main page

## Setup Instructions

### Database Setup

1. Execute the three SQL scripts in infrastructure_initiation/ folder in order:
   - dimensional_database_creation.sql (creates ORDER_DDS database)
   - staging_raw_table_creation.sql (creates staging tables)
   - dimensional_db_table_creation.sql (creates dimension and fact tables)

### Python Setup

1. Install dependencies: `pip install -r requirements.txt`
2. Configure database connection in sql_server_config.cfg
3. Load staging data: `python load_staging_data.py`
4. Run pipeline: `python main.py --start_date=YYYY-MM-DD --end_date=YYYY-MM-DD`

### Power BI Setup

1. Open Power BI Desktop
2. Connect to SQL Server (localhost\SQLEXPRESS, database ORDER_DDS)
3. Import FactOrders and all Dim* tables
4. Create DateTable for date intelligence measures
5. Build visualizations as specified

## Testing

We tested the pipeline with various date ranges and verified:
- All dimension tables populate correctly with proper SCD handling
- Fact table populates with INSERT-based approach
- Error table captures invalid rows appropriately
- Logging system tracks all executions with unique IDs
- Pipeline can be run multiple times safely (dimensions use MERGE, fact uses INSERT)

## Group Contribution

All three group members contributed with multiple commits:
- Member 1: Infrastructure setup, configuration files, and SCD1 dimension scripts
- Member 2: SCD2, SCD3, and SCD4 dimension scripts, and package configuration
- Member 3: Fact table scripts, Python pipeline code, and main entry point

## GitHub Repository

- Repository Name: DS206_Project2_Group4
- Shared with: https://github.com/Arman-Asryan
- All code has been committed and pushed

---

**Group:** 4  
**Course:** DS206  
**Submission Date:** December 2025

"""
Load data from Excel file into SQL Server staging tables

Usage: python load_staging_data.py
"""

import pandas as pd
import pymssql
from utils import parse_database_config
import sys
import os


def load_data_to_staging():
    """Load data from Excel into staging tables"""
    
    excel_file = '../DS206_Project2_Group4 3/raw_data_source.xlsx'
    
    # Check if Excel file exists
    if not os.path.exists(excel_file):
        print(f"✗ Error: Excel file '{excel_file}' not found!")
        print(f"  Please make sure the file is in the project root directory.")
        sys.exit(1)
    
    # Parse database config
    try:
        config = parse_database_config('sql_server_config.cfg')
        # Use pymssql instead of pyodbc (no ODBC driver needed)
        # pymssql requires username/password, so if empty, try Windows auth with current user
        if config['username'] and config['password']:
            conn = pymssql.connect(
                server=config['server'],
                user=config['username'],
                password=config['password'],
                database=config['database'],
                port=1433  # Default SQL Server port
            )
        else:
            # For Windows Authentication, pymssql needs the Windows username
            # Try connecting without explicit credentials (uses Windows auth)
            import getpass
            conn = pymssql.connect(
                server=config['server'],
                user=getpass.getuser(),  # Current Windows/Mac user
                password='',  # Empty for Windows auth
                database=config['database'],
                port=1433
            )
        cursor = conn.cursor()
        print("✓ Connected to database successfully\n")
    except Exception as e:
        print(f"✗ Failed to connect to database: {e}")
        print(f"  Server: {config.get('server', 'N/A')}")
        print(f"  Database: {config.get('database', 'N/A')}")
        print(f"  Username: {config.get('username', 'N/A (Windows Auth)')}")
        sys.exit(1)
    
    # Mapping of Excel sheets to staging tables (using our actual table names)
    sheet_mapping = {
        'Categories': 'stg_Categories_raw',
        'Customers': 'stg_Customers_raw',
        'Employees': 'stg_Employees_raw',
        'Order Details': 'stg_OrderDetails_raw',  # Note: Excel might have space
        'OrderDetails': 'stg_OrderDetails_raw',   # Or no space
        'Orders': 'stg_Orders_raw',
        'Products': 'stg_Products_raw',
        'Region': 'stg_Region_raw',
        'Shippers': 'stg_Shippers_raw',
        'Suppliers': 'stg_Suppliers_raw',
        'Territories': 'stg_Territories_raw'
    }
    
    try:
        # Read Excel file to get sheet names
        excel_file_obj = pd.ExcelFile(excel_file)
        available_sheets = excel_file_obj.sheet_names
        print(f"Available sheets in Excel: {', '.join(available_sheets)}\n")
        
        for sheet_name in available_sheets:
            # Map sheet name to table (handle both 'Order Details' and 'OrderDetails')
            if sheet_name in ['Order Details', 'OrderDetails']:
                table_name = 'stg_OrderDetails_raw'
            elif sheet_name in sheet_mapping:
                table_name = sheet_mapping[sheet_name]
            else:
                print(f"⚠ Skipping sheet '{sheet_name}' (not in mapping)")
                continue
            
            print(f"Loading {sheet_name} → {table_name}...")
            
            # Read Excel sheet
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            
            # Clear existing data in staging table
            cursor.execute(f"TRUNCATE TABLE dbo.{table_name}")
            conn.commit()
            
            # Prepare and insert data based on table
            # Note: pymssql uses %s placeholders instead of ?
            if sheet_name in ['Categories', 'Category']:
                for _, row in df.iterrows():
                    cursor.execute(f"""
                        INSERT INTO dbo.{table_name} 
                        (CategoryID, CategoryName, Description)
                        VALUES (%s, %s, %s)
                    """, (int(row['CategoryID']) if pd.notna(row['CategoryID']) else None,
                        str(row['CategoryName']) if pd.notna(row['CategoryName']) else None,
                        str(row['Description']) if pd.notna(row['Description']) else None))
            
            elif sheet_name in ['Customers', 'Customer']:
                for _, row in df.iterrows():
                    cursor.execute(f"""
                        INSERT INTO dbo.{table_name} 
                        (CustomerID, CompanyName, ContactName, ContactTitle, Address, 
                         City, Region, PostalCode, Country, Phone, Fax)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                    str(row['CustomerID']) if pd.notna(row['CustomerID']) else None,
                    str(row['CompanyName']) if pd.notna(row['CompanyName']) else None,
                    str(row['ContactName']) if pd.notna(row['ContactName']) else None,
                    str(row['ContactTitle']) if pd.notna(row['ContactTitle']) else None,
                    str(row['Address']) if pd.notna(row['Address']) else None,
                    str(row['City']) if pd.notna(row['City']) else None,
                    str(row['Region']) if pd.notna(row['Region']) else None,
                    str(row['PostalCode']) if pd.notna(row['PostalCode']) else None,
                    str(row['Country']) if pd.notna(row['Country']) else None,
                    str(row['Phone']) if pd.notna(row['Phone']) else None,
                    str(row['Fax']) if pd.notna(row['Fax']) else None))
            
            elif sheet_name in ['Employees', 'Employee']:
                for _, row in df.iterrows():
                    cursor.execute(f"""
                        INSERT INTO dbo.{table_name} 
                        (EmployeeID, LastName, FirstName, Title, TitleOfCourtesy, 
                         BirthDate, HireDate, Address, City, Region, PostalCode, 
                         Country, HomePhone, Extension, Notes, ReportsTo, PhotoPath)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                    int(row['EmployeeID']) if pd.notna(row['EmployeeID']) else None,
                    str(row['LastName']) if pd.notna(row['LastName']) else None,
                    str(row['FirstName']) if pd.notna(row['FirstName']) else None,
                    str(row['Title']) if pd.notna(row['Title']) else None,
                    str(row['TitleOfCourtesy']) if pd.notna(row['TitleOfCourtesy']) else None,
                    row['BirthDate'] if pd.notna(row['BirthDate']) else None,
                    row['HireDate'] if pd.notna(row['HireDate']) else None,
                    str(row['Address']) if pd.notna(row['Address']) else None,
                    str(row['City']) if pd.notna(row['City']) else None,
                    str(row['Region']) if pd.notna(row['Region']) else None,
                    str(row['PostalCode']) if pd.notna(row['PostalCode']) else None,
                    str(row['Country']) if pd.notna(row['Country']) else None,
                    str(row['HomePhone']) if pd.notna(row['HomePhone']) else None,
                    str(row['Extension']) if pd.notna(row['Extension']) else None,
                    str(row['Notes']) if pd.notna(row['Notes']) else None,
                    int(row['ReportsTo']) if pd.notna(row['ReportsTo']) else None,
                    str(row['PhotoPath']) if pd.notna(row['PhotoPath']) else None))
            
            elif sheet_name in ['Order Details', 'OrderDetails']:
                for _, row in df.iterrows():
                    cursor.execute(f"""
                        INSERT INTO dbo.{table_name} 
                        (OrderID, ProductID, UnitPrice, Quantity, Discount)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                    int(row['OrderID']) if pd.notna(row['OrderID']) else None,
                    int(row['ProductID']) if pd.notna(row['ProductID']) else None,
                    float(row['UnitPrice']) if pd.notna(row['UnitPrice']) else None,
                    int(row['Quantity']) if pd.notna(row['Quantity']) else None,
                    float(row['Discount']) if pd.notna(row['Discount']) else None))
            
            elif sheet_name in ['Orders', 'Order']:
                for _, row in df.iterrows():
                    cursor.execute(f"""
                        INSERT INTO dbo.{table_name} 
                        (OrderID, CustomerID, EmployeeID, OrderDate, RequiredDate, 
                         ShippedDate, ShipVia, Freight, TerritoryID)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                    int(row['OrderID']) if pd.notna(row['OrderID']) else None,
                    str(row['CustomerID']) if pd.notna(row['CustomerID']) else None,
                    int(row['EmployeeID']) if pd.notna(row['EmployeeID']) else None,
                    row['OrderDate'] if pd.notna(row['OrderDate']) else None,
                    row['RequiredDate'] if pd.notna(row['RequiredDate']) else None,
                    row['ShippedDate'] if pd.notna(row['ShippedDate']) else None,
                    int(row['ShipVia']) if pd.notna(row['ShipVia']) else None,
                    float(row['Freight']) if pd.notna(row['Freight']) else None,
                    str(row['TerritoryID']) if pd.notna(row['TerritoryID']) else None))
            
            elif sheet_name in ['Products', 'Product']:
                for _, row in df.iterrows():
                    cursor.execute(f"""
                        INSERT INTO dbo.{table_name} 
                        (ProductID, ProductName, SupplierID, CategoryID, QuantityPerUnit,
                         UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                    int(row['ProductID']) if pd.notna(row['ProductID']) else None,
                    str(row['ProductName']) if pd.notna(row['ProductName']) else None,
                    int(row['SupplierID']) if pd.notna(row['SupplierID']) else None,
                    int(row['CategoryID']) if pd.notna(row['CategoryID']) else None,
                    str(row['QuantityPerUnit']) if pd.notna(row['QuantityPerUnit']) else None,
                    float(row['UnitPrice']) if pd.notna(row['UnitPrice']) else None,
                    int(row['UnitsInStock']) if pd.notna(row['UnitsInStock']) else None,
                    int(row['UnitsOnOrder']) if pd.notna(row['UnitsOnOrder']) else None,
                    int(row['ReorderLevel']) if pd.notna(row['ReorderLevel']) else None,
                    bool(row['Discontinued']) if pd.notna(row['Discontinued']) else False))
            
            elif sheet_name in ['Region', 'Regions']:
                for _, row in df.iterrows():
                    cursor.execute(f"""
                        INSERT INTO dbo.{table_name} 
                        (RegionID, RegionDescription)
                        VALUES (%s, %s)
                    """, (
                    int(row['RegionID']) if pd.notna(row['RegionID']) else None,
                    str(row['RegionDescription']) if pd.notna(row['RegionDescription']) else None))
            
            elif sheet_name in ['Shippers', 'Shipper']:
                for _, row in df.iterrows():
                    cursor.execute(f"""
                        INSERT INTO dbo.{table_name} 
                        (ShipperID, CompanyName, Phone)
                        VALUES (%s, %s, %s)
                    """, (
                    int(row['ShipperID']) if pd.notna(row['ShipperID']) else None,
                    str(row['CompanyName']) if pd.notna(row['CompanyName']) else None,
                    str(row['Phone']) if pd.notna(row['Phone']) else None))
            
            elif sheet_name in ['Suppliers', 'Supplier']:
                for _, row in df.iterrows():
                    cursor.execute(f"""
                        INSERT INTO dbo.{table_name} 
                        (SupplierID, CompanyName, ContactName, ContactTitle, Address,
                         City, Region, PostalCode, Country, Phone, Fax, HomePage)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                    int(row['SupplierID']) if pd.notna(row['SupplierID']) else None,
                    str(row['CompanyName']) if pd.notna(row['CompanyName']) else None,
                    str(row['ContactName']) if pd.notna(row['ContactName']) else None,
                    str(row['ContactTitle']) if pd.notna(row['ContactTitle']) else None,
                    str(row['Address']) if pd.notna(row['Address']) else None,
                    str(row['City']) if pd.notna(row['City']) else None,
                    str(row['Region']) if pd.notna(row['Region']) else None,
                    str(row['PostalCode']) if pd.notna(row['PostalCode']) else None,
                    str(row['Country']) if pd.notna(row['Country']) else None,
                    str(row['Phone']) if pd.notna(row['Phone']) else None,
                    str(row['Fax']) if pd.notna(row['Fax']) else None,
                    str(row['HomePage']) if pd.notna(row['HomePage']) else None))
            
            elif sheet_name in ['Territories', 'Territory']:
                for _, row in df.iterrows():
                    cursor.execute(f"""
                        INSERT INTO dbo.{table_name} 
                        (TerritoryID, TerritoryDescription, RegionID)
                        VALUES (%s, %s, %s)
                    """, (
                    str(row['TerritoryID']) if pd.notna(row['TerritoryID']) else None,
                    str(row['TerritoryDescription']) if pd.notna(row['TerritoryDescription']) else None,
                    int(row['RegionID']) if pd.notna(row['RegionID']) else None))
            
            conn.commit()
            cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
            row_count = cursor.fetchone()[0]
            print(f"  ✓ Loaded {row_count} rows\n")
        
        print("\n" + "="*60)
        print("✓ All data loaded successfully!")
        print("="*60)
        
        # Show summary
        print("\nData Summary:")
        for table_name in ['stg_Categories_raw', 'stg_Customers_raw', 'stg_Employees_raw', 
                           'stg_Products_raw', 'stg_Region_raw', 'stg_Shippers_raw', 
                           'stg_Suppliers_raw', 'stg_Territories_raw', 'stg_Orders_raw', 
                           'stg_OrderDetails_raw']:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
                count = cursor.fetchone()[0]
                print(f"  {table_name}: {count} rows")
            except:
                print(f"  {table_name}: 0 rows")
        
    except Exception as e:
        print(f"\n✗ Error loading data: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
        sys.exit(1)
    
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    print("="*60)
    print("Loading Staging Data from Excel")
    print("="*60)
    print()
    
    load_data_to_staging()


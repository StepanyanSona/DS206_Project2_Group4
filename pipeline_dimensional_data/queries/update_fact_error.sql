-- Update FactOrders_Error (for rows with missing/invalid natural keys)
-- Parameters: @database_name, @schema_name, @fact_error_table_name, @start_date, @end_date

USE {database_name};
GO

DECLARE @sor_orders_sk INT;
DECLARE @sor_orderdetails_sk INT;

SELECT @sor_orders_sk = SOR_SK FROM {schema_name}.Dim_SOR WHERE StagingTableName = 'stg_Orders_raw';
SELECT @sor_orderdetails_sk = SOR_SK FROM {schema_name}.Dim_SOR WHERE StagingTableName = 'stg_OrderDetails_raw';

-- Insert rows that failed to load into fact table due to missing/invalid natural keys
INSERT INTO {schema_name}.{fact_error_table_name} (
    OrderID, ProductID, ErrorReason,
    Customer_SK, Employee_SK, Shipper_SK, Territory_SK, Region_SK,
    Product_SK, Category_SK, Supplier_SK,
    SOR_SK, staging_raw_id_nk
)
SELECT 
    o.OrderID,
    od.ProductID,
    -- Build error reason based on which keys are missing
    CASE 
        WHEN dc.Customer_SK IS NULL THEN 'Missing Customer'
        WHEN de.Employee_SK IS NULL THEN 'Missing Employee'
        WHEN ds.Shipper_SK IS NULL THEN 'Missing Shipper'
        WHEN dt.Territory_SK IS NULL THEN 'Missing Territory'
        WHEN dr.Region_SK IS NULL THEN 'Missing Region'
        WHEN dp.Product_SK IS NULL THEN 'Missing Product'
        WHEN dc2.Category_SK IS NULL THEN 'Missing Category'
        WHEN dsup.Supplier_SK IS NULL THEN 'Missing Supplier'
        ELSE 'Unknown Error'
    END AS ErrorReason,
    -- Include dimension keys that were found (NULL if not found)
    dc.Customer_SK,
    de.Employee_SK,
    ds.Shipper_SK,
    dt.Territory_SK,
    dr.Region_SK,
    dp.Product_SK,
    dc2.Category_SK,
    dsup.Supplier_SK,
    -- SOR tracking (using order details since that's the grain of the fact table)
    @sor_orderdetails_sk AS SOR_SK,
    od.staging_raw_id_sk AS staging_raw_id_nk
FROM {schema_name}.stg_Orders_raw AS o
INNER JOIN {schema_name}.stg_OrderDetails_raw AS od 
    ON o.OrderID = od.OrderID
-- Join with dimensions (using current records where applicable)
LEFT JOIN {schema_name}.DimCustomers AS dc 
    ON o.CustomerID = dc.CustomerID AND dc.IsCurrent = 1
LEFT JOIN {schema_name}.DimEmployees AS de 
    ON o.EmployeeID = de.EmployeeID AND de.IsDeleted = 0
LEFT JOIN {schema_name}.DimShippers AS ds 
    ON o.ShipVia = ds.ShipperID AND ds.IsDeleted = 0
LEFT JOIN {schema_name}.DimTerritories AS dt 
    ON o.TerritoryID = dt.TerritoryID
LEFT JOIN {schema_name}.DimRegion AS dr 
    ON dt.RegionID = dr.RegionID
LEFT JOIN {schema_name}.DimProducts AS dp 
    ON od.ProductID = dp.ProductID AND dp.IsCurrent = 1 AND dp.IsDeleted = 0
LEFT JOIN {schema_name}.DimCategories AS dc2 
    ON dp.CategoryID = dc2.CategoryID
LEFT JOIN {schema_name}.DimSuppliers AS dsup 
    ON dp.SupplierID = dsup.SupplierID
WHERE CAST(o.OrderDate AS DATE) >= '{start_date}'
  AND CAST(o.OrderDate AS DATE) <= '{end_date}'
  -- Only insert rows where at least one required dimension key is missing
  AND (
    dc.Customer_SK IS NULL OR
    de.Employee_SK IS NULL OR
    ds.Shipper_SK IS NULL OR
    dt.Territory_SK IS NULL OR
    dr.Region_SK IS NULL OR
    dp.Product_SK IS NULL OR
    dc2.Category_SK IS NULL OR
    dsup.Supplier_SK IS NULL
  );

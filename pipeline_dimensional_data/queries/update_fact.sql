-- Update FactOrders (INSERT-based for Group 4)
-- Parameters: @database_name, @schema_name, @fact_table_name, @start_date, @end_date

USE {database_name};
GO

DECLARE @sor_orders_sk INT;
DECLARE @sor_orderdetails_sk INT;

SELECT @sor_orders_sk = SOR_SK FROM {schema_name}.Dim_SOR WHERE StagingTableName = 'stg_Orders_raw';
SELECT @sor_orderdetails_sk = SOR_SK FROM {schema_name}.Dim_SOR WHERE StagingTableName = 'stg_OrderDetails_raw';

-- INSERT-based fact table population
-- Join staging tables with dimension tables to get surrogate keys
INSERT INTO {schema_name}.{fact_table_name} (
    OrderID, OrderDate, RequiredDate, ShippedDate, Freight,
    Customer_SK, Employee_SK, Shipper_SK, Territory_SK, Region_SK,
    Product_SK, Category_SK, Supplier_SK,
    Quantity, UnitPrice, Discount,
    SOR_SK, staging_raw_id_nk
)
SELECT 
    o.OrderID,
    CAST(o.OrderDate AS DATE) AS OrderDate,
    CAST(o.RequiredDate AS DATE) AS RequiredDate,
    CAST(o.ShippedDate AS DATE) AS ShippedDate,
    o.Freight,
    -- Dimension surrogate keys
    dc.Customer_SK,
    de.Employee_SK,
    ds.Shipper_SK,
    dt.Territory_SK,
    dr.Region_SK,
    dp.Product_SK,
    dc2.Category_SK,
    dsup.Supplier_SK,
    -- Order detail measures
    od.Quantity,
    od.UnitPrice,
    od.Discount,
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
  -- Only insert rows where all required dimension keys are found
  AND dc.Customer_SK IS NOT NULL
  AND de.Employee_SK IS NOT NULL
  AND ds.Shipper_SK IS NOT NULL
  AND dt.Territory_SK IS NOT NULL
  AND dr.Region_SK IS NOT NULL
  AND dp.Product_SK IS NOT NULL
  AND dc2.Category_SK IS NOT NULL
  AND dsup.Supplier_SK IS NOT NULL;

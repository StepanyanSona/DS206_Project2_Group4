-- Update DimProducts (SCD2 with delete closing)
-- Parameters: @database_name, @schema_name, @dim_table_name, @staging_table_name

USE {database_name};
GO

DECLARE @sor_sk INT;
SELECT @sor_sk = SOR_SK FROM {schema_name}.Dim_SOR WHERE StagingTableName = '{staging_table_name}';

DECLARE @current_date DATE = CAST(GETDATE() AS DATE);

-- SCD2 with delete closing: Close existing current records that have changed or are deleted, insert new current records
MERGE {schema_name}.{dim_table_name} AS target
USING (
    SELECT 
        ProductID,
        ProductName,
        SupplierID,
        CategoryID,
        QuantityPerUnit,
        UnitPrice,
        UnitsInStock,
        UnitsOnOrder,
        ReorderLevel,
        Discontinued,
        staging_raw_id_sk
    FROM {schema_name}.{staging_table_name}
) AS source
ON target.ProductID = source.ProductID AND target.IsCurrent = 1
WHEN MATCHED AND (
    ISNULL(target.ProductName, '') <> ISNULL(source.ProductName, '') OR
    ISNULL(target.SupplierID, 0) <> ISNULL(source.SupplierID, 0) OR
    ISNULL(target.CategoryID, 0) <> ISNULL(source.CategoryID, 0) OR
    ISNULL(target.QuantityPerUnit, '') <> ISNULL(source.QuantityPerUnit, '') OR
    ISNULL(target.UnitPrice, 0) <> ISNULL(source.UnitPrice, 0) OR
    ISNULL(target.UnitsInStock, 0) <> ISNULL(source.UnitsInStock, 0) OR
    ISNULL(target.UnitsOnOrder, 0) <> ISNULL(source.UnitsOnOrder, 0) OR
    ISNULL(target.ReorderLevel, 0) <> ISNULL(source.ReorderLevel, 0) OR
    ISNULL(target.Discontinued, 0) <> ISNULL(source.Discontinued, 0)
) THEN
    UPDATE SET
        EffectiveEndDate = DATEADD(DAY, -1, @current_date),
        IsCurrent = 0,
        UpdatedAt = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (
        ProductID, ProductName, SupplierID, CategoryID, QuantityPerUnit, UnitPrice,
        UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued,
        EffectiveStartDate, EffectiveEndDate, IsCurrent, IsDeleted, SOR_SK, staging_raw_id_nk
    )
    VALUES (
        source.ProductID, source.ProductName, source.SupplierID, source.CategoryID,
        source.QuantityPerUnit, source.UnitPrice, source.UnitsInStock, source.UnitsOnOrder,
        source.ReorderLevel, source.Discontinued, @current_date, NULL, 1, 0, @sor_sk, source.staging_raw_id_sk
    );

-- Insert new current records for changed products
INSERT INTO {schema_name}.{dim_table_name} (
    ProductID, ProductName, SupplierID, CategoryID, QuantityPerUnit, UnitPrice,
    UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued,
    EffectiveStartDate, EffectiveEndDate, IsCurrent, IsDeleted, SOR_SK, staging_raw_id_nk
)
SELECT 
    source.ProductID,
    source.ProductName,
    source.SupplierID,
    source.CategoryID,
    source.QuantityPerUnit,
    source.UnitPrice,
    source.UnitsInStock,
    source.UnitsOnOrder,
    source.ReorderLevel,
    source.Discontinued,
    @current_date AS EffectiveStartDate,
    NULL AS EffectiveEndDate,
    1 AS IsCurrent,
    0 AS IsDeleted,
    @sor_sk AS SOR_SK,
    source.staging_raw_id_sk
FROM {schema_name}.{staging_table_name} AS source
WHERE EXISTS (
    SELECT 1
    FROM {schema_name}.{dim_table_name} AS target
    WHERE target.ProductID = source.ProductID
    AND target.IsCurrent = 0
    AND target.EffectiveEndDate = DATEADD(DAY, -1, @current_date)
)
AND NOT EXISTS (
    SELECT 1
    FROM {schema_name}.{dim_table_name} AS target
    WHERE target.ProductID = source.ProductID
    AND target.IsCurrent = 1
);

-- Close current records for products that no longer exist in staging (delete closing)
UPDATE {schema_name}.{dim_table_name}
SET 
    EffectiveEndDate = DATEADD(DAY, -1, @current_date),
    IsCurrent = 0,
    IsDeleted = 1,
    UpdatedAt = SYSUTCDATETIME()
WHERE ProductID NOT IN (
    SELECT ProductID FROM {schema_name}.{staging_table_name}
)
AND IsCurrent = 1
AND IsDeleted = 0;

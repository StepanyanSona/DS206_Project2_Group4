-- Update DimShippers (SCD1 with delete)
-- Parameters: @database_name, @schema_name, @dim_table_name, @staging_table_name

USE {database_name};
GO

DECLARE @sor_sk INT;
SELECT @sor_sk = SOR_SK FROM {schema_name}.Dim_SOR WHERE StagingTableName = '{staging_table_name}';

-- SCD1 with delete: Update existing, insert new, mark deleted records
MERGE {schema_name}.{dim_table_name} AS target
USING (
    SELECT 
        ShipperID,
        CompanyName,
        Phone,
        staging_raw_id_sk
    FROM {schema_name}.{staging_table_name}
) AS source
ON target.ShipperID = source.ShipperID
WHEN MATCHED THEN
    UPDATE SET
        CompanyName = source.CompanyName,
        Phone = source.Phone,
        IsDeleted = 0,
        SOR_SK = @sor_sk,
        staging_raw_id_nk = source.staging_raw_id_sk,
        UpdatedAt = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (ShipperID, CompanyName, Phone, IsDeleted, SOR_SK, staging_raw_id_nk)
    VALUES (source.ShipperID, source.CompanyName, source.Phone, 0, @sor_sk, source.staging_raw_id_sk);

-- Mark shippers as deleted if they don't exist in staging
UPDATE {schema_name}.{dim_table_name}
SET 
    IsDeleted = 1,
    UpdatedAt = SYSUTCDATETIME()
WHERE ShipperID NOT IN (
    SELECT ShipperID FROM {schema_name}.{staging_table_name}
)
AND IsDeleted = 0;

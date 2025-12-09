-- Update DimCategories (SCD1)
-- Parameters: @database_name, @schema_name, @dim_table_name, @staging_table_name

USE {database_name};
GO

DECLARE @sor_sk INT;
SELECT @sor_sk = SOR_SK FROM {schema_name}.Dim_SOR WHERE StagingTableName = '{staging_table_name}';

-- SCD1: Update existing records, insert new ones
MERGE {schema_name}.{dim_table_name} AS target
USING (
    SELECT 
        CategoryID,
        CategoryName,
        Description,
        staging_raw_id_sk
    FROM {schema_name}.{staging_table_name}
) AS source
ON target.CategoryID = source.CategoryID
WHEN MATCHED THEN
    UPDATE SET
        CategoryName = source.CategoryName,
        Description = source.Description,
        SOR_SK = @sor_sk,
        staging_raw_id_nk = source.staging_raw_id_sk,
        UpdatedAt = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (CategoryID, CategoryName, Description, SOR_SK, staging_raw_id_nk)
    VALUES (source.CategoryID, source.CategoryName, source.Description, @sor_sk, source.staging_raw_id_sk);

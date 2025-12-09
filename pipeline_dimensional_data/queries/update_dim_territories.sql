-- Update DimTerritories (SCD4)
-- Parameters: @database_name, @schema_name, @dim_table_name, @staging_table_name

USE {database_name};
GO

DECLARE @sor_sk INT;
SELECT @sor_sk = SOR_SK FROM {schema_name}.Dim_SOR WHERE StagingTableName = '{staging_table_name}';

DECLARE @current_date DATE = CAST(GETDATE() AS DATE);

-- SCD4: Update main table and track changes in history table
MERGE {schema_name}.{dim_table_name} AS target
USING (
    SELECT 
        TerritoryID,
        TerritoryDescription,
        RegionID,
        staging_raw_id_sk
    FROM {schema_name}.{staging_table_name}
) AS source
ON target.TerritoryID = source.TerritoryID
WHEN MATCHED AND (
    ISNULL(target.TerritoryDescription, '') <> ISNULL(source.TerritoryDescription, '') OR
    ISNULL(target.RegionID, 0) <> ISNULL(source.RegionID, 0)
) THEN
    UPDATE SET
        TerritoryDescription = source.TerritoryDescription,
        RegionID = source.RegionID,
        SOR_SK = @sor_sk,
        staging_raw_id_nk = source.staging_raw_id_sk,
        UpdatedAt = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (TerritoryID, TerritoryDescription, RegionID, SOR_SK, staging_raw_id_nk)
    VALUES (source.TerritoryID, source.TerritoryDescription, source.RegionID, @sor_sk, source.staging_raw_id_sk);

-- Insert into history table for changed records
INSERT INTO {schema_name}.DimTerritories_Hist (TerritoryID, TerritoryDescription, ChangeDate, ChangeType)
SELECT 
    source.TerritoryID,
    source.TerritoryDescription,
    @current_date AS ChangeDate,
    CASE 
        WHEN target.TerritoryID IS NULL THEN 'INSERT'
        WHEN ISNULL(target.TerritoryDescription, '') <> ISNULL(source.TerritoryDescription, '') 
          OR ISNULL(target.RegionID, 0) <> ISNULL(source.RegionID, 0) THEN 'UPDATE'
        ELSE 'NO CHANGE'
    END AS ChangeType
FROM {schema_name}.{staging_table_name} AS source
LEFT JOIN {schema_name}.{dim_table_name} AS target ON source.TerritoryID = target.TerritoryID
WHERE target.TerritoryID IS NULL 
   OR ISNULL(target.TerritoryDescription, '') <> ISNULL(source.TerritoryDescription, '')
   OR ISNULL(target.RegionID, 0) <> ISNULL(source.RegionID, 0);

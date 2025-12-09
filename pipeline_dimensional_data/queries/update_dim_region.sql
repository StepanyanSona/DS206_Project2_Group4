-- Update DimRegion (SCD4)
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
        RegionID,
        RegionDescription,
        staging_raw_id_sk
    FROM {schema_name}.{staging_table_name}
) AS source
ON target.RegionID = source.RegionID
WHEN MATCHED AND ISNULL(target.RegionDescription, '') <> ISNULL(source.RegionDescription, '') THEN
    UPDATE SET
        RegionDescription = source.RegionDescription,
        SOR_SK = @sor_sk,
        staging_raw_id_nk = source.staging_raw_id_sk,
        UpdatedAt = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (RegionID, RegionDescription, SOR_SK, staging_raw_id_nk)
    VALUES (source.RegionID, source.RegionDescription, @sor_sk, source.staging_raw_id_sk);

-- Insert into history table for changed records
INSERT INTO {schema_name}.DimRegion_Hist (RegionID, RegionDescription, ChangeDate, ChangeType)
SELECT 
    source.RegionID,
    source.RegionDescription,
    @current_date AS ChangeDate,
    CASE 
        WHEN target.RegionID IS NULL THEN 'INSERT'
        WHEN ISNULL(target.RegionDescription, '') <> ISNULL(source.RegionDescription, '') THEN 'UPDATE'
        ELSE 'NO CHANGE'
    END AS ChangeType
FROM {schema_name}.{staging_table_name} AS source
LEFT JOIN {schema_name}.{dim_table_name} AS target ON source.RegionID = target.RegionID
WHERE target.RegionID IS NULL 
   OR ISNULL(target.RegionDescription, '') <> ISNULL(source.RegionDescription, '');

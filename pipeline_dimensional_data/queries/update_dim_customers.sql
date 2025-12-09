-- Update DimCustomers (SCD2)
-- Parameters: @database_name, @schema_name, @dim_table_name, @staging_table_name

USE {database_name};
GO

DECLARE @sor_sk INT;
SELECT @sor_sk = SOR_SK FROM {schema_name}.Dim_SOR WHERE StagingTableName = '{staging_table_name}';

DECLARE @current_date DATE = CAST(GETDATE() AS DATE);

-- SCD2: Close existing current records that have changed, insert new current records
MERGE {schema_name}.{dim_table_name} AS target
USING (
    SELECT 
        CustomerID,
        CompanyName,
        ContactName,
        ContactTitle,
        Address,
        City,
        Region,
        PostalCode,
        Country,
        Phone,
        Fax,
        staging_raw_id_sk
    FROM {schema_name}.{staging_table_name}
) AS source
ON target.CustomerID = source.CustomerID AND target.IsCurrent = 1
WHEN MATCHED AND (
    ISNULL(target.CompanyName, '') <> ISNULL(source.CompanyName, '') OR
    ISNULL(target.ContactName, '') <> ISNULL(source.ContactName, '') OR
    ISNULL(target.ContactTitle, '') <> ISNULL(source.ContactTitle, '') OR
    ISNULL(target.Address, '') <> ISNULL(source.Address, '') OR
    ISNULL(target.City, '') <> ISNULL(source.City, '') OR
    ISNULL(target.Region, '') <> ISNULL(source.Region, '') OR
    ISNULL(target.PostalCode, '') <> ISNULL(source.PostalCode, '') OR
    ISNULL(target.Country, '') <> ISNULL(source.Country, '') OR
    ISNULL(target.Phone, '') <> ISNULL(source.Phone, '') OR
    ISNULL(target.Fax, '') <> ISNULL(source.Fax, '')
) THEN
    UPDATE SET
        EffectiveEndDate = DATEADD(DAY, -1, @current_date),
        IsCurrent = 0,
        UpdatedAt = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (
        CustomerID, CompanyName, ContactName, ContactTitle, Address, City, 
        Region, PostalCode, Country, Phone, Fax, 
        EffectiveStartDate, EffectiveEndDate, IsCurrent, SOR_SK, staging_raw_id_nk
    )
    VALUES (
        source.CustomerID, source.CompanyName, source.ContactName, source.ContactTitle,
        source.Address, source.City, source.Region, source.PostalCode, source.Country,
        source.Phone, source.Fax, @current_date, NULL, 1, @sor_sk, source.staging_raw_id_sk
    );

-- Insert new current records for changed customers
INSERT INTO {schema_name}.{dim_table_name} (
    CustomerID, CompanyName, ContactName, ContactTitle, Address, City,
    Region, PostalCode, Country, Phone, Fax,
    EffectiveStartDate, EffectiveEndDate, IsCurrent, SOR_SK, staging_raw_id_nk
)
SELECT 
    source.CustomerID,
    source.CompanyName,
    source.ContactName,
    source.ContactTitle,
    source.Address,
    source.City,
    source.Region,
    source.PostalCode,
    source.Country,
    source.Phone,
    source.Fax,
    @current_date AS EffectiveStartDate,
    NULL AS EffectiveEndDate,
    1 AS IsCurrent,
    @sor_sk AS SOR_SK,
    source.staging_raw_id_sk
FROM {schema_name}.{staging_table_name} AS source
WHERE EXISTS (
    SELECT 1
    FROM {schema_name}.{dim_table_name} AS target
    WHERE target.CustomerID = source.CustomerID
    AND target.IsCurrent = 0
    AND target.EffectiveEndDate = DATEADD(DAY, -1, @current_date)
)
AND NOT EXISTS (
    SELECT 1
    FROM {schema_name}.{dim_table_name} AS target
    WHERE target.CustomerID = source.CustomerID
    AND target.IsCurrent = 1
);

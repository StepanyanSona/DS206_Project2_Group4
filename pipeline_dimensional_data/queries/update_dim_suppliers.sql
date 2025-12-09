-- Update DimSuppliers (SCD3 - one attribute: CompanyName)
-- Parameters: @database_name, @schema_name, @dim_table_name, @staging_table_name

USE {database_name};
GO

DECLARE @sor_sk INT;
SELECT @sor_sk = SOR_SK FROM {schema_name}.Dim_SOR WHERE StagingTableName = '{staging_table_name}';

-- SCD3: Update CompanyName_Current and move old value to CompanyName_Prior when CompanyName changes
MERGE {schema_name}.{dim_table_name} AS target
USING (
    SELECT 
        SupplierID,
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
        HomePage,
        staging_raw_id_sk
    FROM {schema_name}.{staging_table_name}
) AS source
ON target.SupplierID = source.SupplierID
WHEN MATCHED THEN
    UPDATE SET
        -- If CompanyName changed, move current to prior and update current
        CompanyName_Prior = CASE 
            WHEN ISNULL(target.CompanyName_Current, '') <> ISNULL(source.CompanyName, '') 
            THEN target.CompanyName_Current 
            ELSE target.CompanyName_Prior 
        END,
        CompanyName_Current = source.CompanyName,
        ContactName = source.ContactName,
        ContactTitle = source.ContactTitle,
        Address = source.Address,
        City = source.City,
        Region = source.Region,
        PostalCode = source.PostalCode,
        Country = source.Country,
        Phone = source.Phone,
        Fax = source.Fax,
        HomePage = source.HomePage,
        SOR_SK = @sor_sk,
        staging_raw_id_nk = source.staging_raw_id_sk,
        UpdatedAt = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (
        SupplierID, CompanyName_Current, CompanyName_Prior,
        ContactName, ContactTitle, Address, City, Region, PostalCode, Country,
        Phone, Fax, HomePage, SOR_SK, staging_raw_id_nk
    )
    VALUES (
        source.SupplierID, source.CompanyName, NULL,
        source.ContactName, source.ContactTitle, source.Address, source.City,
        source.Region, source.PostalCode, source.Country, source.Phone, source.Fax,
        source.HomePage, @sor_sk, source.staging_raw_id_sk
    );

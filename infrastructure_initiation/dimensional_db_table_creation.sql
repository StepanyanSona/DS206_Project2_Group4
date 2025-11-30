USE ORDER_DDS;
GO

/* =====================
   Dim_SOR
   ===================== */
IF OBJECT_ID('dbo.Dim_SOR','U') IS NOT NULL DROP TABLE dbo.Dim_SOR;
CREATE TABLE dbo.Dim_SOR (
    SOR_SK INT IDENTITY(1,1) PRIMARY KEY,
    StagingTableName NVARCHAR(128) NOT NULL
);
INSERT INTO dbo.Dim_SOR (StagingTableName) VALUES
 ('stg_Categories_raw'),
 ('stg_Customers_raw'),
 ('stg_Employees_raw'),
 ('stg_Region_raw'),
 ('stg_Territories_raw'),
 ('stg_Shippers_raw'),
 ('stg_Suppliers_raw'),
 ('stg_Products_raw'),
 ('stg_Orders_raw'),
 ('stg_OrderDetails_raw');

/* =====================
   DimCategories – SCD1
   ===================== */
IF OBJECT_ID('dbo.DimCategories','U') IS NOT NULL DROP TABLE dbo.DimCategories;
CREATE TABLE dbo.DimCategories (
    Category_SK INT IDENTITY(1,1) PRIMARY KEY,
    CategoryID INT NOT NULL,
    CategoryName NVARCHAR(255),
    Description NVARCHAR(MAX),
    SOR_SK INT NOT NULL,
    staging_raw_id_nk INT NOT NULL,
    CreatedAt DATETIME2 DEFAULT SYSUTCDATETIME(),
    UpdatedAt DATETIME2 NULL
);

/* =====================
   DimCustomers – SCD2
   ===================== */
IF OBJECT_ID('dbo.DimCustomers','U') IS NOT NULL DROP TABLE dbo.DimCustomers;
CREATE TABLE dbo.DimCustomers (
    Customer_SK INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID NVARCHAR(10) NOT NULL,
    CompanyName NVARCHAR(255),
    ContactName NVARCHAR(255),
    ContactTitle NVARCHAR(255),
    Address NVARCHAR(255),
    City NVARCHAR(255),
    Region NVARCHAR(255),
    PostalCode NVARCHAR(20),
    Country NVARCHAR(255),
    Phone NVARCHAR(50),
    Fax NVARCHAR(50),
    EffectiveStartDate DATE NOT NULL,
    EffectiveEndDate DATE NULL,
    IsCurrent BIT NOT NULL DEFAULT 1,
    SOR_SK INT NOT NULL,
    staging_raw_id_nk INT NOT NULL,
    CreatedAt DATETIME2 DEFAULT SYSUTCDATETIME(),
    UpdatedAt DATETIME2 NULL
);

/* =====================
   DimEmployees – SCD1 + delete
   ===================== */
IF OBJECT_ID('dbo.DimEmployees','U') IS NOT NULL DROP TABLE dbo.DimEmployees;
CREATE TABLE dbo.DimEmployees (
    Employee_SK INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeID INT NOT NULL,
    LastName NVARCHAR(255),
    FirstName NVARCHAR(255),
    Title NVARCHAR(255),
    IsDeleted BIT NOT NULL DEFAULT 0,
    SOR_SK INT NOT NULL,
    staging_raw_id_nk INT NOT NULL,
    CreatedAt DATETIME2 DEFAULT SYSUTCDATETIME(),
    UpdatedAt DATETIME2 NULL
);

/* =====================
   DimProducts – SCD2 + delete closing
   ===================== */
IF OBJECT_ID('dbo.DimProducts','U') IS NOT NULL DROP TABLE dbo.DimProducts;
CREATE TABLE dbo.DimProducts (
    Product_SK INT IDENTITY(1,1) PRIMARY KEY,
    ProductID INT NOT NULL,
    ProductName NVARCHAR(255),
    SupplierID INT,
    CategoryID INT,
    QuantityPerUnit NVARCHAR(255),
    UnitPrice DECIMAL(18,2),
    UnitsInStock SMALLINT,
    UnitsOnOrder SMALLINT,
    ReorderLevel SMALLINT,
    Discontinued BIT,
    EffectiveStartDate DATE NOT NULL,
    EffectiveEndDate DATE NULL,
    IsCurrent BIT NOT NULL DEFAULT 1,
    IsDeleted BIT NOT NULL DEFAULT 0,
    SOR_SK INT NOT NULL,
    staging_raw_id_nk INT NOT NULL,
    CreatedAt DATETIME2 DEFAULT SYSUTCDATETIME(),
    UpdatedAt DATETIME2 NULL
);

/* =====================
   DimRegion – SCD4
   ===================== */
IF OBJECT_ID('dbo.DimRegion','U') IS NOT NULL DROP TABLE dbo.DimRegion;
CREATE TABLE dbo.DimRegion (
    Region_SK INT IDENTITY(1,1) PRIMARY KEY,
    RegionID INT NOT NULL,
    RegionDescription NVARCHAR(255),
    SOR_SK INT NOT NULL,
    staging_raw_id_nk INT NOT NULL,
    CreatedAt DATETIME2 DEFAULT SYSUTCDATETIME(),
    UpdatedAt DATETIME2 NULL
);

IF OBJECT_ID('dbo.DimRegion_Hist','U') IS NOT NULL DROP TABLE dbo.DimRegion_Hist;
CREATE TABLE dbo.DimRegion_Hist (
    Region_Hist_SK INT IDENTITY(1,1) PRIMARY KEY,
    RegionID INT NOT NULL,
    RegionDescription NVARCHAR(255),
    ChangeDate DATE NOT NULL,
    ChangeType NVARCHAR(20) NOT NULL
);

/* =====================
   DimShippers – SCD1 + delete
   ===================== */
IF OBJECT_ID('dbo.DimShippers','U') IS NOT NULL DROP TABLE dbo.DimShippers;
CREATE TABLE dbo.DimShippers (
    Shipper_SK INT IDENTITY(1,1) PRIMARY KEY,
    ShipperID INT NOT NULL,
    CompanyName NVARCHAR(255),
    Phone NVARCHAR(50),
    IsDeleted BIT NOT NULL DEFAULT 0,
    SOR_SK INT NOT NULL,
    staging_raw_id_nk INT NOT NULL,
    CreatedAt DATETIME2 DEFAULT SYSUTCDATETIME(),
    UpdatedAt DATETIME2 NULL
);

/* =====================
   DimSuppliers – SCD3
   ===================== */
IF OBJECT_ID('dbo.DimSuppliers','U') IS NOT NULL DROP TABLE dbo.DimSuppliers;
CREATE TABLE dbo.DimSuppliers (
    Supplier_SK INT IDENTITY(1,1) PRIMARY KEY,
    SupplierID INT NOT NULL,
    CompanyName_Current NVARCHAR(255),
    CompanyName_Prior NVARCHAR(255),
    ContactName NVARCHAR(255),
    ContactTitle NVARCHAR(255),
    Address NVARCHAR(255),
    City NVARCHAR(255),
    Region NVARCHAR(255),
    PostalCode NVARCHAR(20),
    Country NVARCHAR(255),
    Phone NVARCHAR(50),
    Fax NVARCHAR(50),
    HomePage NVARCHAR(MAX),
    SOR_SK INT NOT NULL,
    staging_raw_id_nk INT NOT NULL,
    CreatedAt DATETIME2 DEFAULT SYSUTCDATETIME(),
    UpdatedAt DATETIME2 NULL
);

/* =====================
   DimTerritories – SCD4
   ===================== */
IF OBJECT_ID('dbo.DimTerritories','U') IS NOT NULL DROP TABLE dbo.DimTerritories;
CREATE TABLE dbo.DimTerritories (
    Territory_SK INT IDENTITY(1,1) PRIMARY KEY,
    TerritoryID NVARCHAR(20) NOT NULL,
    TerritoryDescription NVARCHAR(255),
    RegionID INT,
    SOR_SK INT NOT NULL,
    staging_raw_id_nk INT NOT NULL,
    CreatedAt DATETIME2 DEFAULT SYSUTCDATETIME(),
    UpdatedAt DATETIME2 NULL
);

IF OBJECT_ID('dbo.DimTerritories_Hist','U') IS NOT NULL DROP TABLE dbo.DimTerritories_Hist;
CREATE TABLE dbo.DimTerritories_Hist (
    Territory_Hist_SK INT IDENTITY(1,1) PRIMARY KEY,
    TerritoryID NVARCHAR(20) NOT NULL,
    TerritoryDescription NVARCHAR(255),
    ChangeDate DATE NOT NULL,
    ChangeType NVARCHAR(20) NOT NULL
);

/* =====================
   FactOrders – INSERT
   ===================== */
IF OBJECT_ID('dbo.FactOrders','U') IS NOT NULL DROP TABLE dbo.FactOrders;
CREATE TABLE dbo.FactOrders (
    OrderFact_SK BIGINT IDENTITY(1,1) PRIMARY KEY,
    OrderID INT NOT NULL,
    OrderDate DATE,
    RequiredDate DATE,
    ShippedDate DATE,
    Freight DECIMAL(18,2),
    Customer_SK INT,
    Employee_SK INT,
    Shipper_SK INT,
    Territory_SK INT,
    Region_SK INT,
    Product_SK INT,
    Category_SK INT,
    Supplier_SK INT,
    Quantity INT,
    UnitPrice DECIMAL(18,2),
    Discount FLOAT,
    SOR_SK INT NOT NULL,
    staging_raw_id_nk INT NOT NULL,
    LoadDate DATETIME2 DEFAULT SYSUTCDATETIME()
);

/* =====================
   FactOrders_Error
   ===================== */
IF OBJECT_ID('dbo.FactOrders_Error','U') IS NOT NULL DROP TABLE dbo.FactOrders_Error;
CREATE TABLE dbo.FactOrders_Error (
    ErrorFact_SK BIGINT IDENTITY(1,1) PRIMARY KEY,
    OrderID INT,
    ProductID INT,
    ErrorReason NVARCHAR(255),
    Customer_SK INT NULL,
    Employee_SK INT NULL,
    Shipper_SK INT NULL,
    Territory_SK INT NULL,
    Region_SK INT NULL,
    Product_SK INT NULL,
    Category_SK INT NULL,
    Supplier_SK INT NULL,
    SOR_SK INT NOT NULL,
    staging_raw_id_nk INT NOT NULL,
    LoadDate DATETIME2 DEFAULT SYSUTCDATETIME()
);
GO
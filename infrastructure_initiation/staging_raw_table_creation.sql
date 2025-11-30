USE ORDER_DDS;
GO

/* =====================
   STAGING: Categories
   ===================== */
IF OBJECT_ID('dbo.stg_Categories_raw', 'U') IS NOT NULL DROP TABLE dbo.stg_Categories_raw;
CREATE TABLE dbo.stg_Categories_raw (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    CategoryID INT,
    CategoryName NVARCHAR(255),
    Description NVARCHAR(MAX),
    LoadDate DATETIME2 DEFAULT SYSUTCDATETIME()
);

/* =====================
   STAGING: Customers
   ===================== */
IF OBJECT_ID('dbo.stg_Customers_raw', 'U') IS NOT NULL DROP TABLE dbo.stg_Customers_raw;
CREATE TABLE dbo.stg_Customers_raw (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID NVARCHAR(10),
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
    LoadDate DATETIME2 DEFAULT SYSUTCDATETIME()
);

/* =====================
   STAGING: Employees
   ===================== */
IF OBJECT_ID('dbo.stg_Employees_raw', 'U') IS NOT NULL DROP TABLE dbo.stg_Employees_raw;
CREATE TABLE dbo.stg_Employees_raw (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeID INT,
    LastName NVARCHAR(255),
    FirstName NVARCHAR(255),
    Title NVARCHAR(255),
    TitleOfCourtesy NVARCHAR(25),
    BirthDate DATETIME,
    HireDate DATETIME,
    Address NVARCHAR(255),
    City NVARCHAR(255),
    Region NVARCHAR(255),
    PostalCode NVARCHAR(20),
    Country NVARCHAR(255),
    HomePhone NVARCHAR(50),
    Extension NVARCHAR(10),
    Notes NVARCHAR(MAX),
    ReportsTo INT,
    PhotoPath NVARCHAR(255),
    LoadDate DATETIME2 DEFAULT SYSUTCDATETIME()
);

/* =====================
   STAGING: Region
   ===================== */
IF OBJECT_ID('dbo.stg_Region_raw', 'U') IS NOT NULL DROP TABLE dbo.stg_Region_raw;
CREATE TABLE dbo.stg_Region_raw (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    RegionID INT,
    RegionDescription NVARCHAR(255),
    LoadDate DATETIME2 DEFAULT SYSUTCDATETIME()
);

/* =====================
   STAGING: Territories
   ===================== */
IF OBJECT_ID('dbo.stg_Territories_raw', 'U') IS NOT NULL DROP TABLE dbo.stg_Territories_raw;
CREATE TABLE dbo.stg_Territories_raw (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    TerritoryID NVARCHAR(20),
    TerritoryDescription NVARCHAR(255),
    RegionID INT,
    LoadDate DATETIME2 DEFAULT SYSUTCDATETIME()
);

/* =====================
   STAGING: Shippers
   ===================== */
IF OBJECT_ID('dbo.stg_Shippers_raw', 'U') IS NOT NULL DROP TABLE dbo.stg_Shippers_raw;
CREATE TABLE dbo.stg_Shippers_raw (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    ShipperID INT,
    CompanyName NVARCHAR(255),
    Phone NVARCHAR(50),
    LoadDate DATETIME2 DEFAULT SYSUTCDATETIME()
);

/* =====================
   STAGING: Suppliers
   ===================== */
IF OBJECT_ID('dbo.stg_Suppliers_raw', 'U') IS NOT NULL DROP TABLE dbo.stg_Suppliers_raw;
CREATE TABLE dbo.stg_Suppliers_raw (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    SupplierID INT,
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
    HomePage NVARCHAR(MAX),
    LoadDate DATETIME2 DEFAULT SYSUTCDATETIME()
);

/* =====================
   STAGING: Products
   ===================== */
IF OBJECT_ID('dbo.stg_Products_raw', 'U') IS NOT NULL DROP TABLE dbo.stg_Products_raw;
CREATE TABLE dbo.stg_Products_raw (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    ProductID INT,
    ProductName NVARCHAR(255),
    SupplierID INT,
    CategoryID INT,
    QuantityPerUnit NVARCHAR(255),
    UnitPrice DECIMAL(18,2),
    UnitsInStock SMALLINT,
    UnitsOnOrder SMALLINT,
    ReorderLevel SMALLINT,
    Discontinued BIT,
    LoadDate DATETIME2 DEFAULT SYSUTCDATETIME()
);

/* =====================
   STAGING: Orders
   ===================== */
IF OBJECT_ID('dbo.stg_Orders_raw', 'U') IS NOT NULL DROP TABLE dbo.stg_Orders_raw;
CREATE TABLE dbo.stg_Orders_raw (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    OrderID INT,
    CustomerID NVARCHAR(10),
    EmployeeID INT,
    OrderDate DATETIME,
    RequiredDate DATETIME,
    ShippedDate DATETIME,
    ShipVia INT,
    Freight DECIMAL(18,2),
    TerritoryID NVARCHAR(20),
    LoadDate DATETIME2 DEFAULT SYSUTCDATETIME()
);

/* =====================
   STAGING: OrderDetails
   ===================== */
IF OBJECT_ID('dbo.stg_OrderDetails_raw', 'U') IS NOT NULL DROP TABLE dbo.stg_OrderDetails_raw;
CREATE TABLE dbo.stg_OrderDetails_raw (
    staging_raw_id_sk INT IDENTITY(1,1) PRIMARY KEY,
    OrderID INT,
    ProductID INT,
    UnitPrice DECIMAL(18,2),
    Quantity INT,
    Discount FLOAT,
    LoadDate DATETIME2 DEFAULT SYSUTCDATETIME()
);

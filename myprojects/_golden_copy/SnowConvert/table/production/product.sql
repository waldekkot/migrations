USE DATABASE AdventureWorks2017;
CREATE OR REPLACE TABLE Production.Product (
    ProductID INT NOT NULL,
    Name STRING NOT NULL,
    ProductNumber NVARCHAR(50) NOT NULL,
    MakeFlag BOOLEAN NOT NULL DEFAULT TRUE,
    FinishedGoodsFlag BOOLEAN NOT NULL DEFAULT TRUE,
    Color NVARCHAR(30) NULL,
    SafetyStockLevel SMALLINT NOT NULL,
    ReorderPoint SMALLINT NOT NULL,
    StandardCost NUMBER(38, 4) NOT NULL,
    ListPrice NUMBER(38, 4) NOT NULL,
    Size NVARCHAR(10) NULL,
    SizeUnitMeasureCode NCHAR(6) NULL,
    WeightUnitMeasureCode NCHAR(6) NULL,
    Weight DECIMAL(8, 2) NULL,
    DaysToManufacture INT NOT NULL,
    ProductLine NCHAR(4) NULL,
    Class NCHAR(4) NULL,
    Style NCHAR(4) NULL,
    ProductSubcategoryID INT NULL,
    ProductModelID INT NULL,
    SellStartDate TIMESTAMP_NTZ(3) NOT NULL,
    SellEndDate TIMESTAMP_NTZ(3) NULL,
    DiscontinuedDate TIMESTAMP_NTZ(3) NULL,
    rowguid VARCHAR NOT NULL DEFAULT (UUID_STRING()),
    ModifiedDate TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3))
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
;
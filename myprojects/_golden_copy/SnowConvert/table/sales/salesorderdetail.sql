USE DATABASE AdventureWorks2017;

CREATE OR REPLACE TABLE Sales.SalesOrderDetail (
    SalesOrderID INT NOT NULL,
    SalesOrderDetailID INT NOT NULL,
    CarrierTrackingNumber NVARCHAR(50) NULL,
    OrderQty SMALLINT NOT NULL,
    ProductID INT NOT NULL,
    SpecialOfferID INT NOT NULL,
    UnitPrice NUMBER(38, 4) NOT NULL,
    UnitPriceDiscount NUMBER(38, 4) NOT NULL DEFAULT ((0.0)),
    LineTotal NUMERIC(38, 6) NOT NULL,
    rowguid VARCHAR NOT NULL DEFAULT (UUID_STRING()),
    ModifiedDate TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3))
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
;
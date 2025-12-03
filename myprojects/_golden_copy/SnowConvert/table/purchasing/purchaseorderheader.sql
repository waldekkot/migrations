USE DATABASE AdventureWorks2017;

CREATE OR REPLACE TABLE Purchasing.PurchaseOrderHeader (
    PurchaseOrderID INT NOT NULL,
    RevisionNumber TINYINT NOT NULL DEFAULT ((0)),
    Status TINYINT NOT NULL DEFAULT ((1)),
    EmployeeID INT NOT NULL,
    VendorID INT NOT NULL,
    ShipMethodID INT NOT NULL,
    OrderDate TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3)),
    ShipDate TIMESTAMP_NTZ(3) NULL,
    SubTotal NUMBER(38, 4) NOT NULL DEFAULT ((0.00)),
    TaxAmt NUMBER(38, 4) NOT NULL DEFAULT ((0.00)),
    Freight NUMBER(38, 4) NOT NULL DEFAULT ((0.00)),
    TotalDue NUMBER(38, 4) NOT NULL,
    ModifiedDate TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3))
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
;
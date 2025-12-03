USE DATABASE AdventureWorks2017;

CREATE OR REPLACE TABLE Purchasing.PurchaseOrderDetail (
    PurchaseOrderID INT NOT NULL,
    PurchaseOrderDetailID INT NOT NULL,
    DueDate TIMESTAMP_NTZ(3) NOT NULL,
    OrderQty SMALLINT NOT NULL,
    ProductID INT NOT NULL,
    UnitPrice NUMBER(38, 4) NOT NULL,
    LineTotal NUMBER(38, 4) NOT NULL,
    ReceivedQty DECIMAL(8, 2) NOT NULL,
    RejectedQty DECIMAL(8, 2) NOT NULL,
    StockedQty DECIMAL(9, 2) NOT NULL,
    ModifiedDate TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3))
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
;
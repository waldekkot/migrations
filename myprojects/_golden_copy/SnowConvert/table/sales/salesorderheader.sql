USE DATABASE AdventureWorks2017;

CREATE OR REPLACE TABLE Sales.SalesOrderHeader (
    SalesOrderID INT NOT NULL,
    RevisionNumber TINYINT NOT NULL DEFAULT ((0)),
    OrderDate TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3)),
    DueDate TIMESTAMP_NTZ(3) NOT NULL,
    ShipDate TIMESTAMP_NTZ(3) NULL,
    Status TINYINT NOT NULL DEFAULT ((1)),
    OnlineOrderFlag BOOLEAN NOT NULL DEFAULT TRUE,
    SalesOrderNumber NVARCHAR(50) NOT NULL,
    PurchaseOrderNumber STRING NULL,
    AccountNumber STRING NULL,
    CustomerID INT NOT NULL,
    SalesPersonID INT NULL,
    TerritoryID INT NULL,
    BillToAddressID INT NOT NULL,
    ShipToAddressID INT NOT NULL,
    ShipMethodID INT NOT NULL,
    CreditCardID INT NULL,
    CreditCardApprovalCode VARCHAR(15) NULL,
    CurrencyRateID INT NULL,
    SubTotal NUMBER(38, 4) NOT NULL DEFAULT ((0.00)),
    TaxAmt NUMBER(38, 4) NOT NULL DEFAULT ((0.00)),
    Freight NUMBER(38, 4) NOT NULL DEFAULT ((0.00)),
    TotalDue NUMBER(38, 4) NOT NULL,
    Comment NVARCHAR(256) NULL,
    rowguid VARCHAR NOT NULL DEFAULT (UUID_STRING()),
    ModifiedDate TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3))
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
;
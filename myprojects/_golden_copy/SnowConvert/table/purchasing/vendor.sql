USE DATABASE AdventureWorks2017;

CREATE OR REPLACE TABLE Purchasing.Vendor (
    BusinessEntityID INT NOT NULL,
    AccountNumber STRING NOT NULL,
    Name STRING NOT NULL,
    CreditRating TINYINT NOT NULL,
    PreferredVendorStatus BOOLEAN NOT NULL DEFAULT TRUE,
    ActiveFlag BOOLEAN NOT NULL DEFAULT TRUE,
    PurchasingWebServiceURL NVARCHAR(2048) NULL,
    ModifiedDate TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3))
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
;
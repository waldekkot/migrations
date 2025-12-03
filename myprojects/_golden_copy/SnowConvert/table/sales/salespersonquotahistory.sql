USE DATABASE AdventureWorks2017;

CREATE OR REPLACE TABLE Sales.SalesPersonQuotaHistory (
    BusinessEntityID INT NOT NULL,
    QuotaDate TIMESTAMP_NTZ(3) NOT NULL,
    SalesQuota NUMBER(38, 4) NOT NULL,
    rowguid VARCHAR NOT NULL DEFAULT (UUID_STRING()),
    ModifiedDate TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3))
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
;
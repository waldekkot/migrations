USE DATABASE AdventureWorks2017;

CREATE OR REPLACE TABLE Sales.SpecialOffer (
    SpecialOfferID INT NOT NULL,
    Description NVARCHAR(510) NOT NULL,
    DiscountPct NUMBER(38, 4) NOT NULL DEFAULT ((0.00)),
    Type NVARCHAR(100) NOT NULL,
    Category NVARCHAR(100) NOT NULL,
    StartDate TIMESTAMP_NTZ(3) NOT NULL,
    EndDate TIMESTAMP_NTZ(3) NOT NULL,
    MinQty INT NOT NULL DEFAULT ((0)),
    MaxQty INT NULL,
    rowguid VARCHAR NOT NULL DEFAULT (UUID_STRING()),
    ModifiedDate TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3))
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
;
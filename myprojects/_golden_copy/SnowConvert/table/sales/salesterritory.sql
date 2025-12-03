USE DATABASE AdventureWorks2017;

CREATE OR REPLACE TABLE Sales.SalesTerritory (
    TerritoryID INT NOT NULL,
    Name STRING NOT NULL,
    CountryRegionCode NVARCHAR(6) NOT NULL,
    "Group" NVARCHAR(100) NOT NULL,
    SalesYTD NUMBER(38, 4) NOT NULL DEFAULT ((0.00)),
    SalesLastYear NUMBER(38, 4) NOT NULL DEFAULT ((0.00)),
    CostYTD NUMBER(38, 4) NOT NULL DEFAULT ((0.00)),
    CostLastYear NUMBER(38, 4) NOT NULL DEFAULT ((0.00)),
    rowguid VARCHAR NOT NULL DEFAULT (UUID_STRING()),
    ModifiedDate TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3))
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
;
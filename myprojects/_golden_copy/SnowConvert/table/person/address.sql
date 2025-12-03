USE DATABASE AdventureWorks2017;

CREATE OR REPLACE TABLE Person.Address (
    AddressID INT NOT NULL,
    AddressLine1 NVARCHAR(120) NOT NULL,
    AddressLine2 NVARCHAR(120) NULL,
    City NVARCHAR(60) NOT NULL,
    StateProvinceID INT NOT NULL,
    PostalCode NVARCHAR(30) NOT NULL,
    SpatialLocation STRING NULL,
    rowguid VARCHAR NOT NULL DEFAULT (UUID_STRING()),
    ModifiedDate TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3))
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
;
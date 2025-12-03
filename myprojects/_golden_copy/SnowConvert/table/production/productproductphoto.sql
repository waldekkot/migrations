USE DATABASE AdventureWorks2017;

CREATE OR REPLACE TABLE Production.ProductProductPhoto (
    ProductID INT NOT NULL,
    ProductPhotoID INT NOT NULL,
    "Primary" BOOLEAN NOT NULL DEFAULT FALSE,
    ModifiedDate TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3))
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'

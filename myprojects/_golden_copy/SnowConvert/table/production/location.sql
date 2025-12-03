USE DATABASE AdventureWorks2017;

CREATE OR REPLACE TABLE Production.Location (
    LocationID SMALLINT NOT NULL,
    Name STRING NOT NULL,
    CostRate NUMBER(38, 4) NOT NULL DEFAULT ((0.00)),
    Availability DECIMAL(8, 2) NOT NULL DEFAULT ((0.00)),
    ModifiedDate TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3))
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
;
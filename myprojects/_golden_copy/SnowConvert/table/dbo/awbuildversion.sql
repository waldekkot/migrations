USE DATABASE AdventureWorks2017;

CREATE OR REPLACE TABLE dbo.AWBuildVersion (
    SystemInformationID TINYINT NOT NULL,
       "Database Version" NVARCHAR(50) NOT NULL,
    VersionDate TIMESTAMP_NTZ(3) NOT NULL,
    ModifiedDate TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3))
   )
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
;
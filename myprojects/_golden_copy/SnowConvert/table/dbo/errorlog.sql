USE DATABASE AdventureWorks2017;

CREATE OR REPLACE TABLE dbo.ErrorLog (
    ErrorLogID INT NOT NULL,
    ErrorTime TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3)),
    UserName VARCHAR(128) NOT NULL,
    ErrorNumber INT NOT NULL,
    ErrorSeverity INT NULL,
    ErrorState INT NULL,
    ErrorProcedure NVARCHAR(252) NULL,
    ErrorLine INT NULL,
    ErrorMessage NVARCHAR(8000) NOT NULL
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
;
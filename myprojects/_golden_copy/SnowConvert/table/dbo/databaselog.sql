USE DATABASE AdventureWorks2017;

CREATE OR REPLACE TABLE dbo.DatabaseLog (
    DatabaseLogID INT NOT NULL,
    PostTime TIMESTAMP_NTZ(3) NOT NULL,
    DatabaseUser VARCHAR(128) NOT NULL,
    Event VARCHAR(128) NOT NULL,
       "Schema" VARCHAR(128) NULL,
    Object VARCHAR(128) NULL,
    TSQL NVARCHAR NOT NULL,
    XmlEvent VARIANT NOT NULL
   )
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
;
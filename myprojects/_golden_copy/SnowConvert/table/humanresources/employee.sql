CREATE OR REPLACE TABLE HumanResources.Employee (
    BusinessEntityID INT NOT NULL,
    NationalIDNumber NVARCHAR(30) NOT NULL,
    LoginID NVARCHAR(512) NOT NULL,
    OrganizationNode VARCHAR NULL,
    OrganizationLevel SMALLINT NULL,
    JobTitle NVARCHAR(100) NOT NULL,
    BirthDate DATE NOT NULL,
    MaritalStatus NCHAR(2) NOT NULL,
    Gender NCHAR(2) NOT NULL,
    HireDate DATE NOT NULL,
    SalariedFlag BOOLEAN NOT NULL DEFAULT TRUE, -- Changed from VARIANT to BOOLEAN (was Flag type in SQL Server)
    VacationHours SMALLINT NOT NULL DEFAULT ((0)),
    SickLeaveHours SMALLINT NOT NULL DEFAULT ((0)),
    CurrentFlag BOOLEAN NOT NULL DEFAULT TRUE, -- Changed from VARIANT to BOOLEAN (was Flag type in SQL Server)
    rowguid VARCHAR NOT NULL DEFAULT (UUID_STRING()),
    ModifiedDate TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3))
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'

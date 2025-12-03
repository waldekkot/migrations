USE DATABASE AdventureWorks2017;

CREATE OR REPLACE TABLE Production.Document (
    DocumentNode VARCHAR NOT NULL,
    DocumentLevel SMALLINT NULL,
    Title NVARCHAR(100) NOT NULL,
    Owner INT NOT NULL,
    FolderFlag BOOLEAN NOT NULL DEFAULT false,
    FileName NVARCHAR(800) NOT NULL,
    FileExtension NVARCHAR(16) NOT NULL,
    Revision NCHAR(10) NOT NULL,
    ChangeNumber INT NOT NULL DEFAULT ((0)),
    Status TINYINT NOT NULL,
    DocumentSummary NVARCHAR NULL,
    Document VARBINARY NULL,
    rowguid VARCHAR NOT NULL DEFAULT (UUID_STRING()),
    ModifiedDate TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3))
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
;
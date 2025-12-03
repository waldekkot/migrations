USE DATABASE AdventureWorks2017;

CREATE OR REPLACE TABLE Production.BillOfMaterials (
    BillOfMaterialsID INT NOT NULL,
    ProductAssemblyID INT NULL,
    ComponentID INT NOT NULL,
    StartDate TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3)),
    EndDate TIMESTAMP_NTZ(3) NULL,
    UnitMeasureCode NCHAR(6) NOT NULL,
    BOMLevel SMALLINT NOT NULL,
    PerAssemblyQty DECIMAL(8, 2) NOT NULL DEFAULT ((1.00)),
    ModifiedDate TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3))
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
;
USE DATABASE AdventureWorks2017;

CREATE OR REPLACE TABLE Production.WorkOrder (
    WorkOrderID INT NOT NULL,
    ProductID INT NOT NULL,
    OrderQty INT NOT NULL,
    StockedQty INT NOT NULL,
    ScrappedQty SMALLINT NOT NULL,
    StartDate TIMESTAMP_NTZ(3) NOT NULL,
    EndDate TIMESTAMP_NTZ(3) NULL,
    DueDate TIMESTAMP_NTZ(3) NOT NULL,
    ScrapReasonID SMALLINT NULL,
    ModifiedDate TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3))
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
;
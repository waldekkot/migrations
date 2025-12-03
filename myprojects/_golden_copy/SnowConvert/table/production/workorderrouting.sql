USE DATABASE AdventureWorks2017;

CREATE OR REPLACE TABLE Production.WorkOrderRouting (
    WorkOrderID INT NOT NULL,
    ProductID INT NOT NULL,
    OperationSequence SMALLINT NOT NULL,
    LocationID SMALLINT NOT NULL,
    ScheduledStartDate TIMESTAMP_NTZ(3) NOT NULL,
    ScheduledEndDate TIMESTAMP_NTZ(3) NOT NULL,
    ActualStartDate TIMESTAMP_NTZ(3) NULL,
    ActualEndDate TIMESTAMP_NTZ(3) NULL,
    ActualResourceHrs DECIMAL(9, 4) NULL,
    PlannedCost NUMBER(38, 4) NOT NULL,
    ActualCost NUMBER(38, 4) NULL,
    ModifiedDate TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3))
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
;
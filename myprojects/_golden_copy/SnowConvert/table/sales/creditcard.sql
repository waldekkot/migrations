USE DATABASE AdventureWorks2017;

CREATE OR REPLACE TABLE Sales.CreditCard (
    CreditCardID INT NOT NULL,
    CardType NVARCHAR(100) NOT NULL,
    CardNumber NVARCHAR(50) NOT NULL,
    ExpMonth TINYINT NOT NULL,
    ExpYear SMALLINT NOT NULL,
    ModifiedDate TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3))
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
;
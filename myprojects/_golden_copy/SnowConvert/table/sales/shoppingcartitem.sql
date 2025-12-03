USE DATABASE AdventureWorks2017;

CREATE OR REPLACE TABLE Sales.ShoppingCartItem (
    ShoppingCartItemID INT NOT NULL,
    ShoppingCartID NVARCHAR(100) NOT NULL,
    Quantity INT NOT NULL DEFAULT ((1)),
    ProductID INT NOT NULL,
    DateCreated TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3)),
    ModifiedDate TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3))
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
;
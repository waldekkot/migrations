USE DATABASE AdventureWorks2017;

CREATE OR REPLACE TABLE Production.ProductReview (
    ProductReviewID INT NOT NULL,
    ProductID INT NOT NULL,
    ReviewerName STRING NOT NULL,
    ReviewDate TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3)),
    EmailAddress NVARCHAR(100) NOT NULL,
    Rating INT NOT NULL,
    Comments NVARCHAR(7700) NULL,
    ModifiedDate TIMESTAMP_NTZ(3) NOT NULL DEFAULT (CURRENT_TIMESTAMP() :: TIMESTAMP_NTZ(3))
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
;
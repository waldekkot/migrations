USE DATABASE AdventureWorks2017;

CREATE OR REPLACE VIEW Production.vProductAndDescription
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
AS
-- View (indexed or standard) to display products and product descriptions by language.
SELECT
    p.ProductID
    ,
    p.Name
    ,
    pm.Name AS ProductModel
    ,
    pmx.CultureID
    ,
    pd.Description
FROM
    Production.Product p
    INNER JOIN
        Production.ProductModel pm
    ON p.ProductModelID = pm.ProductModelID
    INNER JOIN
        Production.ProductModelProductDescriptionCulture pmx
    ON pm.ProductModelID = pmx.ProductModelID
    INNER JOIN
        Production.ProductDescription pd
    ON pmx.ProductDescriptionID = pd.ProductDescriptionID;
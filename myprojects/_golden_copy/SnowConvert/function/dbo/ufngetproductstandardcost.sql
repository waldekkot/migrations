USE DATABASE AdventureWorks2017;

CREATE OR REPLACE FUNCTION dbo.ufnGetProductStandardCost (PRODUCTID INT, ORDERDATE TIMESTAMP_NTZ(3))
RETURNS NUMBER(38, 4)
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
AS
$$
    WITH CTE1 AS
    (

        SELECT
            pch.StandardCost AS STANDARDCOST
        FROM
            Production.Product p
            INNER JOIN
                Production.ProductCostHistory pch
            ON p.ProductID = pch.ProductID
                AND p.ProductID = PRODUCTID
                AND ORDERDATE BETWEEN pch.StartDate AND COALESCE(pch.EndDate, CAST('99991231' AS TIMESTAMP_NTZ(3)))
    )
    SELECT
        STANDARDCOST
    FROM
        CTE1
$$;
USE DATABASE AdventureWorks2017;

CREATE OR REPLACE FUNCTION dbo.ufnGetProductDealerPrice (PRODUCTID INT, ORDERDATE TIMESTAMP_NTZ(3))
RETURNS NUMBER(38, 4)
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
AS
$$
    WITH CTE1 AS
    (
        SELECT
            0.60  -- 60% of list price
            AS DEALERDISCOUNT
    ),
    CTE2 AS
    (
        SELECT
            plph.ListPrice * (
                SELECT
                    DEALERDISCOUNT
                FROM
                    CTE1
            ) AS DEALERPRICE
        FROM
            Production.Product p
            INNER JOIN
                Production.ProductListPriceHistory plph
            ON p.ProductID = plph.ProductID
                AND p.ProductID = PRODUCTID
                AND ORDERDATE BETWEEN plph.StartDate AND COALESCE(plph.EndDate, CAST('99991231' AS TIMESTAMP_NTZ(3)))
    )
    SELECT
        DEALERPRICE
    FROM
        CTE2
$$;
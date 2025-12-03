USE DATABASE AdventureWorks2017;

CREATE OR REPLACE FUNCTION dbo.ufnGetStock (PRODUCTID INT)
RETURNS INT
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
AS
$$
    WITH CTE1 AS
    (

        SELECT
            SUM(p.Quantity) AS RET
        FROM
            Production.ProductInventory p
        WHERE
            p.ProductID = PRODUCTID
            AND p.LocationID = '6'
    ),
    CTE2 AS
    (
        SELECT
            CASE
                WHEN ((
                    SELECT
                        RET
                    FROM
                        CTE1
                ) IS NULL)
                    THEN 0
            END AS RET
    )
    SELECT
        RET
    FROM
        CTE2
$$;
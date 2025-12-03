USE DATABASE AdventureWorks2017;

CREATE OR REPLACE PROCEDURE dbo.uspGetBillOfMaterials (
    STARTPRODUCTID INT, 
    CHECKDATE TIMESTAMP_NTZ(3)
)
RETURNS TABLE (
    ProductAssemblyID INT,
    ComponentID INT,
    ComponentDesc VARCHAR,
    TotalQuantity DECIMAL(8,2),
    StandardCost DECIMAL(19,4),
    ListPrice DECIMAL(19,4),
    BOMLevel SMALLINT,
    RecursionLevel INT
)
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
EXECUTE AS CALLER
AS
$$
DECLARE
    res RESULTSET;
BEGIN
    -- Use recursive query to generate a multi-level Bill of Material (i.e. all level 1 
    -- components of a level 0 assembly, all level 2 components of a level 1 assembly)
    -- The CheckDate eliminates any components that are no longer used in the product on this date.
    
    res := (
        WITH RECURSIVE BOM_cte (
            ProductAssemblyID,
            ComponentID,
            ComponentDesc,
            PerAssemblyQty,
            StandardCost,
            ListPrice,
            BOMLevel,
            RecursionLevel
        ) AS (
            -- Anchor member: Get the initial list of components for the bike assembly
            SELECT
                b.ProductAssemblyID,
                b.ComponentID,
                p.Name,
                b.PerAssemblyQty,
                p.StandardCost,
                p.ListPrice,
                b.BOMLevel,
                0 AS RecursionLevel
            FROM
                Production.BillOfMaterials b
                INNER JOIN Production.Product p
                    ON b.ComponentID = p.ProductID
            WHERE 
                b.ProductAssemblyID = :STARTPRODUCTID
                AND :CHECKDATE >= b.StartDate
                AND :CHECKDATE <= IFNULL(b.EndDate, :CHECKDATE)
            
            UNION ALL
            
            -- Recursive member: Join recursive member to anchor
            SELECT
                b.ProductAssemblyID,
                b.ComponentID,
                p.Name,
                b.PerAssemblyQty,
                p.StandardCost,
                p.ListPrice,
                b.BOMLevel,
                cte.RecursionLevel + 1
            FROM
                BOM_cte cte
                INNER JOIN Production.BillOfMaterials b
                    ON b.ProductAssemblyID = cte.ComponentID
                INNER JOIN Production.Product p
                    ON b.ComponentID = p.ProductID
            WHERE 
                :CHECKDATE >= b.StartDate
                AND :CHECKDATE <= IFNULL(b.EndDate, :CHECKDATE)
                AND cte.RecursionLevel < 25  -- Equivalent to MAXRECURSION 25
        )
        -- Outer select from the CTE
        SELECT 
            b.ProductAssemblyID,
            b.ComponentID,
            b.ComponentDesc,
            SUM(b.PerAssemblyQty) AS TotalQuantity,
            b.StandardCost,
            b.ListPrice,
            b.BOMLevel,
            b.RecursionLevel
        FROM BOM_cte b
        GROUP BY 
            b.ComponentID,
            b.ComponentDesc,
            b.ProductAssemblyID,
            b.BOMLevel,
            b.RecursionLevel,
            b.StandardCost,
            b.ListPrice
        ORDER BY 
            b.BOMLevel,
            b.ProductAssemblyID,
            b.ComponentID
    );
    
    RETURN TABLE(res);
END;
$$;

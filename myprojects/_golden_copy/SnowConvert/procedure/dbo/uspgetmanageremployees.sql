CREATE OR REPLACE PROCEDURE dbo.uspGetManagerEmployees (BUSINESSENTITYID INT)
RETURNS TABLE(
    RecursionLevel INT,
    OrganizationNode VARCHAR,
    ManagerFirstName VARCHAR,
    ManagerLastName VARCHAR,
    BusinessEntityID INT,
    FirstName VARCHAR,
    LastName VARCHAR
)
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    LET res RESULTSET := (
        WITH RECURSIVE EMP_cte (
            BusinessEntityID,
            OrganizationNode,
            FirstName,
            LastName,
            RecursionLevel
        ) AS (
            SELECT
                e.BusinessEntityID,
                e.OrganizationNode,
                p.FirstName,
                p.LastName, 
                0
            FROM
                HumanResources.Employee e
            INNER JOIN Person.Person p
                ON p.BusinessEntityID = e.BusinessEntityID
            WHERE
                e.BusinessEntityID = :BUSINESSENTITYID
            UNION ALL
            SELECT
                e.BusinessEntityID,
                e.OrganizationNode,
                p.FirstName,
                p.LastName,
                RecursionLevel + 1
            FROM
                HumanResources.Employee e
            INNER JOIN EMP_cte
                ON (
                    -- Handle NULL values with COALESCE or explicit NULL check
                    (EMP_cte.OrganizationNode IS NOT NULL 
                     AND PUBLIC.HIERARCHY_GET_ANCESTOR_UDF(e.OrganizationNode, 1) = EMP_cte.OrganizationNode)
                    OR
                    (EMP_cte.OrganizationNode IS NULL 
                     AND PUBLIC.HIERARCHY_GET_ANCESTOR_UDF(e.OrganizationNode, 1) IS NULL)
                )
            INNER JOIN Person.Person p
                ON p.BusinessEntityID = e.BusinessEntityID
            WHERE RecursionLevel < 25
        )
        SELECT
            EMP_cte.RecursionLevel,
            COALESCE(EMP_cte.OrganizationNode, 'ROOT') AS OrganizationNode,
            mgr_p.FirstName AS ManagerFirstName,
            mgr_p.LastName AS ManagerLastName,
            EMP_cte.BusinessEntityID,
            EMP_cte.FirstName,
            EMP_cte.LastName
        FROM
            EMP_cte
        LEFT JOIN HumanResources.Employee mgr_e
            ON (
                (EMP_cte.OrganizationNode IS NOT NULL 
                 AND PUBLIC.HIERARCHY_GET_ANCESTOR_UDF(EMP_cte.OrganizationNode, 1) = mgr_e.OrganizationNode)
                OR
                (EMP_cte.OrganizationNode IS NULL 
                 AND PUBLIC.HIERARCHY_GET_ANCESTOR_UDF(EMP_cte.OrganizationNode, 1) IS NULL)
            )
        LEFT JOIN Person.Person mgr_p
            ON mgr_p.BusinessEntityID = mgr_e.BusinessEntityID
        ORDER BY RecursionLevel, EMP_cte.OrganizationNode
    );
    RETURN TABLE(res);
END;
$$;

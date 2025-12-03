USE DATABASE AdventureWorks2017;

CREATE OR REPLACE PROCEDURE dbo.uspGetEmployeeManagers (BUSINESSENTITYID INT)
RETURNS TABLE (
    RecursionLevel INT,
    BusinessEntityID INT,
    FirstName VARCHAR,
    LastName VARCHAR,
    OrganizationNode VARCHAR,
    ManagerFirstName VARCHAR,
    ManagerLastName VARCHAR
)
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
EXECUTE AS CALLER
AS
$$
DECLARE
    res RESULTSET;
BEGIN
    res := (
        WITH RECURSIVE EMP_cte (
            BusinessEntityID,
            OrganizationNode,
            FirstName,
            LastName,
            JobTitle,
            RecursionLevel
        ) AS (
            -- Anchor member: Get the initial Employee
            SELECT
                e.BusinessEntityID,
                e.OrganizationNode,
                p.FirstName,
                p.LastName,
                e.JobTitle,
                0 AS RecursionLevel
            FROM
                HumanResources.Employee e
            INNER JOIN Person.Person AS p
                ON p.BusinessEntityID = e.BusinessEntityID
            WHERE
                e.BusinessEntityID = :BUSINESSENTITYID
                
            UNION ALL
            
            -- Recursive member: Get managers up the hierarchy
            SELECT
                e.BusinessEntityID,
                e.OrganizationNode,
                p.FirstName,
                p.LastName,
                e.JobTitle,
                EMP_cte.RecursionLevel + 1
            FROM
                HumanResources.Employee e
            INNER JOIN EMP_cte
                ON e.OrganizationNode = 
                    CASE 
                        -- Handle empty/null OrganizationNode (CEO case)
                        WHEN EMP_cte.OrganizationNode IS NULL OR EMP_cte.OrganizationNode = '' THEN NULL
                        -- Get parent node by removing last segment: /1/2/3/ -> /1/2/
                        WHEN EMP_cte.OrganizationNode LIKE '%/%/%' THEN 
                            RTRIM(SUBSTRING(EMP_cte.OrganizationNode, 1, 
                                LENGTH(EMP_cte.OrganizationNode) - 
                                POSITION('/' IN REVERSE(SUBSTRING(EMP_cte.OrganizationNode, 1, LENGTH(EMP_cte.OrganizationNode) - 1))) - 1
                            ), '/') || '/'
                        -- If only one level deep /1/ -> empty string (CEO)
                        ELSE ''
                    END
            INNER JOIN Person.Person p
                ON p.BusinessEntityID = e.BusinessEntityID
            WHERE EMP_cte.RecursionLevel < 25
        )
        -- Join back to Employee to return the manager name
        SELECT
            EMP_cte.RecursionLevel,
            EMP_cte.BusinessEntityID,
            EMP_cte.FirstName,
            EMP_cte.LastName,
            COALESCE(EMP_cte.OrganizationNode, '') AS OrganizationNode,
            p.FirstName AS ManagerFirstName,
            p.LastName AS ManagerLastName
        FROM EMP_cte
        INNER JOIN HumanResources.Employee e
            ON COALESCE(e.OrganizationNode, '') = 
                CASE 
                    WHEN EMP_cte.OrganizationNode IS NULL OR EMP_cte.OrganizationNode = '' THEN NULL
                    WHEN EMP_cte.OrganizationNode LIKE '%/%/%' THEN 
                        RTRIM(SUBSTRING(EMP_cte.OrganizationNode, 1, 
                            LENGTH(EMP_cte.OrganizationNode) - 
                            POSITION('/' IN REVERSE(SUBSTRING(EMP_cte.OrganizationNode, 1, LENGTH(EMP_cte.OrganizationNode) - 1))) - 1
                        ), '/') || '/'
                    ELSE ''
                END
        INNER JOIN Person.Person p
            ON p.BusinessEntityID = e.BusinessEntityID
        WHERE EMP_cte.OrganizationNode IS NOT NULL AND EMP_cte.OrganizationNode != ''
        ORDER BY EMP_cte.RecursionLevel, EMP_cte.OrganizationNode
    );
    
    RETURN TABLE(res);
END;
$$;

CREATE OR REPLACE FUNCTION dbo.ufnGetContactInformation(PERSONID INT)
RETURNS TABLE
(
    PersonID INT,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    JobTitle VARCHAR(50),
    BusinessEntityType VARCHAR(50)
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": { "major": 2, "minor": 0, "patch": "34.0" }, "attributes": { "component": "transact", "convertedOn": "12/02/2025", "domain": "no-domain-provided", "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
AS
$$
    -- Employee records
    SELECT 
        PERSONID AS PersonID,
        p.FirstName,
        p.LastName,
        e.JobTitle,
        'Employee' AS BusinessEntityType
    FROM HumanResources.Employee AS e
    INNER JOIN Person.Person p
        ON p.BusinessEntityID = e.BusinessEntityID
    WHERE e.BusinessEntityID = PERSONID
    
    UNION ALL
    
    -- Vendor Contact records
    SELECT 
        PERSONID AS PersonID,
        p.FirstName,
        p.LastName,
        ct.Name AS JobTitle,
        'Vendor Contact' AS BusinessEntityType
    FROM Purchasing.Vendor AS v
    INNER JOIN Person.BusinessEntityContact bec
        ON bec.BusinessEntityID = v.BusinessEntityID
    INNER JOIN Person.ContactType ct
        ON ct.ContactTypeID = bec.ContactTypeID
    INNER JOIN Person.Person p
        ON p.BusinessEntityID = bec.PersonID
    WHERE bec.PersonID = PERSONID
    
    UNION ALL
    
    -- Store Contact records
    SELECT 
        PERSONID AS PersonID,
        p.FirstName,
        p.LastName,
        ct.Name AS JobTitle,
        'Store Contact' AS BusinessEntityType
    FROM Sales.Store AS s
    INNER JOIN Person.BusinessEntityContact bec
        ON bec.BusinessEntityID = s.BusinessEntityID
    INNER JOIN Person.ContactType ct
        ON ct.ContactTypeID = bec.ContactTypeID
    INNER JOIN Person.Person p
        ON p.BusinessEntityID = bec.PersonID
    WHERE bec.PersonID = PERSONID
    
    UNION ALL
    
    -- Consumer records
    SELECT 
        PERSONID AS PersonID,
        p.FirstName,
        p.LastName,
        NULL AS JobTitle,
        'Consumer' AS BusinessEntityType
    FROM Person.Person AS p
    INNER JOIN Sales.Customer AS c
        ON c.PersonID = p.BusinessEntityID
    WHERE p.BusinessEntityID = PERSONID 
        AND c.StoreID IS NULL
$$;

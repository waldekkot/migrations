USE DATABASE AdventureWorks2017;

CREATE OR REPLACE VIEW Sales.vSalesPersonSalesByFiscalYears
COMMENT = '{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 2,  \"minor\": 0,  \"patch\": \"34.0\" }, \"attributes\": {  \"component\": \"transact\",  \"convertedOn\": \"12/02/2025\",  \"domain\": \"no-domain-provided\",  \"migrationid\": \"5d+aAa3cv3OCsNech69fjw==\" }}'
AS
SELECT *
FROM (
    SELECT
        soh.SalesPersonID,
        p.FirstName || ' ' || COALESCE(p.MiddleName, '') || ' ' || p.LastName AS FullName,
        e.JobTitle,
        st.Name AS SalesTerritory,
        soh.SubTotal,
        YEAR(DATEADD(month, 6, soh.OrderDate)::TIMESTAMP) AS FiscalYear
    FROM Sales.SalesPerson sp
    INNER JOIN Sales.SalesOrderHeader soh ON sp.BusinessEntityID = soh.SalesPersonID
    INNER JOIN Sales.SalesTerritory st ON sp.TerritoryID = st.TerritoryID
    INNER JOIN HumanResources.Employee e ON soh.SalesPersonID = e.BusinessEntityID
    INNER JOIN Person.Person p ON p.BusinessEntityID = sp.BusinessEntityID
) AS soh
PIVOT (
    SUM(SubTotal)
    FOR FiscalYear IN (ANY ORDER BY FiscalYear)
) AS pvt;
USE DATABASE AdventureWorks2017;

CREATE OR REPLACE VIEW Sales.vSalesPerson
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
AS
SELECT
	s.BusinessEntityID
    ,
	p.Title
    ,
	p.FirstName
    ,
	p.MiddleName
    ,
	p.LastName
    ,
	p.Suffix
    ,
	e.JobTitle
    ,
	pp.PhoneNumber
	,
	pnt.Name AS PhoneNumberType
    ,
	ea.EmailAddress
    ,
	p.EmailPromotion
    ,
	a.AddressLine1
    ,
	a.AddressLine2
    ,
	a.City
    ,
	sp.Name AS StateProvinceName
    ,
	a.PostalCode
    ,
	cr.Name AS CountryRegionName
    ,
	st.Name AS TerritoryName
    ,
	st."Group" AS TerritoryGroup
    ,
	s.SalesQuota
    ,
	s.SalesYTD
    ,
	s.SalesLastYear
FROM
	Sales.SalesPerson s
    INNER JOIN
		HumanResources.Employee e
    ON e.BusinessEntityID = s.BusinessEntityID
	INNER JOIN
		Person.Person p
	ON p.BusinessEntityID = s.BusinessEntityID
    INNER JOIN
		Person.BusinessEntityAddress bea
    ON bea.BusinessEntityID = s.BusinessEntityID
    INNER JOIN
		Person.Address a
    ON a.AddressID = bea.AddressID
    INNER JOIN
		Person.StateProvince sp
    ON sp.StateProvinceID = a.StateProvinceID
    INNER JOIN
		Person.CountryRegion cr
    ON cr.CountryRegionCode = sp.CountryRegionCode
    LEFT OUTER JOIN
		Sales.SalesTerritory st
    ON st.TerritoryID = s.TerritoryID
	LEFT OUTER JOIN
		Person.EmailAddress ea
	ON ea.BusinessEntityID = p.BusinessEntityID
	LEFT OUTER JOIN
		Person.PersonPhone pp
	ON pp.BusinessEntityID = p.BusinessEntityID
	LEFT OUTER JOIN
		Person.PhoneNumberType pnt
	ON pnt.PhoneNumberTypeID = pp.PhoneNumberTypeID;
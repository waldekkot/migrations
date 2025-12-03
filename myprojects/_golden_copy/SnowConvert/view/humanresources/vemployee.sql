USE DATABASE AdventureWorks2017;

CREATE OR REPLACE VIEW HumanResources.vEmployee
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
AS
SELECT
	e.BusinessEntityID
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
	p.AdditionalContactInfo
FROM
	HumanResources.Employee e
	INNER JOIN
		Person.Person p
	ON p.BusinessEntityID = e.BusinessEntityID
    INNER JOIN
		Person.BusinessEntityAddress bea
    ON bea.BusinessEntityID = e.BusinessEntityID
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
		Person.PersonPhone pp
    ON pp.BusinessEntityID = p.BusinessEntityID
    LEFT OUTER JOIN
		Person.PhoneNumberType pnt
    ON pp.PhoneNumberTypeID = pnt.PhoneNumberTypeID
    LEFT OUTER JOIN
		Person.EmailAddress ea
    ON p.BusinessEntityID = ea.BusinessEntityID;
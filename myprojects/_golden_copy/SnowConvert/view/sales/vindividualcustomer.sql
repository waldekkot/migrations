USE DATABASE AdventureWorks2017;

CREATE OR REPLACE VIEW Sales.vIndividualCustomer
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
AS
SELECT
	p.BusinessEntityID
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
	pp.PhoneNumber
	,
	pnt.Name AS PhoneNumberType
    ,
	ea.EmailAddress
    ,
	p.EmailPromotion
    ,
	at.Name AS AddressType
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
	p.Demographics
FROM
	Person.Person p
    INNER JOIN
		Person.BusinessEntityAddress bea
    ON bea.BusinessEntityID = p.BusinessEntityID
    INNER JOIN
		Person.Address a
    ON a.AddressID = bea.AddressID
    INNER JOIN
		Person.StateProvince sp
    ON sp.StateProvinceID = a.StateProvinceID
    INNER JOIN
		Person.CountryRegion cr
    ON cr.CountryRegionCode = sp.CountryRegionCode
    INNER JOIN
		Person.AddressType at
    ON at.AddressTypeID = bea.AddressTypeID
	INNER JOIN
		Sales.Customer c
	ON c.PersonID = p.BusinessEntityID
	LEFT OUTER JOIN
		Person.EmailAddress ea
	ON ea.BusinessEntityID = p.BusinessEntityID
	LEFT OUTER JOIN
		Person.PersonPhone pp
	ON pp.BusinessEntityID = p.BusinessEntityID
	LEFT OUTER JOIN
		Person.PhoneNumberType pnt
	ON pnt.PhoneNumberTypeID = pp.PhoneNumberTypeID
WHERE
	c.StoreID IS NULL;
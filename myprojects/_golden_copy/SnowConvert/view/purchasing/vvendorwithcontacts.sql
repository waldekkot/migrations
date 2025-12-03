USE DATABASE AdventureWorks2017;

CREATE OR REPLACE VIEW Purchasing.vVendorWithContacts
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
AS
SELECT
	v.BusinessEntityID
    ,
	v.Name
    ,
	ct.Name AS ContactType
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
FROM
	Purchasing.Vendor v
    INNER JOIN
		Person.BusinessEntityContact bec
    ON bec.BusinessEntityID = v.BusinessEntityID
	INNER JOIN
		Person.ContactType ct
	ON ct.ContactTypeID = bec.ContactTypeID
	INNER JOIN
		Person.Person p
	ON p.BusinessEntityID = bec.PersonID
	LEFT OUTER JOIN
		Person.EmailAddress ea
	ON ea.BusinessEntityID = p.BusinessEntityID
	LEFT OUTER JOIN
		Person.PersonPhone pp
	ON pp.BusinessEntityID = p.BusinessEntityID
	LEFT OUTER JOIN
		Person.PhoneNumberType pnt
	ON pnt.PhoneNumberTypeID = pp.PhoneNumberTypeID;
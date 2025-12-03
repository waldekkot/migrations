USE DATABASE AdventureWorks2017;

CREATE OR REPLACE VIEW Purchasing.vVendorWithAddresses
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
AS
SELECT
    v.BusinessEntityID
    ,
    v.Name
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
FROM
    Purchasing.Vendor v
    INNER JOIN
        Person.BusinessEntityAddress bea
    ON bea.BusinessEntityID = v.BusinessEntityID
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
    ON at.AddressTypeID = bea.AddressTypeID;
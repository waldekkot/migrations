USE DATABASE AdventureWorks2017;

CREATE OR REPLACE VIEW Person.vStateProvinceCountryRegion
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
AS
SELECT
    sp.StateProvinceID
    ,
    sp.StateProvinceCode
    ,
    sp.IsOnlyStateProvinceFlag
    ,
    sp.Name AS StateProvinceName
    ,
    sp.TerritoryID
    ,
    cr.CountryRegionCode
    ,
    cr.Name AS CountryRegionName
FROM
    Person.StateProvince sp
    INNER JOIN
        Person.CountryRegion cr
    ON sp.CountryRegionCode = cr.CountryRegionCode;
USE DATABASE AdventureWorks2017;

CREATE OR REPLACE VIEW HumanResources.vEmployeeDepartment
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
	d.Name AS Department
    ,
	d.GroupName
    ,
	edh.StartDate
FROM
	HumanResources.Employee e
	INNER JOIN
		Person.Person p
	ON p.BusinessEntityID = e.BusinessEntityID
    INNER JOIN
		HumanResources.EmployeeDepartmentHistory edh
    ON e.BusinessEntityID = edh.BusinessEntityID
    INNER JOIN
		HumanResources.Department d
    ON edh.DepartmentID = d.DepartmentID
WHERE
	edh.EndDate IS NULL;
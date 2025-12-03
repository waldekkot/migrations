USE DATABASE AdventureWorks2017;


CREATE OR REPLACE PROCEDURE HumanResources.uspUpdateEmployeeLogin (
    BUSINESSENTITYID INT, 
    ORGANIZATIONNODE VARCHAR, 
    LOGINID VARCHAR(256), 
    JOBTITLE VARCHAR(50), 
    HIREDATE TIMESTAMP_NTZ(3), 
    CURRENTFLAG BOOLEAN
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
EXECUTE AS CALLER
AS
$$
    BEGIN
        BEGIN
            UPDATE HumanResources.Employee
            SET
                OrganizationNode = :ORGANIZATIONNODE,
                LoginID = :LOGINID,
                JobTitle = :JOBTITLE,
                HireDate = :HIREDATE,
                CurrentFlag = :CURRENTFLAG
            WHERE
                BusinessEntityID = :BUSINESSENTITYID;
        EXCEPTION
            WHEN OTHER THEN
                CALL dbo.uspLogError();
        END;
    END;
$$;

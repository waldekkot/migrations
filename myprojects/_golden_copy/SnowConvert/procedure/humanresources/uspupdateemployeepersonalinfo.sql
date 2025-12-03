USE DATABASE AdventureWorks2017;

CREATE OR REPLACE PROCEDURE HumanResources.uspUpdateEmployeePersonalInfo (
    BUSINESSENTITYID INT, 
    NATIONALIDNUMBER STRING, 
    BIRTHDATE TIMESTAMP_NTZ(3), 
    MARITALSTATUS STRING, 
    GENDER STRING
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
                NationalIDNumber = :NATIONALIDNUMBER,
                BirthDate = :BIRTHDATE,
                MaritalStatus = :MARITALSTATUS,
                Gender = :GENDER
            WHERE
                BusinessEntityID = :BUSINESSENTITYID;
        EXCEPTION
            WHEN OTHER THEN
                CALL dbo.uspLogError();
        END;
    END;
$$;

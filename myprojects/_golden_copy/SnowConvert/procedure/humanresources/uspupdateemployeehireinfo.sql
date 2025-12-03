USE DATABASE AdventureWorks2017;

CREATE OR REPLACE PROCEDURE HumanResources.uspUpdateEmployeeHireInfo (
    BUSINESSENTITYID INT, 
    JOBTITLE STRING, 
    HIREDATE TIMESTAMP_NTZ(3), 
    RATECHANGEDATE TIMESTAMP_NTZ(3), 
    RATE NUMBER(38, 4), 
    PAYFREQUENCY TINYINT, 
    CURRENTFLAG BOOLEAN
)
RETURNS STRING
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
EXECUTE AS CALLER
AS
$$
DECLARE
    result STRING DEFAULT 'SUCCESS';
BEGIN
    BEGIN TRANSACTION;
    
    UPDATE HumanResources.Employee
    SET
        JobTitle = :JOBTITLE,
        HireDate = :HIREDATE,
        CurrentFlag = :CURRENTFLAG
    WHERE
        BusinessEntityID = :BUSINESSENTITYID;
    
    INSERT INTO HumanResources.EmployeePayHistory (
        BusinessEntityID, 
        RateChangeDate, 
        Rate, 
        PayFrequency
    )
    VALUES (
        :BUSINESSENTITYID, 
        :RATECHANGEDATE, 
        :RATE, 
        :PAYFREQUENCY
    );
    
    COMMIT;
    RETURN result;
    
EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        -- Log error (assuming uspLogError exists and is converted)
        CALL dbo.uspLogError();
        result := 'ERROR: ' || SQLERRM;
        RETURN result;
END;
$$;

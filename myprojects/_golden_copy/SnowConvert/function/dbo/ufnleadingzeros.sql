USE DATABASE AdventureWorks2017;

CREATE OR REPLACE FUNCTION dbo.ufnLeadingZeros (VALUE INT)
RETURNS VARCHAR(8)
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
AS
$$
    DECLARE
        RETURNVALUE VARCHAR(8);
    BEGIN
         
        RETURNVALUE := CAST(:VALUE AS VARCHAR(8));
        RETURNVALUE := REPEAT('0', 8 - OCTET_LENGTH(:RETURNVALUE)) || :RETURNVALUE;
        RETURN (:RETURNVALUE);
    END;
$$;
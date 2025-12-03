USE DATABASE AdventureWorks2017;

CREATE OR REPLACE FUNCTION dbo.ufnGetAccountingStartDate ()
RETURNS TIMESTAMP_NTZ(3)
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
AS
$$
    BEGIN
        RETURN CAST('20030701' AS TIMESTAMP_NTZ(3));
    END;
$$;
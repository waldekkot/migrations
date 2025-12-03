USE DATABASE AdventureWorks2017;

CREATE OR REPLACE FUNCTION dbo.ufnGetPurchaseOrderStatusText (STATUS TINYINT)
RETURNS NVARCHAR(15)
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
-- Returns the sales order status text representation for the status value.
AS
$$
    DECLARE
        RET NVARCHAR(15);
    BEGIN
         
        CASE (:STATUS)
            WHEN 1 THEN
                RET := 'Pending';
            WHEN 2 THEN
                RET := 'Approved';
            WHEN 3 THEN
                RET := 'Rejected';
            WHEN 4 THEN
                RET := 'Complete';
            ELSE
                RET := '** Invalid **';
        END;
        RETURN :RET;
    END;
$$;
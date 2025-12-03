USE DATABASE AdventureWorks2017;
-- uspPrintError prints error information about the error that caused 
-- execution to jump to the CATCH block of a TRY...CATCH construct. 
-- Should be executed from within the scope of a CATCH block otherwise 
-- it will return without printing any error information.
CREATE OR REPLACE PROCEDURE ADVENTUREWORKS2017.DBO.USPPRINTERROR()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Prints error information - can only be used in exception context'
EXECUTE AS CALLER
AS '
BEGIN
    RETURN ''USPPRINTERROR should be called from within an exception handler. Use SQLCODE and SQLERRM in EXCEPTION block.'';
END;
';
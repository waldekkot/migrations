USE DATABASE AdventureWorks2017;

-- ============================================================
-- SNOWFLAKE: Complete iuPerson Trigger as Stored Procedure
-- ============================================================

CREATE OR REPLACE PROCEDURE ADVENTUREWORKS2017.PERSON.SP_IUPERSON_TRIGGER()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_default_demographics VARCHAR DEFAULT ''<IndividualSurvey xmlns="http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/IndividualSurvey"><TotalPurchaseYTD>0.00</TotalPurchaseYTD></IndividualSurvey>'';
    v_rows_null NUMBER DEFAULT 0;
    v_rows_missing NUMBER DEFAULT 0;
    v_rows_total NUMBER DEFAULT 0;
BEGIN
    -- ============================================================
    -- Replicates SQL Server Trigger: [Person].[iuPerson]
    -- AFTER INSERT, UPDATE on [Person].[Person]
    -- 
    -- Logic:
    -- 1. If Demographics IS NULL -> Set default XML with TotalPurchaseYTD=0.00
    -- 2. If Demographics exists but missing TotalPurchaseYTD -> Add it
    -- 3. If Demographics already has TotalPurchaseYTD -> Leave unchanged
    -- ============================================================

    -- Check if stream has data (equivalent to @@ROWCOUNT check)
    IF (NOT SYSTEM$STREAM_HAS_DATA(''Person.Person_Stream'')) THEN
        RETURN ''No changes to process'';
    END IF;

    -- CASE 1: Demographics IS NULL
    -- Set to default XML with TotalPurchaseYTD = 0.00
    UPDATE Person.Person t
    SET 
        Demographics = :v_default_demographics,
        ModifiedDate = CURRENT_TIMESTAMP()
    WHERE t.BusinessEntityID IN (
        SELECT s.BusinessEntityID 
        FROM Person.Person_Stream s
        WHERE s.METADATA$ACTION = ''INSERT''
          AND s.Demographics IS NULL
    );
    
    v_rows_null := SQLROWCOUNT;

    -- CASE 2: Demographics exists but TotalPurchaseYTD is missing
    -- Add TotalPurchaseYTD as first child element
    UPDATE Person.Person t
    SET 
        Demographics = 
            SUBSTR(t.Demographics, 1, POSITION(''>'' IN t.Demographics)) 
            || ''<TotalPurchaseYTD>0.00</TotalPurchaseYTD>'' 
            || SUBSTR(t.Demographics, POSITION(''>'' IN t.Demographics) + 1),
        ModifiedDate = CURRENT_TIMESTAMP()
    WHERE t.BusinessEntityID IN (
        SELECT s.BusinessEntityID 
        FROM Person.Person_Stream s
        WHERE s.METADATA$ACTION = ''INSERT''
          AND s.Demographics IS NOT NULL
          AND POSITION(''TotalPurchaseYTD'' IN s.Demographics) = 0
    );
    
    v_rows_missing := SQLROWCOUNT;
    
    v_rows_total := v_rows_null + v_rows_missing;

    RETURN ''Trigger executed: '' || v_rows_total::VARCHAR || '' rows processed ('' ||
           v_rows_null::VARCHAR || '' null demographics, '' || 
           v_rows_missing::VARCHAR || '' missing TotalPurchaseYTD)'';
END;
';

-- ============================================================
-- Create Stream (if not exists)
-- ============================================================

CREATE STREAM IF NOT EXISTS Person.Person_Stream
    ON TABLE Person.Person;

-- ============================================================
-- Create Task to auto-execute the procedure
-- ============================================================

CREATE OR REPLACE TASK Person.Task_iuPerson
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    TARGET_COMPLETION_INTERVAL = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('Person.Person_Stream')
AS
    CALL Person.sp_iuPerson_Trigger();

-- Resume the task
ALTER TASK Person.Task_iuPerson RESUME;


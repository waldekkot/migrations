USE DATABASE AdventureWorks2017;

-- ============================================================
-- SNOWFLAKE: dEmployee Trigger Equivalent
-- Replicates SQL Server INSTEAD OF DELETE trigger behavior
-- ============================================================
-- Original SQL Server Trigger:
--   CREATE TRIGGER HumanResources.dEmployee ON HumanResources.[Employee]
--   INSTEAD OF DELETE NOT FOR REPLICATION AS
--   BEGIN
--       RAISERROR(N'Employees cannot be deleted. They can only be marked as not current.', 10, 1);
--       IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
--   END;
-- ============================================================
-- Snowflake Approach:
--   Since Snowflake doesn't support INSTEAD OF triggers, we:
--   1. Create a Stream to capture DELETE operations
--   2. Create a Procedure that re-inserts deleted rows
--   3. Log an error message to ErrorLog table
--   4. Create a Task to automatically execute the procedure
-- ============================================================

-- ============================================================
-- Step 1: Create Stream to capture DELETE operations
-- ============================================================
CREATE STREAM IF NOT EXISTS HumanResources.Employee_Delete_Stream
    ON TABLE HumanResources.Employee;

-- ============================================================
-- Step 2: Stored Procedure SP_DEMPLOYEE_TRIGGER (JavaScript)
-- Restores deleted employees and logs error message
-- ============================================================
CREATE OR REPLACE PROCEDURE HumanResources.SP_DEMPLOYEE_TRIGGER()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    // Check if stream has data
    var checkStream = snowflake.execute({
        sqlText: "SELECT SYSTEM$STREAM_HAS_DATA('HumanResources.Employee_Delete_Stream') AS has_data"
    });
    checkStream.next();
    if (!checkStream.getColumnValue(1)) {
        return 'No changes to process';
    }

    // Copy deletes to temp table (this consumes the stream)
    snowflake.execute({
        sqlText: "CREATE OR REPLACE TEMPORARY TABLE HumanResources._temp_deletes AS " +
                 "SELECT * FROM HumanResources.Employee_Delete_Stream WHERE METADATA$ACTION = 'DELETE'"
    });

    // Count deletes
    var countResult = snowflake.execute({sqlText: 'SELECT COUNT(*) FROM HumanResources._temp_deletes'});
    countResult.next();
    var deletedCount = countResult.getColumnValue(1);

    if (deletedCount == 0) {
        snowflake.execute({sqlText: 'DROP TABLE IF EXISTS HumanResources._temp_deletes'});
        return 'No DELETE operations detected';
    }

    // Restore deleted rows from temp table
    var insertSql = 
        "INSERT INTO HumanResources.Employee (" +
        "BusinessEntityID, NationalIDNumber, LoginID, OrganizationNode, OrganizationLevel, " +
        "JobTitle, BirthDate, MaritalStatus, Gender, HireDate, SalariedFlag, " +
        "VacationHours, SickLeaveHours, CurrentFlag, rowguid, ModifiedDate) " +
        "SELECT BusinessEntityID, NationalIDNumber, LoginID, OrganizationNode, OrganizationLevel, " +
        "JobTitle, BirthDate, MaritalStatus, Gender, HireDate, SalariedFlag, " +
        "VacationHours, SickLeaveHours, CurrentFlag, rowguid, CURRENT_TIMESTAMP() " +
        "FROM HumanResources._temp_deletes";
    snowflake.execute({sqlText: insertSql});

    // Log error message (equivalent to RAISERROR in SQL Server)
    var errorMsg = 'Employees cannot be deleted. They can only be marked as not current. ' + 
                   deletedCount + ' delete attempt(s) were reversed.';
    var logSql = 
        "INSERT INTO dbo.ErrorLog (ErrorLogID, ErrorTime, UserName, ErrorNumber, " +
        "ErrorSeverity, ErrorState, ErrorProcedure, ErrorLine, ErrorMessage) VALUES (" +
        "dbo.ErrorLog_SEQ.NEXTVAL, CURRENT_TIMESTAMP(), CURRENT_USER(), 50001, 10, 1, " +
        "'HumanResources.dEmployee', 0, '" + errorMsg + "')";
    snowflake.execute({sqlText: logSql});

    // Cleanup temp table
    snowflake.execute({sqlText: 'DROP TABLE IF EXISTS HumanResources._temp_deletes'});

    return 'Trigger executed: ' + deletedCount + ' delete(s) blocked and restored. Error logged.';
$$;

-- ============================================================
-- Step 3: Create Task for automatic execution
-- Runs when stream detects DELETE operations
-- ============================================================
CREATE OR REPLACE TASK HumanResources.Task_dEmployee
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    TARGET_COMPLETION_INTERVAL = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('HumanResources.Employee_Delete_Stream')
AS
    CALL HumanResources.SP_DEMPLOYEE_TRIGGER();

-- ============================================================
-- Step 4: Resume the task (requires appropriate privileges)
-- ============================================================
-- ALTER TASK HumanResources.Task_dEmployee RESUME;

-- ============================================================
-- SOFT DELETE PROCEDURE (Recommended Alternative)
-- The proper way to "delete" employees - sets CurrentFlag = FALSE
-- ============================================================
CREATE OR REPLACE PROCEDURE HumanResources.SP_SOFT_DELETE_EMPLOYEE(P_BUSINESSENTITYID NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    UPDATE HumanResources.Employee
    SET CurrentFlag = FALSE, ModifiedDate = CURRENT_TIMESTAMP()
    WHERE BusinessEntityID = :P_BUSINESSENTITYID;

    IF (SQLROWCOUNT = 0) THEN
        RETURN 'Employee not found: ' || :P_BUSINESSENTITYID::VARCHAR;
    END IF;

    RETURN 'Employee ' || :P_BUSINESSENTITYID::VARCHAR || ' marked as not current (soft deleted)';
END;
$$;

-- ============================================================
-- USAGE NOTES:
-- ============================================================
-- 1. To enable automatic protection, run:
--    ALTER TASK HumanResources.Task_dEmployee RESUME;
--
-- 2. To manually test the trigger behavior:
--    a) Delete an employee: DELETE FROM HumanResources.Employee WHERE BusinessEntityID = 2;
--    b) Call the procedure: CALL HumanResources.SP_DEMPLOYEE_TRIGGER();
--    c) The deleted row will be restored and an error logged
--
-- 3. To properly "delete" an employee (soft delete):
--    CALL HumanResources.SP_SOFT_DELETE_EMPLOYEE(2);
--
-- 4. To check error log:
--    SELECT * FROM dbo.ErrorLog WHERE ErrorProcedure = 'HumanResources.dEmployee';
-- ============================================================

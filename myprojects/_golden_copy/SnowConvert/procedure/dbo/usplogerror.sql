USE DATABASE AdventureWorks2017;
-- uspLogError logs error information in the ErrorLog table about the 
-- error that caused execution to jump to the CATCH block of a 
-- TRY...CATCH construct. This should be executed from within the scope 
-- of a CATCH block otherwise it will return without inserting error 
-- information. 

CREATE OR REPLACE PROCEDURE ADVENTUREWORKS2017.DBO.uspLogError(
    ERROR_CODE IN NUMBER,
    ERROR_STATE IN VARCHAR,
    ERROR_MSG IN VARCHAR,
    ERROR_PROC IN VARCHAR,
    ERRORLOGID OUT NUMBER
)
RETURNS NUMBER
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    new_id NUMBER;
    error_state_num NUMBER;
BEGIN
    ERRORLOGID := 0;
    
    BEGIN
        IF (ERROR_CODE IS NULL) THEN
            RETURN 0;
        END IF;
        
        IF (CURRENT_TRANSACTION() = -1) THEN
            SYSTEM$LOG_INFO('Cannot log error since the current transaction is in an uncommittable state.');
            RETURN 0;
        END IF;

        -- Get sequence value using SELECT INTO
        SELECT ADVENTUREWORKS2017.DBO.ERRORLOG_SEQ.NEXTVAL INTO :new_id;
        
        -- Convert SQLSTATE to number or NULL
        error_state_num := TRY_CAST(ERROR_STATE AS NUMBER);
        
        INSERT INTO ADVENTUREWORKS2017.DBO.ERRORLOG 
            (ErrorLogID, UserName, ErrorNumber, ErrorSeverity, ErrorState, ErrorProcedure, ErrorLine, ErrorMessage)
        VALUES
            (
                :new_id,
                CURRENT_USER, 
                :ERROR_CODE,
                0,
                :error_state_num,
                :ERROR_PROC,
                0,
                :ERROR_MSG
            );
            
        ERRORLOGID := new_id;
        RETURN ERRORLOGID;
        
    EXCEPTION
        WHEN OTHER THEN
            SYSTEM$LOG_INFO('An error occurred in stored procedure uspLogError: ' || SQLERRM);
            RETURN -1;
    END;
END;
$$;
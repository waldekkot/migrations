USE DATABASE AdventureWorks2017;
--A stored procedure which demonstrates integrated full text search
CREATE OR REPLACE PROCEDURE dbo.uspSearchCandidateResumes (
    SEARCHSTRING STRING, 
    USEINFLECTIONAL BOOLEAN DEFAULT FALSE, 
    USETHESAURUS BOOLEAN DEFAULT FALSE, 
    LANGUAGE INT DEFAULT 0
)
RETURNS TABLE (JobCandidateID INT, RANK INT)
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    res RESULTSET;
BEGIN
    -- Basic text search using ILIKE (case-insensitive)
    res := (
        SELECT 
            JobCandidateID,
            1 AS RANK
        FROM 
            HumanResources.JobCandidate
        WHERE 
            Resume ILIKE '%' || :SEARCHSTRING || '%'
        ORDER BY 
            JobCandidateID
    );
    
    RETURN TABLE(res);
END;
$$;

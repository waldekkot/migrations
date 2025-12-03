USE DATABASE AdventureWorks2017;

CREATE OR REPLACE VIEW HumanResources.vJobCandidateEducation
COMMENT = 'Converted from SQL Server by SnowConvert - Extracts education records from Resume XML'
AS
WITH parsed_resumes AS (
    -- Step 1: Parse the XML Resume column for all candidates
    SELECT
        JobCandidateID,
        PARSE_XML(Resume) AS resume_xml
    FROM HumanResources.JobCandidate
    WHERE Resume IS NOT NULL
),
flattened_elements AS (
    -- Step 2: Flatten the XML to get all child elements of Resume
    SELECT
        pr.JobCandidateID,
        elem.index AS element_index,
        elem.value AS element_xml
    FROM parsed_resumes pr,
         TABLE(FLATTEN(input => pr.resume_xml:"$")) elem
),
education_elements AS (
    -- Step 3: Filter to get only Education elements
    SELECT
        JobCandidateID,
        element_xml,
        TO_VARCHAR(element_xml) AS education_xml_string
    FROM flattened_elements
    WHERE TO_VARCHAR(element_xml) LIKE '<ns:Education>%'
),
extracted_fields AS (
    -- Step 4: Extract individual fields using regex patterns
    SELECT
        JobCandidateID,
        
        -- Basic education info
        REGEXP_SUBSTR(education_xml_string, '<ns:Edu\\.Level>(.*?)</ns:Edu\\.Level>', 1, 1, 'e', 1) 
            AS edu_level,
        
        REGEXP_SUBSTR(education_xml_string, '<ns:Edu\\.StartDate>(.*?)</ns:Edu\\.StartDate>', 1, 1, 'e', 1) 
            AS edu_start_date_raw,
        
        REGEXP_SUBSTR(education_xml_string, '<ns:Edu\\.EndDate>(.*?)</ns:Edu\\.EndDate>', 1, 1, 'e', 1) 
            AS edu_end_date_raw,
        
        REGEXP_SUBSTR(education_xml_string, '<ns:Edu\\.Degree>(.*?)</ns:Edu\\.Degree>', 1, 1, 'e', 1) 
            AS edu_degree,
        
        REGEXP_SUBSTR(education_xml_string, '<ns:Edu\\.Major>(.*?)</ns:Edu\\.Major>', 1, 1, 'e', 1) 
            AS edu_major,
        
        REGEXP_SUBSTR(education_xml_string, '<ns:Edu\\.Minor>(.*?)</ns:Edu\\.Minor>', 1, 1, 'e', 1) 
            AS edu_minor,
        
        REGEXP_SUBSTR(education_xml_string, '<ns:Edu\\.GPA>(.*?)</ns:Edu\\.GPA>', 1, 1, 'e', 1) 
            AS edu_gpa,
        
        REGEXP_SUBSTR(education_xml_string, '<ns:Edu\\.GPAScale>(.*?)</ns:Edu\\.GPAScale>', 1, 1, 'e', 1) 
            AS edu_gpa_scale,
        
        REGEXP_SUBSTR(education_xml_string, '<ns:Edu\\.School>(.*?)</ns:Edu\\.School>', 1, 1, 'e', 1) 
            AS edu_school,
        
        -- Location info (nested within Edu.Location/Location)
        REGEXP_SUBSTR(education_xml_string, '<ns:Loc\\.CountryRegion>(.*?)</ns:Loc\\.CountryRegion>', 1, 1, 'e', 1) 
            AS loc_country_region,
        
        REGEXP_SUBSTR(education_xml_string, '<ns:Loc\\.State>(.*?)</ns:Loc\\.State>', 1, 1, 'e', 1) 
            AS loc_state,
        
        REGEXP_SUBSTR(education_xml_string, '<ns:Loc\\.City>(.*?)</ns:Loc\\.City>', 1, 1, 'e', 1) 
            AS loc_city
    
    FROM education_elements
)
-- Step 5: Final output with proper data types and column names
SELECT
    JobCandidateID,
    
    -- Education details
    edu_level::VARCHAR(50) AS "Edu.Level",
    
    TRY_TO_TIMESTAMP_NTZ(REPLACE(edu_start_date_raw, 'Z', '')) AS "Edu.StartDate",
    
    TRY_TO_TIMESTAMP_NTZ(REPLACE(edu_end_date_raw, 'Z', '')) AS "Edu.EndDate",
    
    edu_degree::VARCHAR(200) AS "Edu.Degree",
    
    edu_major::VARCHAR(200) AS "Edu.Major",
    
    edu_minor::VARCHAR(200) AS "Edu.Minor",
    
    edu_gpa::VARCHAR(50) AS "Edu.GPA",
    
    edu_gpa_scale::VARCHAR(50) AS "Edu.GPAScale",
    
    edu_school::VARCHAR(200) AS "Edu.School",
    
    -- Location details
    loc_country_region::VARCHAR(100) AS "Edu.Loc.CountryRegion",
    
    loc_state::VARCHAR(100) AS "Edu.Loc.State",
    
    loc_city::VARCHAR(100) AS "Edu.Loc.City"

FROM extracted_fields;


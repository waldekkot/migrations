USE DATABASE AdventureWorks2017;

CREATE OR REPLACE VIEW HumanResources.vJobCandidateEmployment
AS
WITH employment_nodes AS (
    -- Parse XML and extract Employment elements
    SELECT 
        jc.JobCandidateID,
        emp.value AS emp,
        XMLGET(XMLGET(emp.value, 'ns:Emp.Location'), 'ns:Location') AS loc
    FROM HumanResources.JobCandidate jc,
    LATERAL FLATTEN(input => GET(PARSE_XML(jc.Resume::VARCHAR), '$')) emp
    WHERE jc.Resume IS NOT NULL
      AND GET(emp.value, '@') = 'ns:Employment'
)
SELECT
    JobCandidateID,
    TRY_TO_TIMESTAMP_NTZ(REPLACE(GET(XMLGET(emp, 'ns:Emp.StartDate'), '$')::VARCHAR, 'Z', '')) AS "Emp.StartDate",
    TRY_TO_TIMESTAMP_NTZ(REPLACE(GET(XMLGET(emp, 'ns:Emp.EndDate'), '$')::VARCHAR, 'Z', '')) AS "Emp.EndDate",
    GET(XMLGET(emp, 'ns:Emp.OrgName'), '$')::VARCHAR(100) AS "Emp.OrgName",
    GET(XMLGET(emp, 'ns:Emp.JobTitle'), '$')::VARCHAR(100) AS "Emp.JobTitle",
    GET(XMLGET(emp, 'ns:Emp.Responsibility'), '$')::VARCHAR AS "Emp.Responsibility",
    GET(XMLGET(emp, 'ns:Emp.FunctionCategory'), '$')::VARCHAR AS "Emp.FunctionCategory",
    GET(XMLGET(emp, 'ns:Emp.IndustryCategory'), '$')::VARCHAR AS "Emp.IndustryCategory",
    GET(XMLGET(loc, 'ns:Loc.CountryRegion'), '$')::VARCHAR AS "Emp.Loc.CountryRegion",
    GET(XMLGET(loc, 'ns:Loc.State'), '$')::VARCHAR AS "Emp.Loc.State",
    GET(XMLGET(loc, 'ns:Loc.City'), '$')::VARCHAR AS "Emp.Loc.City"
FROM employment_nodes;
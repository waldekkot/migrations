USE DATABASE AdventureWorks2017;

CREATE OR REPLACE VIEW HumanResources.vJobCandidate
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
AS
WITH parsed_resumes AS (
    -- Parse XML from Resume column
    SELECT 
        JobCandidateID,
        BusinessEntityID,
        ModifiedDate,
        PARSE_XML(Resume) AS resume_xml
    FROM HumanResources.JobCandidate
),
extracted_nodes AS (
    -- Extract main XML nodes
    SELECT 
        JobCandidateID,
        BusinessEntityID,
        ModifiedDate,
        resume_xml,
        XMLGET(resume_xml, 'ns:Name', 0) AS name_node,
        XMLGET(resume_xml, 'ns:Address', 0) AS address_node,
        XMLGET(resume_xml, 'ns:Skills', 0) AS skills_node,
        XMLGET(resume_xml, 'ns:EMail', 0) AS email_node,
        XMLGET(resume_xml, 'ns:WebSite', 0) AS website_node
    FROM parsed_resumes
),
extracted_location AS (
    -- Extract nested location information
    SELECT 
        JobCandidateID,
        BusinessEntityID,
        ModifiedDate,
        name_node,
        address_node,
        skills_node,
        email_node,
        website_node,
        XMLGET(XMLGET(address_node, 'ns:Addr.Location', 0), 'ns:Location', 0) AS location_node
    FROM extracted_nodes
)
-- Final SELECT with all extracted values
SELECT
    JobCandidateID,
    BusinessEntityID,
    -- Name fields
    XMLGET(name_node, 'ns:Name.Prefix', 0):"$"::VARCHAR(30) AS "Name.Prefix",
    XMLGET(name_node, 'ns:Name.First', 0):"$"::VARCHAR(30) AS "Name.First",
    XMLGET(name_node, 'ns:Name.Middle', 0):"$"::VARCHAR(30) AS "Name.Middle",
    XMLGET(name_node, 'ns:Name.Last', 0):"$"::VARCHAR(30) AS "Name.Last",
    XMLGET(name_node, 'ns:Name.Suffix', 0):"$"::VARCHAR(30) AS "Name.Suffix",
    -- Skills
    skills_node:"$"::VARCHAR AS Skills,
    -- Address fields
    XMLGET(address_node, 'ns:Addr.Type', 0):"$"::VARCHAR(30) AS "Addr.Type",
    XMLGET(location_node, 'ns:Loc.CountryRegion', 0):"$"::VARCHAR(100) AS "Addr.Loc.CountryRegion",
    XMLGET(location_node, 'ns:Loc.State', 0):"$"::VARCHAR(100) AS "Addr.Loc.State",
    XMLGET(location_node, 'ns:Loc.City', 0):"$"::VARCHAR(100) AS "Addr.Loc.City",
    XMLGET(address_node, 'ns:Addr.PostalCode', 0):"$"::VARCHAR(20) AS "Addr.PostalCode",
    -- Contact information
    email_node:"$"::VARCHAR AS EMail,
    website_node:"$"::VARCHAR AS WebSite,
    ModifiedDate
FROM extracted_location;
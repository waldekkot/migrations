USE DATABASE AdventureWorks2017;

CREATE OR REPLACE VIEW Sales.vStoreWithDemographics AS
WITH ParsedXML AS (
    -- Step 1: Parse XML once
    SELECT 
        BusinessEntityID,
        Name,
        PARSE_XML(Demographics) AS xml_data
    FROM Sales.Store
),
Survey AS (
    -- Step 2: Extract all XML elements
    SELECT
        BusinessEntityID,
        Name,
        XMLGET(xml_data, 'AnnualSales') AS AnnualSales_xml,
        XMLGET(xml_data, 'AnnualRevenue') AS AnnualRevenue_xml,
        XMLGET(xml_data, 'BankName') AS BankName_xml,
        XMLGET(xml_data, 'BusinessType') AS BusinessType_xml,
        XMLGET(xml_data, 'YearOpened') AS YearOpened_xml,
        XMLGET(xml_data, 'Specialty') AS Specialty_xml,
        XMLGET(xml_data, 'SquareFeet') AS SquareFeet_xml,
        XMLGET(xml_data, 'Brands') AS Brands_xml,
        XMLGET(xml_data, 'Internet') AS Internet_xml,
        XMLGET(xml_data, 'NumberEmployees') AS NumberEmployees_xml
    FROM ParsedXML
)
-- Step 3: Extract content and cast to final types
SELECT
    BusinessEntityID,
    Name,
    GET(AnnualSales_xml, '$')::NUMBER(19,4) AS AnnualSales,
    GET(AnnualRevenue_xml, '$')::NUMBER(19,4) AS AnnualRevenue,
    GET(BankName_xml, '$')::VARCHAR(50) AS BankName,
    GET(BusinessType_xml, '$')::VARCHAR(5) AS BusinessType,
    GET(YearOpened_xml, '$')::INTEGER AS YearOpened,
    GET(Specialty_xml, '$')::VARCHAR(50) AS Specialty,
    GET(SquareFeet_xml, '$')::INTEGER AS SquareFeet,
    GET(Brands_xml, '$')::VARCHAR(30) AS Brands,
    GET(Internet_xml, '$')::VARCHAR(30) AS Internet,
    GET(NumberEmployees_xml, '$')::INTEGER AS NumberEmployees
FROM Survey;
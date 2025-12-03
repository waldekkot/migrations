USE DATABASE AdventureWorks2017;
CREATE OR REPLACE VIEW Person.vAdditionalContactInfo
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 2,  "minor": 0,  "patch": "34.0" }, "attributes": {  "component": "transact",  "convertedOn": "12/02/2025",  "domain": "no-domain-provided",  "migrationid": "5d+aAa3cv3OCsNech69fjw==" }}'
AS
WITH 
-- Step 1: Filter valid records with XML data
valid_records AS (
    SELECT 
        BusinessEntityID,
        FirstName,
        MiddleName,
        LastName,
        rowguid,
        ModifiedDate,
        AdditionalContactInfo
    FROM Person.Person
    WHERE AdditionalContactInfo IS NOT NULL
      AND TRIM(AdditionalContactInfo) != ''
      AND AdditionalContactInfo != 'null'
      AND CHECK_XML(AdditionalContactInfo) IS NULL
),
-- Step 2: Remove namespace prefixes and parse XML
parsed_xml AS (
    SELECT 
        BusinessEntityID,
        FirstName,
        MiddleName,
        LastName,
        rowguid,
        ModifiedDate,
        AdditionalContactInfo as original_xml,
        PARSE_XML(
            REGEXP_REPLACE(
                REGEXP_REPLACE(AdditionalContactInfo, '<act:', '<'),
                '</act:', '</'
            )
        ) as xml_data
    FROM valid_records
),
-- Step 3: Extract telephone information
telephone_info AS (
    SELECT 
        BusinessEntityID,
        XMLGET(XMLGET(xml_data, 'telephoneNumber', 0), 'number'):"$"::VARCHAR(50) AS TelephoneNumber,
        XMLGET(XMLGET(xml_data, 'telephoneNumber', 0), 'SpecialInstructions'):"$"::VARCHAR AS TelephoneSpecialInstructions
    FROM parsed_xml
),
-- Step 4: Extract home address information
address_info AS (
    SELECT 
        BusinessEntityID,
        XMLGET(XMLGET(xml_data, 'homePostalAddress', 0), 'Street'):"$"::VARCHAR(50) AS Street,
        XMLGET(XMLGET(xml_data, 'homePostalAddress', 0), 'City'):"$"::VARCHAR(50) AS City,
        XMLGET(XMLGET(xml_data, 'homePostalAddress', 0), 'StateProvince'):"$"::VARCHAR(50) AS StateProvince,
        XMLGET(XMLGET(xml_data, 'homePostalAddress', 0), 'PostalCode'):"$"::VARCHAR(50) AS PostalCode,
        XMLGET(XMLGET(xml_data, 'homePostalAddress', 0), 'CountryRegion'):"$"::VARCHAR(50) AS CountryRegion,
        XMLGET(XMLGET(xml_data, 'homePostalAddress', 0), 'SpecialInstructions'):"$"::VARCHAR AS HomeAddressSpecialInstructions
    FROM parsed_xml
),
-- Step 5: Extract email information
email_info AS (
    SELECT 
        p.BusinessEntityID,
        XMLGET(XMLGET(xml_data, 'eMail', 0), 'eMailAddress'):"$"::VARCHAR(128) AS EMailAddress,
        LTRIM(RTRIM(
            REGEXP_SUBSTR(
                REGEXP_SUBSTR(original_xml, '<act:eMail>.*?</act:eMail>', 1, 1, 's'),
                '<act:SpecialInstructions>([^<]+)',
                1, 1, 'se', 1
            )
        )) AS EMailSpecialInstructions,
        XMLGET(
            XMLGET(
                XMLGET(XMLGET(xml_data, 'eMail', 0), 'SpecialInstructions'), 
                'telephoneNumber'
            ), 
            'number'
        ):"$"::VARCHAR(50) AS EMailTelephoneNumber
    FROM parsed_xml p
)
-- Step 6: Combine all extracted information
SELECT
    p.BusinessEntityID,
    p.FirstName,
    p.MiddleName,
    p.LastName,
    t.TelephoneNumber,
    LTRIM(RTRIM(t.TelephoneSpecialInstructions)) AS TelephoneSpecialInstructions,
    a.Street,
    a.City,
    a.StateProvince,
    a.PostalCode,
    a.CountryRegion,
    a.HomeAddressSpecialInstructions,
    e.EMailAddress,
    e.EMailSpecialInstructions,
    e.EMailTelephoneNumber,
    p.rowguid,
    p.ModifiedDate
FROM parsed_xml p
LEFT JOIN telephone_info t ON p.BusinessEntityID = t.BusinessEntityID
LEFT JOIN address_info a ON p.BusinessEntityID = a.BusinessEntityID
LEFT JOIN email_info e ON p.BusinessEntityID = e.BusinessEntityID;
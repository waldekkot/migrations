USE DATABASE AdventureWorks2017;

CREATE OR REPLACE VIEW Sales.vPersonDemographics
AS
-- Step 1: Parse XML from VARIANT
WITH parsed_demographics AS (
    SELECT 
        p.BusinessEntityID,
        PARSE_XML(p.Demographics::VARCHAR) AS survey
    FROM Person.Person p
    WHERE p.Demographics IS NOT NULL
),

-- Step 2: Extract raw XML elements (before type conversion)
extracted_fields AS (
    SELECT
        BusinessEntityID,
        XMLGET(survey, 'TotalPurchaseYTD') AS total_purchase_ytd_node,
        XMLGET(survey, 'DateFirstPurchase') AS date_first_purchase_node,
        XMLGET(survey, 'BirthDate') AS birth_date_node,
        XMLGET(survey, 'MaritalStatus') AS marital_status_node,
        XMLGET(survey, 'YearlyIncome') AS yearly_income_node,
        XMLGET(survey, 'Gender') AS gender_node,
        XMLGET(survey, 'TotalChildren') AS total_children_node,
        XMLGET(survey, 'NumberChildrenAtHome') AS children_at_home_node,
        XMLGET(survey, 'Education') AS education_node,
        XMLGET(survey, 'Occupation') AS occupation_node,
        XMLGET(survey, 'HomeOwnerFlag') AS home_owner_node,
        XMLGET(survey, 'NumberCarsOwned') AS cars_owned_node
    FROM parsed_demographics
),

-- Step 3: Extract text content from XML nodes
raw_values AS (
    SELECT
        BusinessEntityID,
        GET(total_purchase_ytd_node, '$') AS total_purchase_ytd_raw,
        GET(date_first_purchase_node, '$') AS date_first_purchase_raw,
        GET(birth_date_node, '$') AS birth_date_raw,
        GET(marital_status_node, '$') AS marital_status_raw,
        GET(yearly_income_node, '$') AS yearly_income_raw,
        GET(gender_node, '$') AS gender_raw,
        GET(total_children_node, '$') AS total_children_raw,
        GET(children_at_home_node, '$') AS children_at_home_raw,
        GET(education_node, '$') AS education_raw,
        GET(occupation_node, '$') AS occupation_raw,
        GET(home_owner_node, '$') AS home_owner_raw,
        GET(cars_owned_node, '$') AS cars_owned_raw
    FROM extracted_fields
)

-- Step 4: Final SELECT with type conversions
SELECT
    BusinessEntityID,
    
    -- Money
    total_purchase_ytd_raw::NUMBER(19,4) AS TotalPurchaseYTD,
    
    -- Dates (remove 'Z' suffix)
    TRY_TO_TIMESTAMP_NTZ(REPLACE(date_first_purchase_raw::VARCHAR, 'Z', '')) AS DateFirstPurchase,
    TRY_TO_TIMESTAMP_NTZ(REPLACE(birth_date_raw::VARCHAR, 'Z', '')) AS BirthDate,
    
    -- Strings
    marital_status_raw::VARCHAR(1) AS MaritalStatus,
    yearly_income_raw::VARCHAR(30) AS YearlyIncome,
    gender_raw::VARCHAR(1) AS Gender,
    
    -- Integers
    total_children_raw::INTEGER AS TotalChildren,
    children_at_home_raw::INTEGER AS NumberChildrenAtHome,
    
    -- Strings
    education_raw::VARCHAR(30) AS Education,
    occupation_raw::VARCHAR(30) AS Occupation,
    
    -- Boolean (convert '1'/'0' to TRUE/FALSE)
    CASE home_owner_raw::VARCHAR 
        WHEN '1' THEN TRUE 
        WHEN '0' THEN FALSE 
        ELSE NULL 
    END AS HomeOwnerFlag,
    
    -- Integer
    cars_owned_raw::INTEGER AS NumberCarsOwned

FROM raw_values;
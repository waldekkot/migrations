USE DATABASE AdventureWorks2017;

CREATE OR REPLACE VIEW Production.vProductModelInstructions
AS
WITH parsed_instructions AS (
    SELECT 
        pm.ProductModelID,
        pm.Name,
        pm.rowguid,
        pm.ModifiedDate,
        PARSE_XML(pm.Instructions::VARCHAR) AS xml_doc
    FROM Production.ProductModel pm
    WHERE pm.Instructions IS NOT NULL
      AND NOT IS_NULL_VALUE(pm.Instructions)
      AND LENGTH(pm.Instructions::VARCHAR) > 10
),

instructions_summary AS (
    SELECT
        ProductModelID,
        Name,
        rowguid,
        ModifiedDate,
        xml_doc,
        GET(xml_doc, '$')[0]::VARCHAR AS instructions_text
    FROM parsed_instructions
),

locations AS (
    SELECT
        s.ProductModelID,
        s.Name,
        s.rowguid,
        s.ModifiedDate,
        s.instructions_text,
        loc.value AS location_element,
        loc.index AS loc_idx,
        GET(loc.value, '@LocationID')::INT AS location_id,
        GET(loc.value, '@SetupHours')::DECIMAL(9,4) AS setup_hours,
        GET(loc.value, '@MachineHours')::DECIMAL(9,4) AS machine_hours,
        GET(loc.value, '@LaborHours')::DECIMAL(9,4) AS labor_hours,
        GET(loc.value, '@LotSize')::INT AS lot_size
    FROM instructions_summary s,
    LATERAL FLATTEN(input => GET(s.xml_doc, '$')) loc
    WHERE GET(loc.value, '@') = 'Location'
),

steps_raw AS (
    SELECT
        l.ProductModelID,
        l.Name,
        l.rowguid,
        l.ModifiedDate,
        l.instructions_text,
        l.location_id,
        l.setup_hours,
        l.machine_hours,
        l.labor_hours,
        l.lot_size,
        step.index AS step_idx,
        GET(step.value, '$') AS step_content_raw
    FROM locations l,
    LATERAL FLATTEN(input => GET(l.location_element, '$')) step
    WHERE GET(step.value, '@') = 'step'
),

step_parts AS (
    SELECT
        s.ProductModelID,
        s.Name,
        s.rowguid,
        s.ModifiedDate,
        s.instructions_text,
        s.location_id,
        s.setup_hours,
        s.machine_hours,
        s.labor_hours,
        s.lot_size,
        s.step_idx,
        s.step_content_raw,
        CASE 
            WHEN IS_VARCHAR(s.step_content_raw) THEN s.step_content_raw::VARCHAR
            ELSE NULL
        END AS simple_step_text,
        CASE 
            WHEN IS_ARRAY(s.step_content_raw) THEN s.step_content_raw
            ELSE NULL
        END AS array_step_content
    FROM steps_raw s
),

array_step_texts AS (
    SELECT
        sp.ProductModelID,
        sp.location_id,
        sp.step_idx,
        LISTAGG(
            CASE 
                WHEN IS_VARCHAR(part.value) THEN part.value::VARCHAR
                WHEN IS_OBJECT(part.value) THEN GET(part.value, '$')::VARCHAR
                ELSE ''
            END, ' '
        ) WITHIN GROUP (ORDER BY part.index) AS aggregated_text
    FROM step_parts sp,
    LATERAL FLATTEN(input => sp.array_step_content, OUTER => TRUE) part
    WHERE sp.array_step_content IS NOT NULL
    GROUP BY sp.ProductModelID, sp.location_id, sp.step_idx
)

SELECT
    sp.ProductModelID,
    sp.Name,
    sp.instructions_text AS Instructions,
    sp.location_id AS LocationID,
    sp.setup_hours AS SetupHours,
    sp.machine_hours AS MachineHours,
    sp.labor_hours AS LaborHours,
    sp.lot_size AS LotSize,
    COALESCE(sp.simple_step_text, ast.aggregated_text)::VARCHAR(1024) AS Step,
    sp.rowguid,
    sp.ModifiedDate
FROM step_parts sp
LEFT JOIN array_step_texts ast
    ON sp.ProductModelID = ast.ProductModelID
    AND sp.location_id = ast.location_id
    AND sp.step_idx = ast.step_idx;
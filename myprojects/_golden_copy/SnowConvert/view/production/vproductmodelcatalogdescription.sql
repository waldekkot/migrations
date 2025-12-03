USE DATABASE AdventureWorks2017;

CREATE OR REPLACE VIEW Production.vProductModelCatalogDescription
AS
WITH parsed_catalog AS (
    SELECT 
        pm.ProductModelID,
        pm.Name,
        pm.rowguid,
        pm.ModifiedDate,
        PARSE_XML(pm.CatalogDescription::VARCHAR) AS xml_doc
    FROM Production.ProductModel pm
    WHERE pm.CatalogDescription IS NOT NULL
      AND NOT IS_NULL_VALUE(pm.CatalogDescription)
      AND LENGTH(pm.CatalogDescription::VARCHAR) > 10
),

catalog_sections AS (
    SELECT
        ProductModelID,
        Name,
        rowguid,
        ModifiedDate,
        xml_doc,
        XMLGET(xml_doc, 'p1:Summary') AS summary_section,
        XMLGET(xml_doc, 'p1:Manufacturer') AS manufacturer_section,
        XMLGET(xml_doc, 'p1:Features') AS features_section,
        XMLGET(xml_doc, 'p1:Picture') AS picture_section,
        XMLGET(xml_doc, 'p1:Specifications') AS specifications_section
    FROM parsed_catalog
),

catalog_elements AS (
    SELECT
        ProductModelID,
        Name,
        rowguid,
        ModifiedDate,
        XMLGET(summary_section, 'html:p') AS summary_p,
        XMLGET(manufacturer_section, 'p1:Name') AS mfr_name,
        XMLGET(manufacturer_section, 'p1:Copyright') AS mfr_copyright,
        XMLGET(manufacturer_section, 'p1:ProductURL') AS mfr_url,
        XMLGET(XMLGET(features_section, 'wm:Warranty'), 'wm:WarrantyPeriod') AS warranty_period,
        XMLGET(XMLGET(features_section, 'wm:Warranty'), 'wm:Description') AS warranty_desc,
        XMLGET(XMLGET(features_section, 'wm:Maintenance'), 'wm:NoOfYears') AS maint_years,
        XMLGET(XMLGET(features_section, 'wm:Maintenance'), 'wm:Description') AS maint_desc,
        XMLGET(features_section, 'wf:wheel') AS wheel,
        XMLGET(features_section, 'wf:saddle') AS saddle_raw,
        XMLGET(features_section, 'wf:pedal') AS pedal_raw,
        XMLGET(features_section, 'wf:BikeFrame') AS bike_frame_raw,
        XMLGET(features_section, 'wf:crankset') AS crankset,
        XMLGET(picture_section, 'p1:Angle') AS pic_angle,
        XMLGET(picture_section, 'p1:Size') AS pic_size,
        XMLGET(picture_section, 'p1:ProductPhotoID') AS pic_photo_id,
        XMLGET(specifications_section, 'Material') AS spec_material,
        XMLGET(specifications_section, 'Color') AS spec_color,
        XMLGET(specifications_section, 'ProductLine') AS spec_product_line,
        XMLGET(specifications_section, 'Style') AS spec_style,
        XMLGET(specifications_section, 'RiderExperience') AS spec_rider_exp
    FROM catalog_sections
)

SELECT
    ProductModelID,
    Name,
    GET(summary_p, '$')::VARCHAR AS Summary,
    GET(mfr_name, '$')::VARCHAR AS Manufacturer,
    GET(mfr_copyright, '$')::VARCHAR(30) AS Copyright,
    GET(mfr_url, '$')::VARCHAR(256) AS ProductURL,
    GET(warranty_period, '$')::VARCHAR(256) AS WarrantyPeriod,
    GET(warranty_desc, '$')::VARCHAR(256) AS WarrantyDescription,
    GET(maint_years, '$')::VARCHAR(256) AS NoOfYears,
    GET(maint_desc, '$')::VARCHAR(256) AS MaintenanceDescription,
    GET(wheel, '$')::VARCHAR(256) AS Wheel,
    -- Saddle: handle mixed content
    CASE 
        WHEN IS_ARRAY(GET(saddle_raw, '$')) THEN
            CONCAT(COALESCE(GET(GET(saddle_raw, '$')[0], '$')::VARCHAR, ''), ' ', COALESCE(TRIM(GET(saddle_raw, '$')[1]::VARCHAR), ''))
        ELSE GET(saddle_raw, '$')::VARCHAR
    END::VARCHAR(256) AS Saddle,
    -- Pedal: handle mixed content
    CASE 
        WHEN IS_ARRAY(GET(pedal_raw, '$')) THEN
            CONCAT(COALESCE(GET(GET(pedal_raw, '$')[0], '$')::VARCHAR, ''), ' ', COALESCE(TRIM(GET(pedal_raw, '$')[1]::VARCHAR), ''))
        ELSE GET(pedal_raw, '$')::VARCHAR
    END::VARCHAR(256) AS Pedal,
    -- BikeFrame: handle mixed content (same issue as Saddle/Pedal)
    CASE 
        WHEN IS_ARRAY(GET(bike_frame_raw, '$')) THEN
            CONCAT(COALESCE(GET(GET(bike_frame_raw, '$')[0], '$')::VARCHAR, ''), ' ', COALESCE(TRIM(GET(bike_frame_raw, '$')[1]::VARCHAR), ''))
        ELSE GET(bike_frame_raw, '$')::VARCHAR
    END::VARCHAR AS BikeFrame,
    GET(crankset, '$')::VARCHAR(256) AS Crankset,
    GET(pic_angle, '$')::VARCHAR(256) AS PictureAngle,
    GET(pic_size, '$')::VARCHAR(256) AS PictureSize,
    GET(pic_photo_id, '$')::VARCHAR(256) AS ProductPhotoID,
    GET(spec_material, '$')::VARCHAR(256) AS Material,
    GET(spec_color, '$')::VARCHAR(256) AS Color,
    GET(spec_product_line, '$')::VARCHAR(256) AS ProductLine,
    GET(spec_style, '$')::VARCHAR(256) AS Style,
    GET(spec_rider_exp, '$')::VARCHAR(1024) AS RiderExperience,
    rowguid,
    ModifiedDate
FROM catalog_elements;
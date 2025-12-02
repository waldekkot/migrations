USE AdventureWorks2017;
GO

-- Find ALL geography columns in the database
SELECT 
    SCHEMA_NAME(t.schema_id) AS SchemaName,
    t.name AS TableName,
    c.name AS ColumnName,
    TYPE_NAME(c.user_type_id) AS DataType
FROM sys.columns c
JOIN sys.tables t ON c.object_id = t.object_id
WHERE TYPE_NAME(c.user_type_id) = 'geography'
ORDER BY SchemaName, TableName;
use AdventureWorks2017
GO

-- -- Step 0: backup the table
-- ---------------------------
SELECT * 
INTO HumanResources.Employee_Backup
FROM HumanResources.Employee;

-- drop table HumanResources.Employee_Backup;

-- Step 1: Add a new varchar column
ALTER TABLE HumanResources.Employee
ADD OrganizationNodeString nvarchar(256);
GO

-- Step 2: Populate with converted values
UPDATE HumanResources.Employee
SET OrganizationNodeString = OrganizationNode.ToString();
GO

-- Step 3: quick verify
-- select top 100 * from HumanResources.Employee;

-- Step 4. drop indexes
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Employee_OrganizationNode')
    DROP INDEX IX_Employee_OrganizationNode ON HumanResources.Employee;
GO

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Employee_OrganizationLevel_OrganizationNode')
    DROP INDEX IX_Employee_OrganizationLevel_OrganizationNode ON HumanResources.Employee;
GO

-- Step 5. drop the computed column OrganizationLevel (otherwise, you cannot delete the actual OrganizationNode column)
ALTER TABLE HumanResources.Employee
DROP COLUMN OrganizationLevel;
GO

-- Step 6. Drop column
ALTER TABLE HumanResources.Employee
DROP COLUMN OrganizationNode;
GO

-- Step 7. Rename the new column (string) to the old name
EXEC sp_rename 'HumanResources.Employee.OrganizationNodeString', 'OrganizationNode', 'COLUMN';
GO

-- Step 8. drop the computed column OrganizationLevel (otherwise, you cannot delete the actual OrganizationNode column)
ALTER TABLE HumanResources.Employee
ADD OrganizationLevel AS (
    CAST(LEN(OrganizationNode) - LEN(REPLACE(OrganizationNode, '/', '')) - 1 AS smallint)
) PERSISTED;
GO

-- Step 9: final verify
select top 100 * from HumanResources.Employee;
GO



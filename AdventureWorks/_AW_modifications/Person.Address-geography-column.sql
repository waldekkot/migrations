use AdventureWorks2017
GO

-- Step 0: backup the table
---------------------------
SELECT * 
INTO Person.Address_Backup
FROM Person.Address;
GO

-- Step 1: Add a new varchar column
ALTER TABLE Person.Address
ADD SpatialLocationString nvarchar(256);
GO

-- Step 2: Populate with converted values
UPDATE Person.Address
SET SpatialLocationString = SpatialLocation.ToString();
GO

-- Step 3: quick verify
-- select top 100 * from Person.Address;


-- Step 4. drop indexes

-- Step 5. Drop column
ALTER TABLE Person.Address
DROP COLUMN SpatialLocation;
GO

-- Step 6. Rename the new column (string) to the old name
EXEC sp_rename 'Person.Address.SpatialLocationString', 'SpatialLocation', 'COLUMN';
GO

-- Step 7: final verify
select top 100 * from Person.Address;
GO




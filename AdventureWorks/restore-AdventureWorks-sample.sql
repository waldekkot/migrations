-------------------------------------------------------------------------------------------
-- this part just checks what are the paths in the .bak file
:setvar YEAR "2017"
GO

USE [master];
GO

RESTORE FILELISTONLY 
FROM DISK = N'/var/opt/mssql/backup/AdventureWorks$(YEAR).bak';
GO

---------------------------------------------------------------------------------------------
-- database restoration from the backup (.bak file)
-- if you different version change the YEAR variable: e.g. 2012, 2016, 2019, 2022, 2025 
--    - you will need to download the corresponding .bak files first
--    from: https://github.com/Microsoft/sql-server-samples/releases/tag/adventureworks
:setvar YEAR "2017"

USE [master];
GO

RESTORE DATABASE [AdventureWorks$(YEAR)]
FROM DISK = N'/var/opt/mssql/backup/AdventureWorks$(YEAR).bak'
WITH
    MOVE 'AdventureWorks$(YEAR)' TO '/var/opt/mssql/data/AdventureWorks$(YEAR).mdf',
    MOVE 'AdventureWorks$(YEAR)_log' TO '/var/opt/mssql/data/AdventureWorks$(YEAR)_log.ldf',
    FILE = 1,
    NOUNLOAD,
    STATS = 5;
GO

---------------------------------------------------------------------------------------------
-- Verify
SELECT name, database_id, create_date 
FROM sys.databases 
WHERE name = 'AdventureWorks$(YEAR)';
GO

---------------------------------------------------------------------------------------------
select top 10 * from AdventureWorks2017.Person.Address;
GO
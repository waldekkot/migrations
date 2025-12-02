:setvar YEAR "2017"
GO

USE [master];
GO

RESTORE FILELISTONLY 
FROM DISK = N'/var/opt/mssql/backup/AdventureWorks$(YEAR).bak';
GO

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

-- Verify
SELECT name, database_id, create_date 
FROM sys.databases 
WHERE name = 'AdventureWorks$(YEAR)';
GO

select top 10 * from AdventureWorks2017.Person.Address;
GO
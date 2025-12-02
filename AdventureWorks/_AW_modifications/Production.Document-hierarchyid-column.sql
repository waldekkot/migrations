/*============================================================================
  File:     migrate_document_tables_hierarchyid_to_varchar.sql

  Summary:  Migrates Production.Document and Production.ProductDocument tables
            from hierarchyid to nvarchar for the DocumentNode columns.
            
  Database: AdventureWorks2017
  
  Features:
    - IDEMPOTENT: Safe to run multiple times
    - Full-text index handled outside transaction (SQL Server requirement)
    - Original tables preserved as _Backup for rollback
            
  Date:     2025-11-28
============================================================================*/

USE AdventureWorks2017;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

-- Temp table to track state across GO batches
IF OBJECT_ID('tempdb..#MigrationState') IS NOT NULL DROP TABLE #MigrationState;
CREATE TABLE #MigrationState (ShouldProceed BIT DEFAULT 1, AlreadyMigrated BIT DEFAULT 0);
INSERT INTO #MigrationState VALUES (1, 0);
GO

PRINT '============================================================================';
PRINT ' Migration: hierarchyid -> nvarchar (IDEMPOTENT)';
PRINT ' Started: ' + CONVERT(VARCHAR(30), GETDATE(), 121);
PRINT '============================================================================';
PRINT '';
GO

-- ============================================================================
-- PHASE 0: CLEANUP _New TABLES FROM PREVIOUS FAILED RUNS
-- ============================================================================
PRINT '=== Phase 0: Cleanup ===';

-- Drop _New table FKs if exist
IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_ProductDocument_New_Document_DocumentNode')
    ALTER TABLE Production.ProductDocument_New DROP CONSTRAINT FK_ProductDocument_New_Document_DocumentNode;
IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_ProductDocument_New_Product_ProductID')
    ALTER TABLE Production.ProductDocument_New DROP CONSTRAINT FK_ProductDocument_New_Product_ProductID;
IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_Document_New_Employee_Owner')
    ALTER TABLE Production.Document_New DROP CONSTRAINT FK_Document_New_Employee_Owner;

-- Drop _New tables (from failed runs only)
IF OBJECT_ID('Production.ProductDocument_New', 'U') IS NOT NULL DROP TABLE Production.ProductDocument_New;
IF OBJECT_ID('Production.Document_New', 'U') IS NOT NULL DROP TABLE Production.Document_New;

PRINT '  OK';
GO

-- ============================================================================
-- PHASE 1: PRE-FLIGHT VALIDATION
-- ============================================================================
PRINT '';
PRINT '=== Phase 1: Pre-flight Validation ===';

DECLARE @DocType NVARCHAR(128), @PDType NVARCHAR(128);

SELECT @DocType = t.name FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('Production.Document') AND c.name = 'DocumentNode';

SELECT @PDType = t.name FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('Production.ProductDocument') AND c.name = 'DocumentNode';

-- Check if already migrated
IF @DocType IN ('nvarchar', 'varchar') AND @PDType IN ('nvarchar', 'varchar')
BEGIN
    PRINT '  Document.DocumentNode is already ' + @DocType;
    PRINT '  Migration already complete - skipping to final status';
    UPDATE #MigrationState SET ShouldProceed = 0, AlreadyMigrated = 1;
END
ELSE IF @DocType = 'hierarchyid' AND @PDType = 'hierarchyid'
BEGIN
    PRINT '  Document.DocumentNode: hierarchyid (will migrate)';
    PRINT '  ProductDocument.DocumentNode: hierarchyid (will migrate)';
    
    -- Validate lengths
    DECLARE @MaxLen INT;
    SELECT @MaxLen = MAX(LEN(DocumentNode.ToString())) FROM Production.Document;
    SELECT @MaxLen = CASE WHEN MAX(LEN(DocumentNode.ToString())) > @MaxLen 
                         THEN MAX(LEN(DocumentNode.ToString())) ELSE @MaxLen END 
    FROM Production.ProductDocument;
    
    IF ISNULL(@MaxLen, 0) > 448
    BEGIN
        RAISERROR('ERROR: DocumentNode length exceeds 448 char limit', 16, 1);
        UPDATE #MigrationState SET ShouldProceed = 0;
    END
    ELSE
        PRINT '  OK: Max DocumentNode length: ' + CAST(ISNULL(@MaxLen, 0) AS VARCHAR(10));
END
ELSE
BEGIN
    RAISERROR('ERROR: Unexpected column types - Doc: %s, PD: %s', 16, 1, @DocType, @PDType);
    UPDATE #MigrationState SET ShouldProceed = 0;
END
GO

-- ============================================================================
-- PHASES 2-7: CREATE NEW TABLES, MIGRATE DATA, VERIFY
-- ============================================================================
IF EXISTS (SELECT 1 FROM #MigrationState WHERE ShouldProceed = 1)
BEGIN
    PRINT '';
    PRINT '=== Phase 2: Create Document_New ===';
    
    CREATE TABLE [Production].[Document_New](
        [DocumentNode] [nvarchar](448) NOT NULL,
        [DocumentLevel] AS (CASE WHEN [DocumentNode] = '/' THEN CAST(0 AS smallint)
            ELSE CAST(LEN([DocumentNode]) - LEN(REPLACE([DocumentNode], '/', '')) - 1 AS smallint) END),
        [Title] [nvarchar](50) NOT NULL,
        [Owner] [int] NOT NULL,
        [FolderFlag] [bit] NOT NULL CONSTRAINT [DF_Document_New_FolderFlag] DEFAULT (0),
        [FileName] [nvarchar](400) NOT NULL,
        [FileExtension] [nvarchar](8) NOT NULL,
        [Revision] [nchar](5) NOT NULL,
        [ChangeNumber] [int] NOT NULL CONSTRAINT [DF_Document_New_ChangeNumber] DEFAULT (0),
        [Status] [tinyint] NOT NULL,
        [DocumentSummary] [nvarchar](max) NULL,
        [Document] [varbinary](max) NULL,
        [rowguid] [uniqueidentifier] ROWGUIDCOL NOT NULL 
            CONSTRAINT [UQ_Document_New_rowguid] UNIQUE
            CONSTRAINT [DF_Document_New_rowguid] DEFAULT (NEWID()),
        [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_Document_New_ModifiedDate] DEFAULT (GETDATE()),
        CONSTRAINT [CK_Document_New_Status] CHECK ([Status] BETWEEN 1 AND 3),
        CONSTRAINT [PK_Document_New_DocumentNode] PRIMARY KEY CLUSTERED ([DocumentNode])
    );
    PRINT '  OK';

    PRINT '';
    PRINT '=== Phase 3: Create ProductDocument_New ===';
    
    CREATE TABLE [Production].[ProductDocument_New](
        [ProductID] [int] NOT NULL,
        [DocumentNode] [nvarchar](448) NOT NULL,
        [ModifiedDate] [datetime] NOT NULL CONSTRAINT [DF_ProductDocument_New_ModifiedDate] DEFAULT (GETDATE()),
        CONSTRAINT [PK_ProductDocument_New_ProductID_DocumentNode] PRIMARY KEY CLUSTERED ([ProductID], [DocumentNode])
    );
    PRINT '  OK';
END
GO

IF EXISTS (SELECT 1 FROM #MigrationState WHERE ShouldProceed = 1)
BEGIN
    PRINT '';
    PRINT '=== Phase 4: Migrate Data ===';

    INSERT INTO [Production].[Document_New] 
        ([DocumentNode], [Title], [Owner], [FolderFlag], [FileName], [FileExtension],
         [Revision], [ChangeNumber], [Status], [DocumentSummary], [Document], [rowguid], [ModifiedDate])
    SELECT [DocumentNode].ToString(), [Title], [Owner], [FolderFlag], [FileName], [FileExtension],
           [Revision], [ChangeNumber], [Status], [DocumentSummary], [Document], [rowguid], [ModifiedDate]
    FROM [Production].[Document];
    PRINT '  Document rows: ' + CAST(@@ROWCOUNT AS VARCHAR(10));

    INSERT INTO [Production].[ProductDocument_New] ([ProductID], [DocumentNode], [ModifiedDate])
    SELECT [ProductID], [DocumentNode].ToString(), [ModifiedDate]
    FROM [Production].[ProductDocument];
    PRINT '  ProductDocument rows: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
END
GO

IF EXISTS (SELECT 1 FROM #MigrationState WHERE ShouldProceed = 1)
BEGIN
    PRINT '';
    PRINT '=== Phase 5: Create Indexes ===';

    CREATE UNIQUE INDEX [AK_Document_New_DocumentLevel_DocumentNode] ON [Production].[Document_New] ([DocumentLevel], [DocumentNode]);
    CREATE UNIQUE INDEX [AK_Document_New_rowguid] ON [Production].[Document_New] ([rowguid]);
    CREATE INDEX [IX_Document_New_FileName_Revision] ON [Production].[Document_New] ([FileName], [Revision]);
    PRINT '  OK';

    PRINT '';
    PRINT '=== Phase 6: Create Foreign Keys ===';

    ALTER TABLE [Production].[Document_New] ADD CONSTRAINT [FK_Document_New_Employee_Owner] 
        FOREIGN KEY ([Owner]) REFERENCES [HumanResources].[Employee] ([BusinessEntityID]);
    ALTER TABLE [Production].[ProductDocument_New] ADD CONSTRAINT [FK_ProductDocument_New_Product_ProductID] 
        FOREIGN KEY ([ProductID]) REFERENCES [Production].[Product] ([ProductID]);
    ALTER TABLE [Production].[ProductDocument_New] ADD CONSTRAINT [FK_ProductDocument_New_Document_DocumentNode] 
        FOREIGN KEY ([DocumentNode]) REFERENCES [Production].[Document_New] ([DocumentNode]);
    PRINT '  OK';

    PRINT '';
    PRINT '=== Phase 7: Verify Data ===';

    DECLARE @OrigDoc INT, @NewDoc INT, @OrigPD INT, @NewPD INT;
    SELECT @OrigDoc = COUNT(*) FROM Production.Document;
    SELECT @NewDoc = COUNT(*) FROM Production.Document_New;
    SELECT @OrigPD = COUNT(*) FROM Production.ProductDocument;
    SELECT @NewPD = COUNT(*) FROM Production.ProductDocument_New;

    IF @OrigDoc <> @NewDoc OR @OrigPD <> @NewPD
    BEGIN
        RAISERROR('ERROR: Row count mismatch!', 16, 1);
        UPDATE #MigrationState SET ShouldProceed = 0;
    END
    ELSE
        PRINT '  OK: Row counts verified';
END
GO

-- ============================================================================
-- PHASE 8: TABLE SWAP (in transaction)
-- ============================================================================
-- Drop full-text index before transaction (cannot be in transaction)
IF EXISTS (SELECT 1 FROM #MigrationState WHERE ShouldProceed = 1)
   AND EXISTS (SELECT 1 FROM sys.fulltext_indexes WHERE object_id = OBJECT_ID('Production.Document'))
BEGIN
    DROP FULLTEXT INDEX ON [Production].[Document];
    PRINT '  Dropped full-text index';
END
GO

DECLARE @ShouldSwap BIT;
SELECT @ShouldSwap = ShouldProceed FROM #MigrationState;

IF @ShouldSwap = 1
BEGIN
    PRINT '';
    PRINT '=== Phase 8: Table Swap ===';
    
    BEGIN TRANSACTION;
    BEGIN TRY
        -- Drop existing FKs
        IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_ProductDocument_Document_DocumentNode')
            ALTER TABLE Production.ProductDocument DROP CONSTRAINT FK_ProductDocument_Document_DocumentNode;
        IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_ProductDocument_Product_ProductID')
            ALTER TABLE Production.ProductDocument DROP CONSTRAINT FK_ProductDocument_Product_ProductID;
        IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_Document_Employee_Owner')
            ALTER TABLE Production.Document DROP CONSTRAINT FK_Document_Employee_Owner;

        -- Drop _New FKs (will recreate with final names)
        IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_ProductDocument_New_Document_DocumentNode')
            ALTER TABLE Production.ProductDocument_New DROP CONSTRAINT FK_ProductDocument_New_Document_DocumentNode;
        IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_ProductDocument_New_Product_ProductID')
            ALTER TABLE Production.ProductDocument_New DROP CONSTRAINT FK_ProductDocument_New_Product_ProductID;
        IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_Document_New_Employee_Owner')
            ALTER TABLE Production.Document_New DROP CONSTRAINT FK_Document_New_Employee_Owner;

        -- Rename original constraints to _Backup (constraint names are database-scoped)
        IF EXISTS (SELECT 1 FROM sys.objects WHERE name = 'PK_Document_DocumentNode' AND type = 'PK')
            EXEC sp_rename 'Production.PK_Document_DocumentNode', 'PK_Document_Backup_DocumentNode', 'OBJECT';
        IF EXISTS (SELECT 1 FROM sys.objects WHERE name = 'CK_Document_Status' AND type = 'C')
            EXEC sp_rename 'Production.CK_Document_Status', 'CK_Document_Backup_Status', 'OBJECT';
        IF EXISTS (SELECT 1 FROM sys.objects WHERE name = 'DF_Document_FolderFlag' AND type = 'D')
            EXEC sp_rename 'Production.DF_Document_FolderFlag', 'DF_Document_Backup_FolderFlag', 'OBJECT';
        IF EXISTS (SELECT 1 FROM sys.objects WHERE name = 'DF_Document_ChangeNumber' AND type = 'D')
            EXEC sp_rename 'Production.DF_Document_ChangeNumber', 'DF_Document_Backup_ChangeNumber', 'OBJECT';
        IF EXISTS (SELECT 1 FROM sys.objects WHERE name = 'DF_Document_rowguid' AND type = 'D')
            EXEC sp_rename 'Production.DF_Document_rowguid', 'DF_Document_Backup_rowguid', 'OBJECT';
        IF EXISTS (SELECT 1 FROM sys.objects WHERE name = 'DF_Document_ModifiedDate' AND type = 'D')
            EXEC sp_rename 'Production.DF_Document_ModifiedDate', 'DF_Document_Backup_ModifiedDate', 'OBJECT';
        IF EXISTS (SELECT 1 FROM sys.objects WHERE name = 'UQ_Document_rowguid' AND type = 'UQ')
            EXEC sp_rename 'Production.UQ_Document_rowguid', 'UQ_Document_Backup_rowguid', 'OBJECT';
        IF EXISTS (SELECT 1 FROM sys.objects WHERE name = 'PK_ProductDocument_ProductID_DocumentNode' AND type = 'PK')
            EXEC sp_rename 'Production.PK_ProductDocument_ProductID_DocumentNode', 'PK_ProductDocument_Backup_ProductID_DocumentNode', 'OBJECT';
        IF EXISTS (SELECT 1 FROM sys.objects WHERE name = 'DF_ProductDocument_ModifiedDate' AND type = 'D')
            EXEC sp_rename 'Production.DF_ProductDocument_ModifiedDate', 'DF_ProductDocument_Backup_ModifiedDate', 'OBJECT';

        -- Rename original indexes to _Backup
        IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('Production.Document') AND name = 'AK_Document_DocumentLevel_DocumentNode')
            EXEC sp_rename 'Production.Document.AK_Document_DocumentLevel_DocumentNode', 'AK_Document_Backup_DocumentLevel_DocumentNode', 'INDEX';
        IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('Production.Document') AND name = 'AK_Document_rowguid')
            EXEC sp_rename 'Production.Document.AK_Document_rowguid', 'AK_Document_Backup_rowguid', 'INDEX';
        IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('Production.Document') AND name = 'IX_Document_FileName_Revision')
            EXEC sp_rename 'Production.Document.IX_Document_FileName_Revision', 'IX_Document_Backup_FileName_Revision', 'INDEX';

        -- Swap tables
        EXEC sp_rename 'Production.ProductDocument', 'ProductDocument_Backup';
        EXEC sp_rename 'Production.Document', 'Document_Backup';
        EXEC sp_rename 'Production.Document_New', 'Document';
        EXEC sp_rename 'Production.ProductDocument_New', 'ProductDocument';
        PRINT '  Tables swapped';

        -- Rename _New constraints to final names
        EXEC sp_rename 'Production.PK_Document_New_DocumentNode', 'PK_Document_DocumentNode', 'OBJECT';
        EXEC sp_rename 'Production.CK_Document_New_Status', 'CK_Document_Status', 'OBJECT';
        EXEC sp_rename 'Production.DF_Document_New_FolderFlag', 'DF_Document_FolderFlag', 'OBJECT';
        EXEC sp_rename 'Production.DF_Document_New_ChangeNumber', 'DF_Document_ChangeNumber', 'OBJECT';
        EXEC sp_rename 'Production.DF_Document_New_rowguid', 'DF_Document_rowguid', 'OBJECT';
        EXEC sp_rename 'Production.DF_Document_New_ModifiedDate', 'DF_Document_ModifiedDate', 'OBJECT';
        EXEC sp_rename 'Production.UQ_Document_New_rowguid', 'UQ_Document_rowguid', 'OBJECT';
        EXEC sp_rename 'Production.PK_ProductDocument_New_ProductID_DocumentNode', 'PK_ProductDocument_ProductID_DocumentNode', 'OBJECT';
        EXEC sp_rename 'Production.DF_ProductDocument_New_ModifiedDate', 'DF_ProductDocument_ModifiedDate', 'OBJECT';

        -- Rename indexes
        EXEC sp_rename 'Production.Document.AK_Document_New_DocumentLevel_DocumentNode', 'AK_Document_DocumentLevel_DocumentNode', 'INDEX';
        EXEC sp_rename 'Production.Document.AK_Document_New_rowguid', 'AK_Document_rowguid', 'INDEX';
        EXEC sp_rename 'Production.Document.IX_Document_New_FileName_Revision', 'IX_Document_FileName_Revision', 'INDEX';

        -- Recreate FKs
        ALTER TABLE Production.Document ADD CONSTRAINT FK_Document_Employee_Owner 
            FOREIGN KEY (Owner) REFERENCES HumanResources.Employee (BusinessEntityID);
        ALTER TABLE Production.ProductDocument ADD CONSTRAINT FK_ProductDocument_Product_ProductID 
            FOREIGN KEY (ProductID) REFERENCES Production.Product (ProductID);
        ALTER TABLE Production.ProductDocument ADD CONSTRAINT FK_ProductDocument_Document_DocumentNode 
            FOREIGN KEY (DocumentNode) REFERENCES Production.Document (DocumentNode);

        COMMIT;
        PRINT '  OK: Transaction committed';
    END TRY
    BEGIN CATCH
        ROLLBACK;
        PRINT '  ERROR: ' + ERROR_MESSAGE();
        UPDATE #MigrationState SET ShouldProceed = 0;
    END CATCH
END
GO

-- Recreate full-text index (outside transaction)
IF EXISTS (SELECT 1 FROM #MigrationState WHERE ShouldProceed = 1)
   AND FULLTEXTSERVICEPROPERTY('IsFullTextInstalled') = 1
   AND EXISTS (SELECT 1 FROM sys.fulltext_catalogs)
BEGIN
    DECLARE @CatalogName NVARCHAR(128);
    SELECT TOP 1 @CatalogName = name FROM sys.fulltext_catalogs;
    IF @CatalogName IS NOT NULL AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('Production.Document') AND name = 'PK_Document_DocumentNode')
    BEGIN
        DECLARE @SQL NVARCHAR(MAX) = 'CREATE FULLTEXT INDEX ON Production.Document (Document TYPE COLUMN FileExtension, DocumentSummary) KEY INDEX PK_Document_DocumentNode ON ' + QUOTENAME(@CatalogName);
        EXEC sp_executesql @SQL;
        PRINT '  Full-text index recreated';
    END
END
GO

-- ============================================================================
-- PHASE 9: HELPER FUNCTIONS (always run - idempotent)
-- ============================================================================
PRINT '';
PRINT '=== Phase 9: Helper Functions ===';
GO

CREATE OR ALTER FUNCTION dbo.ufnGetParentDocumentNode(@Node nvarchar(448))
RETURNS nvarchar(448) WITH SCHEMABINDING AS
BEGIN
    IF @Node IS NULL OR @Node = '/' RETURN NULL;
    DECLARE @Trimmed nvarchar(448) = LEFT(@Node, LEN(@Node) - 1);
    DECLARE @LastSlash INT = LEN(@Trimmed) - CHARINDEX('/', REVERSE(@Trimmed)) + 1;
    RETURN IIF(@LastSlash <= 1, '/', LEFT(@Trimmed, @LastSlash));
END;
GO

CREATE OR ALTER FUNCTION dbo.ufnIsDescendantOf(@Child nvarchar(448), @Parent nvarchar(448))
RETURNS bit WITH SCHEMABINDING AS
BEGIN
    IF @Child IS NULL OR @Parent IS NULL RETURN 0;
    IF @Parent = '/' RETURN 1;
    RETURN IIF(LEFT(@Child, LEN(@Parent)) = @Parent, 1, 0);
END;
GO

CREATE OR ALTER FUNCTION dbo.ufnGetDocumentLevel(@Node nvarchar(448))
RETURNS smallint WITH SCHEMABINDING AS
BEGIN
    IF @Node IS NULL RETURN NULL;
    IF @Node = '/' RETURN 0;
    RETURN CAST(LEN(@Node) - LEN(REPLACE(@Node, '/', '')) - 1 AS smallint);
END;
GO

CREATE OR ALTER FUNCTION dbo.ufnGetChildDocumentNode(@Parent nvarchar(448), @ChildNum int)
RETURNS nvarchar(448) WITH SCHEMABINDING AS
BEGIN
    IF @Parent IS NULL SET @Parent = '/';
    RETURN @Parent + CAST(@ChildNum AS nvarchar(20)) + '/';
END;
GO

PRINT '  OK';
GO

-- ============================================================================
-- FINAL STATUS
-- ============================================================================
PRINT '';
PRINT '============================================================================';

DECLARE @DocType NVARCHAR(128), @PDType NVARCHAR(128), @WasAlreadyMigrated BIT = 0;
SELECT @WasAlreadyMigrated = AlreadyMigrated FROM #MigrationState;

SELECT @DocType = t.name FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('Production.Document') AND c.name = 'DocumentNode';

SELECT @PDType = t.name FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('Production.ProductDocument') AND c.name = 'DocumentNode';

IF @DocType IN ('nvarchar', 'varchar') AND @PDType IN ('nvarchar', 'varchar')
BEGIN
    IF @WasAlreadyMigrated = 1
        PRINT ' STATUS: ALREADY MIGRATED (no changes made)';
    ELSE
        PRINT ' STATUS: SUCCESS';
    
    PRINT '';
    PRINT ' Document.DocumentNode: ' + @DocType;
    PRINT ' ProductDocument.DocumentNode: ' + @PDType;
    PRINT '';
    IF OBJECT_ID('Production.Document_Backup', 'U') IS NOT NULL
        PRINT ' Backup: Production.Document_Backup, Production.ProductDocument_Backup';
    PRINT '';
    PRINT ' To cleanup: DROP TABLE Production.ProductDocument_Backup; DROP TABLE Production.Document_Backup;';
END
ELSE
BEGIN
    PRINT ' STATUS: FAILED';
    PRINT ' Document.DocumentNode: ' + ISNULL(@DocType, 'UNKNOWN');
    PRINT ' ProductDocument.DocumentNode: ' + ISNULL(@PDType, 'UNKNOWN');
END

PRINT '============================================================================';
PRINT ' Finished: ' + CONVERT(VARCHAR(30), GETDATE(), 121);
PRINT '============================================================================';

DROP TABLE #MigrationState;
GO

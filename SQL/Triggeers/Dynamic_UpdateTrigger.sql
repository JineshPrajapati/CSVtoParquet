GO
/***DYNAMIC*** Object:  Trigger [dbo].[trgEmployeeDynamicAudit]    Script Date: 22-May-24 10:15:33 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create TRIGGER [dbo].[trgEmployeeDynamicAudit]
ON [dbo].[TABLENAME] -- update table name with your table name
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

      DECLARE @Id INT, @ModifiedDate DATETIME;
      DECLARE @OldValue varchar(40), @NewValue varchar(40) ;
      DECLARE @ColumnName varchar(100),@identityColumn varchar(20);
      DECLARE @SQL NVARCHAR(max), @ParamDefinition NVARCHAR(max);
	
      select * into #tempInserted from inserted;  
      select * into #tempDeleted from deleted;
  
      set @identityColumn = (select C.COLUMN_NAME FROM
                						 INFORMATION_SCHEMA. TABLE_CONSTRAINTS T
                						 JOIN INFORMATION_SCHEMA. CONSTRAINT_COLUMN_USAGE C ON C.CONSTRAINT_NAME=T.CONSTRAINT_NAME
                						 WHERE C.TABLE_NAME='Employee' and T.CONSTRAINT_TYPE='PRIMARY KEY'
                            );

    -- Cursor to iterate through the columns
    DECLARE column_cursor CURSOR FOR
    SELECT COLUMN_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TABLENAME' AND COLUMN_NAME NOT IN ('Id', 'OtherNonUpdatedColumns');   -- update table name with your table name

    SET @ModifiedDate = GETDATE();

    OPEN column_cursor;
    FETCH NEXT FROM column_cursor INTO @ColumnName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
      
        -- Construct the dynamic SQL to check for update and log the change
        SET @SQL = N'  
                	INSERT INTO Employee_Audit_log (Id, ModifidDate, Field, oldValue, newValue)
                        SELECT distinct i.ID,GetDate(),'''+QUOTENAME(@ColumnName)+''',d.' + QUOTENAME(@ColumnName) + ',i.' + QUOTENAME(@ColumnName) + ' FROM #tempInserted i
                	inner join #tempDeleted d on d.'+QUOTENAME(@ColumnName)+'<>i.'+QUOTENAME(@ColumnName)+' AND d.'+QUOTENAME(@identityColumn)+'=i.'+QUOTENAME(@identityColumn)+';
                   ';
        SET @ParamDefinition = N'@ModifiedDate DATETIME, @ColumnName varchar(100), @OldValue varchar(40), @NewValue varchar(40)';

        -- Execute the dynamic SQL
        EXEC sp_executesql @SQL, @ParamDefinition, @ModifiedDate, @ColumnName, @OldValue, @NewValue;

        FETCH NEXT FROM column_cursor INTO @ColumnName;
    END
      
    CLOSE column_cursor;
    DEALLOCATE column_cursor;
END;

-- crerate table for reference --

---EMPLOYEE table
CREATE TABLE [dbo].[Employee](
	[Id] [int] NOT NULL,
	[Name] [varchar](45) NULL,
	[Salary] [int] NULL,
	[Gender] [varchar](12) NULL,
	[DepartmentId] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


---- Employee_Audit_log table
CREATE TABLE [dbo].[Employee_Audit_log](
	[Id] [int] NULL,
	[ModifidDate] [datetime] NULL,
	[Field] [varchar](20) NULL,
	[oldValue] [varchar](45) NULL,
	[newValue] [varchar](45) NULL
) ON [PRIMARY]

----------------------------------------------------------------------------------------------------------------------------------------------
-- Update Triggers in 2 ways
Create TRIGGER trUpdateEmployee
ON Employee
AFTER UPDATE
AS
BEGIN

   DECLARE @Id INT, @OldSalary INT, @NewSalary INT, @OldName VARCHAR(45), @NewName VARCHAR(45), @OldGender VARCHAR(12), @NewGender VARCHAR(12), @OldDepartmentId INT, @NewDepartmentId INT	   
   -- INSERT INTO Employee_Audit_log (Id,ModifidDate,Field,oldValue,newValue)
    --SELECT 
      --CAST(i.Id AS VARCHAR(20)),getdate(),   
        CASE 
            WHEN d.Name <> i.Name THEN  set @OldName=d.Name;   set @NewName= i.Name;  ELSE '' 
     
            WHEN d.Salary <> i.Salary THEN set @OldSalary= CAST(d.Salary AS VARCHAR(20)); set @NewSalary= CAST(i.Salary AS VARCHAR(20));  ELSE '' 
      
            WHEN d.Gender <> i.Gender THEN set @OldGender=d.Gender; @NewGender = i.Gender; ELSE '' 
  
            WHEN d.DepartmentId <> i.DepartmentId THEN  set @OldDepartmentId=CAST(d.DepartmentId AS VARCHAR(20)); set @NewDepartmentId + CAST(i.DepartmentId AS VARCHAR(20));
            ELSE '' 
        END
    FROM inserted i
    INNER JOIN deleted d ON i.Id = d.Id
    WHERE 
        d.Name <> i.Name OR
        d.Salary <> i.Salary OR
        d.Gender <> i.Gender OR
        d.DepartmentId <> i.DepartmentId;
END;

----- **********    update trigger 2

Create TRIGGER trUpdateEmployee1
ON Employee
AFTER UPDATE
AS
BEGIN
    -- For Name changes
    INSERT INTO Employee_Audit_log (Id, ModifidDate, Field, OldValue, NewValue)
    SELECT 
        d.Id,
        GETDATE(),
        'Name',
        d.Name,
        i.Name
    FROM inserted i
    INNER JOIN deleted d ON i.Id = d.Id
    WHERE d.Name <> i.Name;

    -- For Salary changes
    INSERT INTO Employee_Audit_log (Id, ModifidDate, Field, OldValue, NewValue)
    SELECT 
        d.Id,
        GETDATE(),
        'Salary',
        CAST(d.Salary AS VARCHAR(45)),
        CAST(i.Salary AS VARCHAR(45))
    FROM inserted i
    INNER JOIN deleted d ON i.Id = d.Id
    WHERE d.Salary <> i.Salary;

    -- For Gender changes
    INSERT INTO Employee_Audit_log (Id, ModifidDate, Field, OldValue, NewValue)
    SELECT 
        d.Id,
        GETDATE(),
        'Gender',
        d.Gender,
        i.Gender
    FROM inserted i
    INNER JOIN deleted d ON i.Id = d.Id
    WHERE d.Gender <> i.Gender;

    -- For DepartmentId changes
    INSERT INTO Employee_Audit_log (Id, ModifidDate, Field, OldValue, NewValue)
    SELECT 
        d.Id,
        GETDATE(),
        'DepartmentId',
        CAST(d.DepartmentId AS VARCHAR(45)),
        CAST(i.DepartmentId AS VARCHAR(45))
    FROM inserted i
    INNER JOIN deleted d ON i.Id = d.Id
    WHERE d.DepartmentId <> i.DepartmentId;
END;

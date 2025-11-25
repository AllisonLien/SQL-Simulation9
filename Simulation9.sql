USE AdventureWorks2019;
GO

--task1:Schema and Object Design
CREATE SCHEMA SalesOpsSim;
GO

CREATE TABLE SalesOpsSim.OrderReviewQueue (
    ReviewID INT IDENTITY PRIMARY KEY,
    SalesOrderID INT NOT NULL,
    CustomerID INT NOT NULL,
    EmployeeID INT NULL,
    OrderDate DATETIME NOT NULL,
    TotalDue MONEY NULL,
    Freight MONEY NULL,
    IsDataComplete BIT NOT NULL DEFAULT 1
);
CREATE TABLE SalesOpsSim.EmployeeOrderLoad (
    LoadID INT IDENTITY PRIMARY KEY,
    EmployeeID INT NOT NULL,
    SalesOrderID INT NOT NULL,
    LoadAssignedDate DATETIME DEFAULT GETDATE()
);
CREATE TABLE SalesOpsSim.OrderRiskLog (
    LogID INT IDENTITY PRIMARY KEY,
    EmployeeID INT,
    SalesOrderID INT,
    LoadFactor FLOAT,
    DaysOutstanding INT,
    RiskScore FLOAT,
    LoggedAt DATETIME DEFAULT GETDATE()
);
--task2:Reconstruction of the Legacy Order Intelligence Logic
DECLARE @EmployeeID INT;
DECLARE employee_cursor CURSOR FOR
SELECT DISTINCT SalesPersonID FROM Sales.SalesOrderHeader WHERE SalesPersonID IS NOT NULL;
OPEN employee_cursor;
FETCH NEXT FROM employee_cursor INTO @EmployeeID;

WHILE @@FETCH_STATUS = 0
BEGIN
    DECLARE @SalesOrderID INT, @TotalDue MONEY, @Freight MONEY, @OrderDate DATETIME;
    DECLARE order_cursor CURSOR FOR
    SELECT SalesOrderID, TotalDue, Freight, OrderDate
    FROM Sales.SalesOrderHeader
    WHERE SalesPersonID = @EmployeeID;

    OPEN order_cursor;
    FETCH NEXT FROM order_cursor INTO @SalesOrderID, @TotalDue, @Freight, @OrderDate;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @LoadFactor FLOAT, @DaysOutstanding INT, @RiskScore FLOAT;

        SET @LoadFactor = CASE WHEN @Freight = 0 THEN NULL ELSE @TotalDue / @Freight END;
        SET @DaysOutstanding = DATEDIFF(DAY, @OrderDate, GETDATE());
        SET @RiskScore = @LoadFactor * @DaysOutstanding;

        INSERT INTO SalesOpsSim.OrderRiskLog
        (EmployeeID, SalesOrderID, LoadFactor, DaysOutstanding, RiskScore)
        VALUES
        (@EmployeeID, @SalesOrderID, @LoadFactor, @DaysOutstanding, @RiskScore);

        FETCH NEXT FROM order_cursor INTO @SalesOrderID, @TotalDue, @Freight, @OrderDate;
    END

    CLOSE order_cursor;
    DEALLOCATE order_cursor;

    FETCH NEXT FROM employee_cursor INTO @EmployeeID;
END

CLOSE employee_cursor;
DEALLOCATE employee_cursor;
go

SELECT TOP 20 * FROM SalesOpsSim.OrderRiskLog;

--Task 4 — Modernized Implementation
INSERT INTO SalesOpsSim.OrderRiskLog 
(EmployeeID, SalesOrderID, LoadFactor, DaysOutstanding, RiskScore)
SELECT  
    soh.SalesPersonID,
    soh.SalesOrderID,
    CASE WHEN soh.Freight = 0 THEN NULL ELSE soh.TotalDue / soh.Freight END,
    DATEDIFF(DAY, soh.OrderDate, GETDATE()),
    CASE 
        WHEN soh.Freight = 0 THEN NULL 
        ELSE (soh.TotalDue / soh.Freight) * DATEDIFF(DAY, soh.OrderDate, GETDATE()) 
    END
FROM Sales.SalesOrderHeader soh
WHERE soh.SalesPersonID IS NOT NULL;
GO
--Task 5 — Index Strategy Design
CREATE NONCLUSTERED INDEX IX_SOH_SalesPersonID
ON Sales.SalesOrderHeader (SalesPersonID);

CREATE NONCLUSTERED INDEX IX_SOH_Date_Cover
ON Sales.SalesOrderHeader (OrderDate)
INCLUDE (TotalDue, Freight);

CREATE NONCLUSTERED INDEX IX_SOH_Freight_Positive
ON Sales.SalesOrderHeader (Freight)
WHERE Freight > 0;
GO
--Task 6 — Performance Diagnostics
-- Legacy Cursor
SET STATISTICS IO ON;
SET STATISTICS TIME ON;

TRUNCATE TABLE SalesOpsSim.OrderRiskLog;

DECLARE @EmployeeID INT;
DECLARE employee_cursor CURSOR FOR
SELECT DISTINCT SalesPersonID FROM Sales.SalesOrderHeader WHERE SalesPersonID IS NOT NULL;

OPEN employee_cursor;
FETCH NEXT FROM employee_cursor INTO @EmployeeID;

WHILE @@FETCH_STATUS = 0
BEGIN
    DECLARE @SalesOrderID INT, @TotalDue MONEY, @Freight MONEY, @OrderDate DATETIME;
    DECLARE order_cursor CURSOR FOR
    SELECT SalesOrderID, TotalDue, Freight, OrderDate
    FROM Sales.SalesOrderHeader
    WHERE SalesPersonID = @EmployeeID;

    OPEN order_cursor;
    FETCH NEXT FROM order_cursor INTO @SalesOrderID, @TotalDue, @Freight, @OrderDate;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @LoadFactor FLOAT, @DaysOutstanding INT, @RiskScore FLOAT;
        SET @LoadFactor = CASE WHEN @Freight = 0 THEN NULL ELSE @TotalDue / @Freight END;
        SET @DaysOutstanding = DATEDIFF(DAY, @OrderDate, GETDATE());
        SET @RiskScore = @LoadFactor * @DaysOutstanding;

        INSERT INTO SalesOpsSim.OrderRiskLog
        (EmployeeID, SalesOrderID, LoadFactor, DaysOutstanding, RiskScore)
        VALUES
        (@EmployeeID, @SalesOrderID, @LoadFactor, @DaysOutstanding, @RiskScore);

        FETCH NEXT FROM order_cursor INTO @SalesOrderID, @TotalDue, @Freight, @OrderDate;
    END

    CLOSE order_cursor;
    DEALLOCATE order_cursor;

    FETCH NEXT FROM employee_cursor INTO @EmployeeID;
END

CLOSE employee_cursor;
DEALLOCATE employee_cursor;
GO

-- Modernized Set-Based
TRUNCATE TABLE SalesOpsSim.OrderRiskLog;

INSERT INTO SalesOpsSim.OrderRiskLog 
(EmployeeID, SalesOrderID, LoadFactor, DaysOutstanding, RiskScore)
SELECT  
    soh.SalesPersonID,
    soh.SalesOrderID,
    CASE WHEN soh.Freight = 0 THEN NULL ELSE soh.TotalDue / soh.Freight END,
    DATEDIFF(DAY, soh.OrderDate, GETDATE()),
    CASE 
        WHEN soh.Freight = 0 THEN NULL 
        ELSE (soh.TotalDue / soh.Freight) * DATEDIFF(DAY, soh.OrderDate, GETDATE()) 
    END
FROM Sales.SalesOrderHeader soh
WHERE soh.SalesPersonID IS NOT NULL;
GO
-- Modernized with Indexes
TRUNCATE TABLE SalesOpsSim.OrderRiskLog;

INSERT INTO SalesOpsSim.OrderRiskLog 
(EmployeeID, SalesOrderID, LoadFactor, DaysOutstanding, RiskScore)
SELECT  
    soh.SalesPersonID,
    soh.SalesOrderID,
    CASE WHEN soh.Freight = 0 THEN NULL ELSE soh.TotalDue / soh.Freight END,
    DATEDIFF(DAY, soh.OrderDate, GETDATE()),
    CASE 
        WHEN soh.Freight = 0 THEN NULL 
        ELSE (soh.TotalDue / soh.Freight) * DATEDIFF(DAY, soh.OrderDate, GETDATE()) 
    END
FROM Sales.SalesOrderHeader soh
WHERE soh.SalesPersonID IS NOT NULL;
GO

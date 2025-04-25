/*

This scripts looks for existing database named DataWarehouseRetail, drops if exists, then creates a new one.
It creates three new schemas named bronze, silver, and gold.

*/


USE master;
GO


IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouseRetail')
BEGIN
	ALTER DATABASE DataWarehouseRetail SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouseRetail;
END;
GO

--Create new database for this project
CREATE DATABASE DataWarehouseRetail;
GO
USE DataWarehouseRetail;
GO


--Create schemas to organize layers
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

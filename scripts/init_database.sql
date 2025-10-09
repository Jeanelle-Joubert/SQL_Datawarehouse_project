/*
----------------------------------------------------------------------------------------
Create Database and Schemas
----------------------------------------------------------------------------------------
Scipt Purpose:
  This script creates a new database 'DataWarehouse' after checking if it already exists.
  If the database exists, it is dropped and recreated. The script also sets up three schemas within the database:
  'bronze','silver', and 'gold'.

WARNING:
  Running this script will drop the database if it already exists and all data within database will be permanently deleted.
  Ensure for proper backups before running this script
*/


-- create database DataWarehouse 
USE master; 
GO

  -- drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
END;
GO

-- create "DataWarehouse' database
CREATE DATABASE DataWarehouse;

USE DataWarehouse;

-- create schemas 
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

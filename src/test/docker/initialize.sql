CREATE DATABASE dbmigration;
GO
USE dbmigration;
GO
CREATE LOGIN dbmigration WITH PASSWORD='dbmigration', CHECK_POLICY=OFF;
GO
CREATE SCHEMA dbmigration;
GO
CREATE USER dbmigration FOR LOGIN dbmigration WITH DEFAULT_SCHEMA=dbmigration;
GO
EXEC sp_addrolemember db_owner, dbmigration;
GO
EXIT

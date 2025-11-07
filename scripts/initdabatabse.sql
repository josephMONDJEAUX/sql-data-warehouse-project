/*
===============================================================================
On crée la base de données et les couches
===============================================================================

WARNING: Si vous exécutez ce script vous allez supprimer la base de données si elle existe déjà 
*/

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = "DataWarehouse")
   BEGIN
     ALTER DATABASE DataWarehouse;  
     DROP DATABASE DataWarehouse;
   END;
   GO;



CREATE DATABASE DataWarehouse;

USE DataWarehouse;

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

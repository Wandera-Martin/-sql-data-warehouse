-- Drop the DataWarehouse database if it exists.
DROP DATABASE IF EXISTS "DataWarehouse";


-- Create the DataWarehouse database.
CREATE DATABASE "DataWarehouse";

-- Create the schemas.
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
    
/*
====================================================================================================
The script creates the DataWarehouse database and three schemas: bronze, silver, and gold. 
The next step is to create the tables in the bronze schema. 
Create the tables in the bronze schema 
The bronze schema contains the tables that store the raw data.

WARNING:
    Running this script will drop the entire DataWarehouse database and 
    all its contents, if it exists.
    All data in the database will be permanently lost. Proceed with caution
    and ensure you have proper backups before running this script.
*/




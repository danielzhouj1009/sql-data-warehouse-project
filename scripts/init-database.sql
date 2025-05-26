/*
Create Database and Schemas

Script Purpose: 
  This script creates a new database named 'DataWarehouse' after checkingn if it already exists. 
  If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas within the database:
  'bronze', 'silver', and 'gold'.
WARNING: 
  Running this script will drop the entire 'DataWarehouse' database if it exists. All data in the database will be permanently deleted. 
  Proceed with caution and ensure you have proper backups before running this script. 
*/


USE master;

-- Drop and recreate the 'Datawarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse; 
  GO
    
-- Create the 'Datawarehouse' database
CREATE DATABASE DataWarehouse;

USE DataWarehouse;

-- Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

-- Create tables with headers
IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info(
cst_id INT, 
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_material_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE
);

IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info(
prd_id INT, 
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE
);

IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details(
sls_ord_num NVARCHAR(50), 
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;

CREATE TABLE bronze.erp_loc_a101(
cid NVARCHAR(50), 
cntry NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;

CREATE TABLE bronze.erp_cust_az12(
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;

CREATE TABLE bronze.erp_px_cat_g1v2(
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50)
);


-- Bulk upload csv tables and save as a procedure. Also the procude displayed the running time and errors if any

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '==============================================================';
		PRINT 'Loading Bonze Layer';
		PRINT '==============================================================';

		PRINT '--------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT'>> Inserting Data Into: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\danie\OneDrive\Documents\Data with Baraa\Warehouse Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'------------------'

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT'>> Inserting Data Into: bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\danie\OneDrive\Documents\Data with Baraa\Warehouse Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'------------------'

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT'>> Inserting Data Into: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\danie\OneDrive\Documents\Data with Baraa\Warehouse Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'------------------'

		
		PRINT '--------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '--------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT'>> Inserting Data Into: bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\danie\OneDrive\Documents\Data with Baraa\Warehouse Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.CSV'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'------------------'

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT'>> Inserting Data Into: bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\danie\OneDrive\Documents\Data with Baraa\Warehouse Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.CSV'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'------------------'

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT'>> Inserting Data Into: bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\danie\OneDrive\Documents\Data with Baraa\Warehouse Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.CSV'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'------------------'

			SET @batch_end_time = GETDATE();
			PRINT'==================================================='
			PRINT'Loading Bronze Layer is Completed';
			PRINT'	- Total Load Duration:' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
			PRINT'==================================================='
	END TRY
	BEGIN CATCH 
		PRINT'====================================================='
		PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT'Error Message' + ERROR_MESSAGE();
		PRINT'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT'====================================================='
	END CATCH
END

-- Execuete the saved procedure 
EXEC bronze.load_bronze

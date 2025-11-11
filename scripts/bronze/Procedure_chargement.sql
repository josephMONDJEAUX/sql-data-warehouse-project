USE DataWarehouse;


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN

DECLARE @start_time DATETIME, @end_time DATETIME;
RAISERROR('==========================================================================', 0, 1) WITH NOWAIT;
RAISERROR('LOADING BRONZE LAYER', 0, 1) WITH NOWAIT;
RAISERROR('==========================================================================', 0, 1) WITH NOWAIT;

PRINT '==========================================================================='
PRINT 'LOADING BRONZE LAYER'
PRINT '==========================================================================='

SET @start_time = GETDATE();
PRINT '==========================================================================='
PRINT 'LOADING CRM SOURCES'
PRINT '==========================================================================='
TRUNCATE TABLE bronze.crm_cust_infos
BULK INSERT bronze.crm_cust_infos
FROM 'C:\Users\hp\Desktop\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	)

	select * from bronze.crm_cust_infos
	select count (*) from bronze.crm_cust_infos

TRUNCATE TABLE bronze.crm_prd_info
BULK INSERT bronze.crm_prd_info
FROM 'C:\Users\hp\Desktop\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	)

	select * from bronze.crm_prd_info
	select count (*) from bronze.crm_prd_info



TRUNCATE TABLE bronze.crm_sales_details
BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\hp\Desktop\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	)

	select * from bronze.crm_sales_details
	select count (*) from bronze.crm_sales_details

	
TRUNCATE TABLE bronze.crm_prd_info
BULK INSERT bronze.crm_prd_info
FROM 'C:\Users\hp\Desktop\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	)

	select * from bronze.crm_prd_info
	select count (*) from bronze.crm_prd_info

	SET @end_time = GETDATE();
PRINT '==========================================================================='
PRINT 'LOADING ERP SOURCES'
PRINT '==========================================================================='
	
SET @start_time = GETDATE();
TRUNCATE TABLE bronze.erp_cust_az12
BULK INSERT bronze.erp_cust_az12
FROM 'C:\Users\hp\Desktop\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	)

	select * from bronze.erp_cust_az12
	select count (*) from bronze.erp_cust_az12

TRUNCATE TABLE bronze.erp_loc_a101
BULK INSERT bronze.erp_loc_a101
FROM 'C:\Users\hp\Desktop\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	)

	select * from bronze.erp_loc_a101
	select count (*) from bronze.erp_loc_a101

TRUNCATE TABLE bronze.erp_px_cat_g1v2
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'C:\Users\hp\Desktop\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	)
	SET @end_time = GETDATE();
	select * from bronze.erp_px_cat_g1v2
	select count (*) from bronze.erp_px_cat_g1v2

	PRINT '***Load duration: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	END


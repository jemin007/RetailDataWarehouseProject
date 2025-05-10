/*

Bulk Load Script: Insert data to all the six required tables

Script Purpose:
    This script inserts tables from a location to the desired table.

*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
BEGIN TRY
	DECLARE @start_time DATETIME, @end_time DATETIME;
	TRUNCATE TABLE bronze.crm_cust_info_load;

	SET @start_time = GETDATE();
		BULK INSERT bronze.crm_cust_info_load
		FROM 'D:\Projects\RetailDWH\data\source_crm\cust_info.csv'
		WITH (
			FIRSTROW =2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
	SET @end_time = GETDATE();
	PRINT('Load time: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds');

	TRUNCATE TABLE bronze.crm_prd_info_load;

	BULK INSERT bronze.crm_prd_info_load
	FROM 'D:\Projects\RetailDWH\data\source_crm\prd_info.csv'
	WITH (
		FIRSTROW =2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	TRUNCATE TABLE bronze.crm_sales_details_load;

	SET @start_time = GETDATE();
	BULK INSERT bronze.crm_sales_details_load
	FROM 'D:\Projects\RetailDWH\data\source_crm\sales_details.csv'
	WITH (
		FIRSTROW =2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT('Load time: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds');
	----------------------------------------------------------------------------------------------------

	TRUNCATE TABLE bronze.erp_cust_az12_load;

	SET @start_time = GETDATE();
	BULK INSERT bronze.erp_cust_az12_load
	FROM 'D:\Projects\RetailDWH\data\source_erp\CUST_AZ12.csv'
	WITH (
		FIRSTROW =2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT('Load time: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds');

	TRUNCATE TABLE bronze.erp_loc_a101_load;

	SET @start_time = GETDATE();
	BULK INSERT bronze.erp_loc_a101_load
	FROM 'D:\Projects\RetailDWH\data\source_erp\LOC_A101.csv'
	WITH (
		FIRSTROW =2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT('Load time: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds');


	TRUNCATE TABLE bronze.erp_px_cat_g1v2_load;

	SET @start_time = GETDATE();
	BULK INSERT bronze.erp_px_cat_g1v2_load
	FROM 'D:\Projects\RetailDWH\data\source_erp\PX_CAT_G1V2.csv'
	WITH (
		FIRSTROW =2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT('Load time: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds');
END TRY

BEGIN CATCH
	PRINT'ERROR ORRCURED DURING LOAD BRONZE LAYER'
	PRINT 'Error Message: ' + ERROR_MESSAGE();
	PRINT 'Error Numeber: ' + CAST(ERROR_MESSAGE() AS NVARCHAR);
	PRINT 'Error Message: ' + CAST(ERROR_MESSAGE() AS NVARCHAR);

END CATCH
END
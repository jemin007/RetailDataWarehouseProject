/*

Bulk Load Script: Insert data to all the six required tables

Script Purpose:
    This script inserts tables from a location to the desired table.

*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN

TRUNCATE TABLE bronze.crm_cust_info_load;

BULK INSERT bronze.crm_cust_info_load
FROM 'D:\Projects\RetailDWH\data\source_crm\cust_info.csv'
WITH (
	FIRSTROW =2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

TRUNCATE TABLE bronze.crm_prd_info_load;

BULK INSERT bronze.crm_prd_info_load
FROM 'D:\Projects\RetailDWH\data\source_crm\prd_info.csv'
WITH (
	FIRSTROW =2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

TRUNCATE TABLE bronze.crm_sales_details_load;

BULK INSERT bronze.crm_sales_details_load
FROM 'D:\Projects\RetailDWH\data\source_crm\sales_details.csv'
WITH (
	FIRSTROW =2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

----------------------------------------------------------------------------------------------------

TRUNCATE TABLE bronze.erp_cust_az12_load;

BULK INSERT bronze.erp_cust_az12_load
FROM 'D:\Projects\RetailDWH\data\source_erp\CUST_AZ12.csv'
WITH (
	FIRSTROW =2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

TRUNCATE TABLE bronze.erp_loc_a101_load;

BULK INSERT bronze.erp_loc_a101_load
FROM 'D:\Projects\RetailDWH\data\source_erp\LOC_A101.csv'
WITH (
	FIRSTROW =2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

TRUNCATE TABLE bronze.erp_px_cat_g1v2_load;

BULK INSERT bronze.erp_px_cat_g1v2_load
FROM 'D:\Projects\RetailDWH\data\source_erp\PX_CAT_G1V2.csv'
WITH (
	FIRSTROW =2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

END
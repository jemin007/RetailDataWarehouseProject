/*

Bronze Merge Script: Merge all data from load table to main tables in bronze layer

Script Purpose:
    This procedure loads all data from _load table to main tables.

*/


CREATE OR ALTER PROCEDURE bronze.merge_load AS
BEGIN

	SET NOCOUNT ON;

	DECLARE @today DATE = CAST(GETDATE() AS DATE);
	DECLARE @load_count INT, @main_count INT, @new_insert_count INT;


	--crm_cust_info
	SET @load_count = (SELECT COUNT(*) FROM bronze.crm_cust_info_load)


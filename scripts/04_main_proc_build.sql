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
	SET @load_count = (SELECT COUNT(*) FROM bronze.crm_cust_info_load);

	UPDATE target
	SET 
		cst_key = source.cst_key,
        cst_firstname = source.cst_firstname,
        cst_lastname = source.cst_lastname,
        cst_marital_status = source.cst_marital_status,
        cst_gndr = source.cst_gndr,
        cst_create_date = source.cst_create_date
	FROM bronze.crm_cust_info target
	INNER JOIN 
	(
		SELECT * FROM bronze.crm_cust_info_load AS l
		WHERE cst_create_date = (
			SELECT MAX(cst_create_date) 
			FROM bronze.crm_cust_info_load
			WHERE cst_id = l.cst_id
		)
	)
	source
	ON target.cst_id = source.cst_id
	WHERE source.cst_id IS NOT NULL AND source.cst_key IS NOT NULL;


	INSERT INTO bronze.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
	SELECT cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
    FROM (
		SELECT * FROM bronze.crm_cust_info_load AS l
		WHERE cst_create_date = (SELECT MAX(cst_create_date) FROM bronze.crm_cust_info_load WHERE cst_id = l.cst_id)
	) source
	WHERE NOT EXISTS (
		SELECT 1 FROM bronze.crm_cust_info target WHERE target.cst_id = source.cst_id
	)
	AND source.cst_id IS NOT NULL AND source.cst_key IS NOT NULL;
	;

	SET @main_count = (SELECT COUNT(*) FROM bronze.crm_cust_info);
	SET @new_insert_count = (
	SELECT COUNT(*) 
		FROM bronze.crm_cust_info_load source 
		WHERE NOT EXISTS(
			SELECT 1 FROM bronze.crm_cust_info target WHERE target.cst_id = source.cst_id
		)
		);

	UPDATE target
	SET [load_tbl_records] = @load_count,
	[load_tbl_date] = @today,
	[man_tbl_records]= @main_count,
	[last_load] = @today,
	[new_insert] =@new_insert_count
	FROM [bronze].[bronze_main] target
	WHERE target.tbl_name = 'crm_cust_info'




	--crm_prd_info

	UPDATE target
	SET 
		prd_key = source.prd_key,
		prd_nm = source.prd_nm,
		prd_cost = source.prd_cost,
		prd_line = source.prd_line,
		prd_start_dt = source.prd_start_dt,
		prd_end_dt = source.prd_end_dt
	FROM bronze.crm_prd_info_load source
	WHERE bronze.crm_prd_info target 

END

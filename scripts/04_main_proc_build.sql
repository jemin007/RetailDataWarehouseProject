/*

Bronze Merge Script: Merge all data from load table to main tables in bronze layer

Script Purpose:
    This procedure loads all data from _load table to main tables.

*/


CREATE OR ALTER PROCEDURE bronze.merge_load @archive INT =0
AS
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

	PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' rows updated into crm_cust_info.';

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
	
	PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' rows inserted into crm_cust_info.';

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
	WHERE target.tbl_name = 'crm_cust_info';



	--crm_prd_info
	SET @load_count = (SELECT COUNT(*) FROM bronze.crm_prd_info_load);

	UPDATE target
	SET 
		prd_key = source.prd_key,
		prd_nm = source.prd_nm,
		prd_cost = source.prd_cost,
		prd_line = source.prd_line,
		prd_start_dt = source.prd_start_dt,
		prd_end_dt = source.prd_end_dt
	FROM bronze.crm_prd_info_load source
	INNER JOIN bronze.crm_prd_info target ON target.prd_id = source.prd_id
	WHERE target.prd_line IS NOT NULL OR target.prd_start_dt IS NOT NULL ; 

	PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' rows updated into crm_prd_info.';

	INSERT INTO bronze.crm_prd_info (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
	SELECT * FROM bronze.crm_prd_info_load source
	WHERE NOT EXISTS 
	(
		SELECT 1 FROM bronze.crm_prd_info target WHERE target.prd_id = source.prd_id
	)
	AND source.prd_id IS NOT NULL AND source.prd_key IS NOT NULL 

	PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' rows inserted into crm_prd_info.';

	SET @main_count = (SELECT COUNT(*) FROM bronze.crm_prd_info);
	SET @new_insert_count = (
	SELECT COUNT(*) 
		FROM bronze.crm_prd_info_load source 
		WHERE NOT EXISTS(
			SELECT 1 FROM bronze.crm_prd_info target WHERE target.prd_id = source.prd_id
		)
		);

	UPDATE target
	SET [load_tbl_records] = @load_count,
	[load_tbl_date] = @today,
	[man_tbl_records]= @main_count,
	[last_load] = @today,
	[new_insert] =@new_insert_count
	FROM [bronze].[bronze_main] target
	WHERE target.tbl_name = 'crm_prd_info';

	
	-- ========== crm_sales_details ==========
	SET @load_count = (SELECT COUNT(*) FROM bronze.crm_sales_details_load);

	UPDATE target
	SET 
		sls_order_dt = source.sls_order_dt,
		sls_ship_dt = source.sls_ship_dt,
		sls_due_dt = source.sls_due_dt,
		sls_sales = source.sls_sales,
		sls_quantity = source.sls_quantity,
		sls_price = source.sls_price
	FROM bronze.crm_sales_details target
	INNER JOIN bronze.crm_sales_details_load source
		ON target.sls_ord_num = source.sls_ord_num
		AND target.sls_prd_key = source.sls_prd_key
		AND target.sls_cust_id = source.sls_cust_id
	WHERE source.sls_ord_num IS NOT NULL
	AND source.sls_prd_key IS NOT NULL
	AND source.sls_cust_id IS NOT NULL;

	PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' rows updated into crm_sales_details.';


	INSERT INTO bronze.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
	SELECT sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
	FROM bronze.crm_sales_details_load source
	WHERE NOT EXISTS (
		SELECT 1 FROM bronze.crm_sales_details target
		WHERE target.sls_ord_num = source.sls_ord_num 
		  AND target.sls_prd_key = source.sls_prd_key 
		  AND target.sls_cust_id = source.sls_cust_id
	);


	PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' rows inserted into crm_sales_details.';


	SET @main_count = (SELECT COUNT(*) FROM bronze.crm_sales_details);
	SET @new_insert_count = @main_count - (SELECT COUNT(*) FROM bronze.crm_sales_details);

	UPDATE target
	SET [load_tbl_records] = @load_count,
	[load_tbl_date] = @today,
	[man_tbl_records] = @main_count,
	[last_load] = @today,
	[new_insert] = @new_insert_count
	FROM bronze.bronze_main target
	WHERE tbl_name = 'crm_sales_details';

	
	-- ========== erp_loc_a101 ==========
	SET @load_count = (SELECT COUNT(*) FROM bronze.erp_loc_a101_load);

	UPDATE target
	SET cntry = source.cntry
	FROM bronze.erp_loc_a101 target
	INNER JOIN bronze.erp_loc_a101_load source ON target.cid = source.cid;

	PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' rows updated into erp_loc_a101.';

	INSERT INTO bronze.erp_loc_a101 (cid, cntry)
	SELECT cid, cntry FROM bronze.erp_loc_a101_load source
	WHERE NOT EXISTS (
		SELECT 1 FROM bronze.erp_loc_a101 target WHERE target.cid = source.cid
	);

	PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' rows inserted into erp_loc_a101.';

	SET @main_count = (SELECT COUNT(*) FROM bronze.erp_loc_a101);
	SET @new_insert_count = (
		SELECT COUNT(*) FROM bronze.erp_loc_a101_load source
		WHERE NOT EXISTS (
			SELECT 1 FROM bronze.erp_loc_a101 target WHERE target.cid = source.cid
		)
	);

	UPDATE target
	SET [load_tbl_records] = @load_count,
	[load_tbl_date] = @today,
	[man_tbl_records] = @main_count,
	[last_load] = @today,
	[new_insert] = @new_insert_count
	FROM bronze.bronze_main target
	WHERE tbl_name = 'erp_loc_a101';



		-- ========== erp_cust_az12 ==========
	SET @load_count = (SELECT COUNT(*) FROM bronze.erp_cust_az12_load);

	UPDATE target
	SET bdate = source.bdate, gen = source.gen
	FROM bronze.erp_cust_az12 target
	INNER JOIN bronze.erp_cust_az12_load source ON target.cid = source.cid;

	PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' rows updated into erp_cust_az12.';

	INSERT INTO bronze.erp_cust_az12 (cid, bdate, gen)
	SELECT cid, bdate, gen FROM bronze.erp_cust_az12_load source
	WHERE NOT EXISTS (
		SELECT 1 FROM bronze.erp_cust_az12 target WHERE target.cid = source.cid
	);

	PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' rows inserted into erp_cust_az12.';


	SET @main_count = (SELECT COUNT(*) FROM bronze.erp_cust_az12);
	SET @new_insert_count = (
		SELECT COUNT(*) FROM bronze.erp_cust_az12_load source
		WHERE NOT EXISTS (
			SELECT 1 FROM bronze.erp_cust_az12 target WHERE target.cid = source.cid
		)
	);

	UPDATE target
	SET [load_tbl_records] = @load_count,
	[load_tbl_date] = @today,
	[man_tbl_records] = @main_count,
	[last_load] = @today,
	[new_insert] = @new_insert_count
	FROM bronze.bronze_main target
	WHERE tbl_name = 'erp_cust_az12';




	-- ========== erp_px_cat_g1v2 ==========
	SET @load_count = (SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2_load);

	UPDATE target
	SET cat = source.cat, subcat = source.subcat, maintenance = source.maintenance
	FROM bronze.erp_px_cat_g1v2 target
	INNER JOIN bronze.erp_px_cat_g1v2_load source ON target.id = source.id;

	PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' rows updated into erp_px_cat_g1v2.';

	INSERT INTO bronze.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
	SELECT id, cat, subcat, maintenance FROM bronze.erp_px_cat_g1v2_load source
	WHERE NOT EXISTS (
		SELECT 1 FROM bronze.erp_px_cat_g1v2 target WHERE target.id = source.id
	);

	PRINT CAST(@@ROWCOUNT AS VARCHAR) + ' rows inserted into erp_px_cat_g1v2.';

	SET @main_count = (SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2);
	SET @new_insert_count = (
		SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2_load source
		WHERE NOT EXISTS (
			SELECT 1 FROM bronze.erp_px_cat_g1v2 target WHERE target.id = source.id
		)
	);

	UPDATE target
	SET [load_tbl_records] = @load_count,
	[load_tbl_date] = @today,
	[man_tbl_records] = @main_count,
	[last_load] = @today,
	[new_insert] = @new_insert_count
	FROM bronze.bronze_main target
	WHERE tbl_name = 'erp_px_cat_g1v2';




	--Optional (To archive the load tables after loaded)
	IF @archive = 1
	BEGIN
		DECLARE @suffix NVARCHAR(20) = FORMAT(GETDATE(), 'yyyyMMdd');
		DECLARE @sql NVARCHAR(MAX);

		PRINT 'Archiving (renaming) all _load tables...';

		SET @sql = 'EXEC sp_rename ''bronze.crm_cust_info_load'', ''crm_cust_info_archive_' + @suffix + '''';
		EXEC(@sql);

		SET @sql = 'EXEC sp_rename ''bronze.crm_prd_info_load'', ''crm_prd_info_archive_' + @suffix + '''';
		EXEC(@sql);

		SET @sql = 'EXEC sp_rename ''bronze.crm_sales_details_load'', ''crm_sales_details_archive_' + @suffix + '''';
		EXEC(@sql);

		SET @sql = 'EXEC sp_rename ''bronze.erp_loc_a101_load'', ''erp_loc_a101_archive_' + @suffix + '''';
		EXEC(@sql);

		SET @sql = 'EXEC sp_rename ''bronze.erp_cust_az12_load'', ''erp_cust_az12_archive_' + @suffix + '''';
		EXEC(@sql);

		SET @sql = 'EXEC sp_rename ''bronze.erp_px_cat_g1v2_load'', ''erp_px_cat_g1v2_archive_' + @suffix + '''';
		EXEC(@sql);

		PRINT 'All _load tables renamed successfully.';
	END
END

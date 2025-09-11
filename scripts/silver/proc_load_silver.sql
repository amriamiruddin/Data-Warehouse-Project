/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    call load_silver();
===============================================================================
*/


CREATE OR REPLACE PROCEDURE load_silver_data()
AS $$
DECLARE
		-- Variables 
    start_time TIMESTAMP;
    end_time   TIMESTAMP;
		batch_start_time TIMESTAMP;
		batch_end_time TIMESTAMP;
BEGIN
-- Start timing the full load
batch_start_time := now();
RAISE NOTICE '🚀 Starting Full Silver Load at %', batch_start_time;	

RAISE NOTICE '========================================';
RAISE NOTICE 'Loading Silver Layer';
RAISE NOTICE '========================================';	
	
RAISE NOTICE '----------------------------------------';
RAISE NOTICE 'Loading CRM Tables';
RAISE NOTICE '----------------------------------------';

	BEGIN -- Top-level TRY block
		start_time := now();
		RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
		INSERT
			INTO
			silver.crm_cust_info (cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				ELSE 'n/a'
			END cst_marital_status,
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END cst_gndr,
			cst_create_date
		FROM
			(
			SELECT
				*,
				ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_id, cst_create_date DESC) AS flag_last
			FROM
				bronze.crm_cust_info cci
			WHERE
				cst_id IS NOT NULL
		) t
		WHERE
			flag_last = 1;
		end_time := now();
		RAISE NOTICE '>> silver.crm_cust_info load time: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '----------------------------------------';

		start_time := now();
		RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
		INSERT
			INTO
			silver.crm_prd_info (
				prd_id ,
			cat_id ,
			prd_key ,
			prd_nm ,
			prd_cost ,
			prd_line ,
			prd_start_dt ,
			prd_end_dt
		)
		SELECT
			prd_id,
		--		prd_key,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
			SUBSTRING(prd_key FROM 7 ) AS prd_key,
			prd_nm,
			COALESCE(prd_cost, 0) AS prd_cost,
			CASE
				UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line, 
			prd_start_dt,
			LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt
		--	LEAD(prd_start_dt) OVER(ORDER BY prd_start_dt) AS prd_end_dts
		FROM
			bronze.crm_prd_info cpi ;
			end_time := now();
		RAISE NOTICE '>> silver.crm_prd_info load time: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '----------------------------------------';

		start_time := now();
		RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
		INSERT
			INTO
			silver.crm_sales_details
		(sls_prd_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price)
		SELECT
			sls_prd_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0
				OR CHAR_LENGTH(CAST(sls_order_dt AS TEXT)) != 8 THEN NULL
				ELSE TO_DATE(CAST(sls_order_dt AS VARCHAR), 'YYYYMMDD')
			END AS sls_order_dt,
				CASE 
				WHEN sls_ship_dt = 0
				OR CHAR_LENGTH(CAST(sls_ship_dt AS TEXT)) != 8 THEN NULL
				ELSE TO_DATE(CAST(sls_ship_dt AS VARCHAR), 'YYYYMMDD')
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0
				OR CHAR_LENGTH(CAST(sls_due_dt AS TEXT)) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL
				OR sls_sales <= 0
				OR sls_sales != ABS(sls_quantity) * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END sls_sales,
				sls_quantity,
			CASE 
				WHEN sls_price IS NULL
				OR sls_price <= 0 THEN ABS(sls_sales / NULLIF(sls_quantity, 0))
				ELSE sls_price
			END AS sls_price
		FROM
			bronze.crm_sales_details;
			end_time := now();
		RAISE NOTICE '>> silver.crm_sales_details load time: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

		RAISE NOTICE '----------------------------------------';
		RAISE NOTICE 'Loading ERP Tables';
		RAISE NOTICE '----------------------------------------';

		start_time := now();
		RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12 ;
		RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12 ';
		INSERT
			INTO silver.erp_cust_az12 
			(cid,
			bdate,
			gen)
		SELECT
			CASE 
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)
				ELSE cid
			END cid,
			CASE 
				WHEN bdate > CURRENT_DATE THEN NULL
				ELSE bdate
			END bdate,
			CASE 
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				ELSE 'n/a'
			END gen
		FROM
			bronze.erp_cust_az12 eca;
		end_time := now();
		RAISE NOTICE '>> silver.erp_cust_az12 load time: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '----------------------------------------';

		start_time := now();
		RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101
		(cid, cntry)
		SELECT
			CASE 
				WHEN UPPER(TRIM(cid)) LIKE '%-%' THEN REPLACE(cid, '-', '')
				ELSE 'n/a'
			END AS cid,
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US' ,'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END
		FROM
		bronze.erp_loc_a101 ela;
		end_time := now();
		RAISE NOTICE '>> silver.erp_loc_a101 load time: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '----------------------------------------';

		start_time := now();
		RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT
			INTO
			silver.erp_px_cat_g1v2
		(id,
			cat,
			subcat,
			maintenance)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM
			bronze.erp_px_cat_g1v2 ;
		end_time := now();
    RAISE NOTICE '>> silver.erp_px_cat_g1v2 load time: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '----------------------------------------';

		batch_end_time := now();
		RAISE NOTICE '🕒 Total Load Duration: % seconds', EXTRACT(EPOCH FROM batch_end_time - batch_start_time);
		RAISE NOTICE '✅ Silver Layer Load Complete.';

		EXCEPTION WHEN OTHERS THEN
		RAISE WARNING '❌ Procedure failed: % [%]', SQLERRM, SQLSTATE;
	END;
END;
$$ LANGUAGE plpgsql;

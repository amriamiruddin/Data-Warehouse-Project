/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR REPLACE PROCEDURE load_bronze_data()
AS $$
DECLARE
		-- Variables 
    start_time TIMESTAMP;
    end_time   TIMESTAMP;
		batch_start_time TIMESTAMP;
		batch_end_time TIMESTAMP;
BEGIN
	BEGIN -- Top-level TRY block
		-- Start timing the full load
		batch_start_time := now();
		RAISE NOTICE '🚀 Starting Full Bronze Load at %', batch_start_time;


		RAISE NOTICE '========================================';
		RAISE NOTICE 'Loading Bronze Layer';
		RAISE NOTICE '========================================';

		RAISE NOTICE '----------------------------------------';
		RAISE NOTICE 'Loading CRM Tables';
		RAISE NOTICE '----------------------------------------';

		-- CRM_CUST_INFO
		start_time := now();
		RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info ';
		COPY bronze.crm_cust_info 
		FROM '/home/volts/temp/source_crm/cust_info.csv'
		WITH (
				FORMAT csv,
				HEADER true,
				DELIMITER ','
		);
		end_time := now();
		RAISE NOTICE '>> bronze.crm_cust_info load time: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '----------------------------------------';

		-- CRM_PRD_INFO
		start_time := now();
		RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
		COPY bronze.crm_prd_info 
		FROM '/home/volts/temp/source_crm/prd_info.csv'
		WITH (
				FORMAT csv,
				HEADER true,
				DELIMITER ','
		);
 		end_time := now();
    RAISE NOTICE '>> bronze.crm_prd_info load time: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '----------------------------------------';

		-- CRM_SALES_DETAILS
		start_time := now();
		RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		
		RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
		COPY bronze.crm_sales_details
		FROM '/home/volts/temp/source_crm/sales_details.csv'
		WITH (
				FORMAT csv,
				HEADER true,
				DELIMITER ','
		);
    end_time := now();
    RAISE NOTICE '>> bronze.crm_sales_details load time: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

		RAISE NOTICE '----------------------------------------';
		RAISE NOTICE 'Loading ERP Tables';
		RAISE NOTICE '----------------------------------------';

		-- ERP_CUST_AZ12
		start_time := now();
		RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		
		RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
		COPY bronze.erp_cust_az12
		FROM '/home/volts/temp/source_erp/cust_az12.csv'
		WITH (
				FORMAT csv,
				HEADER true,
				DELIMITER ','
		);
 		end_time := now();
    RAISE NOTICE '>> bronze.erp_cust_az12 load time: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '----------------------------------------';

		-- ERP_LOC_A101
		start_time := now();
		RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
		COPY bronze.erp_loc_a101 
		FROM '/home/volts/temp/source_erp/loc_a101.csv'
		WITH (
				FORMAT csv,
				HEADER true,
				DELIMITER ','
		);
		end_time := now();
    RAISE NOTICE '>> bronze.erp_loc_a101 load time: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '----------------------------------------';

		-- ERP_PX_CAT_G1V2
		start_time := now();
		RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		COPY bronze.erp_px_cat_g1v2 
		FROM '/home/volts/temp/source_erp/px_cat_g1v2.csv'
		WITH (
				FORMAT csv,
				HEADER true,
				DELIMITER ','
		);
		end_time := now();
    RAISE NOTICE '>> bronze.erp_px_cat_g1v2 load time: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '----------------------------------------';
		
		batch_end_time := now();
		RAISE NOTICE '🕒 Total Load Duration: % seconds', EXTRACT(EPOCH FROM batch_end_time - batch_start_time);
		RAISE NOTICE '✅ Bronze Layer Load Complete.';

	EXCEPTION WHEN OTHERS THEN
		RAISE WARNING '❌ Procedure failed: % [%]', SQLERRM, SQLSTATE;
	END;
END;
$$ LANGUAGE plpgsql; 

CALL load_bronze_data();
drop procedure load_bronze_data;


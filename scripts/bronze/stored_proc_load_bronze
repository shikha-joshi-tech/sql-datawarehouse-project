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
CREATE OR ALTER PROCEDURE bronze.load_bronze as
BEGIN
	DECLARE @start_time datetime ,@end_time DATETIME,@batch_start_time datetime,@batch_end_time datetime,@crm_start_time datetime,@crm_end_time datetime,@erp_start_time datetime,@erp_end_time datetime;    
	BEGIN TRY
		SET @batch_start_time=GETDATE();
		PRINT'******************************'
		PRINT'Loading bronze layer';
		PRINT'******************************'
		SET @crm_start_time=GETDATE();
		PRINT'====================='
		PRINT'Loading crm section';
		PRINT'====================='

		SET @start_time=GETDATE();
		PRINT'Truncating data from bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info
		PRINT'Inserting data into bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\pc\Desktop\SQL\01. Introduction to SQL\01. Introduction to SQL\assets\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SELECT COUNT(*)FROM bronze.crm_cust_info
		SET @end_time=GETDATE();
		print'>>Load duration:'+CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds';


		SET @start_time=GETDATE();
		PRINT'Truncating data from bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info
		PRINT'Inserting data into bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\pc\Desktop\SQL\01. Introduction to SQL\01. Introduction to SQL\assets\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SELECT COUNT(*)FROM bronze.crm_prd_info
		SET @end_time=GETDATE();
		PRINT'>>Load duration:'+CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds';


		SET @start_time=GETDATE();
		PRINT'Truncating data from bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details
		PRINT'Inserting data into bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\pc\Desktop\SQL\01. Introduction to SQL\01. Introduction to SQL\assets\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SELECT COUNT(*) FROM bronze.crm_sales_details
		SET @end_time=GETDATE();
		PRINT'>>Load duration:'+CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds';

		SET @crm_end_time=GETDATE();
		PRINT'>>Loading crm duration:'+CAST(DATEDIFF(second,@crm_start_time,@crm_end_time)AS NVARCHAR)+'seconds';


		SET @erp_start_time=GETDATE();
		PRINT'====================='
		PRINT'Loading erp section';
		PRINT'====================='
		SET @start_time=GETDATE();
		PRINT'Truncating data from bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_CUST_AZ12
		PRINT'Inserting data into bronze.erp_cust_az12';
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\pc\Desktop\SQL\01. Introduction to SQL\01. Introduction to SQL\assets\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SELECT COUNT(*)FROM bronze.erp_CUST_AZ12
		SET @end_time=GETDATE();
		PRINT'>>Load duration:'+CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds';

		SET @start_time=GETDATE();
		PRINT'Truncating data from bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_LOC_A101
		PRINT'Inserting data into bronze.erp_loc_a101';
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Users\pc\Desktop\SQL\01. Introduction to SQL\01. Introduction to SQL\assets\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SELECT COUNT(*)FROM  bronze.erp_LOC_A101
		SET @end_time=GETDATE();
		PRINT'>>Load duration:'+CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds';

	
		SET @start_time=GETDATE();
		PRINT'Truncating data from bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE  bronze.erp_PX_CAT_G1V2
		PRINT'Inserting data into bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\pc\Desktop\SQL\01. Introduction to SQL\01. Introduction to SQL\assets\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SELECT COUNT(*) FROM bronze.erp_PX_CAT_G1V2
		SET @end_time=GETDATE();
		PRINT'>>Load duration:'+CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds';

		SET @erp_end_time=GETDATE();
		PRINT'>>Load erp duration:'+CAST(DATEDIFF(second,@erp_start_time,@erp_end_time)AS NVARCHAR)+'seconds';

		SET @batch_end_time=GETDATE();
		PRINT'>>Loading bronze layer duration:'+CAST(DATEDIFF(second,@batch_start_time,@batch_end_time)AS NVARCHAR)+'seconds';

	END TRY
	BEGIN CATCH
		PRINT'============================================'
		PRINT'ERROR OCCURED DURING LOADING OF BRONZE LAYER'
		PRINT'Error message'+ERROR_MESSAGE();
		PRINT'Error number'+CAST(ERROR_NUMBER()AS NVARCHAR);
		PRINT'Error state'+CAST(ERROR_STATE()AS NVARCHAR);
		PRINT'============================================'
	END CATCH
END

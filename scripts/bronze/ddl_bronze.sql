--Create database 'DataWarehouse'

use master;

Create database DataWarehouse;
go
use DataWarehouse;
go
create schema bronze;
go
create schema silver;
go
create schema gold;

drop table bronze.crm_cust_info 
create table bronze.crm_cust_info (
	cst_id INT,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_material_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date NVARCHAR(50)
);

create table bronze.crm_prd_info(
	prd_id INT, 
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost nvarchar(50),
	prd_line nvarchar(50),
	prd_start DATE,
	prd_end_dt DATE
);
drop table bronze.crm_sales_details;
create table bronze.crm_sales_details(
	sls_ord_num nvarchar(50), 
	sls_prd_key nvarchar(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);

CREATE TABLE bronze.erp_cust_az12(
	cid nvarchar(50),
	bdate DATE,
	gen nvarchar(20)
);

CREATE TABLE bronze.erp_loc_a101(
	cid nvarchar(50),
	cntry nvarchar(50)
);

CREATE TABLE bronze.erp_px_cat_g1v2(
	id nvarchar(50),
	cat nvarchar(50),
	subcat nvarchar(50),
	maintenance nvarchar(50)
);


CREATE OR ALTER PROCEDURE bronze.load_bronze as 
begin
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	begin try
			SET @batch_start_time = GETDATE();
			print '===================================';
			print 'Loading Bronze Layer';
			print '===================================';
	
			print '----------------------------------------';
			print 'Loading CRM tables';
			print '----------------------------------------';

		--TRUNCATE SIGUROHET QE NIHER E SHPRAZ TABELEN TANI E MUSH ME TE DHENAT
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		from 'D:\DataWarehouseProject\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			--PER SHkak se i pari eshte headeri
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print '>> -------------';


		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		from 'D:\DataWarehouseProject\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
	
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print '>> -------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		from 'D:\DataWarehouseProject\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
	
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print '>> -------------';





			print '----------------------------------------';
			print 'Loading ERP tables';
			print '----------------------------------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		from 'D:\DataWarehouseProject\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
	
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print '>> -------------';

		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		from 'D:\DataWarehouseProject\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
	
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print '>> -------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		from 'D:\DataWarehouseProject\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
	
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		print '>> -------------';
		SET @batch_end_time = GETDATE();
		print '==========================================='
		print 'Loading Bronze Layer is Completed';
		print '  - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds';
		print '==========================================='
	end try

	begin catch
		print '=========================================='
		print 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		print 'Erro Message' + ERROR_MESSAGE();
		print 'Erro Message' + CAST(ERROR_MESSAGE()AS VARCHAR);
		print '=========================================='
	end catch 

end


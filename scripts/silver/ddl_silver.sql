CREATE OR ALTER PROCEDURE silver.load_silver as

begin

	TRUNCATE TABLE silver.erp_px_cat_g1v2
	insert into silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
	select id,
	cat,
	subcat, maintenance 
	from 
	bronze.erp_px_cat_g1v2


	
	TRUNCATE TABLE silver.crm_cust_info
	INSERT INTO silver.crm_cust_info(
	cst_id, cst_key, cst_firstname, cst_lastname, cst_material_status, cst_gndr, cst_create_date	
	)

	select 
	cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname,
	trim(cst_lastname) as cst_lastname,
	case when UPPER(TRIM(cst_material_status)) = 'S'  THEN 'Single'
		 when UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
		 ELSE 'n/a'
		 end cst_material_status,
	case when UPPER(TRIM(cst_gndr)) = 'F'  THEN 'FEMALE'
		 when UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
		 ELSE 'n/a'
		 end cst_gndr,
	cst_create_date
	from (
	select *,
	row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
	from bronze.crm_cust_info
	where cst_id is not null
	) t
	where flag_last = 1


	TRUNCATE TABLE silver.erp_cust_az12
	insert into silver.erp_cust_az12(cid,bdate,gen) 
	--fillimisht e hjekim qat NAS se sna duhet
	select
	case when cid like 'NAS%' THEN SUBSTRING(cid, 4, len(cid))
		ELSE cid
	end cid,
	case when bdate > GETDATE() THEN Null
		else bdate
	end as bdate,
	case when upper(trim(gen)) in ('F','Female') THEN 'Female'
		 when upper(trim(gen)) in ('M','Male') THEN 'Male'
		 else 'n\a'
	end as gen
	from bronze.erp_cust_az12


	TRUNCATE TABLE silver.erp_loc_a101
	insert into silver.erp_loc_a101 (cid,cntry) 

	select REPLACE(cid, '-','')cid,
	case when trim(cntry) = 'DE' THEN 'Germany'
		 when trim(cntry) IN ('US','USA') THEN 'United States'
		 when trim(cntry) = '' or cntry is null then 'n/a'
		 else trim(cntry)
	end as cntry
	from bronze.erp_loc_a101

	TRUNCATE TABLE silver.crm_prd_info;

	INSERT INTO silver.crm_prd_info (
	  prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
	)
	SELECT 
	  prd_id,
	  REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	  SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	  prd_nm,
	  ISNULL(prd_cost, 0), 
	  CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'         -- Pick one meaning for 'R'
		WHEN 'S' THEN 'Other Sales'
		ELSE 'n/a'
	  END AS prd_line,
	  prd_start,
	  DATEADD(DAY, -1, LEAD(prd_start) OVER (PARTITION BY prd_key ORDER BY prd_start)) AS prd_end_dt
	FROM bronze.crm_prd_info;


	TRUNCATE TABLE silver.crm_sales_details
	insert into silver.crm_sales_details(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,
	sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)

	select sls_ord_num, sls_prd_key, sls_cust_id,
	case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
		else cast(cast(sls_order_dt as varchar) as date)
		end as sls_order_dt, 
	case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
		else cast(cast(sls_ship_dt as varchar) as date)
		end as sls_ship_dt,
	case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
		else cast(cast(sls_due_dt as varchar) as date)
		end as sls_due_dt,
	 CASE 
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		  THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	  END AS sls_sales,
	 sls_quantity,   CASE 
		WHEN sls_price IS NULL OR sls_price <= 0
		  THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	  END AS sls_price
	from bronze.crm_sales_details

end


exec silver.load_silver

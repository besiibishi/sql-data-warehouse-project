create view gold.dim_products as 
select
ROW_NUMBER() over(order by pn.prd_start_dt, pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number, 
pn.prd_nm as product_name, 
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance,
pn.prd_cost as cost,
pn.prd_line as product_line, 
pn.prd_start_dt as start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null --filter out all historical data


select * from gold.dim_products

--komplet te dhenat qe na duhen per customers
-- inner query vec i bashkon tri tabelat qe na vyjn per customers
-- na vyn me e kriju ni primary key qe quhet surragate key

create view gold.dim_customers as
SELECT
	row_number() over(order by cst_id) as customer_key,
    ci.cst_id as customer_id, 
    ci.cst_key as customer_number, 
    ci.cst_firstname as first_name,
    ci.cst_lastname as last_name, 
	la.cntry as country,
    ci.cst_material_status as marital_status, 
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN 
            REPLACE(REPLACE(ci.cst_gndr, 'MALE', 'Male'), 'FEMALE', 'Female')
        ELSE 
            REPLACE(REPLACE(COALESCE(ca.gen, 'n/a'), 'MALE', 'Male'), 'FEMALE', 'Female')
    END AS gender, 
	ca.bdate as birthdate ,
    ci.cst_create_date as create_date
    
FROM silver.crm_cust_info ci 
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid;


create view gold.fact_sales as 
select 
sd.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date, 
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount, 
sd.sls_quantity as quantity,
sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id

select * from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key

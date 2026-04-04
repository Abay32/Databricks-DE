-- Read fact sales from volume to bronze schema 
create streaming live table lakeflow_dlt_uc.bronze.fact_sales
comment "Raw fact sales from volume to bronze schema"
as 
select * from cloud_files(
  '/Volumes/lakeflow_dlt_uc/landing_zone/facts_and_dimensions_files/fact_sales/',
  'csv', 
  map('header', 'true')
);


-- Read dimensions products from volume to bronze schema 
create streaming live table lakeflow_dlt_uc.bronze.products
comment "Raw dimensions products from volume to bronze schema"
as 
select * from cloud_files(
  '/Volumes/lakeflow_dlt_uc/landing_zone/facts_and_dimensions_files/dim_products/',
  'csv', 
  map('header', 'true')
);

-- Read dimensions customers from volume to bronze schema 
create streaming live table lakeflow_dlt_uc.bronze.customers
comment "Raw dimensions customers from volume to bronze schema"
as 
select * from cloud_files(
  '/Volumes/lakeflow_dlt_uc/landing_zone/facts_and_dimensions_files/dim_customers/',
  'csv', 
  map('header', 'true')
);

-- Read dimensions regions from volume to bronze schema 
create streaming live table lakeflow_dlt_uc.bronze.regions
comment "Raw dimensions regions from volume to bronze schema"
as 
select * from cloud_files(
  '/Volumes/lakeflow_dlt_uc/landing_zone/facts_and_dimensions_files/dim_regines/',
  'csv', 
  map('header', 'true')
);
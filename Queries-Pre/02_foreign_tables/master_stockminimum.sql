DROP FOREIGN TABLE IF EXISTS master_stockminimum;

CREATE FOREIGN TABLE master_stockminimum (
	itemcategory_id int4 OPTIONS(column_name 'itemcategory_id') NULL,
	stockminimum_qty int4 OPTIONS(column_name 'stockminimum_qty') NULL,
	department_id varchar(50) OPTIONS(column_name 'department_id') NULL
)
SERVER inventory
OPTIONS (schema_name 'dbo', table_name 'master_stockminimum');
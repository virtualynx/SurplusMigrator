DROP FOREIGN TABLE IF EXISTS master_itemcategoryprice;

CREATE FOREIGN TABLE master_itemcategoryprice (
	itemcategory_id int4 OPTIONS(column_name 'itemcategory_id') NULL,
	itemcategory_price numeric(18) OPTIONS(column_name 'itemcategory_price') NULL
)
SERVER inventory
OPTIONS (schema_name 'dbo', table_name 'master_itemcategoryprice');
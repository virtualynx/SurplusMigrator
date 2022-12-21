DROP VIEW IF EXISTS view_inv_itemsize;

CREATE VIEW view_inv_itemsize AS 
	SELECT 
		tbl.size_id as "itemsizeid",
		tbl.size_name as "name",
		tbl.size_isdisabled as "isdisabled"
	FROM 
		dblink(
			'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7'::text, 
			'
				SELECT 
					size_id, 
					size_name, 
					size_isdisabled
				FROM 
					inventory.master_size
			'::text
		) tbl(
			size_id int4,
			size_name varchar(255),
			size_isdisabled bool
		);


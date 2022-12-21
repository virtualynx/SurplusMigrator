DROP VIEW IF EXISTS view_inv_itembrand;

CREATE VIEW view_inv_itembrand AS 
	SELECT 
		tbl.brand_id as "itembrandid",
		tbl.brand_name as "itembrandname",
		tbl.brand_isdisabled as "isdisabled"
	FROM 
		dblink(
			'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7'::text, 
			'
				SELECT 
					brand_id, 
					brand_name,
					brand_isdisabled
				FROM inventory.master_brand 
			'::text
		) tbl(
			brand_id int4,
			brand_name varchar(255),
			brand_isdisabled bool
		);


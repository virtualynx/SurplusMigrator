DROP VIEW IF EXISTS view_inv_itemmaterial;

CREATE VIEW view_inv_itemmaterial AS 
	SELECT 
		tbl.material_id as "itemmaterialid",
		tbl.material_name as "name",
		tbl.material_isdisabled as "isdisabled"
	FROM 
		dblink(
			'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7'::text, 
			'
				SELECT 
					material_id, 
					material_name, 
					material_isdisabled
				FROM 
					inventory.master_material
			'::text
		) tbl(
			material_id int4,
			material_name varchar(255),
			material_isdisabled bool
		);


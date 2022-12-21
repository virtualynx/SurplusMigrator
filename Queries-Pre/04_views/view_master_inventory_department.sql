DROP VIEW IF EXISTS view_master_inventory_department;

CREATE VIEW view_master_inventory_department AS 
	SELECT 
		tbl.inventorydepartmentid,
		tbl.name
	FROM dblink(
		'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7'::text, 
		'
			SELECT 
				department_id, 
				department_name
			FROM 
				inventory."master_department"'::text
	) tbl(
		inventorydepartmentid character varying(50), 
		name character varying(50)
	);


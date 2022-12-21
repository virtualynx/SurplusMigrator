-- dev.view_master_program source
DROP VIEW IF EXISTS view_organization_structure;

CREATE VIEW view_organization_structure AS 
	SELECT 
		structure_code,
		structure_name,
		structure_typeid,
		structure_order,
		structure_active,
		structure_level,
		structure_scope
	FROM 
		dblink(
			'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7', 
			'
				SELECT 
					structure_code, 
					structure_name, 
					structure_typeid,
					structure_order,
					structure_active,
					structure_level,
					structure_scope
				FROM 
					hris."Organization_structure"
			'
		) tbl (
--			tvprogramid character varying(25), 
--			name character varying(200)
			structure_code varchar(50),
			structure_name varchar(255),
			structure_typeid int2,
			structure_order int2,
			structure_active int2,
			structure_level int2,
			structure_scope int2
		);
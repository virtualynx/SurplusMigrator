-- dev.view_master_program source
DROP VIEW IF EXISTS view_structure_active_wparent;

CREATE VIEW view_structure_active_wparent AS 
	SELECT 
		code,
		"name",
		parent,
		stype,
		structure_order,
		sidenode_parent
	FROM 
		dblink(
			'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7', 
			'
				SELECT 
					code,
					"name",
					parent,
					stype,
					structure_order,
					sidenode_parent
				FROM 
					hris."vStructureActiveWParent"
			'
		) tbl (
			code varchar(50),
			"name" varchar(255),
			parent varchar(50),
			stype int2,
			structure_order int2,
			sidenode_parent varchar(50)
		);
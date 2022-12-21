DROP VIEW IF EXISTS view_master_program;

CREATE OR REPLACE VIEW view_master_program AS 
	SELECT 
		mc.tvprogramid,
		mc.name
	FROM 
		dblink(
			'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7', 
			'
				SELECT 
					DISTINCT("ROW_ID_BMS") as tvprogramid, 
					"FILM_BMS" as "name" 
				FROM 
					programming.aired_programs
			'
		) mc(
			tvprogramid character varying(25), 
			name character varying(200)
		);
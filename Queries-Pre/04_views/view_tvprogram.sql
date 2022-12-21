DROP VIEW IF EXISTS view_tvprogram;

CREATE VIEW view_tvprogram AS 
	SELECT 
		mc.bms_id as "tvprogramid",
		mc.name
	FROM 
		dblink(
			'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7'::text, 
			'
				SELECT 
					DISTINCT(row_id) as bms_id, 
					film_poc_title as "name"
				FROM gen21.mp_film mf 
				order by film_poc_title
			'::text
		) mc(
			bms_id character varying(20), 
			name character varying(50)
		);


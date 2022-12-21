DROP VIEW IF EXISTS view_master_program_v2;

CREATE OR REPLACE VIEW view_master_program_v2 AS 
	SELECT 
		mc.bms_id,
		mc.name
	FROM 
		dblink(
			'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7', 
			'
				SELECT 
					DISTINCT(row_id) as bms_id, 
					film_poc_title as "name"
				FROM gen21.mp_film mf 
				where 
					catslot_code <> ''DUM''
				order by film_poc_title
			'
		) mc(
			bms_id character varying(20), 
			name character varying(50)
		);
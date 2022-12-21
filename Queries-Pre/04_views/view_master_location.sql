DROP VIEW IF EXISTS view_master_location;

CREATE VIEW view_master_location AS 
	SELECT 
		tbl.location_id as locationid,
		tbl.location_area as area
	FROM dblink(
		'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7'::text, 
		'
			SELECT 
				location_id, 
				location_area
			FROM 
				inventory."master_location"
		'::text
	) tbl(
		location_id int4,
		location_area varchar(255)
	);


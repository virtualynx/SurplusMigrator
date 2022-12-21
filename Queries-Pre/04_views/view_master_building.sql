DROP VIEW IF EXISTS view_master_building;

CREATE VIEW view_master_building AS 
	SELECT 
		tbl.building_id as buildingid,
		tbl.building_name as "name",
		tbl.location_id as locationid
	FROM dblink(
		'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7'::text, 
		'
			SELECT 
				building_id, 
				building_name,
				location_id
			FROM 
				inventory."master_building"
		'::text
	) tbl(
		building_id int4,
		building_name varchar(150),
		location_id int4
	);


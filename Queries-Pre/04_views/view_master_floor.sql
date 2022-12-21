DROP VIEW IF EXISTS view_master_floor;

CREATE VIEW view_master_floor AS 
	SELECT 
		tbl.floor_id as floorid,
		tbl.floor_name as "name",
		tbl.building_id as buildingid
	FROM dblink(
		'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7'::text, 
		'
			SELECT 
				floor_id, 
				floor_name,
				building_id
			FROM 
				inventory."master_floor"
		'::text
	) tbl(
		floor_id int4,
		floor_name varchar(150),
		building_id int4
	);


DROP VIEW IF EXISTS view_master_roomfull;

CREATE VIEW view_master_roomfull AS 
	SELECT 
		tbl.room_id as roomid,
		tbl.room_name as roomname,
		tbl.floor_id as floorid,
		tbl.floor_name as floorname,
		tbl.building_id as buildingid,
		tbl.building_name as buildingname,
		tbl.location_id as locationid,
		tbl.location_area as locationname
	FROM dblink(
		'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7'::text, 
		'
			SELECT 
				mr.room_id, 
				mr.room_name,
				mf.floor_id,
				mf.floor_name,
				mb.building_id,
				mb.building_name,
				ml.location_id,
				ml.location_area
			FROM 
				inventory."master_room" mr
				join inventory."master_floor" mf on mr.floor_id = mf.floor_id
				join inventory."master_building" mb on mb.building_id = mf.building_id
				join inventory."master_location" ml on ml.location_id = mb.location_id
		'::text
	) tbl(
		room_id varchar(10),
		room_name varchar(150),
		floor_id varchar(10),
		floor_name varchar(150),
		building_id varchar(10),
		building_name varchar(150),
		location_id varchar(10),
		location_area varchar(255)
	);


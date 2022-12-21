DROP VIEW IF EXISTS view_inv_itemgender;

CREATE VIEW view_inv_itemgender AS 
	SELECT 
		tbl.gender_id as "itemgenderid",
		tbl.gender_name as "name"
	FROM 
		dblink(
			'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7'::text, 
			'
				SELECT 
					gender_id, 
					gender_name
				FROM 
					inventory.master_gender
			'::text
		) tbl(
			gender_id varchar(1),
			gender_name varchar(50)
		);


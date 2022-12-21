DROP VIEW IF EXISTS view_inv_itemcolor;

CREATE VIEW view_inv_itemcolor AS 
	SELECT 
		tbl.color_id as "itemcolorid",
		tbl.color_name as "name",
		tbl.color_isdisabled as "isdisabled"
	FROM 
		dblink(
			'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7'::text, 
			'
				SELECT 
					color_id, 
					color_name, 
					color_isdisabled
				FROM 
					inventory.master_color
			'::text
		) tbl(
			color_id int4,
			color_name varchar(255),
			color_isdisabled bool
		);


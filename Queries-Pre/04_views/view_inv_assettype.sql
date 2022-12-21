DROP VIEW IF EXISTS view_inv_assettype;

CREATE VIEW view_inv_assettype AS 
	SELECT 
		tbl.assettype_id as "assettypeid",
		tbl.assettype_name as "name",
		tbl.assettype_desc as "description",
		tbl.assettype_isdisabled as "isdisabled"
	FROM 
		dblink(
			'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7'::text, 
			'
				SELECT 
					assettype_id, 
					assettype_name,
					assettype_desc,
					assettype_isdisabled
				FROM inventory.master_assettype 
			'::text
		) tbl(
			assettype_id int4,
			assettype_name varchar(50),
			assettype_desc varchar(255),
			assettype_isdisabled bool
		);


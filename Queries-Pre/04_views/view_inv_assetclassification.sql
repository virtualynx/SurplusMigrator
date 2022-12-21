DROP VIEW IF EXISTS view_inv_assetclassification;

CREATE VIEW view_inv_assetclassification AS 
	SELECT 
		tbl.assetclassification_id as "assetclassificationid",
		tbl.assetclassification_name as "name",
		tbl.assetclassification_isdisabled as "isdisabled"
	FROM 
		dblink(
			'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7'::text, 
			'
				SELECT 
					assetclassification_id, 
					assetclassification_name,
					assetclassification_isdisabled
				FROM inventory.master_assetclassification 
			'::text
		) tbl(
			assetclassification_id int4,
			assetclassification_name varchar(255),
			assetclassification_isdisabled bool
		);


DROP VIEW IF EXISTS view_inv_assetcategory;

CREATE VIEW view_inv_assetcategory AS 
	SELECT 
		tbl.assetcategory_id as "assetcategoryid",
		tbl.assetcategory_name as "name",
		tbl.assetcategory_isdisabled as "isdisabled"
	FROM 
		dblink(
			'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7'::text, 
			'
				SELECT 
					assetcategory_id, 
					assetcategory_name,
					assetcategory_isdisabled
				FROM inventory.master_assetcategory 
			'::text
		) tbl(
			assetcategory_id int4,
			assetcategory_name varchar(50),
			assetcategory_isdisabled bool
		);


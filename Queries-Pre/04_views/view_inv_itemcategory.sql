DROP VIEW IF EXISTS view_inv_itemcategory;

CREATE VIEW view_inv_itemcategory AS 
	SELECT 
		tbl.itemcategory_id as inventoryitemcategoryid,
		tbl.itemcategory_name as name,
		tbl.department_id as inventorydepartmentid,
		tbl.itemcategory_isdisabled as isdisabled,
		tbl.itemcategory_issupplyitem as issupplyitem
	FROM dblink(
		'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7'::text, 
		'
			SELECT 
				itemcategory_id, 
				itemcategory_name,
				department_id,
				itemcategory_isdisabled,
				itemcategory_issupplyitem
			FROM 
				inventory."master_itemcategory"
		'::text
	) tbl(
		itemcategory_id int4,
		itemcategory_name character varying(255), 
		department_id character varying(50),
		itemcategory_isdisabled int2,
		itemcategory_issupplyitem int2
	);


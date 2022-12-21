DROP VIEW IF EXISTS view_inv_itemtype;

CREATE VIEW view_inv_itemtype AS 
	SELECT 
		tbl.itemtype_id as "itemtypeid",
		tbl.itemtype_name as "name",
		tbl.department_id as "inventorydepartmentid",
		tbl.itemtype_isdisabled as "isdisabled"
	FROM 
		dblink(
			'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7'::text, 
			'
				SELECT 
					itemtype_id, 
					itemtype_name,
					department_id,
					itemtype_isdisabled
				FROM inventory.master_itemtype 
			'::text
		) tbl(
			itemtype_id int4,
			itemtype_name varchar(255),
			department_id varchar(50),
			itemtype_isdisabled bool
		);


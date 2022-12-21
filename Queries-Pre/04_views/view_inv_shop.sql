DROP VIEW IF EXISTS view_inv_shop;

CREATE VIEW view_inv_shop AS 
	SELECT 
		tbl.shop_id as "shopid",
		tbl.shop_name as "name",
		tbl.shop_address as "address",
		tbl.shop_telp as "phone",
		tbl.shoptype_id as "shoptypeid",
		tbl.shoptype_name as "shoptypename",
		tbl.shop_isdisabled as "isdisabled"
	FROM 
		dblink(
			'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7'::text, 
			'
				SELECT 
					ms.shop_id, 
					ms.shop_name, 
					ms.shop_address, 
					ms.shop_telp, 
					ms.shoptype_id, 
					mst.shoptype_name, 
					ms.shop_isdisabled
				FROM 
					inventory.master_shop ms
					join inventory.master_shoptype mst on ms.shoptype_id = mst.shoptype_id
			'::text
		) tbl(
			shop_id int4,
			shop_name varchar(500),
			shop_address varchar(1000),
			shop_telp varchar(50),
			shoptype_id int4,
			shoptype_name varchar(50),
			shop_isdisabled bool
		);


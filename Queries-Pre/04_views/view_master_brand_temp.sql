DROP MATERIALIZED VIEW IF EXISTS view_master_brand_temp;

CREATE materialized VIEW view_master_brand_temp as
	SELECT 
		mp.prod_code AS advertiserbrandid,
    	mp.prod_name AS name
--    	,mcp.client_code AS advertiserid
--    	,mc.client_com_name AS advertiser_name
	FROM 
		dblink(
			'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7', 
			'SELECT prod_code, prod_name FROM gen21.mt_prod'
		) mp(
			prod_code character varying(20), 
			prod_name character varying(50)
		)
--		JOIN dev.dblink(
--			'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7', 
--			'SELECT prod_code, client_code FROM gen21.mt_client_prod'
--		) mcp(
--			prod_code character varying(20), 
--			client_code character varying(20)
--		) ON mp.prod_code = mcp.prod_code
--		join dev.dblink(
--   			'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7 ', 
--   			'SELECT client_code, client_com_name FROM gen21.mt_client'
--   		) mc(
--   			client_code character varying(20), 
--   			client_com_name character varying(40)
--		) ON mcp.client_code = mc.client_code
;
CREATE index idx_view_master_brand_temp_name_gin ON view_master_brand_temp USING gin (name gin_trgm_ops);

select 
	*
from view_master_brand_temp vmbt  
where 
	name % 'susu'
;
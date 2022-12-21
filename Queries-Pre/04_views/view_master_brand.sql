DROP MATERIALIZED VIEW IF EXISTS view_master_brand;

CREATE MATERIALIZED VIEW view_master_brand as
	select
		mp.prod_code as "advertiserbrandid"
		,mp.prod_name as "name"
		,mcp.client_code as "advertiserid"
		,mc.client_com_name as "advertiser_name"
		,mv.vendorid as "agency_id"
		,mv."name" as "agency_name"
	from
		dblink(
			'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7 ',
            'SELECT prod_code, prod_name FROM gen21.mt_prod'
		) as mp (
			prod_code varchar(20),
            prod_name varchar(50)
        )
		join dblink(
			'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7',
            'SELECT prod_code, client_code FROM gen21.mt_client_prod'
		) as mcp (
			prod_code varchar(20),
            client_code varchar(20)
        ) on mp.prod_code = mcp.prod_code
		join dblink(
			'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7',
            'SELECT client_code, client_com_name FROM gen21.mt_client'
		) as mc (
			client_code varchar(20),
            client_com_name varchar(40)
        ) on mcp.client_code = mc.client_code
		join dblink(
			'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7',
            'SELECT client_code, agen_code FROM gen21.mt_client1'
		) as mc1 (
			client_code varchar(20),
            agen_code varchar(20)
        ) on mcp.client_code = mc1.client_code
        join relation_vendorsurplus_agencygen21 rva on mc1.agen_code = rva.agentcode
        join master_vendor mv on rva.vendorid = mv.vendorid
with data;

REFRESH MATERIALIZED view view_master_brand with data;

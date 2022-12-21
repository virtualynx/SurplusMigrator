DROP MATERIALIZED VIEW IF EXISTS view_master_advertiser;

CREATE MATERIALIZED VIEW view_master_advertiser as
	select
		mc.client_code as "advertiserid",
		mc.client_com_name as "name",
		rva.vendorid as "agency_id",
		mv."name" as "agency_name"
	from
		dblink(
			'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7 ',
            'SELECT client_code, client_com_name FROM gen21.mt_client'
		) as mc (
			client_code varchar(20),
            client_com_name varchar(40)
        )
		join dblink(
			'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7',
            'SELECT client_code, agen_code FROM gen21.mt_client1'
		) as mc1 (
			client_code varchar(20),
            agen_code varchar(20)
        ) on mc.client_code = mc1.client_code
        join relation_vendorsurplus_agencygen21 rva on mc1.agen_code = rva.agentcode
        join master_vendor mv on rva.vendorid = mv.vendorid
with data;

REFRESH MATERIALIZED view view_master_advertiser with data;

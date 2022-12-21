DROP FOREIGN TABLE IF EXISTS mt_client;

CREATE FOREIGN TABLE mt_client (
	client_code varchar(20) OPTIONS(key 'true') NOT NULL,
	client_com_name varchar(40) NULL,
	client_com_addr1 varchar(100) NULL,
	client_com_addr2 varchar(100) NULL,
	client_com_phone varchar(20) NULL,
	client_com_fax varchar(20) NULL,
	client_com_email varchar(100) NULL,
	client_com_web varchar(60) NULL,
	client_pic varchar(30) NULL,
	clientg_code varchar(10) NULL,
	client_priority int4 NULL,
	tcountry_code varchar(3) NULL,
	client_blockd timestamp(0) NULL,
	client_deposit bpchar(1) NULL,
	non_active timestamp(0) NULL,
	update_user varchar(10) NOT NULL,
	update_date timestamp(0) NOT NULL,
	row_id varchar(18) NOT NULL,
	client_dateend timestamp(0) NULL,
	client_com_addr3 varchar(100) NULL,
	client_direct_flag bpchar(1) NULL,
	client_com_postcd varchar(6) NULL,
	clientname_full varchar(100) NULL,
	client_com_addr3_full varchar(100) NULL,
	entry_date timestamp(0) NULL,
	entry_user varchar(20) NULL,
	xcode varchar(20) NULL,
	client_vat_no varchar(50) NULL
)
SERVER gen21
OPTIONS (schema 'GEN21_TRANS7', table 'MT_CLIENT');
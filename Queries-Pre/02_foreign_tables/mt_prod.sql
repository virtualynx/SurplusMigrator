DROP FOREIGN TABLE IF EXISTS mt_prod;

CREATE FOREIGN TABLE mt_prod (
	prod_code varchar(20) OPTIONS(key 'true') NOT NULL,
	prod_name varchar(50) NULL,
	prodg_code varchar(3) NULL,
	prodg_code1 varchar(3) NULL,
	client_code varchar(20) NULL,
	update_user varchar(10) NOT NULL,
	update_date timestamp(0) NOT NULL,
	row_id varchar(18) NOT NULL,
	prod_dateend timestamp(0) NULL,
	old_prod_code varchar(20) NULL,
	target_audience varchar(20) NULL,
	entry_date timestamp(0) NULL,
	entry_user varchar(20) NULL,
	prod_alt_name varchar(100) NULL
)
SERVER gen21
OPTIONS (schema 'GEN21_TRANS7', table 'MT_PROD');
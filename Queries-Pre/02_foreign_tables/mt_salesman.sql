DROP FOREIGN TABLE IF EXISTS mt_salesman;

CREATE FOREIGN TABLE mt_salesman (
	sales_code varchar(5) OPTIONS(key 'true') NOT NULL,
	sales_name varchar(40) NULL,
	sales_nik varchar(10) NULL,
	non_active timestamp(0) NULL,
	update_user varchar(10) NOT NULL,
	update_date timestamp(0) NOT NULL,
	row_id varchar(18) NOT NULL,
	costc_code varchar(3) NULL,
	phone_number varchar(50) NULL,
	handphone varchar(50) NULL,
	email varchar(50) NULL,
	salesg_code varchar(3) NULL,
	user_code varchar(10) NULL
)
SERVER gen21
OPTIONS (schema 'GEN21_TRANS7', table 'MT_SALESMAN');
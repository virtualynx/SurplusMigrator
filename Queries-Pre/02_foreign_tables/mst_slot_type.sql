DROP FOREIGN TABLE IF EXISTS mst_slot_type;

CREATE FOREIGN TABLE mst_slot_type (
	vc_slot_code varchar(10) OPTIONS(key 'true') NOT NULL,
	vc_description varchar(100) NOT NULL,
	dt_date_from timestamp(0) OPTIONS(key 'true') NOT NULL,
	dt_date_until timestamp(0) NOT NULL,
	vc_time_from varchar(10) NOT NULL,
	vc_time_until varchar(10) NOT NULL,
	update_user varchar(10) NULL,
	update_date timestamp(0) NULL,
	row_id varchar(18) NULL,
	bg_rgb numeric NULL,
	fg_rgb numeric NULL,
	isprime bpchar(1) NULL,
	tz_priority numeric NULL
)
SERVER gen21
OPTIONS (schema 'GEN21_TRANS7', table 'MST_SLOT_TYPE');
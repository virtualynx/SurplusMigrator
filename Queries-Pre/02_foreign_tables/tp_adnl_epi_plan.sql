DROP FOREIGN TABLE IF EXISTS tp_adnl_epi_plan;

CREATE FOREIGN TABLE tp_adnl_epi_plan (
	row_id_slot varchar(18) OPTIONS(key 'true') NOT NULL,
	row_id_epi varchar(18) OPTIONS(key 'true') NOT NULL,
	po_number varchar(30) NULL,
	epi_no numeric NULL,
	revision_no numeric NULL,
	update_date timestamp(0) NULL,
	update_user varchar(10) NULL,
	row_id varchar(18) NULL,
	cbs_date timestamp(0) OPTIONS(key 'true') NOT NULL,
	channel_code varchar(3) OPTIONS(key 'true') NOT NULL,
	run_id numeric NULL,
	run_split numeric NULL,
	run_repeat numeric NULL
)
SERVER gen21
OPTIONS (schema 'GEN21_TRANS7', table 'TP_ADNL_EPI_PLAN');
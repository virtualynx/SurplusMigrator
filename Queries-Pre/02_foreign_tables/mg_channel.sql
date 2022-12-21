DROP FOREIGN TABLE IF EXISTS mg_channel;

CREATE FOREIGN TABLE mg_channel (
	channel_code varchar(3) OPTIONS(key 'true') NOT NULL,
	channel_name varchar(40) NOT NULL,
	non_active timestamp(0) NULL,
	update_user varchar(10) NOT NULL,
	update_date timestamp(0) NOT NULL,
	row_id varchar(18) NOT NULL,
	start_time varchar(8) NOT NULL,
	end_time varchar(8) NOT NULL,
	default_f bpchar(1) NULL,
	vat_per numeric(5, 2) NULL,
	mcr_channel_code varchar(10) NULL,
	ch_group_code varchar(3) NULL,
	ch_genre_code varchar(3) NULL,
	passthrough_flag bpchar(1) NULL,
	catslot_code varchar(3) NULL,
	playout_machine_code varchar(5) NULL,
	is_vod varchar(1) NULL,
	hd_sd_flag varchar(2) NULL,
	frame_rate numeric NULL,
	simulcast bpchar(1) NULL,
	simulcast_main_chn varchar(3) NULL
)
SERVER gen21
OPTIONS (schema 'GEN21_TRANS7', table 'MG_CHANNEL');
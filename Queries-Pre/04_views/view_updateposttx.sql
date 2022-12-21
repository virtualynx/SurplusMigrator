DROP VIEW IF EXISTS view_updateposttx;

CREATE VIEW view_updateposttx AS 
	SELECT 
		mcn.channel_name AS channel,
		tm1.tx_code AS region,
		tm1.po_number AS mo,
		tm1.mo_no AS contract,
		(('01'::text || to_char(tm1.mo_book_date, 'MMYYYY'::text)) || '-'::text) || to_char(dev.last_day(tm1.mo_book_date::timestamp with time zone)::timestamp with time zone, 'DDMMYYYY'::text) AS period,
		( SELECT mt_agen.agen_com_name
			   FROM dev.mt_agen
			  WHERE mt_agen.agen_code::text = sr1.agen_code::text
		) AS agency,
		( SELECT mt_client.client_com_name
			   FROM dev.mt_client
			  WHERE mt_client.client_code::text = sr1.client_code::text
		) AS advertiser,
		( SELECT mt_salesman.sales_name
			   FROM dev.mt_salesman
			  WHERE mt_salesman.sales_code::text = sr1.sales_code::text
		) AS ae,
		( SELECT mt_prod.prod_name
			   FROM dev.mt_prod
			  WHERE mt_prod.prod_code::text = mp1.prod_code::text
		) AS brand,
		tm1.po_type AS type,
		sr1.job_id AS package,
		tm1.mo_book_date AS actdate,
		f_time_format(
			COALESCE(
				( SELECT tt_mo2.tx_time
			   FROM dev.tt_mo2
			  WHERE tt_mo2.mo_no::text = tm1.mo_no::text AND tt_mo2.row_id_spot::text = tm1.row_id::text
				), tm1.mo_book_time
			)::text
		) AS acttime,
		f_time_format(tm1.mo_book_time::text) AS schtime,
		tm1.mo_book_date AS schdate,
		mf.film_poc_title AS programme,
		f_get_episode(tcd1.channel_code, to_char(tcd1.cbs_date::timestamp with time zone, 'YYYYMMDD'::text)::character varying, tcd1.row_id, 'Y'::bpchar) AS episode,
		f_get_prod_version_name(mp1.prod_code::text, tm1.prod_version::text) AS version,
		f_jam_to_detik(mp1.prod_duration::text) AS length,
		CASE 
			WHEN tm1.for_pos_block::text = 'Y'::text THEN NULL::smallint
			ELSE tm1.mo_book_cb
		END AS pos,
		tm1.po_disc_home AS rate,
		0 AS priority,
		0 AS charge,
		sr1.agency_discount / 100::numeric AS discount,
		sr1.agency_commission / 100::numeric AS comission,
		CASE
			WHEN tm1.verified::text = 'Y'::text THEN 'V'::text
			ELSE 'N'::text
		END AS logstatus,
		NULL::text AS error,
		tm1.mo_dpp_home AS netsales,
		0 AS id,
		to_char(tm1.mo_book_date, 'mm'::text) AS month,
		to_char(tm1.mo_book_date, 'yyyy'::text) AS year
	FROM 
		dev.tt_mo1 tm1
		JOIN dev.mg_channel mcn ON tm1.channel_code::text = mcn.channel_code::text
		JOIN dev.st_reservations sr1 ON sr1.row_id::text = tm1.row_id_res::text
		JOIN dev.st_reservation_matrix1 srm1 ON srm1.row_id::text = tm1.row_id_matrix1::text
		LEFT JOIN dev.mp_film mf ON tm1.film_poc::text = mf.film_poc::text AND tm1.row_id_poc::text = mf.row_id::text
		LEFT JOIN dev.tp_cbs_dps1 tcd1 ON tm1.row_id_slot::text = tcd1.row_id::text
		LEFT JOIN dev.mt_prod1 mp1 ON tm1.prod_code::text = mp1.prod_code::text AND tm1.prod_version::text = mp1.prod_version::text
		LEFT JOIN dev.st_pkg_int_hdr spkg ON sr1.rid_pkg_ext::text = spkg.row_id::text
	WHERE 
		tm1.channel_code::text = 'TV7'::text 
		AND tm1.mo_book_status = '2'::bpchar;
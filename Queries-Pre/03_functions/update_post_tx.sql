CREATE OR REPLACE FUNCTION update_post_tx(month_date character varying, year_date character varying)
 RETURNS TABLE(channel character varying, region character varying, mo character varying, contract character varying, period text, agency character varying, advertiser character varying, ae character varying, brand character varying, type character varying, package character varying, actdate timestamp without time zone, acttime text, schtime text, schdate timestamp without time zone, programme character varying, episode text, version text, length integer, pos smallint, rate numeric, priority integer, charge integer, discount numeric, commission numeric, logstatus text, error text, netsales numeric, id integer)
 LANGUAGE plpgsql
AS $function$
--declare
--	enddate				TIMESTAMP := TO_TIMESTAMP(enddate_input, 'YYYY-MM-DD HH24:MI:SS');
--	agingdate			TIMESTAMP := TO_TIMESTAMP(agingdate_input, 'YYYY-MM-DD HH24:MI:SS');
BEGIN 
	return query
		SELECT 
			MCN.channel_name AS channel,																						
			TM1.TX_CODE	AS REGION,																						
			TM1.PO_NUMBER as MO,																						
			TM1.MO_NO as CONTRACT,
			'01' || TO_CHAR (TM1.MO_BOOK_DATE, 'MMYYYY') || '-' || TO_CHAR (last_day(TM1.MO_BOOK_DATE), 'DDMMYYYY') AS PERIOD,																						
			(
				SELECT 
					MT_AGEN.AGEN_COM_NAME																						
				FROM 
					MT_AGEN																						
				WHERE 
					MT_AGEN.AGEN_CODE = SR1.AGEN_CODE
			) as AGENCY,																						
			(
				SELECT 
					mt_client.client_com_name																						
				FROM 
					mt_client																						
				WHERE 
					mt_client.client_code = SR1.client_code
			) as ADVERTISER,																						
			(
				SELECT 
					mt_salesman.sales_name																							
				FROM 
					mt_salesman																						
				WHERE 
					mt_salesman.sales_code = SR1.sales_code
			) AS AE,																						
			(
				SELECT 
					MT_PROD.PROD_NAME																							
				FROM 
					MT_PROD																						
				WHERE 
					MT_PROD.prod_code = MP1.PROD_CODE
			) AS BRAND,																						
			TM1.PO_TYPE AS TYPE,																						
			SR1.job_id AS PACKAGE,																							
			TM1.mo_book_date AS ACTDATE,																						
			f_time_format(																							
				coalesce(
					(
						SELECT tx_time																						
						FROM tt_mo2																				
						WHERE mo_no = TM1.MO_NO AND row_id_spot = TM1.ROW_ID
					),																				
					TM1.MO_BOOK_TIME
				)
			) AS ACTTIME,																						
			f_time_format(TM1.mo_book_time) AS SCHTIME,																						
			TM1.mo_book_date AS SCHDATE,																						
			MF.film_poc_title AS PROGRAMME,																						
			F_Get_Episode (
				TCD1.CHANNEL_CODE,																							
				TO_CHAR(TCD1.cbs_date, 'YYYYMMDD'),																			
				TCD1.ROW_ID,																			
				'Y'
			) AS EPISODE,																						
			F_GET_PROD_VERSION_NAME (MP1.prod_code, TM1.PROD_VERSION) AS VERSION,																						
			F_JAM_TO_DETIK (mp1.prod_duration) AS LENGTH,																					
			(CASE WHEN TM1.for_pos_block = 'Y' THEN NULL ELSE TM1.mo_book_cb END) AS POS,																																										
			TM1.PO_DISC_HOME AS RATE,																						
			0 AS PRIORITY,																						
			0 AS CHARGE,																						
			SR1.agency_discount / 100 AS DISCOUNT,																						
			SR1.agency_commission / 100 AS COMMISSION,																						
			(CASE WHEN TM1.VERIFIED = 'Y' THEN 'V' ELSE 'N' END) AS LOGSTATUS,																					
			NULL AS ERROR,																						
			TM1.MO_DPP_HOME AS NETSALES,																						
			0 AS ID																																													
		FROM
				tt_mo1                  		TM1																							
				join mg_channel         		MCN on TM1.channel_code = MCN.channel_code																							
				join st_reservations        	SR1 on SR1.ROW_ID = TM1.ROW_ID_RES																							
				join st_reservation_matrix1		SRM1 on SRM1.ROW_ID = TM1.ROW_ID_MATRIX1																								
				left outer join mp_film     	MF on TM1.film_poc = MF.film_poc and TM1.row_id_poc = MF.row_id																							
				left outer join tp_cbs_dps1		TCD1 on TM1.row_id_slot = TCD1.row_id																							
				left outer join mt_prod1    	MP1 on TM1.prod_code = MP1.prod_code and TM1.prod_version = MP1.prod_version																							
				left outer join ST_PKG_INT_HDR	SPKG on SR1.rid_pkg_ext = SPKG.row_id																					
		WHERE
				1 = 1
				and TO_CHAR(TM1.MO_BOOK_DATE, 'mm') = month_date																								
				AND TO_CHAR(TM1.MO_BOOK_DATE, 'yyyy') = year_date																							
				AND TM1.channel_code = 'TV7'																					
				AND TM1.MO_BOOK_STATUS = '2' -- 2 = STATUS CONFIRMED
		;
end;
$function$
;

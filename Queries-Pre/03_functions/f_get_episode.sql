CREATE OR REPLACE FUNCTION f_get_episode(vchannelcode character varying, vcbsdate character varying, vrowidslot character varying, vrettype character DEFAULT 'N'::bpchar)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
declare
--	enddate				TIMESTAMP := TO_TIMESTAMP(enddate_input, 'YYYY-MM-DD HH24:MI:SS');
--	agingdate			TIMESTAMP := TO_TIMESTAMP(agingdate_input, 'YYYY-MM-DD HH24:MI:SS');
	ln_episode 			int;
	ls_epiName 			text := '';
	ls_prevepiName 		text := '';
	vReturn				text := '';
	lc_Episode_n 		cursor for
					   		SELECT DISTINCT TP_ADNL_EPI_PLAN.epi_no
					  		FROM 
					  			TP_ADNL_EPI_PLAN,
					       		PUR_EPISODE_HDR,
					       		PURCHASE_CONTRACT_HDR
					   		WHERE 
					   			( TP_ADNL_EPI_PLAN.row_id_epi       = PUR_EPISODE_HDR.row_id ) AND
					      		( TP_ADNL_EPI_PLAN.revision_no      = PUR_EPISODE_HDR.revision_no ) AND
					            ( PUR_EPISODE_HDR.pur_contract_no   = PURCHASE_CONTRACT_HDR.pur_contract_no ) AND
					      		( PUR_EPISODE_HDR.revision_no    = PURCHASE_CONTRACT_HDR.revision_no ) AND
					            ( TP_ADNL_EPI_PLAN.row_id_slot    = vRowIdSlot ) AND
					      		( TP_ADNL_EPI_PLAN.CHANNEL_CODE    = vChannelCode ) AND
					      		( TP_ADNL_EPI_PLAN.CBS_DATE    = TO_DATE(vCbsDate, 'YYYYMMDD'));
	lc_Episode_2 		cursor for
							SELECT 
								DISTINCT PUR_EPISODE_HDR.FILM_EPI_TITLE, 
								TP_ADNL_EPI_PLAN.epi_no
							FROM 
								TP_ADNL_EPI_PLAN,
							   	PUR_EPISODE_HDR,
							   	PURCHASE_CONTRACT_HDR
							WHERE 
								( TP_ADNL_EPI_PLAN.row_id_epi       = PUR_EPISODE_HDR.row_id ) AND
								( TP_ADNL_EPI_PLAN.revision_no      = PUR_EPISODE_HDR.revision_no ) AND
								( PUR_EPISODE_HDR.pur_contract_no   = PURCHASE_CONTRACT_HDR.pur_contract_no ) AND
								( PUR_EPISODE_HDR.revision_no    = PURCHASE_CONTRACT_HDR.revision_no ) AND
								( TP_ADNL_EPI_PLAN.row_id_slot    = vRowIdSlot ) AND
								( TP_ADNL_EPI_PLAN.CHANNEL_CODE    = vChannelCode ) AND
								( TP_ADNL_EPI_PLAN.CBS_DATE    = TO_DATE(vCbsDate, 'YYYYMMDD'))
							ORDER BY TP_ADNL_EPI_PLAN.epi_no desc;
	lc_Episode_other 	cursor for
							SELECT 
								DISTINCT PUR_EPISODE_HDR.FILM_EPI_TITLE
							FROM 
								TP_ADNL_EPI_PLAN,
							   	PUR_EPISODE_HDR,
							   	PURCHASE_CONTRACT_HDR
							WHERE 
								( TP_ADNL_EPI_PLAN.row_id_epi       = PUR_EPISODE_HDR.row_id ) AND
								( TP_ADNL_EPI_PLAN.revision_no      = PUR_EPISODE_HDR.revision_no ) AND
								( PUR_EPISODE_HDR.pur_contract_no   = PURCHASE_CONTRACT_HDR.pur_contract_no ) AND
								( PUR_EPISODE_HDR.revision_no    = PURCHASE_CONTRACT_HDR.revision_no ) AND
								( TP_ADNL_EPI_PLAN.row_id_slot    = vRowIdSlot ) AND
								( TP_ADNL_EPI_PLAN.CHANNEL_CODE    = vChannelCode ) AND
								( TP_ADNL_EPI_PLAN.CBS_DATE    = TO_DATE(vCbsDate, 'YYYYMMDD'));
BEGIN 
--		RAISE NOTICE 'enddate %', enddate;
--	   	RAISE NOTICE 'agingdate %', agingdate;
	if vRetType = 'N' then
 		-- For returning the Episode Number -
	  	OPEN lc_Episode_n;
	   	LOOP
     		FETCH lc_Episode_n INTO ln_episode;
			exit when not found;
--	    	ln_episode := '';
	     	IF ln_episode IS NOT NULL THEN
	       		vReturn := TO_CHAR(ln_episode) || ',' || vReturn  ;
     		END IF;
--			EXIT WHEN lc_Episode_n%NOTFOUND OR lc_Episode_n%NOTFOUND IS NULL;
	   	END LOOP;
	   	CLOSE lc_Episode_n;
	ElsIf vRetType = '2' then
		-- For returning the Episode Number -
		OPEN lc_Episode_2;
		LOOP
			FETCH lc_Episode_2 INTO ls_epiName,ln_episode;
			exit when not found;
			IF ln_episode IS NOT NULL THEN
				IF ls_prevepiName = ls_epiName THEN
					vReturn := SUBSTR(vReturn, 1, LENGTH(vReturn) - 2);
					vReturn := ',' || trim(TO_CHAR(ln_episode)) || ')' || ',' || trim(vReturn)  ;
				ELSE
					vReturn := trim(ls_epiName) || '(' || trim(TO_CHAR(ln_episode)) || ')' || ',' || trim(vReturn)  ;
				END IF;
			END IF;
			ls_prevepiName := trim(ls_epiName);
--			EXIT WHEN lc_Episode_2%NOTFOUND OR lc_Episode_2%NOTFOUND IS NULL;
		END LOOP;
		CLOSE lc_Episode_2;
	Else
		-- For Returning the Episode Name -
		OPEN lc_Episode_other;
		LOOP
			FETCH lc_Episode_other INTO ls_epiName;
			exit when not found;
			IF ls_epiName IS NOT NULL THEN
--				vReturn := trim(TO_CHAR(ls_epiName)) || ',' || trim(vReturn);
				vReturn := trim(ls_epiName) || ',' || trim(vReturn);
			END IF;
--			EXIT WHEN lc_Episode_other%NOTFOUND OR lc_Episode_other%NOTFOUND IS NULL;
		END LOOP;
		CLOSE lc_Episode_other;
	End if;
	
	RETURN vReturn ;
end;
$function$
;

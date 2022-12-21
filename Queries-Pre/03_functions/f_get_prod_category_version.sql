CREATE OR replace FUNCTION f_get_prod_category_version(p_prod_code text, p_prod_version text, p_iscode IN char)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
declare
	 /*
	   mario 4 Sep 2006,
	   p_iscode = 'c' then return only Category Code
	 */
   	lv_category  	text;
  	lc_category		cursor for
					SELECT MV.PRODG_CODE||'@'||MV.PRODG_CODE1 CATCODE,PC.PRODG_NAME CATNAME,PC.PRODG_NAME||'-'||PCS.PRODG_NAME CATNAMESUB, MV.PRODG_CODE1 CATONLY
					FROM 
					 	MT_VERSIONG MV
					 	left join MT_REL_PROD_CAT MC on MC.PRODG_CODE = MV.PRODG_CODE and MC.PRODG_CODE1 = MV.PRODG_CODE1
					 	join MT_PRODG PC on MV.prodg_code = PC.prodg_code
					 	join MT_PRODG_SUB PCS on PC.ROW_ID = PCS.ROW_ID_PROD and MV.prodg_code1   = PCS.prodg_code1
					WHERE 
					   	MC.PROD_CODE = 'B003727'
					    AND MV.PROD_CODE = 'B003727'
					    AND MV.PROD_VERSION = '0009'
					ORDER BY 3 desc;
BEGIN
	FOR lc_cat IN lc_category
	LOOP
		IF UPPER(p_iscode) = 'Y' THEN
			--Get Category Code
			lv_category := lv_category||','||lc_cat.catcode;
		ELSIF UPPER(p_iscode) = 'N' THEN
			--Get Category Name
			lv_category := lv_category||','||lc_cat.catname;
		ELSIF UPPER(p_iscode) = 'C' THEN
			--Return only category code
			lv_category := lv_category||','||lc_cat.catonly;
		ELSIF UPPER(p_iscode) = 'A' THEN
			--Return only category code
			lv_category := lv_category||','||lc_cat.catnamesub;
		END IF;
	END LOOP;
	
	IF LENGTH(lv_category) > 0 THEN
		lv_category := substring(lv_category from 1 for 2);
	END IF;
	
	RETURN trim(lv_category);
END;
$function$
;
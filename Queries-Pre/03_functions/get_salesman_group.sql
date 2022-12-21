CREATE OR REPLACE FUNCTION get_salesman_group(as_sales_code text, ad_date_po timestamp without time zone, ad_date_book timestamp without time zone)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
declare
	vs_salesg_code text := '';
	vs_date timestamp;
 	vs_config text;
BEGIN
	vs_config := f_get_config('SRP', NULL);
 	IF vs_config = 'P' THEN
 		vs_date := ad_date_po;
  	ELSIF vs_config = 'B' THEN
   		vs_date := ad_date_book;
  	ELSE
   		RETURN NULL;
  	END if;
  
  	SELECT SALESG_CODE
  	INTO vs_salesg_code
  	FROM MT_SALESMAN1
  	WHERE MT_SALESMAN1.SALES_CODE = as_sales_code
  	AND vs_date BETWEEN MT_SALESMAN1.SALES_DFROM AND MT_SALESMAN1.SALES_DTO;
  
  	RETURN vs_salesg_code;
 
--  	EXCEPTION
--     	WHEN NO_DATA_FOUND THEN
--       		RETURN NULL;
--     	WHEN TOO_MANY_ROWS then
--     		RAISE EXCEPTION 'Salesman Group Conflict for AE:'||as_sales_code||', date:'||to_char(vs_date,'dd/mmm/yyyy');
END;
$function$
;

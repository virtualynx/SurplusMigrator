CREATE OR REPLACE FUNCTION f_get_prod_version_name(p_prod_code text, p_version text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
declare
	ls_ver_name text := '';
BEGIN
	select 
		smp1.prod_version_name
  	into ls_ver_name
	from mt_prod1 smp1
	where 
		smp1.prod_code  = p_prod_code
	  	and smp1.prod_version = p_version;

	RETURN ls_ver_name;
END;
$function$
;

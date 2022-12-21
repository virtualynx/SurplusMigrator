CREATE OR REPLACE FUNCTION last_day(timestamp with time zone)
 RETURNS date
 LANGUAGE sql
 IMMUTABLE STRICT
AS $function$
	SELECT (date_trunc('MONTH', $1) + INTERVAL '1 MONTH - 1 day')::date;
$function$
;

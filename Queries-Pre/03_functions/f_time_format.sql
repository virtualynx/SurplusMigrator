CREATE OR REPLACE FUNCTION f_time_format(ar_time text)
 RETURNS text
 LANGUAGE plpgsql
AS $functionx$
--AS $function$
--	DECLARE last_id int;
--	DECLARE sequencer_number int;
--	DECLARE current_month text;
--	DECLARE current_year text;
--	DECLARE last_month text;
--	DECLARE format text;
--	DECLARE formated_str text;
--	DECLARE generated_id text;
begin
	IF ar_time IS NULL THEN
		RETURN NULL;
	END IF;

--	RETURN SUBSTR(ar_time,1,2)||':'||SUBSTR(ar_time,3,2) ||':'||SUBSTR(ar_time,5,2)||':'||SUBSTR(ar_time,7,2);
	
	return CONCAT(substring(ar_time, 1, 2), ':', substring(ar_time, 3, 2), ':', substring(ar_time, 5, 2), ':', substring(ar_time, 7, 2));
end;
--$function$
$functionx$
;

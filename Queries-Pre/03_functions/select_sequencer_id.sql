CREATE OR REPLACE FUNCTION select_sequencer_id(_type text DEFAULT NULL::text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE last_id int;
DECLARE sequencer_number int;
DECLARE current_month text;
DECLARE current_year text;
DECLARE last_month text;
DECLARE format text;
DECLARE formated_str text;
DECLARE generated_id text;
BEGIN
	SELECT lastid FROM master_sequencer WHERE type = _type
	INTO last_id;

	current_year = date_part('year',(current_timestamp));
	current_year = substring(current_year,3);
	current_month = date_part('month',(current_timestamp));
	last_month = date_part('month',(SELECT lastmonth FROM master_sequencer WHERE type = _type));

	if current_month <> last_month then
	   last_id = 0;
	end if;

	if current_month::int4 < 10 then
	   current_month = CONCAT ('0',current_month);
	end if;

	sequencer_number = last_id + 1;


	format = '0000';
    formated_str = substring(format, length(sequencer_number::text)+1);
    generated_id = CONCAT(_type,current_year,current_month,formated_str,sequencer_number);
	return generated_id;


END
$function$
;

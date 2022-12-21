CREATE OR REPLACE PROCEDURE insert_program_projection_cursor(year_date character varying)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
	generated_id text;
	current_month text;
	last_month text;
	rec_scheduler record;
	cur_scheduler cursor(year_date varchar)
		 for
		 SELECT distinct
			row_id_poc bms_id, film_poc_title FILM_BMS,
			(select row_id_cms from view_airedprograms  va2
			 where row_id_bms = msp.row_id_poc
				   and yeardate::varchar = year_date
			 limit 1) CMS_ID,
			(select FILM_CMS  from view_airedprograms  va2
			 where row_id_bms = msp.row_id_poc
				   and yeardate::varchar = year_date
			 limit 1) FILM_CMS,
			1 as TimeZone, 1 as Department, '' Slot,
			CAST(msp.duration as integer)/10000 duration,
			1 as programcategory,
			0 programbudget, count(row_id_poc) totalepisode,
			0 episodebudget, 0 copb,
			sum(CAST(msp.duration as integer)/10000) minutes,
			sum(CAST(msp.duration as integer)/10000)/60 hours,
			CAST(extract(MONTH from cbs_date) as integer) MONTHDATE,
			CAST(extract(year from cbs_date) as integer) YEARDATE,
			0
		FROM MP_SCH_PLANNER msp
		where extract(year from cbs_date)::varchar = year_date
		group by row_id_poc,film_poc_title,duration,
				extract(year from cbs_date),
				extract(MONTH from cbs_date)
		order by film_poc_title;
begin
	--open cursor
	open cur_scheduler(year_date);
	loop
	fetch cur_scheduler into rec_scheduler;
	exit when not found;

	--set generated id for sequencer
	current_month = date_part('month',(current_timestamp));
	last_month = date_part('month',(SELECT lastmonth FROM master_sequencer WHERE type = 'TP'));
	if current_month <> last_month then
	  UPDATE master_sequencer
		SET
		lastid = 0
		WHERE type = 'TP';
	end if;

	--get generated id
	generated_id := (select select_sequencer_id('TP'));

	--UPDATE SEQUENCER
	UPDATE master_sequencer
	SET
	lastid = lastid + 1
	WHERE type = 'TP';

	--insert data
	insert into transaction_budget_programprojection
	(id, programbms_id,programbms_name , programcms_id , programcms_name , daypart_id , departmentid , slot , duration ,
	programcategoryid , programbudget , totalepisode , episodebudget , copb , totalminutes , totalhours , monthdate , yeardate, parentid,
	approved_1, approved1_by, approved1_date,approved_2, approved2_by, approved2_date,approved_3, approved3_by, approved3_date, is_disabled)
	values
	(generated_id, rec_scheduler.bms_id, rec_scheduler.FILM_BMS, rec_scheduler.CMS_ID, rec_scheduler.FILM_CMS, 1,
	 NULL, rec_scheduler.Slot, rec_scheduler.duration, 1, rec_scheduler.programbudget,
	 rec_scheduler.totalepisode, rec_scheduler.episodebudget, rec_scheduler.copb, rec_scheduler.minutes, rec_scheduler.hours,
	 rec_scheduler.MONTHDATE, rec_scheduler.YEARDATE, '',
	 false, NULL,NULL,false, NULL,NULL,false, NULL,null, false
	);

	end loop;
   close cur_scheduler;
	commit;
	   -- close the cursor

end;
$procedure$
;

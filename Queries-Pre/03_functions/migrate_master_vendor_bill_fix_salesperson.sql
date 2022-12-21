CREATE OR REPLACE FUNCTION migrate_master_vendor_bill_fix_salesperson()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
declare
--	enddate				TIMESTAMP := TO_TIMESTAMP(enddate_input, 'YYYY-MM-DD HH24:MI:SS');
--	agingdate			TIMESTAMP := TO_TIMESTAMP(agingdate_input, 'YYYY-MM-DD HH24:MI:SS');
--	ln_episode 			int;
	ls_salespersonid 	text;
	lc_salesperson 		cursor for
					   		SELECT distinct salesperson 
					  		FROM 
					  			master_vendor_bill
					  		where 
					  			salesperson is not null 
					  			and salesperson <> '0';
BEGIN 
--		RAISE NOTICE 'enddate %', enddate;
--	   	RAISE NOTICE 'agingdate %', agingdate;
	OPEN lc_salesperson;
   	LOOP
 		FETCH lc_salesperson INTO ls_salespersonid;
		exit when not found;
	
		update 
			master_vendor_bill 
		set 
			salesperson = (select "name" from master_vendor where vendorid = CAST(ls_salespersonid AS integer))
		where
			salesperson = ls_salespersonid
		;
   	END LOOP;
   	CLOSE lc_salesperson;
end;
$function$
;

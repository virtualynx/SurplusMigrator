insert into master_artist_profession (
	name,
	remark,
	is_disabled
)
select 
	name,
	note,
	not active
from 
	dblink(
		'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7'::text, 
		'
			SELECT 
				name, 
				note,
				active
			FROM 
				e_frm."master_skill"
		'::text
	) tbl (
		name varchar(100), 
		note varchar(200), 
		active bool
	)
;
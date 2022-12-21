insert into master_artist_type(
	artisttypeid,
	name,
	is_disabled
)
select 
	artistype_id,
	artistype_name,
	artistype_isdisabled
from 
	dblink(
		'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7'::text, 
		'
			SELECT 
				artistype_id, 
				artistype_name,
				artistype_isdisabled
			FROM 
				e_frm."master_artistype"
		'::text
	) artistype (
		artistype_id int, 
		artistype_name varchar(50), 
		artistype_isdisabled boolean
	)
;

insert into master_artist(
	artistid,
	name,
	address,
	city,
	sex,
	picturetitle,
	picturepath,
	ktp,
	artisttypeid,
	ishavenpwp,
	npwp,
	is_disabled
)
select 
	code,
	name,
	address,
	city,
	sex,
	null,
	picture_address,
	ktp,
	artistype_id,
	artis_ishavenpwp,
	npwp,
	not active
from 
	dblink(
		'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7'::text, 
		'
			SELECT 
				code, 
				name,
				address,
				city,
				sex,
				picture_address,
				ktp,
				artistype_id,
				artis_ishavenpwp,
				npwp,
				active
			FROM 
				e_frm."master_artis"
		'::text
	) artis (
		code varchar(15), 
		name varchar(100), 
		address varchar(200), 
		city varchar(30), 
		sex varchar(20), 
		picture_address varchar(100), 
		ktp varchar(100), 
		artistype_id int,
		artis_ishavenpwp boolean,
		npwp varchar(100), 
		active boolean
	)
;
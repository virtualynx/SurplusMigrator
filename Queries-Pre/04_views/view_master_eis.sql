DROP MATERIALIZED VIEW IF EXISTS view_master_eis;

CREATE MATERIALIZED VIEW view_master_eis AS
	SELECT 
		nik,
    	"name",
	    unit,
	    "position",
	    "management_level",
	    position_id,
	    department_id,
	    phone
	FROM dblink(
		'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7'::text, 
		'
			SELECT 
				"NIK" as nik, 
				"Nama" as "name",
				"Unit" as unit,
				"Jabatan" as position,
				"ManajemenLevel" as management_level,
				"KodePosisi" as position_id,
				"department_code",
				"NomorHP"
			FROM 
				hris."MasterEis"'::text
	) tbl (
		nik character varying(10), 
		name character varying(100), 
		unit character varying(255), 
		"position" character varying(100), 
		"management_level" character varying(30),
		position_id character varying(50),
		department_id character varying(50),
		phone character varying(25)
	)
WITH DATA;
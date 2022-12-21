DROP VIEW IF EXISTS view_master_position;

CREATE VIEW view_master_position as
	SELECT 
		posisi_kodejabatan as code,
		posisi_jabatan as "name"
	FROM dblink(
		'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7', 
		'
			SELECT 
				posisi_jabatan, 
				posisi_kodejabatan
			FROM 
				hris."Posisi"
		'
	) tbl(
		posisi_jabatan varchar(100),
		posisi_kodejabatan varchar(50)
	)
;

DROP VIEW IF EXISTS view_contractform;

CREATE VIEW view_contractform AS 
	SELECT 
		contractformid,
		"type_id",
		"type",
		code,
		artist_id,
		artist_name,
		program_name,
		job,
		start_date,
		end_date,
		total_episode,
		remuneration,
		created_by
	FROM 
		dblink(
			'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7'::text, 
			'
				SELECT 
					contractform_id, 
					type.contracttype_id,
					type.contracttype_name,
					contractform_code,
					contractform_artist_code,
					contractform_artist_name,
					show_name,
					worktype_name,
					contractform_shooting_startdate,
					contractform_shooting_enddate,
					contractform_episode_total,
					contractform_totalprice,
					created_by
				FROM 
					e_legal.transaction_contractform tcontract
					join e_legal.master_worktype worktype on tcontract.worktype_id = worktype.worktype_id
					join e_legal.master_contracttype type on tcontract.contracttype_id = type.contracttype_id
			'::text
		) tbl(
			contractformid varchar(50),
			"type_id" int4,
			"type" varchar(25),
			code varchar(50),
			artist_id varchar(50),
			artist_name varchar(500),
			program_name varchar(255),
			job varchar(255),
			start_date timestamp,
			end_date timestamp,
			total_episode int4,
			remuneration numeric,
			created_by varchar(255)
		)
;


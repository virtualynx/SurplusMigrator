insert into master_artist_account (
	artistid,
	bankid,
	name,
	"number",
	is_disabled
)
select 
	artis_id,
	case
		when coalesce(bank_id, 0) <> 0 then
			bank_id
		else
			0
	end as bankid,
	case
		when length(trim(coalesce(artisbank_accountname, ''))) > 0 then
			artisbank_accountname
		else
			'<EMPTY AT OLD INSOSYS>'
	end as name,
	artisbank_rekening, 
	false
from 
	dblink(
		'dbname=integration port=5432 host=172.16.123.121 user=postgres password=initrans7'::text, 
		'
			SELECT 
				artis_id, 
				bank_id,
				artisbank_accountname,
				artisbank_rekening
			FROM 
				e_frm."master_artisbank"
			WHERE
				bank_id not in(19, 20)
				and artis_id not in(
					''AT0222120014'',
					''AT0222120015'',
					''AT0222120016'',
					''AT0222120017'',
					''AT0222120018'',
					''AT0222120019'',
					''AT0222120020'',
					''AT0222120021''
				)
		'::text
	) tbl (
		artis_id varchar(15), 
		bank_id int,
		artisbank_accountname varchar(50), 
		artisbank_rekening varchar(50)
	)
;
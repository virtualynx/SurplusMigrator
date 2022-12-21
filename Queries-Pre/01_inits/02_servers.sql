-- E_EFRM
CREATE SERVER IF NOT EXISTS e_frm FOREIGN DATA WRAPPER tds_fdw OPTIONS(
	servername '172.16.20.179',
	port '1433',
	database 'E_FRM'
);
GRANT USAGE ON FOREIGN SERVER e_frm TO postgres;
CREATE USER MAPPING IF NOT EXISTS FOR postgres SERVER e_frm OPTIONS(username 'sa', password 'controlroom');

-- E_START
CREATE SERVER IF NOT EXISTS e_start FOREIGN DATA WRAPPER tds_fdw OPTIONS(
	servername '172.16.20.179',
	port '1433',
	database 'E_START'
);
GRANT USAGE ON FOREIGN SERVER e_start TO postgres;
CREATE USER MAPPING IF NOT EXISTS FOR postgres SERVER e_start OPTIONS(username 'sa', password 'controlroom');

-- INVENTORY
CREATE SERVER IF NOT EXISTS inventory FOREIGN DATA WRAPPER tds_fdw OPTIONS(
	servername '172.16.20.179',
	port '1433',
	database 'INVENTORY'
);
GRANT USAGE ON FOREIGN SERVER inventory TO postgres;
CREATE USER MAPPING IF NOT EXISTS FOR postgres SERVER inventory OPTIONS(username 'sa', password 'controlroom');

-- GEN21
CREATE SERVER IF NOT EXISTS gen21 FOREIGN DATA WRAPPER oracle_fdw OPTIONS(dbserver '//172.16.123.101:1521/GEN21');
GRANT USAGE ON FOREIGN SERVER gen21 TO postgres;
CREATE USER MAPPING IF NOT EXISTS FOR postgres SERVER gen21 OPTIONS(user 'GEN21_TRANS7', password 'Gen21TR4ns7');
GRANT USAGE ON FOREIGN SERVER inventory TO postgres;

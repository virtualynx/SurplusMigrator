CREATE EXTENSION IF NOT EXISTS tds_fdw with SCHEMA "public";
--DROP EXTENSION IF EXISTS tds_fdw CASCADE;

CREATE EXTENSION IF NOT EXISTS oracle_fdw with SCHEMA "public";
--DROP EXTENSION IF EXISTS oracle_fdw CASCADE;

CREATE EXTENSION IF NOT EXISTS dblink with SCHEMA "public";
--DROP EXTENSION IF EXISTS dblink CASCADE;

CREATE EXTENSION IF NOT EXISTS pgagent with SCHEMA "public";
--DROP EXTENSION IF EXISTS pgagent CASCADE;

-- https://stackoverflow.com/questions/56733279/search-words-similarity-in-postgresql
-- https://dba.stackexchange.com/questions/165300/how-to-install-the-additional-module-pg-trgm
CREATE EXTENSION IF NOT EXISTS pg_trgm with schema "public";
--DROP EXTENSION IF EXISTS pg_trgm CASCADE;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp" with SCHEMA "public";
--DROP EXTENSION IF EXISTS uuid-ossp CASCADE;
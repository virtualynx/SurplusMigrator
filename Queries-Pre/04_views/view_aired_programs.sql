-- CREATE OR REPLACE VIEW dev.view_airedprograms
--  AS
--  SELECT b.row_id AS row_id_bms,
--     b.film_poc_title AS film_bms,
--     date_part('day'::text, a.cbs_date) AS daydate,
--     date_part('month'::text, a.cbs_date) AS monthdate,
--     date_part('year'::text, a.cbs_date) AS yeardate,
--     "substring"(a.slot_ttime::text, 1, 4) AS slot_ttime,
--     "substring"(a.slot_dur_define::text, 1, 4) AS slot_dur_define,
--     c.row_id_epi,
--     c.epi_no,
--     d.film_epi,
--     d.film_epi_title,
--     e.row_id AS row_id_cms,
--     e.film_title AS film_cms
--    FROM tp_cbs_dps1 a
--      LEFT JOIN mp_film b ON b.row_id::text = a.row_id_poc::text AND b.film_poc::text = a.film_poc::text
--      LEFT JOIN tp_adnl_epi_plan c ON c.row_id_slot::text = a.row_id::text
--      LEFT JOIN pur_episode_hdr d ON d.row_id::text = c.row_id_epi::text
--      LEFT JOIN purchase_contract_dtl e ON e.row_id::text = d.row_id_film::text;

-- ALTER TABLE dev.view_airedprograms
--     OWNER TO postgres;

DROP MATERIALIZED VIEW IF EXISTS view_airedprograms;

CREATE MATERIALIZED VIEW view_airedprograms as
  SELECT b.row_id AS row_id_bms,
    b.film_poc_title AS film_bms,
    date_part('day', a.cbs_date) AS daydate,
    date_part('month', a.cbs_date) AS monthdate,
    date_part('year', a.cbs_date) AS yeardate,
    "substring"(a.slot_ttime, 1, 4) AS slot_ttime,
    "substring"(a.slot_dur_define, 1, 4) AS slot_dur_define,
    c.row_id_epi,
    c.epi_no,
    d.film_epi,
    d.film_epi_title,
    e.row_id AS row_id_cms,
    e.film_title AS film_cms
  FROM tp_cbs_dps1 a
    LEFT JOIN mp_film b ON b.row_id = a.row_id_poc AND b.film_poc = a.film_poc
    LEFT JOIN tp_adnl_epi_plan c ON c.row_id_slot = a.row_id
    LEFT JOIN pur_episode_hdr d ON d.row_id = c.row_id_epi
    LEFT JOIN purchase_contract_dtl e ON e.row_id = d.row_id_film
with data;

REFRESH MATERIALIZED view view_airedprograms with data;
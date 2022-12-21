CREATE OR REPLACE FUNCTION f_jam_to_detik(p_jam text)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
declare
    v_dtk1   int;
    v_dtk2   int;
    v_dtk3   int;
    v_dtk4   int;
BEGIN
    IF TRIM(P_JAM) IS NULL THEN
        RETURN 0;
    END IF;
--    v_dtk1 := TO_NUMBER(coalesce(SUBSTRing( trim(P_JAM), 1, 2 ), '0')) * 3600;
    v_dtk1 := cast(coalesce(SUBSTRing( trim(P_JAM), 1, 2 ), '0') as integer) * 3600;
--    v_dtk2 := TO_NUMBER(coalesce(SUBSTRing( trim(P_JAM), 3, 2 ), '0')) * 60;
    v_dtk2 := cast(coalesce(SUBSTRing( trim(P_JAM), 3, 2 ), '0') as integer) * 60;
--    v_dtk3 := TO_NUMBER(coalesce(SUBSTRing( trim(P_JAM), 5, 2 ), '0'));
    v_dtk3 := cast(coalesce(SUBSTRing( trim(P_JAM), 5, 2 ), '0') as integer);
--    v_dtk4 := TO_NUMBER(coalesce(SUBSTRing( trim(P_JAM), 7, 2 ), '0')) / 25;
    v_dtk4 := cast(coalesce(SUBSTRing( trim(P_JAM), 7, 2 ), '0') as integer) / 25;
   
    RETURN( v_dtk1 + v_dtk2 + v_dtk3 + v_dtk4 );
END;
$function$
;

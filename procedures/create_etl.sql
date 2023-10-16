CREATE or replace function raw.logger(
        procedure_name varchar,
        step_name varchar,
        step_number float,
        message varchar,
        rows_affected int,
        duration_seconds float
    ) 
    RETURNS boolean
	LANGUAGE plpgsql
    AS 
    $$
    declare
    v_dyn_sql varchar;

    begin 

    v_dyn_sql := 'insert into raw.log_fact values('''||procedure_name||''', '''||step_name||''' ,'||step_number||', '''||message||''', '||rows_affected||' ,'||duration_seconds||') on conflict do nothing';
    execute v_dyn_sql;
    return TRUE;
    end;
    $$;
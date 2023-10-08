CREATE or replace function raw.etl (
    v_start_step float,
    v_stop_step float
    ) 
    RETURNS boolean
	LANGUAGE plpgsql
    AS 
    $$
    declare 

    v_procedure_name varchar := 'etl';
    v_step_nbr float;
    v_sub_step_nbr float;
    v_step_name varchar;
    v_sub_step_name varchar;
    v_message varchar;
    v_dyn_sql varchar;
    v_bool boolean;

    begin 

    v_message := 'SKIPPED';
    v_step_nbr := 1.0;
    v_step_name := 'Load Base Data';
    if v_step_nbr >= v_start_step and v_step_nbr <= v_stop_step then 
        begin 
        v_dyn_sql := 'copy raw.base_data from ''C:\Users\kduba\OneDrive\Documents\budget_eer\raw\2023_01_10_spend.csv'' delimiter '','' csv header';
        execute v_dyn_sql;
        end;
    end if;

    v_message := 'SKIPPED';
    v_step_nbr := 2.0;
    v_step_name := 'Load Dim Tables';
    if v_step_nbr >= v_start_step and v_step_nbr <= v_stop_step then 
        begin 
        v_sub_step_name := 'load category dim';
        v_sub_step_nbr := v_step_nbr + .01;
        v_message := 'SUCCESS';
        v_dyn_sql := 'insert into raw.cat_dim select distinct category, md5(category) as category_id from raw.base_data on conflict DO NOTHING';
        
        execute v_dyn_sql;
        end; 
        end if;
	return TRUE;
end;
    $$;
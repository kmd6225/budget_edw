CREATE OR REPLACE FUNCTION raw.etl(
        v_start_step float,
        v_stop_step float
        )
    RETURNS boolean
    LANGUAGE 'plpgsql'
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
        rows_affected int;
        start_time timestamp;
        end_time timestamp;
        delta double precision; 
        begin 

        v_message := 'SKIPPED';
        v_step_nbr := 1.0;
        v_step_name := 'Load Base Data';
        if v_step_nbr >= v_start_step and v_step_nbr <= v_stop_step then 
            begin 
        ----Loading raw base table from flat files
            v_message := 'SUCCESS';
            start_time := clock_timestamp();
            v_dyn_sql := 'copy raw.base_data from ''C:\Users\kduba\OneDrive\Documents\budget_eer\raw\2023_01_10_spend.csv'' delimiter '','' csv header';

            execute v_dyn_sql;

            get diagnostics rows_affected := row_count;
            end_time := clock_timestamp();
            delta := 1000 * (extract(epoch from end_time) - extract(epoch from start_time));
            perform raw.logger(v_procedure_name, v_step_name, v_step_nbr, v_message, rows_affected, delta);

            exception 
                when others then 
                    perform raw.logger(v_procedure_name, v_step_name, v_step_nbr, 'Error: '||SQLSTATE||' '||SQLERRM, 0, 0);
                    return FALSE;
            end;
        else 
            perform raw.logger(v_procedure_name, v_step_name, v_step_nbr, v_message, 0, 0);
        end if;

        v_message := 'SKIPPED';
        v_step_nbr := 2.0;
        v_step_name := 'Load Dim Tables';
    ----loading tables in the data warehouse
        if v_step_nbr >= v_start_step and v_step_nbr <= v_stop_step then 
            begin 
        ---- Loading Category Dimensional Table    
            v_sub_step_name := 'load category dim';
            v_sub_step_nbr := v_step_nbr + .01;
            v_message := 'SUCCESS';
			start_time := clock_timestamp();
            insert into raw.cat_dim select distinct md5(category), category from raw.base_data on conflict (category_id) DO UPDATE set category = EXCLUDED.category;
            get diagnostics rows_affected := row_count;
            end_time := clock_timestamp();
            delta := 1000 * (extract(epoch from end_time) - extract(epoch from start_time));
            perform raw.logger(v_procedure_name, v_sub_step_name, v_sub_step_nbr, v_message, rows_affected, delta);

            exception 
                when others then 
                perform raw.logger(v_procedure_name, v_sub_step_name, v_sub_step_nbr, 'Error: '||SQLSTATE||' '||SQLERRM, 0, 0);
                return FALSE;
            end; 

            begin 
        ---- Loading Description Dimensional Table    
            v_sub_step_name := 'load description dim';
            v_sub_step_nbr := v_sub_step_nbr + .01;
            v_message := 'SUCCESS';
			start_time := clock_timestamp();
            v_dyn_sql := 'insert into raw.descr_dim select distinct description, md5(description) as description_id from raw.base_data on conflict (description_id) DO NOTHING';
           
            execute v_dyn_sql;          

            get diagnostics rows_affected := row_count;
            end_time := clock_timestamp();
            delta := 1000 * (extract(epoch from end_time) - extract(epoch from start_time));
            perform raw.logger(v_procedure_name, v_sub_step_name, v_sub_step_nbr, v_message, rows_affected, delta);

           exception 
                when others then 
                perform raw.logger(v_procedure_name, v_sub_step_name, v_sub_step_nbr, 'Error: '||SQLSTATE||' '||SQLERRM, 0, 0);
                return FALSE;
            end; 

            begin 
        ---- Loading Transaction Fact Table    
            v_sub_step_name := 'load transaction fact';
            v_sub_step_nbr := v_sub_step_nbr + .01;
            v_message := 'SUCCESS';
			start_time := clock_timestamp();
            v_dyn_sql := 'insert into raw.transaction_fact select md5(base_data::text) as transaction_key, transaction_date,posted_date, md5(description) as description_id,md5(category) as category_id, debit, credit from raw.base_data on conflict (transaction_key) DO NOTHING';
            
            execute v_dyn_sql;          

            get diagnostics rows_affected := row_count;
            end_time := clock_timestamp();
            delta := 1000 * (extract(epoch from end_time) - extract(epoch from start_time));
            perform raw.logger(v_procedure_name, v_sub_step_name, v_sub_step_nbr, v_message, rows_affected, delta);

            exception 
                when others then 
                perform raw.logger(v_procedure_name, v_sub_step_name, v_sub_step_nbr, 'Error: '||SQLSTATE||' '||SQLERRM, 0, 0);
                return FALSE;
            end; 

            else
                perform raw.logger(v_procedure_name, v_step_name, v_step_nbr, v_message, 0, 0);
        end if;
        return TRUE;
    end;
    $$;
create table raw.log_fact(
    procedure_name varchar,
    step_name varchar,
    step_number float,
    message varchar,
    rows_affected int,
    duration_seconds float
) ;
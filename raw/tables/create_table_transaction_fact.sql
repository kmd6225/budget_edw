create table raw.transaction_fact(
    transaction_key text primary key, 
    transaction_date date,
    posted_date date,
    description_id text,
    category_id text,
    debit float,
    credit float
) ;
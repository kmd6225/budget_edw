create table raw.transaction_fact(
    transaction_key int, 
    transaction_date date,
    posted_date date,
    description_id int,
    category_id int,
    debit float,
    credit float
) ;
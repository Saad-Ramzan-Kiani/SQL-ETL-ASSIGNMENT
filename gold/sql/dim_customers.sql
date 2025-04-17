
CREATE TABLE gold.dim_customers AS
SELECT 
    customer_id,
    first_name || ' ' || last_name AS customer_name,
    signup_date
FROM 
    raw.stg_customers;
        

CREATE OR REPLACE VIEW silver.transformed_orders AS
SELECT 
    o.order_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    o.order_date,
    SUM(oi.quantity) AS total_items,
    ROUND(SUM(oi.quantity * oi.unit_price), 2) AS total_amount,
    o.status
FROM 
    raw.stg_orders o
JOIN 
    raw.stg_customers c ON o.customer_id = c.customer_id
JOIN 
    raw.stg_order_items oi ON o.order_id = oi.order_id
WHERE 
    o.status = 'delivered'
GROUP BY 
    o.order_id, customer_name, o.order_date, o.status;
        
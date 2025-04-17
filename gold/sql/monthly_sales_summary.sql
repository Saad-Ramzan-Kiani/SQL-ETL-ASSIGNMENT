
CREATE TABLE gold.monthly_sales_summary AS
SELECT 
    TO_CHAR(order_date, 'YYYY-MM') AS month,
    ROUND(SUM(total_amount), 2) AS total_revenue
FROM 
    gold.fact_orders_summary
GROUP BY 
    TO_CHAR(order_date, 'YYYY-MM')
ORDER BY 
    month;
        
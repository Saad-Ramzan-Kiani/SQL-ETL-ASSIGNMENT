import os
import sqlite3
import pandas as pd

input_folder = r"F:\TenPlus AB Learning Program\Phase 0\task 2\SQL-ETL-ASSIGNMENT\input"
output_folder = r"F:\TenPlus AB Learning Program\Phase 0\task 2\SQL-ETL-ASSIGNMENT\output"
db_path = os.path.join(output_folder, "transformed_orders.db")

os.makedirs(output_folder, exist_ok=True)

customers_df = pd.read_csv(os.path.join(input_folder, "stg_customers.csv"))
orders_df = pd.read_csv(os.path.join(input_folder, "stg_orders.csv"))
order_items_df = pd.read_csv(os.path.join(input_folder, "stg_order_items.csv"))

conn = sqlite3.connect(db_path)

customers_df.to_sql("stg_customers", conn, if_exists="replace", index=False)
orders_df.to_sql("stg_orders", conn, if_exists="replace", index=False)
order_items_df.to_sql("stg_order_items", conn, if_exists="replace", index=False)

cursor = conn.cursor()
cursor.executescript("""
DROP VIEW IF EXISTS transformed_orders;

CREATE VIEW transformed_orders AS
SELECT 
    o.order_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    o.order_date,
    SUM(oi.quantity) AS total_items,
    ROUND(SUM(oi.quantity * oi.unit_price), 2) AS total_amount,
    o.status
FROM stg_orders o
JOIN stg_customers c ON o.customer_id = c.customer_id
JOIN stg_order_items oi ON o.order_id = oi.order_id
WHERE o.status = 'delivered'
GROUP BY o.order_id;
""")

conn.commit()
conn.close()
import sqlite3
import csv
import os

output_dir = 'output'
os.makedirs(output_dir, exist_ok=True)

conn = sqlite3.connect(os.path.join(output_dir, 'transformed_orders.db'))
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS fact_orders_summary")

create_summary_sql = """
CREATE TABLE fact_orders_summary AS
SELECT 
    o.order_id,
    o.customer_id,
    c.customer_name,
    o.order_date,
    SUM(oi.quantity * oi.unit_price) AS total_order_value,
    COUNT(oi.product_id) AS total_items,
    MAX(o.order_status) AS order_status
FROM 
    orders o
JOIN 
    order_items oi ON o.order_id = oi.order_id
JOIN 
    customers c ON o.customer_id = c.customer_id
GROUP BY 
    o.order_id, o.customer_id, c.customer_name, o.order_date;
"""
cursor.execute(create_summary_sql)
conn.commit()

cursor.execute("SELECT * FROM fact_orders_summary")
rows = cursor.fetchall()
columns = [description[0] for description in cursor.description]

csv_path = os.path.join(output_dir, 'fact_orders_summary.csv')
with open(csv_path, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(columns)
    writer.writerows(rows)

print(f"✅ CSV file saved successfully to: {csv_path}")

conn.close()

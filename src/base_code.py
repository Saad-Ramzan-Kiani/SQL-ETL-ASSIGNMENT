import pandas as pd
import sqlite3
import os

base_path = r"F:\TenPlus AB Learning Program\Phase 0\task 2\SQL-ETL-ASSIGNMENT"
input_path = os.path.join(base_path, "input")
db_path = os.path.join(base_path, "output/etl_database.db")
output_path = os.path.join(base_path,"output")

customers_df = pd.read_csv(os.path.join(input_path, "stg_customers.csv"))
orders_df = pd.read_csv(os.path.join(input_path, "stg_orders.csv"))
order_items_df = pd.read_csv(os.path.join(input_path, "stg_order_items.csv"))

conn = sqlite3.connect(db_path)
cursor = conn.cursor()




conn.commit()
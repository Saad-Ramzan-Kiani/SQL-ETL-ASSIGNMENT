import pandas as pd
import numpy as np
import sqlite3
import os
from datetime import datetime

def make_folders():
    """Create the necessary folder structure"""
    os.makedirs("input", exist_ok=True)
    os.makedirs("output", exist_ok=True)
    os.makedirs("src", exist_ok=True)
    os.makedirs("raw", exist_ok=True)
    os.makedirs("silver", exist_ok=True)
    os.makedirs("gold", exist_ok=True)
    
    os.makedirs("raw/sql", exist_ok=True)
    os.makedirs("silver/sql", exist_ok=True)
    os.makedirs("gold/sql", exist_ok=True)

def read_csv(file_path):
    """Read CSV file into a DataFrame"""
    return pd.read_csv(file_path)

def clean_csv(df):
    """Clean the DataFrame by removing whitespace, nulls, and duplicates"""
    df.columns = [col.strip() for col in df.columns]
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    return df

def save_cleaned_csv(df, filename):
    """Save cleaned DataFrame to the raw folder"""
    path = os.path.join("raw", filename)
    df.to_csv(path, index=False)
    return path

def create_database():
    """Create SQLite database and return connection"""
    conn = sqlite3.connect('etl_pipeline.db')
    return conn

def create_raw_schema_tables(conn):
    """Create staging tables in the raw schema"""
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS raw_stg_orders (
        order_id INT,
        customer_id INT,
        order_date DATE,
        status TEXT
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS raw_stg_order_items (
        item_id INT,
        order_id INT,
        product_id INT,
        quantity INT,
        unit_price DECIMAL(10, 2)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS raw_stg_customers (
        customer_id INT,
        first_name TEXT,
        last_name TEXT,
        signup_date DATE
    )
    ''')
    
    conn.commit()
    
    with open("raw/sql/stg_orders.sql", "w") as f:
        f.write('''
CREATE TABLE stg_orders (
    order_id INT,
    customer_id INT,
    order_date DATE,
    status TEXT
);
        ''')
    
    with open("raw/sql/stg_order_items.sql", "w") as f:
        f.write('''
CREATE TABLE stg_order_items (
    item_id INT,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10, 2)
);
        ''')
    
    with open("raw/sql/stg_customers.sql", "w") as f:
        f.write('''
CREATE TABLE stg_customers (
    customer_id INT,
    first_name TEXT,
    last_name TEXT,
    signup_date DATE
);
        ''')

def load_csv_to_raw_tables(conn, input_files):
    """Load CSV data into raw tables using the specified input files"""

    customers_df = pd.read_csv(input_files['stg_customers'])
    order_items_df = pd.read_csv(input_files['stg_order_items'])
    orders_df = pd.read_csv(input_files['stg_orders'])
    
    customers_df = clean_csv(customers_df)
    order_items_df = clean_csv(order_items_df)
    orders_df = clean_csv(orders_df)
    
    customers_path = save_cleaned_csv(customers_df, 'stg_customers_cleaned.csv')
    order_items_path = save_cleaned_csv(order_items_df, 'stg_order_items_cleaned.csv')
    orders_path = save_cleaned_csv(orders_df, 'stg_orders_cleaned.csv')
    
    customers_df.to_sql('raw_stg_customers', conn, if_exists='replace', index=False)
    order_items_df.to_sql('raw_stg_order_items', conn, if_exists='replace', index=False)
    orders_df.to_sql('raw_stg_orders', conn, if_exists='replace', index=False)

    with open("raw/sql/load_stg_customers.sql", "w") as f:
        f.write(f'''
COPY stg_customers FROM '{input_files['stg_customers']}' WITH DELIMITER ',';
        ''')
    
    with open("raw/sql/load_stg_order_items.sql", "w") as f:
        f.write(f'''
COPY stg_order_items FROM '{input_files['stg_order_items']}' WITH DELIMITER ',';
        ''')
    
    with open("raw/sql/load_stg_orders.sql", "w") as f:
        f.write(f'''
COPY stg_orders FROM '{input_files['stg_orders']}' WITH DELIMITER ',';
        ''')
    
    return customers_path, order_items_path, orders_path

def create_silver_tables(conn):
    """Create transformed view in silver schema"""
    cursor = conn.cursor()
    
    transform_query = '''
    CREATE VIEW IF NOT EXISTS silver_transformed_orders AS
    SELECT 
        o.order_id,
        c.first_name || ' ' || c.last_name AS customer_name,
        o.order_date,
        SUM(oi.quantity) AS total_items,
        ROUND(SUM(oi.quantity * oi.unit_price), 2) AS total_amount,
        o.status
    FROM 
        raw_stg_orders o
    JOIN 
        raw_stg_customers c ON o.customer_id = c.customer_id
    JOIN 
        raw_stg_order_items oi ON o.order_id = oi.order_id
    WHERE 
        o.status = 'delivered'
    GROUP BY 
        o.order_id, customer_name, o.order_date, o.status
    '''
    
    cursor.execute(transform_query)
    conn.commit()
    
    with open("silver/sql/transformed_orders_view.sql", "w") as f:
        f.write('''
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
        ''')

def create_gold_tables(conn):
    """Create fact and dimension tables in gold schema"""
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS gold_fact_orders_summary AS
    SELECT * FROM silver_transformed_orders
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS gold_dim_customers AS
    SELECT 
        customer_id,
        first_name || ' ' || last_name AS customer_name,
        signup_date
    FROM 
        raw_stg_customers
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS gold_monthly_sales_summary AS
    SELECT 
        strftime('%Y-%m', order_date) AS month,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM 
        silver_transformed_orders
    GROUP BY 
        strftime('%Y-%m', order_date)
    ORDER BY 
        month
    ''')
    
    conn.commit()

    with open("gold/sql/fact_orders_summary.sql", "w") as f:
        f.write('''
CREATE TABLE gold.fact_orders_summary AS
SELECT * FROM silver.transformed_orders;
        ''')
    
    with open("gold/sql/dim_customers.sql", "w") as f:
        f.write('''
CREATE TABLE gold.dim_customers AS
SELECT 
    customer_id,
    first_name || ' ' || last_name AS customer_name,
    signup_date
FROM 
    raw.stg_customers;
        ''')
    
    with open("gold/sql/monthly_sales_summary.sql", "w") as f:
        f.write('''
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
        ''')

def export_tables_to_csv(conn):
    """Export final tables to CSV files in the output folder"""

    transformed_orders_df = pd.read_sql_query("SELECT * FROM silver_transformed_orders", conn)
    transformed_orders_df.to_csv('output/transformed_orders.csv', index=False)
    
    fact_orders_df = pd.read_sql_query("SELECT * FROM gold_fact_orders_summary", conn)
    fact_orders_df.to_csv('output/fact_orders_summary.csv', index=False)
    
    dim_customers_df = pd.read_sql_query("SELECT * FROM gold_dim_customers", conn)
    dim_customers_df.to_csv('output/dim_customers.csv', index=False)
    
    monthly_sales_df = pd.read_sql_query("SELECT * FROM gold_monthly_sales_summary", conn)
    monthly_sales_df.to_csv('output/monthly_sales_summary.csv', index=False)

def run_etl_pipeline():
    """Execute the complete ETL pipeline"""

    input_files = {
        'stg_customers': 'input/stg_customers.csv',
        'stg_order_items': 'input/stg_order_items.csv',
        'stg_orders': 'input/stg_orders.csv'
    }
    
    make_folders()
    
    for file_path in input_files.values():
        if not os.path.exists(file_path):
            print(f"Warning: Input file {file_path} does not exist. Please ensure all input files are available.")
            return
    
    conn = create_database()
    
    create_raw_schema_tables(conn)
    
    customers_path, order_items_path, orders_path = load_csv_to_raw_tables(conn, input_files)
    
    create_silver_tables(conn)
    
    create_gold_tables(conn)
    
    export_tables_to_csv(conn)
    
    conn.close()
    
    print("ETL pipeline completed successfully!")
    print("Check the 'output' folder for the results.")

if __name__ == "__main__":
    run_etl_pipeline()
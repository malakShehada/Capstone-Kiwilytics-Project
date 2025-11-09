from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import matplotlib.pyplot as plt

PG_CONN_ID = 'postgres_conn'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1)
}

CREATE_SALES_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS sales (
        CustomerID TEXT PRIMARY KEY,
        CustomerName TEXT,
        total_orders INTEGER,
        total_sales NUMERIC
    )
"""

SELECT_AGG_QUERY = """
    SELECT 
        c.CustomerID,
        c.CustomerName,
        COUNT(DISTINCT o.OrderID) AS total_orders,
        SUM(od.Quantity * p.Price) AS total_sales
    FROM customers c
    JOIN orders o ON c.CustomerID = o.CustomerID
    JOIN order_details od ON o.OrderID = od.OrderID
    JOIN products p ON od.ProductID = p.ProductID
    GROUP BY c.CustomerID, c.CustomerName
"""

MERGE_SALES_SQL = f"""
    INSERT INTO sales (CustomerID, CustomerName, total_orders, total_sales)
    {SELECT_AGG_QUERY}
    ON CONFLICT (CustomerID) DO UPDATE SET
        CustomerName = EXCLUDED.CustomerName,
        total_orders = EXCLUDED.total_orders,
        total_sales = EXCLUDED.total_sales
"""

def get_conn():
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    return hook.get_conn()

def create_sales_table():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(CREATE_SALES_TABLE_SQL)
    conn.commit()
    cursor.close()
    conn.close()

def merge_sales_data():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(MERGE_SALES_SQL)
    conn.commit()
    cursor.close()
    conn.close()

def extract_and_plot():
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    sql = """
    SELECT o.orderdate, SUM(s.total_sales) AS daily_revenue
    FROM sales s
    JOIN orders o ON s.CustomerID = o.CustomerID::text
    GROUP BY o.orderdate
    ORDER BY o.orderdate
    """
    df = pg_hook.get_pandas_df(sql)
    df['orderdate'] = pd.to_datetime(df['orderdate'])
    plt.figure(figsize=(10,5))
    plt.plot(df['orderdate'], df['daily_revenue'], marker='o')
    plt.title('Daily Revenue')
    plt.xlabel('Date')
    plt.ylabel('Revenue')
    plt.grid(True)
    plt.savefig('/tmp/daily_revenue_plot.png')
    plt.close()
    print("Revenue plot saved to /tmp/daily_revenue_plot.png")

def get_revenue_on_date():
    target_date = '1996-08-08'
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    sql = f"""
    SELECT 
        o.orderdate,
        SUM(od.quantity * p.price) AS daily_revenue
    FROM orders o
    JOIN order_details od ON o.orderid = od.orderid
    JOIN products p ON od.productid = p.productid
    WHERE o.orderdate = '{target_date}'
    GROUP BY o.orderdate;
    """
    df = pg_hook.get_pandas_df(sql)
    revenue = df['daily_revenue'][0] if not df.empty else None
    print(f"Total revenue on {target_date}: {revenue}")


with DAG('capstone_pipeline', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    t1 = PythonOperator(
        task_id='create_sales_table',
        python_callable=create_sales_table
    )
    t2 = PythonOperator(
        task_id='merge_sales_data',
        python_callable=merge_sales_data
    )
    t3 = PythonOperator(
        task_id='extract_and_plot',
        python_callable=extract_and_plot
    )
    t4 = PythonOperator(
        task_id='get_revenue_on_date',
        python_callable=get_revenue_on_date
    )

    t1 >> t2 >> t3 >> t4

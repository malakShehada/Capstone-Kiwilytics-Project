# 🚀 Capstone Project – Airflow Data Pipeline

## 📘 Overview
This capstone project represents the final stage of the **Kiwilytics Data Engineering course**, integrating multiple tools and technologies to build a real-world **data pipeline** using **Apache Airflow**.

The goal of this pipeline is to automate the process of aggregating sales data, updating a PostgreSQL data warehouse, and generating insights such as total orders, total revenue per customer, and daily revenue trends.

---

## 🧩 Key Features

- **ETL Pipeline Orchestration:** Managed using **Apache Airflow DAG**.  
- **PostgreSQL Integration:** Reads and writes data to a PostgreSQL database via `PostgresHook`.  
- **Data Aggregation:** Computes total orders and total sales per customer.  
- **Data Visualization:** Automatically generates a **daily revenue trend plot** using Matplotlib.  
- **Incremental Updates:** Uses SQL `ON CONFLICT` logic to upsert customer sales data.  
- **Automated Execution:** Scheduled to run **daily** using Airflow’s scheduler.

---

### 🧠 Task Descriptions

| Task ID | Description |
|----------|-------------|
| `create_sales_table` | Creates the `sales` table in PostgreSQL if it doesn’t exist |
| `merge_sales_data` | Aggregates customer order data and updates the `sales` table |
| `extract_and_plot` | Extracts daily revenue data and generates a revenue trend plot |
| `get_revenue_on_date` | Retrieves total revenue for a specific date |

---

## 🗃️ Data Flow

1. **Extract** customer, order, and product data from PostgreSQL.  
2. **Transform** data by calculating `total_orders` and `total_sales`.  
3. **Load** results back into the `sales` table using UPSERT logic.  
4. **Visualize** the daily revenue over time with Matplotlib.

---

## 🧰 Tools & Technologies Used

- **Apache Airflow** – Workflow orchestration and scheduling  
- **PostgreSQL** – Data warehouse for storing aggregated results  
- **Python** – Core programming for DAG logic  
- **Pandas** – Data manipulation and transformation  
- **Matplotlib** – Data visualization  
- **SQL** – Data extraction and transformation queries  

---

## 🧪 How to Run

1. Make sure you have **Airflow** and **PostgreSQL** installed and running.  
2. Define a PostgreSQL connection in Airflow named:
postgres_conn
3. Place the `capstone-pipeline.py` file inside your Airflow DAGs directory:
~/airflow/dags/
4. Start the Airflow scheduler and webserver:
```bash
airflow scheduler
airflow webserver
```
5. Open the Airflow UI (usually at http://localhost:8080) and trigger the DAG named:
capstone_pipeline

## 📈 Output

- A PostgreSQL table named sales containing aggregated customer data.
- A revenue trend plot saved locally at:
/tmp/daily_revenue_plot.png
- Console logs showing the total revenue on the specified target date.

## 📂 Files in This Repository

| File Name | Description |
|------------|-------------|
| `capstone-pipeline.py` | Airflow DAG containing all ETL and analysis logic |
| `README.md` | Project documentation and technical overview |

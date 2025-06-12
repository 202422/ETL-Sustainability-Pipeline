# %% [markdown]
# # Packages Import


# %%
from airflow import DAG
from airflow.operators.python import PythonOperator

# %%
import utils

# %% [markdown]
# # DAG Design

# %%
# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# %%
# Define the DAG
with DAG(
    "sustainability_data_pipeline",
    default_args=default_args,
    description='Sustainability Data Pipeline',
    schedule_interval='@daily',  # Runs daily
    start_date=datetime(2025, 6, 1),
    catchup=False,
) as dag:

    # 1. Extract Task
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=utils.Extract_main
    )

    # 2. Clean Task
    clean_task = PythonOperator(
        task_id='clean',
        python_callable=utils.Cleaning_main
    )

    # 3. Transform Task
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=utils.transform_main
    )

    # 4. Load Task
    load_task = PythonOperator(
        task_id='load',
        python_callable=utils.Load_main
    )

    # Set task dependencies (ETL flow)
    extract_task >> clean_task >> transform_task >> load_task



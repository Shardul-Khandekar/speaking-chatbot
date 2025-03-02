from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
import subprocess


root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_dir)


data_acquisition_script = os.path.join(root_dir, "scripts", "data_acquisition.py")
data_preprocessing_script = os.path.join(root_dir, "scripts", "data_preprocessing.py")
test_folder = os.path.join(root_dir, "tests")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.today() - timedelta(days=1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG(
    dag_id="data_pipeline",
    default_args=default_args,
    description="An Airflow DAG to orchestrate data acquisition, transformation, and testing",
    schedule_interval="0 * * * *",
    catchup=False,
)


# Define task functions
def run_script(script_path):
    subprocess.run(["python", script_path], check=True)


def run_tests():
    subprocess.run(["pytest", test_folder], check=True)


download_data = PythonOperator(
    task_id = "download_data",
    python_callable = run_script,
    op_kwargs ={"script_path": data_acquisition_script},
    dag = dag,
)


preprocess_data = PythonOperator(
    task_id = "clean_data",
    python_callable = run_script,
    op_kwargs = {"script_path": data_preprocessing_script},
    dag = dag,
)


run_tests_task = PythonOperator(
    task_id = "run_tests",
    python_callable = run_tests,
    dag = dag,
)


download_data >> preprocess_data >> run_tests_task
#import the libraries

from datetime import timedelta
#DAG object
from airflow.models import DAG
#Operators
from airflow.operators.bash_operator import BashOperator
#Scheduling
from airflow.utils.dates import days_ago

#Defining DAG arguments

#Define default arguments for the DAG configuration
default_args = {
    'owner': 'Bob',
    'start_date': days_ago(0),
    'email': ['bob@gmail.com'],
    'email_on_failure': True,
    'email_on_retries': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#Define DAG

dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description = 'Apache Airflow Final Assignment',
    schedule_interval  = timedelta(days=1),
)

#define tasks

#first task

unzip_data = BashOperator(
    task_id = 'unzip_data',
    bash_command = 'tar -xvzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment',
    dag=dag,
)

#second task

extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command = "cut -d ',' -f1-4 /home/project/airflow/dags/vehicle-data.csv > /home/project/airflow/dags/csv_data.csv",
    dag=dag,
)

#third task

extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command = "cut -d '-' -f4-6 /home/project/airflow/dags/tollplaza-data.tsv > /home/project/airflow/dags/tsv_data.csv",
    dag=dag,
)

#fourth task

extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command = "awk '{print $(NF-1) \",\" $NF}' /home/project/airflow/dags/payment-data.txt > /home/project/airflow/dags/fixed_width_data.csv",
    dag=dag,
)

#fifth task

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command="""
    paste -d ',' /home/project/airflow/dags/csv_data.csv \
                  /home/project/airflow/dags/tsv_data.csv \
                  /home/project/airflow/dags/fixed_width_data.csv \
    > /home/project/airflow/dags/extracted_data.csv
    """,
    dag=dag,
)

#sixth task

transform = BashOperator(
    task_id='transform_data',
    bash_command="""
    awk -F',' 'BEGIN {OFS=","} {if (NR > 1) $4 = toupper($4); print}' /home/project/airflow/dags/extracted_data.csv \
    > /home/project/airflow/dags/transformed_data.csv
    """,
    dag=dag,
)


#seventh task

#unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform

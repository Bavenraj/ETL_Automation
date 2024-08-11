from datetime import date, datetime
import pymysql
import pymssql
from sqlalchemy import create_engine
from airflow.utils.task_group import TaskGroup
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
import pandas as pd
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.utils.state import State 
from airflow.models import 
from airflow.models import Variable

#[source, destination, column name] 
table_list = [
    ['skyline_recon_data', 'hist_skyline_records', 'REPORTING_DATE'], 
    ['commerce_recon_data', 'hist_commerce_records', 'Reporting_Date'],
    ['inbound_trades_recon', 'hist_inbound_trades', 'Reporting_Date'],
    ['luggage_insure_data', 'hist_luggage_insure', 'Reporting_Date'],
    ['oasis_air_recon_2021', 'hist_oasis_air_2021', 'Reporting Date']
]

def previous_month():
    if date.today().month == 1:
        prev_month = 12
        prev_year = date.today().year - 1
    else:
        prev_month = date.today().month - 1
        prev_year = date.today().year
    return prev_year, prev_month   
print(previous_month()[1])

def dag_status_check(**kwargs):
    dag_id = kwargs['dag'].dag_id
    dag_runs = DagRun.find(dag_id=dag_id)
    
    completed_dag_runs = []
    for run in dag_runs:
        if run.state != State.RUNNING:
            completed_dag_runs.append(run)

    latest_completed_dag_run = max(completed_dag_runs, key=lambda run: run.execution_date)
    print(latest_completed_dag_run.state)
    print(f"{run.execution_date.month}")

    if latest_completed_dag_run.state == State.SUCCESS:
        if latest_completed_dag_run.execution_date.year == date.today().year:
            if latest_completed_dag_run.execution_date.month == date.today().month:
                raise AirflowSkipException(f"The dag has run for this month")


def data_check(**kwargs):
    print("Checking Data")
    task_instance = kwargs['ti']
    d={}

    for x, y, z in table_list:
        key = f"TRAVEL_2_Stage0_Task_State_{x.lower()}"
        task_id = f"TRAVEL_2_Monthly_Stage0_TaskGroup.TRAVEL_2_Stage0_Task_{x.lower()}"
        status = task_instance.xcom_pull(key=key, task_ids = task_id , dag_id = "TRAVEL_2_MONTHLY_STAGE_0", include_prior_dates = True)
        print(f"Key is {key} and Task id {task_id} / status is {status}")
        if key not in d:
            d[key] = status
    print(d)
    successCount=0
    for value in d.values():
        if value == "success":
            successCount = successCount + 1

    if successCount == len(d):
        task_instance.xcom_push(key="status", value = "success")
    else:
        task_instance.xcom_push(key="status", value = "failed")
        raise AirflowFailException(f"Not all task is success")

def Update_Table(**kwargs):

    print("Connecting to MSSQL")
    mssql_conn = pymssql.connect(kwargs['mssql_host'], kwargs['mssql_username'], kwargs['mssql_password'],  'TPG_RECONCILED_DATABASE' )
    print(f"Extracting data from {kwargs['source_table']} ")
    mssql_query = f"""SELECT * FROM {kwargs['source_table']} where Month("{kwargs['Reporting_Date']}") = '{previous_month()[1]}' and year("{kwargs['Reporting_Date']}") = '{previous_month()[0]}' """
    mssql_data = pd.read_sql(mssql_query, mssql_conn)
    print(f"Extracted data {mssql_data.info()}")

    print("Connecting to MySQL")
    mysql_conn = pymysql.connect(host= kwargs['mysql_host'] ,user= kwargs['mysql_username'] , password=kwargs['mysql_password'] ,  database='data_warehouse_stg')
    mysql_query = f"""SELECT * FROM {kwargs['destination_table']} where Month("{kwargs['Reporting_Date']}") = '{previous_month()[1]}' and year("{kwargs['Reporting_Date']}") = '{previous_month()[0]}' """
    mysql_data = pd.read_sql(mysql_query, mysql_conn)
    print(f"Data inside MYSQL {mysql_data.head()}")
    mysql_conn.close()

    print("Connecting to MYSQL")
    engine = create_engine(f"mysql+mysqlconnector://{kwargs['mysql_username']}:{kwargs['mysql_password']}@ {kwargs['mysql_host']}:3306/data_warehouse_stg")
    print(f"Loading data into {kwargs['destination_table']} ")
    mssql_data.to_sql(name=kwargs['destination_table'], con= engine, if_exists="append", index = False)


default_args = {
    'owner': 'Bavenraj',
    'start_date': datetime(2024, 7, 15),
    'retries': 0,
}

with DAG(
    dag_id = "Etl_automation_2",
    default_args = default_args,
    schedule_interval = Variable.get("Hist_Table_Update_2"),
    catchup= False,
    tags = ['UPDATE_HIST_TRAVEL_2', 'MONTHLY_Update_TRAVEL_2']
) as dag:
    

    start_task = EmptyOperator(
        task_id='start_task'
    )

    end_task = EmptyOperator(
        task_id='end_task'
    )

    check_dag_status = PythonOperator(
            task_id=f"Checking_Dag_Status",
            provide_context=True,
            python_callable= dag_status_check,
    )

    with TaskGroup(group_id = "Table_Delete") as Table_Delete:
        for source, destination, column_Name in table_list:
            delete_data = MySqlOperator(
                task_id = f"Delete_{destination}",
                mysql_conn_id = "mysql_RDS_data_warehouse", 
                sql = f"DELETE FROM `data_warehouse_stg`.`{destination}` where Month(`{column_Name}`) = {previous_month()[1]} and year(`{column_Name}`) = {previous_month()[0]} ",
    )
            
    check_data = PythonOperator(
                task_id=f"Checking_Data",
                provide_context=True,
                python_callable= data_check,
            )
    #l=[]
    with TaskGroup(group_id = "Table_Update") as Table_Update:
        for source, destination, column_Name in table_list:
            task = PythonOperator(
                task_id=f"Update_{destination}",
                provide_context=True,
                python_callable= Update_Table,
                op_kwargs= {
                    'source_table': source,
                    'mssql_host': "{{ conn.mssql_conn_db202.host }}" ,
                    'mssql_username': "{{ conn.mssql_conn_db202.login }}",
                    'mssql_password': "{{ conn.mssql_conn_db202.password }}",
                    'destination_table': destination,
                    'mysql_host': "{{ conn.mysql_RDS_data_warehouse.host }}",
                    'mysql_username': "{{ conn.mysql_RDS_data_warehouse.login }}",
                    'mysql_password': "{{ conn.mysql_RDS_data_warehouse.password }}",
                    'Reporting_Date' : column_Name,
                }
            )
            #l.append(task)
    #chain(*l)

    start_task >> check_dag_status >> check_data >> Table_Delete >> Table_Update >> end_task
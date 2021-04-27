from datetime import timedelta
import pandas
import sqlite3, csv

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# These args will get passed on to each operator
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['obentame@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the table dim_currency with all available currencies in the file
def dim_cur():
    table = pandas.read_csv('http://webstat.banque-france.fr/fr/downloadFile.do?id=5385698&exportType=csv', delimiter= ';')
    cur_history = table.drop([0,1,2,3,4])
    # Calculate cur_history with the history of the currencies and their values in euro
    cur_history = cur_history.melt(id_vars=['Titre :'], var_name= 'cur_code', value_name= 'one_euro_value')
    cur_history = cur_history.rename(columns = {'Titre :': 'cur_date'})
    cur_history = cur_history[(cur_history.one_euro_value != '-') & (cur_history.one_euro_value.notnull())]
    cur_history['cur_date']= pandas.to_datetime(cur_history['cur_date'], format='%d/%m/%Y')
    # Calculate the last update date and value of each currency
    last_update = cur_history[['cur_code', 'cur_date']]
    last_update = last_update.groupby('cur_code').max()
    last_update_currency = pandas.merge(cur_history, last_update, 'inner', ['cur_code', 'cur_date'])
    last_update_currency = last_update_currency.rename(columns = {'cur_date': 'last_updated_date'})
    # Calculate ser_code with the currencies and their serial codes
    ser_code = pandas.DataFrame(table.iloc[0:1,1:])
    ser_code = ser_code.melt(var_name= 'cur_code', value_name= 'Serial_code')
    # merge ser_code and last_update_currency
    dim_currency = pandas.merge(ser_code, last_update_currency, 'inner', ['cur_code'])
    dim_currency['cur_code'] = dim_currency.Serial_code.str[6:9]
    dim_currency.to_csv("data/dim_currency.csv")
    
# Load the csv into sqlite   
def sqlLoad():
    con = "sqlite3".connect("sqlite:////home/obentame/airflow/airflow.db") 
    cur = con.cursor()
    cur.execute("CREATE TABLE t (cur_code, Serial_code, last_updated_date, one_euro_value);") # use your column names here
    
    with open('data/dim_currency.csv','r') as fin:
        dr = csv.DictReader(fin) # comma is default delimiter
        to_db = [(i['cur_code'], i['Serial_code'], i['last_updated_date'], i['one_euro_value']) for i in dr]
    
    cur.executemany("INSERT INTO t (cur_code, Serial_code, last_updated_date, one_euro_value) VALUES (?, ?, ?, ?);", to_db)
    con.commit()
    con.close()
    
with DAG(
    'dag1',
    default_args=default_args,
    description='tst_dag',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
) as dag:
    
    dim_cur = PythonOperator(
        task_id="dim_currency",
        python_callable=dim_cur
    )
    
    sqlLoad = PythonOperator(
        task_id="sql_load",
        python_callable=sqlLoad
    )


dim_cur >> sqlLoad





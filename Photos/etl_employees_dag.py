import time
from datetime import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.mysql.hooks.mysql import MySqlHook
#from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.hooks.base import BaseHook
import pandas as pd
from sqlalchemy import create_engine


# extract tasks from mysql
@task()
def get_src_table():
    hook = MySqlHook(mysql_conn_id="Mysql_server_wsl",schema="employees")
    tables = ['departments', 'dept_emp', 'dept_manager', 'employees', 'salaries', 'titles']
    
    tbl_dict = {}
    
    for table in tables:
        sql = f"SELECT * FROM {table};"
        df = hook.get_pandas_df(sql)
        tbl_dict[table] = df.to_dict('dict')
    return tbl_dict


# load to postgress
@task()
def load_src_data(tbl_dict: dict):
    conn = BaseHook.get_connection('postgresql_server_wsl')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    all_tbl_name = []
    start_time = time.time()
    
    for k, v in tbl_dict.items():
        # Use the key 'k' as the table name or any other suitable string
        table_name = f'src_{k}'
        all_tbl_name.append(table_name)
        rows_imported = 0
        df = pd.DataFrame.from_dict(v)
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {table_name}')
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        rows_imported += len(df)
        print(f'Done. {str(round(time.time() - start_time, 2))} total seconds elapsed')
    
    print("Data imported successfully")
    return all_tbl_name

# Transformation Tasks
## adding a new departements
@task()
def transform_src_departements():
    conn = BaseHook.get_connection('postgresql_server_wsl')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    ddf = pd.read_sql_query('SELECT * FROM public."src_departments"', engine)

    ddf['dept_name'].replace({'Development': 'Information Technology'}, inplace=True)
    ddf.loc[10] = {'dept_no': 'd010', 'dept_name': 'Data Engineering'}
    ddf.to_sql(f'src_departments', engine, if_exists='replace', index=False)
    return {"table(s) processed ": "Data imported successful"}

## adding a new employee info
@task()
def transform_src_dept_emp():
    conn = BaseHook.get_connection('postgresql_server_wsl')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    deddf = pd.read_sql_query('SELECT * FROM public."src_dept_emp"', engine)

    deddf.loc[len(deddf)] = {'emp_no': 500000, 'dept_no': 'd010', 'from_date': '2024-04-17'}
    deddf.to_sql(f'src_dept_emp', engine, if_exists='replace', index=False)
    return {"table(s) processed ": "Data imported successful"}

@task()
def transform_src_dept_manager():
    conn = BaseHook.get_connection('postgresql_server_wsl')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    dmdf = pd.read_sql_query('SELECT * FROM public."src_dept_manager"', engine)

    dmdf.loc[len(dmdf)] = {'emp_no': 500000, 'dept_no': 'd010', 'from_date': '2024-04-17'}
    dmdf.to_sql(f'src_dept_manager', engine, if_exists='replace', index=False)
    return {"table(s) processed ": "Data imported successful"}



@task()
def transform_src_employees():
    conn = BaseHook.get_connection('postgresql_server_wsl')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    dedf = pd.read_sql_query('SELECT * FROM public."src_employees"', engine)

    dedf.loc[len(dedf)] = {'emp_no': 500000, 'birth_date': '1980-22-05', 'first_name': 'Sakata', 'last_name': 'Gintoki', 'gender':'M', 'hire_date':'2024-04-17'}
    dedf.to_sql(f'src_employees', engine, if_exists='replace', index=False)
    return {"table(s) processed ": "Data imported successful"}


@task()
def transform_src_salaries():
    conn = BaseHook.get_connection('postgresql_server_wsl')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    dsdf = pd.read_sql_query('SELECT * FROM public."src_salaries"', engine)

    dsdf.loc[len(dsdf)] = {'emp_no': 500000, 'salary': '70000', 'from_date': '2024-04-17'}
    dsdf.to_sql(f'src_salaries', engine, if_exists='replace', index=False)
    return {"table(s) processed ": "Data imported successful"}


@task()
def transform_src_titles():
    conn = BaseHook.get_connection('postgresql_server_wsl')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    dtdf = pd.read_sql_query('SELECT * FROM public."src_titles"', engine)

    dtdf.loc[len(dtdf)] = {'emp_no': 500000, 'title': 'Data Engineer', 'from_date': '2024-04-17'}
    dtdf.to_sql(f'src_titles', engine, if_exists='replace', index=False)
    return {"table(s) processed ": "Data imported successful"}


#load
@task()
def prd_employees_model():
    conn = BaseHook.get_connection('postgresql_server_wsl')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    ddf = pd.read_sql_query('SELECT * FROM public."src_departments" ', engine)
    deddf = pd.read_sql_query('SELECT * FROM public."src_dept_emp" ', engine)
    dmdf = pd.read_sql_query('SELECT * FROM public."src_dept_manager" ', engine)
    dedf = pd.read_sql_query('SELECT * FROM public."src_employees" ', engine)
    dsdf = pd.read_sql_query('SELECT * FROM public."src_salaries" ', engine)
    dtdf = pd.read_sql_query('SELECT * FROM public."src_titles" ', engine)
    # Rename columns in each DataFrame before the merge

    ddf.rename(columns={'to_date': 'to_date_ddf', 'from_date': 'from_date_ddf'}, inplace=True)
    deddf.rename(columns={'to_date': 'to_date_deddf', 'from_date': 'from_date_deddf'}, inplace=True)
    dmdf.rename(columns={'to_date': 'to_date_dmdf', 'from_date': 'from_date_dmdf'}, inplace=True)

    # merging all into a new table
    merged = ddf.merge(deddf, on='dept_no').merge(dmdf, on='emp_no').merge(dedf, on='emp_no').merge(dsdf, on='emp_no').merge(dtdf, on='emp_no')

    # Droping repeated columns
    merged.drop(columns = ['dept_no_y'], inplace = True)

    # renaming columns
    merged = merged.rename(columns={'dept_no_x':'dept_no','from_date_deddf':'from_date_dept_emp','to_date_deddf':'to_date_dept_emp',
                           'from_date_dmdf':'from_date_dept_manager','to_date_dmdf':'to_date_dept_manager',
                           'from_date_x':'from_date_salary','to_date_x':'to_date_salary','from_date_y':'from_date_title',
                           'to_date_y':'to_date_title'}) 

    merged.to_sql(f'revised_employees', engine, if_exists='replace', index=False)
    return {"table(s) processed ": "Data imported successful"}


# [how_to_task_group]
with DAG ( dag_id="employees_etl_dag",  start_date = datetime(2024,8,1),schedule_interval="@once", catchup = False) as dag:
    with TaskGroup("extract_employees_load", tooltip="Extract and load source data") as extract_load_src:
        src_table_employees = get_src_table()
        load_table_employees = load_src_data(src_table_employees)
        #define order
        src_table_employees >> load_table_employees

    # [START howto_task_group_section_2]
    with TaskGroup("transform_employees_tables", tooltip="Transform and stage data") as transform_employees_tables:
        transform_src_departements = transform_src_departements()
        transform_src_dept_emp = transform_src_dept_emp()
        transform_src_dept_manager = transform_src_dept_manager()
        transform_src_employees = transform_src_employees()
        transform_src_salaries  = transform_src_salaries()
        transform_src_titles = transform_src_titles()
        #define task order
        [transform_src_departements, transform_src_dept_emp, transform_src_dept_manager, transform_src_employees, transform_src_salaries, transform_src_titles]

    with TaskGroup("load_employees_model", tooltip="Final Product model") as load_employees_model:
        prd_employees_model = prd_employees_model()
        #define order
        prd_employees_model

extract_load_src >> transform_employees_tables >> load_employees_model

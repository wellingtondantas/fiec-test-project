"""
### ETC Fiec Antaq
This ETL DAG is demonstrating an Extract -> Transform -> Load pipeline
"""

import json
from textwrap import dedent
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
   'owner': 'fiec antaq',
   'email': ['EmailtoReceiveInformation@gmail.com'],
   'start_date': pendulum.datetime(2021, 1, 1, tz="UTC"),
   'retries': 0,
   'depends_on_past': True,
   'email_on_failure': True,
   'email_on_retry': True
   }


with DAG(
    'fiec_antaq2',
    description='Email monitoring ETL',
    default_args = default_args
    
) as dag:


    """Don't finished"""
    def email(contextDict, **kwargs):
        """Send custom email alerts."""
        title = "Airflow alert: {task_name} Failed".format(**contextDict)
        body = """
        Hi, <br>
        <br>
        I like to notify you that your {task_name} job completed successfully.<br>
        <br>Thank you!,<br>
        Airflow Alerting System <br>""".format(**contextDict)
        send_email('EmailtoReceiveInformation@gmail.com', title, body)


    def download(**kwargs):
        ti = kwargs['ti']
        data_string = '{"ANTAQ": "FIEC"}'
        ti.xcom_push('order_data', data_string)
        
        
    def tb_atracacao_fato(**kwargs):
        ti = kwargs['ti']
        data_string = '{"ANTAQ": "FIEC"}'
        ti.xcom_push('order_data', data_string)
        
    def tb_carga_fato(**kwargs):
        ti = kwargs['ti']
        data_string = '{"ANTAQ": "FIEC"}'
        ti.xcom_push('order_data', data_string)
    
    
    t01 = BashOperator(
        task_id= 'alert1',
        bash_command='"echo download finished"'
    )

    t02 = BashOperator(
        task_id= 'alert2',
        bash_command='"echo tb_atracacao_fato finished"'
    )

    t03 = BashOperator(
        task_id= 'alert3',
        bash_command='"echo tb_carga_fato finished"'
    )
    
    
    download = PythonOperator(task_id='download', python_callable = download)

    tb_atracacao_fato = PythonOperator(task_id='tb_atracacao_fato', python_callable = tb_atracacao_fato)

    tb_carga_fato = PythonOperator(task_id='tb_carga_fato', python_callable = tb_carga_fato)
    
    
    
    download >> t01 >> tb_atracacao_fato >> t02 >> tb_carga_fato >> t03
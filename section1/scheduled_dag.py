from datetime import datetime, timedelta
from airflow.decorators import dag, task
import os,sys;
from common.ETLRow import Pipeline1   # Add module to the dags, common folder for pythondag.

@dag(
    schedule_interval="5 1 * * *",
    start_date=datetime.today(),
    dagrun_timeout=timedelta(minutes=60),
)

def section1():
    @task
    def process_df_in_folder():
        src_folder = '/path-to-data'
        destination_folder = '/path-to-destination'
        dump_folder = '/path-to-dump'

        myjob = Pipeline1(
            src_folder=src_folder,
            destination_folder=destination_folder,
            dump_folder=dump_folder
            )
            
        myjob.main()

    process_df_in_folder()

dag = section1()
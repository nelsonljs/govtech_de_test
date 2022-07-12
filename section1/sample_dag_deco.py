from datetime import datetime, timedelta
from airflow.decorators import dag, task
import logging
import os, shutil
import pandas as pd

def process_row(myrow):
    '''
    map function for processing each row of data.
    '''
    error_msg=''
    honorifics = ['Mr.','Mrs.', 'Miss', 'Ms.', 'Dr.', 'DVM', 'DDS', 'PhD', 'MD']

    myname = myrow['name']
    for title in honorifics:
        myname = myname.replace(title,'').strip()

    split_name = myname.split(' ')
    if not split_name:
        error_msg
        return '', '', '', '', True    
    elif len(split_name) == 1:
        logging.error('Only single name is available.')
        first_name = split_name[0]
        last_name = ''
    else:
        first_name, *middle_names, last_name = split_name
        if middle_names:
            error_msg = 'check middle names'

    try:
        myprice = float(myrow['price'])
    except ValueError:
        error_msg = 'bad price'

    return first_name, last_name, myprice, myprice > 100, error_msg

@dag(
    schedule_interval="5 1 * * *",
    start_date=datetime.today(),
    dagrun_timeout=timedelta(minutes=60),
)

def section1():
    @task
    def process_df_in_folder():
        folder = '/path-to-data'
        consolidated_dfs = []
        error_dfs = []

        for myfile in os.listdir(folder):
            if not myfile.endswith('.csv'):
                continue
        
            mydf = pd.read_csv(os.path.join(folder, myfile))
            mydf['firstname'], mydf['lastname'], mydf['price'], mydf['above_100'], mydf['error_msg'] = zip(*mydf.apply(process_row, axis=1))

            ## For downstream processing, split df into good and poor rows.
            consolidated_dfs.append(
                mydf[mydf['error_msg']==''].\
                    loc[:, mydf.columns !='error_msg']
            )

            error_dfs.append(
                mydf[mydf['error_msg']!=''].\
                    to_csv('to_check.csv')
            )

            destination_folder = 'path-to-destination'
            pd.concat(consolidated_dfs).to_csv(destination_folder, 'consolidated.csv')
            pd.concat(error_dfs).to_csv(destination_folder, 'errors.csv')

        ## Move processed file.
        processed_folder = 'path-to-dump'
        shutil.move(os.path.join(folder, myfile), os.path.join(processed_folder, myfile))

    process_df_in_folder()

dag = section1()
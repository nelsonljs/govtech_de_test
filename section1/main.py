import pandas as pd
import os, shutil
import logging

src_folder = 'data'
destination_folder = 'final'
dump_folder = 'dump'

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
        error_msg = 'no name'
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
        is_above_100 = myprice > 100
    except ValueError:
        myprice = myrow['price']
        is_above_100 = 'error'
        error_msg = 'bad price'

    return first_name, last_name, myprice, is_above_100, error_msg

def main():
    consolidated_dfs = []
    error_dfs = []

    for myfile in os.listdir(src_folder):
        if not myfile.endswith('.csv'):
            continue
    
        mydf = pd.read_csv(os.path.join(src_folder, myfile)).fillna('')
        mydf['firstname'], mydf['lastname'], mydf['price'], mydf['above_100'], mydf['error_msg'] = zip(*mydf.apply(process_row, axis=1))

        ## For downstream processing, split df into good and poor rows.
        consolidated_dfs.append(
            mydf[mydf['error_msg']==''].\
                loc[:, mydf.columns !='error_msg']
        )

        error_dfs.append(
            mydf[mydf['error_msg']!='']
        )

        pd.concat(consolidated_dfs).to_csv(os.path.join(destination_folder, 'consolidated.csv'))
        pd.concat(error_dfs).to_csv(os.path.join(destination_folder, 'errors.csv'))

        ## Move processed file.
        shutil.move(os.path.join(src_folder, myfile), os.path.join(dump_folder, myfile))

if __name__=="__main__":

    main()


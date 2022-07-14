import pandas as pd
import os, shutil
import logging
from datetime import datetime

class Pipeline1:
    '''
    Wrap up Pipeline into a module for portability. Can be extended if not working with local file directories.
    '''
    src_folder = 'data'
    destination_folder = 'final'
    dump_folder = 'dump'

    def __init__(self, src_folder=None, destination_folder=None, dump_folder=None):
        if src_folder:
            self.src_folder = src_folder
        if src_folder:
            self.destination_folder = destination_folder
        if src_folder:
            self.dump_folder = dump_folder
        pass
    
    @staticmethod
    def _process_row(myrow):
        '''
        map function for processing each row of data.
        '''
        error_msg=''
        honorifics = ['Mr.','Mrs.', 'Miss', 'Ms.', 'Dr.', 'DVM', 'DDS', 'PhD', 'MD']

        myname = myrow['name']
        for title in honorifics:
            myname = myname.replace(title,'').strip()

        ## Handling Jr., Sr. and Firstname suffixes, assume only 1 can be present.

        name_suffixes = ['Jr.', 'Sr.', 'IV', 'III', 'II', 'I'] # Order of III, II, I matters.
        for suffix in name_suffixes:
            if myname.endswith(suffix):
                mysuffix=suffix
                myname = myname.replace(suffix,'').strip()
                break
            else:
                mysuffix = False

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

        if mysuffix:
            first_name = f'{first_name} {mysuffix}'
        return first_name, last_name, myprice, is_above_100, error_msg

    def process_src_folder(self):
        '''
        Process csvs within a folder.        
        '''
        consolidated_dfs = []
        error_dfs = []

        ## Collecting files from the local source(folder).
        for myfile in os.listdir(self.src_folder):
            if not myfile.endswith('.csv'):
                continue
        
            mydf = pd.read_csv(os.path.join(self.src_folder, myfile)).fillna('')

            ## Vectorized application
            mydf['firstname'], mydf['lastname'], mydf['price'], mydf['above_100'], mydf['error_msg'] = zip(*mydf.apply(self._process_row, axis=1))

            ## For downstream processing, split df into good and poor rows.
            consolidated_dfs.append(
                mydf[mydf['error_msg']==''].\
                    loc[:, mydf.columns !='error_msg']
            )

            error_dfs.append(
                mydf[mydf['error_msg']!='']
            )

            shutil.move(os.path.join(self.src_folder, myfile), os.path.join(self.dump_folder, myfile))

        self.consolidated_df = pd.concat(consolidated_dfs)
        self.error_df = pd.concat(error_dfs)

    def _export_files(self):
        '''
        Export completed files to destination
        '''
        self.consolidated_df.to_csv(os.path.join(self.destination_folder, f'consolidated_{datetime.today().strftime("%Y%m%d")}.csv'))
        self.error_df.to_csv(os.path.join(self.destination_folder, f'error_{datetime.today().strftime("%Y%m%d")}.csv'))

    def main(self):
        '''
        Driver script
        '''
        self.process_src_folder()
        self._export_files()

if __name__=="__main__":
    job = Pipeline1()
    job.main()


"""
Search for which files a patient is present in

Author(s): Daniela Wiepert
Last Modified: 12/30/2023
"""

#IMPORTS
#built-in
import argparse
import os
import pickle

from datetime import datetime

#third-party
import pandas as pd
import numpy as np

from lookup_tables import get_most_recent

def search(mrn, lookup_table):
    """
    Search all files in the lookup table for the MRN
    """
    dates = {}
    for k in lookup_table:
        f = lookup_table[k]
        in_dates = []
        in_files = []
        for l in f:
            df = f[l]
            data = df['data']
            exists = data.loc[data['MRN'] == float(mrn)]
            if not exists.empty:
                if k != 'qualtrics':
                    date = datetime.strptime(df['date'], df['dt_format'])
                else: 
                    d = pd.to_datetime(exists['QualtricsSent'].values[0]) #initial qualtrics appearance is actually when the qualtrics survey was sent
                    date = d.date()
                    date = datetime.combine(date, datetime.min.time())
                in_dates.append(date)
                in_files.append(l)
        dates[k] = {'dates':in_dates, 'files':in_files}

    return dates

def find_mrn(mrns, lookup_table):
    mrn_dates = {}

    for i in range(len(mrns)):
        if i % 100 == 0:
            print(f'{i+1} mrns searched')

        m = mrns[i]
        dates = search(m, lookup_table)
        mrn_dates[m] = dates

    return mrn_dates

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--paths", nargs="+", default=['./uploaded_sheets','./uploaded_sheets/archive','./uploaded_sheets/archive/old_sheets'], help="specify all full directory paths that may contain sheets")
    parser.add_argument("--m_path", default='./output_sheets')
    parser.add_argument("--output_path", default='./output_sheets')
    args = parser.parse_args()

    ##UPdate to most recent master database!
    master_file, master_date = get_most_recent(pat = os.path.join(args.m_path, 'master_database*.csv'), sep="_",
                                               dt_ind=-1, dt_format='%Y%m%d-%H%M%S', data_ext='.csv')
    data = pd.read_csv(master_file)
    mrns = list(set(data['MRN'].to_list()))
    mrns = [m for m in mrns if not np.isnan(m)]
    
    dbfile = open(os.path.join(args.output_path,'lookup_table.pkl'), 'rb')
    lookup_table = pickle.load(dbfile)
    dbfile.close()


    mrn_dates = find_mrn(mrns, lookup_table)

    dbfile = open(os.path.join(args.output_path,'mrn_dates.pkl'), 'wb')
    pickle.dump(mrn_dates, dbfile)
    dbfile.close()

if __name__ == '__main__':
    main()

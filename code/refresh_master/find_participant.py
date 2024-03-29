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

def search(fn, ln, em, lookup_table):
    """
    Search all files in the lookup table for the MRN
    """
    mrn = np.nan
    for k in lookup_table:
        f = lookup_table[k]
        in_dates = []
        in_files = []
        for l in f:
            df = f[l]
            data = df['data']
            exists = data.loc[(data['FirstName'] == fn) & (data['LastName'] == ln) & (data['EmailAddress'] == em)]
            if not exists.empty:
                m = exists['MRN'].values[0]
                if (np.isnan(mrn)) and (not np.isnan(m)):
                    mrn = m
            

    return mrn

def find_participants(firstname, lastname, emailaddress, lookup_table):
    mrns = []

    for i in range(len(firstname)):
        if i % 100 == 0:
            print(f'{i+1} mrns searched')

        f = firstname[i]
        l = lastname[i]
        e = emailaddress[i]
        mrn = search(f, l, e, lookup_table)
        mrns.append(mrn)

    return mrns

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--paths", nargs="+", default=['./uploaded_sheets','./uploaded_sheets/archive','./uploaded_sheets/archive/old_sheets'], help="specify all full directory paths that may contain sheets")
    parser.add_argument("--in_path", default='./temp_uploaded/missing_mrns3.csv')
    parser.add_argument("--output_path", default='./output_sheets')
    args = parser.parse_args()

    ##UPdate to most recent master database!
   
    data = pd.read_csv(args.in_path)
    
    dbfile = open(os.path.join(args.output_path,'lookup_table.pkl'), 'rb')
    lookup_table = pickle.load(dbfile)
    dbfile.close()

    #temp = find_participants(['joe'], ['burnham'], ['jcburnham@charter.net'], lookup_table)
    mrns = find_participants(data['FirstName'].values, data['LastName'].values, data['EmailAddress'].values, lookup_table)

    data['MRN'] = mrns
    data = data[['LastName','FirstName','EmailAddress', 'MRN']]
    data.to_csv('/Volumes/AI_Research/Speech/ParticipantTracking/temp_uploaded/missing_mrns_filled3.csv', index=False)
    print('pause')


if __name__ == '__main__':
    main()

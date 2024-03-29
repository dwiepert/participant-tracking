"""
Generate qualtrics_mrn csv getting MRNS for qualtrics participants WITHOUT an MRN

Author(s): Daniela Wiepert
Last modified: 01/02/2024
"""

##IMPORTS
#built-in
import argparse
import glob
import os
import re
import warnings

from datetime import datetime

#third-party
import pandas as pd
import numpy as np

def get_most_recent(pat, sep="_",dt_ind=-2, dt_format='%Y%m%d', data_ext=".xlsx"):
    """
    Get most recent version of files based on date

    :param pat: pattern of files to compare (for glob)
    :param sep: separation between general file name and datetime string part of file name (default = "_")
    :param dt_ind: index ot list of indices (based on split using sep) where datetime string is located (default = -2)
    :param dt_format: datetime string format formula (default='%Y%m%d' (YYYYMMDD))
    :param data_ext: file extension for the files you are extraction datetime from (default = '.xlsx')
    :param ts: boolean indicating whether a timestamp exists in the file name (default = False)
    :param ts_ind: index (based on split using sep) where timestamp is located (default = -1; assumes last)
    :param ts_format: timestmp format (default = '%H%M' (HHMM))

    :return: file name for most recent file, index of the max date
    """
    #get files with the given pattern
    files = glob.glob(pat)

    #if there are no files
    if files == []:
        #returns empty
        return '', None

    if isinstance(dt_ind, int):
        #split the file names with sep, grab the index with the datetime string, and replace the data extension with an empty string
        date = [s.split(sep)[dt_ind].replace(data_ext,"") for s in files]
    elif isinstance(dt_ind, list):
        date = []
        for s in files:
            dsplit = s.split(sep)
            to_join = []
            for i in dt_ind:
                to_join.append(dsplit[i])
            jdate = "".join(to_join)
            jdate = jdate.replace(data_ext, "")
            date.append(jdate)


    #check for leading zeros (some files missing leading zeros and they need to be readded)
    if dt_format == "%m.%d.%y":
        for f in range(len(date)):
            if date[f][0] != '0':
                date[f] = '0'+date[f]

    try:
        #convert to datetime object
        date = [datetime.strptime(s, dt_format) for s in date]
        #find index of max date
        ind = date.index(max(date))

        return files[ind], max(date)
    except:
        # if it is not possible to convert to datetime because of different string formats, throws an error
        print('Datetime format was not correct across all potential files. Please consider the path given and the datetime format')
        return '', None
    
def merge_xy(data, col):
    colx = col + '_x'
    coly = col + '_y'

    new_col = []
    x_vals = data[colx].values
    y_vals = data[coly].values

    for i in range(len(x_vals)):
        if x_vals[i] == y_vals[i]:
            new_col.append(x_vals[i])
        else:
            if isinstance(y_vals[i], float):
                if np.isnan(x_vals[i]):
                    new_col.append(y_vals[i]) #if x is nan, update with y even if y is nan
                else:
                    new_col.append(x_vals[i])
            else:
                if x_vals[i] is None:
                    new_col.append(y_vals[i])
                else:
                    new_col.append(x_vals[i])
    
    data[col] = new_col
    data = data.drop([colx,coly],axis=1)

    return data

def read_qualtrics(q):
    """
    """
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        data = pd.read_excel(q, engine='openpyxl')

    ### edit column names - remove spaces,add qualtrics in front of name, select only relevant columns
    cols = data.columns.values
    cols = ["".join(c.split(" ")) for c in cols]
    cols = [re.sub("\(.*?\)","",c) for c in cols]
    cols[cols.index('ExternalDataReference')] = 'MRN'

    data.columns = cols
    
    data = data[['MRN','LastName','FirstName','EmailAddress']]
               
    data['LastName'] = data['LastName'].apply(str.lower)
    data['FirstName'] = data['FirstName'].apply(str.lower)
    data['EmailAddress'] = data['EmailAddress'].apply(str.lower)
    return data

def read_ptrax(tl, tl_date):
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        data = pd.read_excel(tl, sheet_name='Participants', engine='openpyxl')
    data['LastName'] = [s.split(",")[0] for s in data['Name'].values]
    data['FirstName'] = [s.split(",")[1].split(" ")[1] for s in data['Name'].values]
    data['MC Number'] = data['MC Number'].astype('string')

    ### edit column names - remove spaces,add qualtrics in front of name, select only relevant columns
    cols = data.columns.values
    cols = ["".join(c.split(" ")) for c in cols]
    cols[cols.index('MCNumber')] = 'MRN'
    cols[cols.index('CurrentStatus')] = 'PtraxStatus'
    cols[cols.index('CurrentStatusDate')] = 'PtraxDate'
    cols[cols.index('CurrentStatusReason')] = 'PtraxReason'
    cols[cols.index('ParticipantComment')] = 'PtraxComment'
    cols[cols.index('Participant#')] = 'ParticipantNo'
    cols[cols.index('Study-Participant#')] = 'StudyParticipantNo'
    cols[cols.index('parentstudynameid')] = 'ParentStudy'

    data.columns = cols
    data = data[['MRN','LastName','FirstName','EmailAddress','PtraxStatus','PtraxDate','PtraxReason','PtraxComment','ParentStudy','Site','ParticipantNo','StudyParticipantNo','PendingActions','AccruedDate','Gender','DateofBirth','Age','AgeatConsent','DeceasedDate','Ethnicity','Race','RecruitmentOption']]
    data['LastName'] = data['LastName'].apply(str.lower)
    data['FirstName'] = data['FirstName'].apply(str.lower)
    data['EmailAddress'] = data['EmailAddress'].replace(np.nan, '').apply(str.lower)
    data['PendingActions'] = data['PendingActions'].replace(np.nan, '')
    data['MRN'] = data[['MRN']].astype(int)
    data['InitialDate'] = tl_date
    data['InitialFile'] = tl
    data['RecentDate'] = tl_date
    data['RecentFile'] = tl
    
    #data.update(qualtrics_mrn)
    nan_mrns = data.loc[np.isnan(data['MRN'])]
    return data
    

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", default="./uploaded_sheets")
    parser.add_argument("--output_path", default="./output_sheets")

    args = parser.parse_args()

    master_file, master_date = get_most_recent(pat = os.path.join(args.output_path, 'master_database*.csv'), sep="_",
                                               dt_ind=-1, dt_format='%Y%m%d-%H%M%S', data_ext='.csv')
    m_df = pd.read_csv(master_file)
    m_df = m_df[['MRN','LastName','FirstName','EmailAddress']]

    ## QUALTRICS
    q, q_date = get_most_recent(pat=os.path.join(args.input_path,'dashboard-export*.xlsx'), sep="-",
                                dt_ind=[2,3,5,6,7], dt_format='%H%M%Y%m%d', data_ext='.xlsx')

    q2, q_date2 = get_most_recent(pat=os.path.join(args.input_path,'qualtrics_tracking*.xlsx'), sep="_",
                                    dt_ind=-1, dt_format='%Y%m%d', data_ext='.xlsx') #########################################Hugo changed sep to '_' 20231208

    p, pdate = get_most_recent(pat=os.path.join(args.input_path,'22-002430_trackingLog*.xlsx'), sep="_",
                                    dt_ind=-1, dt_format='%Y%m%d%H%M', data_ext='.xlsx') 
    if q != '' and q2 != '':
        if q_date2 > q_date:
            q = q2
            q_date = q_date2
    elif q == '' and q2 != '':
        q = q2
        q_date = q_date2
    else:
        assert q != '', 'Error finding qualtrics file in input directory. Please confirm the qualtrics files are either in the form (dashboard-export-HH-MM-*-YYYY-MM-DD.xlsx) or (qualtrics_tracking_YYYYMMDD.xlsx)'
    
    data = read_qualtrics(q)

    data = data.loc[np.isnan(data['MRN'])]
    merged = data.merge(m_df, on=['LastName','FirstName','EmailAddress'], how='left')
    merged = merge_xy(merged, 'MRN')
    merged = merged.drop_duplicates()

    p_data = read_ptrax(p, pdate)
    p_data = p_data[['MRN','LastName','FirstName', 'EmailAddress']]
    merged_2 = merged.merge(p_data, on=['LastName','FirstName','EmailAddress'], how='left')
    merged_2 = merge_xy(merged_2, 'MRN')
    merged_2 = merged_2.drop_duplicates()
    merged_2 = merged_2.reset_index()

    #certain checks:
    # 1) NaN
    nan_mrns = merged_2.loc[np.isnan(merged_2['MRN'])]


    if not nan_mrns.empty:
        to_drop = nan_mrns.index.to_list()
        merged_2 = merged_2.drop(to_drop, axis=0)

    # 2) duplicate MRNS
    if len(merged['MRN'].values) != len(list(set(merged['MRN'].values))):
        raise Exception('Duplicate MRNs within a sheet - was not handled by dropping duplicates or filtering')
        # mrns = list(set(merged['MRN'].to_list()))
        # for i in range(len(mrns)):
        #     m = mrns[i]
        #     d_mrn = merged.loc[merged['MRN'] == m]
        #     if len(d_mrn) > 1 :
                
    # 3) duplicate emails
    #if len(merged['EmailAddress'].values) != len(list(set(merged['EmailAddress'].values))):
     #   raise Exception('Duplicate MRNs within a sheet - was not handled by dropping duplicates or filtering')
    
    merged_2.to_csv(os.path.join(os.path.dirname(os.path.realpath(__file__)),'./qualtrics_mrn.csv'), index=False)
  

if __name__ == '__main__':
    main()

"""
Generate a large lookup table with:
- ALL versions of MC_RSH_AI files
- ALL versions of ptrax tracking log 
- MOST RECENT qualtrics

Author(s): Daniela Wiepert
Last Modified: 12/30/2023
"""
#IMPORTS
#built-in
import argparse
import glob
import io
import os
import pickle
import re
import string
import warnings

from datetime import datetime

#third-party
import msoffcrypto
import pandas as pd
import numpy as np


def decrypt(f, code):
    """
    decrypt an excel file

    :param f: file to decrypt
    :param code: password for decryption

    :return data: read-in dataframe
    """
    assert '.xlsx' in f, f'File {f} is not a xlsx file to decrypt'
    decrypted_wb = io.BytesIO()
    with open(f,'rb') as file:
        of = msoffcrypto.OfficeFile(file)
        of.load_key(password=code)
        of.decrypt(decrypted_wb)
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        data = pd.read_excel(decrypted_wb, engine='openpyxl')
    
    return data

def get_most_recent(pat, sep="_",dt_ind=-2, dt_format='%Y%m%d', data_ext=".xlsx"):
    """
    Get most recent version of files based on date

    :param pat: pattern of files to compare (for glob)
    :param archive_path: path to directory of where to archive old files
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

def merge_xy(data):
    """
    Merge two columns (_x, _y), updating to the value in the _y column if it exists
    This version operates over EVERY _x and _y column so run LAST

    :param data: dataframe to merge cols
    :return df: merged dataframe
    """
    df = data.copy()
    xs = [col for col in df.columns if '_x' in col]

    ### COMPARE X & Y column versions
    for i in range(len(xs)):
        new_col = []
        x = xs[i]
        y = x.replace("_x", "_y")
        x_vals = df[x].values
        y_vals = df[y].values

        for j in range(len(x_vals)):
            if x_vals[j] == y_vals[j]:
                new_col.append(x_vals[j]) #if they're the same, just arbitrarily select one
            else:
                if isinstance(y_vals[j], float):
                    if not np.isnan(y_vals[j]):
                        new_col.append(y_vals[j])
                    else:
                        new_col.append(x_vals[j])
                else:
                    if y_vals[j] is not None:
                        new_col.append(y_vals[j]) # y is from the new column = ignores if it is blank (nan), but otherwise changes it to the updated valuue
                    else:
                        new_col.append(x_vals[j])

        df[x.replace("_x","")] = new_col
        df = df.drop([x,y], axis=1)

    return df

# read in data 
def mc_readin(files, code='', dt_ind=-2, dt_format='%Y%m%d', data_ext ='.xlsx', sep="_"):
    """
    Read-in MC_RSH_AI files
    """
    lookup = {}
    for f in files:
        if isinstance(dt_ind, int):
            date = f.split(sep)[dt_ind].replace(data_ext,"")

        elif isinstance(dt_ind, list):
            dsplit = f.split(sep)
            to_join = []
            for i in dt_ind:
                to_join.append(dsplit[i])
            date = "".join(to_join)
            date = date.replace(data_ext, "")
            
        data = decrypt(f, code)
                 #get epic status
        status = f.split("_")[-4:-2]
        if status[0] == 'Speech':
            status= [status[-1]]
        status = " ".join(status).lower()
        data['EpicStatus'] = status

        #rename columns to not have spaces
        cols = data.columns.values
        cols = ["".join(c.translate(str.maketrans('','', string.punctuation)).split(" ")) for c in cols]
        cols[cols.index('PatientPreferredFirstName')] = "FirstName"
        cols[cols.index('PtEmailAddress')] = "EmailAddress"
        data.columns = cols

        data['LastName'] = data['LastName'].apply(str.lower)
        data['FirstName'] = data['FirstName'].apply(str.lower)
        data['EmailAddress'] = data['EmailAddress'].replace(np.nan,'').apply(str.lower)

        lookup[f] = {'data': data, 'date':date, 'dt_format':dt_format}
    
    return lookup

def t_readin(files, dt_ind=-2, dt_format='%Y%m%d', data_ext ='.xlsx', sep="_"):
    """
    Read in tracking log files
    """
    lookup = {}
    for f in files:
        if isinstance(dt_ind, int):
            date = f.split(sep)[dt_ind].replace(data_ext,"")

        elif isinstance(dt_ind, list):
            dsplit = f.split(sep)
            to_join = []
            for i in dt_ind:
                to_join.append(dsplit[i])
            date = "".join(to_join)
            date = date.replace(data_ext, "")
            
        #read in and edit column names
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            data = pd.read_excel(f, sheet_name='Participants', engine='openpyxl')
        data['LastName'] = [s.split(",")[0] for s in data['Name'].values]
        data['FirstName'] = [s.split(",")[1].split(" ")[1] for s in data['Name'].values]

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

        data.columns = cols
        data = data[['MRN','LastName','FirstName','EmailAddress','PtraxStatus','PtraxDate','PtraxReason','PtraxComment','parentstudynameid','Site','ParticipantNo','StudyParticipantNo','PendingActions','AccruedDate','Gender','DateofBirth','Age','AgeatConsent','DeceasedDate','Ethnicity','Race','RecruitmentOption']]
        
        data['LastName'] = data['LastName'].apply(str.lower)
        data['FirstName'] = data['FirstName'].apply(str.lower)
        data['EmailAddress'] = data['EmailAddress'].replace(np.nan, '').apply(str.lower)
        data['PendingActions'] = data['PendingActions'].replace(np.nan, '')
        lookup[f] = {'data': data, 'date':date, 'dt_format': dt_format}
    
    return lookup

def q_readin(file, qualtrics_mrn, sep="_", dt_ind=-1, dt_format='%Y%m%d', data_ext='.xlsx'):

    if isinstance(dt_ind, int):
        date = file.split(sep)[dt_ind].replace(data_ext,"")

    elif isinstance(dt_ind, list):
        dsplit = file.split(sep)
        to_join = []
        for i in dt_ind:
            to_join.append(dsplit[i])
        date = "".join(to_join)
        date = date.replace(data_ext, "")

    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        data = pd.read_excel(file, engine='openpyxl')

    ### edit column names - remove spaces,add qualtrics in front of name, select only relevant columns
    cols = data.columns.values
    cols = ["".join(c.split(" ")) for c in cols]
    cols = [re.sub("\(.*?\)","",c) for c in cols]
    cols[cols.index('EmailSentTime')] = 'QualtricsSent'
    cols[cols.index('EmailOpenedTime')] = 'QualtricsOpened'
    cols[cols.index('SurveyStartedTime')] = 'QualtricsStarted'
    cols[cols.index('SurveyFinishedTime')] = 'QualtricsFinished'
    cols[cols.index('ContactListUnsubscribedTime')] = 'QualtricsUnsubscribed'
    if 'MayoClinicNbr' in cols:
        cols[cols.index('MayoClinicNbr')] = 'MRN'
    else:
        cols[cols.index('ExternalDataReference')] = 'MRN'
    if 'Link' in cols:
        cols[cols.index('Link')] = 'QualtricsLink'
    else:
        data['QualtricsLink'] = np.nan #TODO:fix
        cols.append('QualtricsLink')

    data.columns = cols
    data = data[['MRN','LastName','FirstName','EmailAddress','QualtricsLink', 'QualtricsSent','QualtricsOpened','QualtricsStarted','QualtricsFinished','QualtricsUnsubscribed']]

    data['LastName'] = data['LastName'].apply(str.lower)
    data['FirstName'] = data['FirstName'].apply(str.lower)
    data['EmailAddress'] = data['EmailAddress'].apply(str.lower)

    qualtrics_mrn = pd.read_csv(qualtrics_mrn)
    data= data.merge(qualtrics_mrn, on=['LastName','FirstName','EmailAddress'], how='left')

    data = merge_xy(data)

    lookup = {}
    lookup[file] = {'data': data, 'date':date, 'dt_format': dt_format}

    return lookup
    

def glob_paths(paths, pat):
    """
    Get files from multiple paths
    """
    files = []
    for p in paths:
        fs = glob.glob(os.path.join(p, pat))
        files.extend(fs)
    return files

def generate_lookup(paths, q_path, qualtrics_mrn, code=''):
    """
    Generate the lookup table
    """

    # 1) MC_RSH_AI - search for all file names
    id = glob_paths(paths, 'MC_RSH_AI_Speech_Identified*.xlsx')
    i = glob_paths(paths, 'MC_RSH_AI_Speech_Interested*.xlsx')
    ni = glob_paths(paths, 'MC_RSH_AI_Speech_Not_Interested*.xlsx')

    lookup_tab = {}


    lookup_tab['identified'] = mc_readin(id, code=code, dt_ind=[-2, -1], dt_format='%Y%m%d%H%M')
    lookup_tab['interested'] = mc_readin(i, code=code, dt_ind = [-2, -1], dt_format='%Y%m%d%H%M')
    lookup_tab['not_interested'] = mc_readin(ni,code=code,dt_ind = [-2, -1], dt_format='%Y%m%d%H%M')


    # 2) Prtax tracking log
    t8 = glob_paths(paths, '22-002430_trackingLog_'+ ('[0-9]' * 8)+'.xlsx')
    t12 = glob_paths(paths, '22-002430_trackingLog_'+ ('[0-9]' * 12)+'*.xlsx')

    temp12 = t_readin(t12, dt_ind=-1, dt_format='%Y%m%d%H%M')
    temp8 = t_readin(t8, dt_ind=-1)
    temp12.update(temp8)

    lookup_tab['ptrax'] = temp12

    # 3) Qualtrics

    q, q_date = get_most_recent(pat=os.path.join(q_path,'dashboard-export*.xlsx'), sep="-",
                                dt_ind=[2,3,5,6,7], dt_format='%H%M%Y%m%d', data_ext='.xlsx')

    q2, q_date2 = get_most_recent(pat=os.path.join(q_path,'qualtrics_tracking*.xlsx'), sep="_",
                                    dt_ind=-1, dt_format='%Y%m%d', data_ext='.xlsx') 
    
    if q != '':
        if q2 != '':
            if q_date > q_date2:
                lookup_tab['qualtrics'] = q_readin(q, qualtrics_mrn, sep="-",
                                dt_ind=[2,3,5,6,7], dt_format='%H%M%Y%m%d', data_ext='.xlsx')
            else:
                lookup_tab['qualtrics'] = q_readin(q, qualtrics_mrn, sep="_",
                                    dt_ind=-1, dt_format='%Y%m%d', data_ext='.xlsx')
        else:
            lookup_tab['qualtrics'] = q_readin(q, qualtrics_mrn, sep="-",
                                dt_ind=[2,3,5,6,7], dt_format='%H%M%Y%m%d', data_ext='.xlsx')
    else:
        lookup_tab['qualtrics'] = q_readin(q, qualtrics_mrn, sep="_",
                                    dt_ind=-1, dt_format='%Y%m%d', data_ext='.xlsx')

    return lookup_tab


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--paths", nargs="+", default=['./uploaded_sheets','./uploaded_sheets/archive','./uploaded_sheets/archive/old_sheets', './temp_uploaded'], help="specify all full directory paths that may contain sheets")
    parser.add_argument("--q_path", default='./uploaded_sheets')
    parser.add_argument("--qualtrics_mrn", default='./code/qualtrics_mrn/qualtrics_mrn.csv')
    parser.add_argument("--output_path", default='./output_sheets')
    parser.add_argument("--decrypt_code", default='', help="specify the decryption code as a string")
    args = parser.parse_args()


    lookup_table = generate_lookup(args.paths, args.q_path, args.qualtrics_mrn, args.decrypt_code)
    # Its important to use binary mode
    dbfile = open(os.path.join(args.output_path, 'lookup_table.pkl'), 'wb')
    # source, destination
    pickle.dump(lookup_table, dbfile)                    
    dbfile.close()

if __name__ == '__main__':
    main()
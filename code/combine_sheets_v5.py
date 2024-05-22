"""
Merge sheets for participant tracking

Author(s): Daniela Wiepert
Last Modified: 03/22/2024
"""

### IMPORTS ###
#built-in
import ast
import argparse
import glob
import io
import os
import pickle
import re
import shutil
import string
import warnings

from datetime import datetime
from pathlib import Path

#third-party
import msoffcrypto
import numpy as np
import pandas as pd

### HELPER FILES
def glob_paths(paths, pat):
    """
    Get files from multiple paths
    """
    files = []
    for p in paths:
        fs = glob.glob(str(p / pat))
        files.extend(fs)
    return files

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
    files = glob.glob(str(pat))

    #check length = there should only be 2 existing in your dataset
    #assert len(files) <= 2, 'There should only be 2 files with given name in dataset.'

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

        #archive any other files
        #archive([x for i,x in enumerate(files) if i!=ind], archive_path)

        return Path(files[ind]), max(date)
    except:
        # if it is not possible to convert to datetime because of different string formats, throws an error
        raise ValueError('Datetime format was not correct across all potential files. Please consider the path given and the datetime format')
    
def decrypt(f, code):
    """
    decrypt an excel file

    :param f: file to decrypt
    :param code: password for decryption

    :return data: read-in dataframe
    """
    assert '.xlsx' in str(f), f'File {f} is not a xlsx file to decrypt'
    decrypted_wb = io.BytesIO()
    with open(f,'rb') as file:
        of = msoffcrypto.OfficeFile(file)
        of.load_key(password=code)
        of.decrypt(decrypted_wb)
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        data = pd.read_excel(decrypted_wb, engine='openpyxl')

    return data

def filter_df(df, to_filter, email_col='EmailAddress'):
    """
    Filter dataframebased on a csv

    :param df: dataframe to filter
    :param to_filter: dataframe with participants to exclude

    :return df: dataframe post-filtering
    """
    to_drop = []
    for ind, row in to_filter.iterrows():
        if 'MRN' in df.columns and not np.isnan(row['MRN']):
            mask = (df['MRN'] == row['MRN']) & (df['LastName'] == row['LastName']) & (df[email_col] == row['EmailAddress']) & (df['FirstName'] == row['FirstName'])
        else:
            mask = (df['LastName'] == row['LastName']) & (df[email_col] == row['EmailAddress']) & (df['FirstName'] == row['FirstName'])

        to_drop.extend(df.loc[mask].index.to_list())

    df = df.drop(index=to_drop, axis=0)
    return df

def merge_xy(data, col, x_first=False):
    colx = col + '_x'
    coly = col + '_y'

    new_col = []
    x_vals = data[colx].values
    y_vals = data[coly].values

    for i in range(len(x_vals)):
        if x_vals[i] == y_vals[i]:
            new_col.append(x_vals[i])
        elif x_first:
            if pd.isnull(x_vals[i]) and not pd.isnull(y_vals[i]):
                new_col.append(y_vals[i])
            else:
                new_col.append(x_vals[i]) #never overwrite x unless x was empty
        else:
            if not pd.isnull(x_vals[i]) and pd.isnull(y_vals[i]):
                new_col.append(x_vals[i]) #keep old col if new one is null
            else:
                new_col.append(y_vals[i])
    
    data[col] = new_col
    data = data.drop([colx,coly],axis=1)

    return data

def convert_todatetime(data, col, dt_format):
    new_vals = []
    orig_vals = data[col].values

    for o in orig_vals:
        if isinstance(o, str):
            new_vals.append(np.datetime64(datetime.strptime(o, dt_format)))
        elif pd.isnull(o):
            new_vals.append(np.datetime64('NaT'))
        else:
            new_vals.append(np.datetime64(datetime.strptime(str(int(o)), dt_format)))
    
    data[col] = new_vals
    return data

def convert_tostring(data, col, dt_format):
    new_vals = []
    orig_vals = data[col].values

    for o in orig_vals:
        #if NaT
        if pd.isnull(o):
            new_vals.append(np.nan)
        else:
            ts = pd.to_datetime(str(o))
            new_vals.append(ts.strftime(dt_format))

    data[col] = new_vals
    return data

### Data specific helper functions
def drop_by_estatus(data):
    """
    Drop duplicate participants based on their epic status. Not interested > interested > identified

    :param data: dataframe pre-dropping
    :return data: dataframe with dropped duplicates
    """
    mrns = list(set(data['MRN'].values))
    to_remove = []
    min_dates = {}
    max_dates = {}
    for i in range(len(mrns)):
        m = mrns[i]
        d_mrn = data.loc[data['MRN']==m]
        if len(d_mrn) > 1:
            inds = d_mrn.index.to_list()
            dates_i = []
            files_i = []
            dates_r = []
            files_r = []
            d_ni = d_mrn.loc[d_mrn['EpicStatus'] == 'not interested']
            if not d_ni.empty:
                dates_i.append(d_ni['InitialDate'].values[0])
                files_i.append(d_ni['InitialFile'].values[0])
                dates_r.append(d_ni['RecentDate'].values[0])
                files_r.append(d_ni['RecentFile'].values[0])
            d_in = d_mrn.loc[d_mrn['EpicStatus'] == 'interested']
            if not d_in.empty:
                dates_i.append(d_in['InitialDate'].values[0])
                files_i.append(d_in['InitialFile'].values[0])
                dates_r.append(d_in['RecentDate'].values[0])
                files_r.append(d_in['RecentFile'].values[0])
            d_id = d_mrn.loc[d_mrn['EpicStatus'] == 'identified']
            if not d_id.empty:
                dates_i.append(d_id['InitialDate'].values[0])
                files_i.append(d_id['InitialFile'].values[0])
                dates_r.append(d_id['RecentDate'].values[0])
                files_r.append(d_id['RecentFile'].values[0])

            min_dates[m] = {'date':min(dates_i), 'file': files_i[dates_i.index(min(dates_i))]}
            max_dates[m] = {'date':max(dates_r), 'file': files_r[dates_r.index(max(dates_r))]}

            if not d_ni.empty:
                inds.remove(d_ni.index.to_list()[0]) #keep not interested if you have more than one
                to_remove.extend(inds)
            elif not d_in.empty:
                inds.remove(d_in.index.to_list()[0]) #keep interested if you have more than one and no not interested
                to_remove.extend(inds)
            else:
                # there would be 2 identified
                raise Exception('There are two identified rows for a single participant. Please examine for errors')
            #to_remove.extend(inds)
    
    data = data.drop(to_remove, axis=0) #is this the right axis?

    for m in min_dates:
        di = min_dates[m]
        dr = max_dates[m]

        ind = data.loc[data['MRN'] == m].index.values[0] 
        data.at[ind,'InitialDate'] = di['date']
        data.at[ind,'InitialFile'] = di['file']
        data.at[ind,'RecentDate'] = dr['date']
        data.at[ind,'RecentFile'] = dr['file']

    return data

def drop_by_action(data):
    """
    Drop duplicate participants based on their PendingActions. Prioritizes non-empty actions

    :param data: dataframe pre-dropping
    :return data: dataframe with dropped duplicates
    """
    mrns = list(set(data['MRN'].values))
    to_remove = []
    for i in range(len(mrns)):
        m = mrns[i]
        d_mrn = data.loc[data['MRN']==m]
        if len(d_mrn) > 1:
            inds = d_mrn.index.to_list()
            #if there are duplicates
            rm_ind = d_mrn.loc[d_mrn['PendingActions'] == ''].index.to_list()
            rm_ind2 = d_mrn.loc[pd.isnull(d_mrn['PendingActions'])].index.to_list()
            rm_ind.extend(rm_ind2)
            if set(rm_ind) == set(d_mrn.index.to_list()): #if they are both empty
                rm_ind = rm_ind[1:] #keeps first
            elif rm_ind == []:
                #select the most recent occurrence
                dates = []
                for j in range(len(inds)):
                    dates.append(d_mrn.iloc[j]['PtraxDate'])
                del inds[dates.index(min(dates))]
                rm_ind = inds

            to_remove.extend(rm_ind)
    data = data.drop(to_remove, axis=0)
    return data

def get_qualtrics_status(data):
    status_cols = ['QualtricsSent','QualtricsOpened','QualtricsStarted','QualtricsFinished','QualtricsUnsubscribed']
    status_choices = ['Email Sent', 'Email Opened', 'Survey Started', 'Survey Finished', 'Unsubscribed']
    status = []
    date = []

    for index, row in data.iterrows():
        curr_status = None
        curr_date = None
        for i in range(len(status_cols)):
            c = status_cols[i]
            s = status_choices[i]
            try:
                d = np.datetime64(row[c])
            except:
                d = None

            if d is not None:
                if curr_date is None:
                    curr_date = d
                    curr_status = s
                else:
                    if d > curr_date:
                        curr_date = d
                        curr_status = s


        status.append(curr_status)
        date.append(curr_date)
    
    return status, date

def latest_qdate(data):
    """
    Check which index has the latest qualtrics date
    :param data: dataframe pre-dropping
    :return ind: index of row w latest qualtrics date
    """
    most_recent = None
    ind = None

    for i, row in data.iterrows():
        date = row['QualtricsDate']
        if most_recent is None:
            most_recent = date
            ind = i
        else:
            if date > most_recent:
                most_recent = date
                ind = i
    return ind

def drop_by_qstatus(data):
    """
    Drop duplicate participants based on their qualtrics status. Survey Finished > Email Opened > Email Sent

    :param data: dataframe pre-dropping
    :return data: dataframe with dropped duplicates
    """
    mrns = list(set(data['MRN'].values))
    to_remove = []
    for i in range(len(mrns)):
        m = mrns[i]
        d_mrn = data.loc[data['MRN']==m]
        if len(d_mrn) > 1:
            inds = d_mrn.index.to_list()
            #options
            #1) unsubscribed
            d_u = d_mrn.loc[d_mrn['QualtricsStatus']=='Unsubscribed']
            #2) survey finished
            d_sf = d_mrn.loc[d_mrn['QualtricsStatus']=='Survey Finished']
            #3) survey started
            d_ss = d_mrn.loc[d_mrn['QualtricsStatus']=='Survey Started']
            #4) email opened
            d_eo = d_mrn.loc[d_mrn['QualtricsStatus']=='Email Opened']
            #5) email sent
            d_es = d_mrn.loc[d_mrn['QualtricsStatus']=='Email Sent']

            if not d_u.empty:
                #quick check that there aren't 2 unsubscribed
                ind = latest_qdate(d_u)
                inds.remove(ind)
            elif not d_sf.empty:
                ind = latest_qdate(d_sf)
                inds.remove(ind)
            elif not d_ss.empty:
                ind = latest_qdate(d_ss)
                inds.remove(ind)
            elif not d_eo.empty:
                ind = latest_qdate(d_eo)
                inds.remove(ind)
            else:
                ind = latest_qdate(d_es)
                inds.remove(ind)
            
            to_remove.extend(inds)

    data = data.drop(to_remove, axis=0)
    return data 

def merge_by_date(data, col, datecols, late=True):
    """
    """
    colx = col + '_x'
    coly = col + '_y'

    new_col = []
    x_vals = data[colx].values
    y_vals = data[coly].values

    #First name is based on the most recent thing, so the comparison would be either:
    #Most recent file? This only works with Epic and Ptrax, not Qualtrics (qualtrics)
    # Initial Date (Epic) vs. PtraxStatus/InitialDate vs. QualtricsStatus/InitialDate

    #date cols given in ORDER of the sheet merge
    assert len(datecols) == 2, 'merging two columns - should only have 2 date columns'
    date_x = data[datecols[0]].values
    date_y = data[datecols[1]].values

    for i in range(len(x_vals)):
        if pd.isnull(date_x[i]) and not pd.isnull(date_y[i]): #if only y has value, then newer column is y
            recent=y_vals
            old=x_vals
        elif not pd.isnull(date_x[i]) and pd.isnull(date_y[i]): #if only x has value, then newer column is x
            recent=x_vals
            old=y_vals
        elif late:
            if date_y[i] > date_x[i]: # if both have a value and  newer column is column y
                recent = y_vals
                old = x_vals
            else: #if both have a value and newer column is x
                recent = x_vals
                old = y_vals
        else:
            if date_y[i] < date_x[i]: # if both have a value and  newer column is column y
                recent = y_vals
                old = x_vals
            else: #if both have a value and newer column is x
                recent = x_vals
                old = y_vals
        
        #check if the more recent values are null
        is_null = pd.isnull(recent[i])
        # if isinstance(recent[i], float):
        #     if np.isnan(recent[i]):
        #         is_null = True
        # elif recent[i] is None:
        #     is_null = True
        
        if recent[i] == old[i] or is_null: #if the newer column has a null value or the cols are the same, just grab the old column
            new_col.append(old[i])
        else: #otherwise grab the new_column
            new_col.append(recent[i])
    
    data[col] = new_col
    data = data.drop([colx,coly],axis=1)



    return data

def merge_interested(data):
    """
    Merge epic status column - has specific rules for overwtiting
    If columns are the same, select old column
    If the old column is 'not interested' or 'interested', they are not overwritten
    If the old column is 'identified' but new one is empty, keep old column
    If the old column is 'idenfitied' but the new one is 'not interested' or 'interested', overwrite with new column

    :param data: dataframe to merge
    :return df: merged dataframe

    """
    df = data.copy()
    new_col = []
    xcol = 'EpicStatus_x'
    ycol = xcol.replace("_x", "_y")
    x_vals = df[xcol].values
    y_vals = df[ycol].values

    rank = {'not interested': 0, 'interested': 1, 'identified':2, 'new interested': 3}
    vals = ['not interested', 'interested','identified', 'new interested']
    for j in range(len(x_vals)):
        x = x_vals[j]
        y = y_vals[j]
        if x == y or (not pd.isnull(x) and pd.isnull(y)):
            new_col.append(x) #if they're the same or y is null, just select x
        elif pd.isnull(x) and not pd.isnull(y):
            new_col.append(y) #if x is null, choose y
        else: #go through the order
            xrank = rank[x]
            yrank = rank[y]
            r = min(xrank, yrank) #get the minimum rank
            new_col.append(vals[r]) 

    df[xcol.replace("_x","")] = new_col
    df = df.drop([xcol,ycol], axis=1)
    return df

def merge_date(data, col, by='early', with_files=True):
    """
    """
    colx = col + '_x'
    coly = col + '_y'

    new_col = []
    x_vals = data[colx].values
    y_vals = data[coly].values

    for i in range(len(x_vals)):
        if pd.isnull(x_vals[i]) and not pd.isnull(y_vals[i]):
            new_col.append(y_vals[i])
        elif not pd.isnull(x_vals[i]) and pd.isnull(y_vals[i]):
            new_col.append(x_vals[i])
        else:
            if by == 'early':
                if x_vals[i] < y_vals[i]:
                    new_col.append(x_vals[i]) #keep the earliest date
                else:
                    new_col.append(y_vals[i])
            else:
                if x_vals[i] > y_vals[i]:
                    new_col.append(x_vals[i]) #keep the latest date
                else:
                    new_col.append(y_vals[i])

    data[col] = new_col
    data = data.drop([colx,coly],axis=1)

    return data

def merge_sheets(sheet1, sheet2, datecols):
    """
    """
    # take earliest initial date
    # merge names and emails, keep from the one with the most recent initial date?
    data = pd.merge(sheet1, sheet2, how='outer',on=['MRN'])

    mcols = [d for d in data.columns.values if '_x' in d]
    mcols = [d.replace('_x','') for d in mcols]
    mcols.remove('InitialDate')
    mcols.remove('InitialFile')
    mcols.remove('RecentDate')
    mcols.remove('RecentFile')
    for c in mcols:
        data = merge_by_date(data, c, datecols)

    ### special thing for email address that is specific to our version, will be ignored otherwise
    cols = data.columns.to_list()
    if 'EmailAddress' in cols and 'EmailAddressQualtrics' in cols:
        cols[cols.index('EmailAddress')] = 'EmailAddress_x'
        #cols[cols.index('EmailAddressQualtrics')] = 'EmailAddress_y'
        data.columns = cols
        data['EmailAddress_y'] = data['EmailAddressQualtrics']
        data = merge_by_date(data, 'EmailAddress', datecols)
    elif 'EmailAddressEpic' in cols and 'EmailAddressPtrax' in cols:
        data['EmailAddress_x'] = data['EmailAddressEpic']
        data['EmailAddress_y'] = data['EmailAddressPtrax']
        data = merge_by_date(data, 'EmailAddress', datecols)

    data = merge_by_date(data, 'InitialFile', ['InitialDate_x', 'InitialDate_y'], late=False)
    data = merge_by_date(data, 'RecentFile', ['RecentDate_x','RecentDate_y'])
    data = merge_date(data, 'InitialDate', 'early')
    data = merge_date(data, 'RecentDate', 'late')
    
    return data

def remove_report(to_remove, table):
    curr_files = table['files']
    curr_studies = table['studies']
    curr_sites = table['sites']
    curr_fnames = table['fnames']
    
    re_search = []
    for r in to_remove:
        curr_files.remove(str(r))
        curr_fnames.remove(str(r.name))

        split_name = str(r).replace(str(r.parents[0] / 'MC_RSH_AI_Speech_'),'').split(sep="_")
        study_id = "_".join(split_name[:-2]).lower()

        study_files = [f for f in curr_files if "_".join(split_name[:-2]) in f]
        for i in study_files:
            if i in curr_files:
                curr_files.remove(i)
        re_search.extend(study_files)

        curr_studies.remove(study_id) #we are just going to fully redo a study if trying to remove one
        del curr_sites[study_id]

        to_delete = []
        for k in table:
            if k != 'files' and k != 'studies' and k != 'sites':
                v = table[k]
                if study_id in v:
                    v.remove(study_id)
                    if v == []:
                        to_delete.append(k)
                    
                    table[k] = v

    for t in to_delete:
        del table[t]

    table['files'] = curr_files
    table['studies'] = curr_studies
    table['sites'] = curr_sites
    table['fnames'] = curr_fnames
    return table, list(set(re_search))


def generate_parentstudy(reports, out_path, archive_path, load_existing, code='', dt_ind=-2, dt_format='%Y%m%d', data_ext ='.xlsx', sep="_"):
    db = out_path / 'parentstudy_lookup.pkl'
    reports = [Path(r) for r in reports]
    
    if load_existing:
        assert db.exists()
        dbfile = open(db, 'rb')
        parentstudy_table = pickle.load(dbfile)
        dbfile.close()

        in_reports = parentstudy_table['files']
        in_reports = [Path(r) for r in in_reports]
        ir_names = parentstudy_table['fnames']

        #parentstudy_table['files'] = in_reports

        #changed name:
        # archived = [r for r in in_reports if r not in reports]
        # if archived != []:
        #     for a in archived:
        #         orig_ind = in_reports.index(a)
        #         new_path = archive_path / a.name 
        #         in_reports[orig_ind] = new_path
            
        #     parentstudy_table['files'] = [str(r) for r in in_reports]

        r_names= [os.path.basename(str(r).replace("\\", "/")) for r in reports]
        rm_reports = [r for r in in_reports if os.path.basename(str(r).replace("\\", "/")) not in r_names]
        reports = [r for r in reports if os.path.basename(str(r).replace("\\", "/")) not in ir_names]

        if len(rm_reports) > 0:
            parentstudy_table, re_search = remove_report(rm_reports, parentstudy_table) #remove any reports that seem to have been deleted/moved
            reports.extend(re_search)


    parentstudy = {}
    studies = []
    sites_dict = {}
    files = []
    for r in reports:
        pr = r
        r = str(r)
  
        if isinstance(dt_ind, int):
            date = r.split(sep)[dt_ind].replace(data_ext,"")

        elif isinstance(dt_ind, list):
            dsplit = r.split(sep)
            to_join = []
            for i in dt_ind:
                to_join.append(dsplit[i])
            date = "".join(to_join)
            date = date.replace(data_ext, "")

        split_name = str(pr.name).replace('MC_RSH_AI_Speech_','').split(sep="_")
        study_id = "_".join(split_name[:-2]).lower()
        if 'jax' in study_id:
            site = 'FLA'
        else:
            site = 'RST'

        #data = decrypt
        data = decrypt(pr, code=code)
        parentstudy[pr.name] = {'studyid': study_id, 'data': data, 'date': date, 'dt_format': dt_format, 'site':site}
        files.append(r)

        if study_id not in studies:
            studies.append(study_id)
            sites_dict[study_id] = site
    
    mrn_parentstudy = {}
    fnames = list(parentstudy.keys())
    #files = [str(Path(i).relative_to(py_path)).replace("\\", os.sep) for i in files]
    
    if load_existing:
        mrn_parentstudy = parentstudy_table
        studies.extend(parentstudy_table['studies'])
        studies = list(set(studies))
        files.extend(parentstudy_table['files'])
        #collapse files that are repeats
        files = list(set(files))
        fnames.extend(parentstudy_table['fnames'])
        fnames = list(set(fnames))
        sites_dict.update(parentstudy_table['sites'])
        

    mrn_parentstudy['studies'] = studies #should reset the whole thing?
    mrn_parentstudy['files'] = files
    mrn_parentstudy['fnames'] = fnames
    mrn_parentstudy['sites'] = sites_dict

    for k in parentstudy:
        data = parentstudy[k]['data']
        mrns = data['MRN'].values
        sid = parentstudy[k]['studyid']

        if isinstance(mrns[0], str):
            mrns = [float(c.translate(str.maketrans('','', string.punctuation))) for c in mrns]
        
        assert isinstance(mrns[0], float)
        
        for m in mrns:
            if m not in mrn_parentstudy:
                mrn_parentstudy[m] = [sid]
            else:
                temp = mrn_parentstudy[m]
                if sid not in temp:
                    temp.append(sid)

                mrn_parentstudy[m] = temp

    if load_existing:
        if len(parentstudy) > 0 and len(reports) > 0:
            dbfile = open(db, 'wb') #save out new version if something has changed
            # source, destination
            pickle.dump(mrn_parentstudy, dbfile)                    
            dbfile.close()
    else:
        dbfile = open(db, 'wb') #save out new version if something has changed
            # source, destination
        pickle.dump(mrn_parentstudy, dbfile)                    
        dbfile.close()

    return mrn_parentstudy

def load_parentstudy(input_path, out_path, load_existing, archive_path, code=''):

    # if to_archive:
    #     ar = glob.glob(str(input_path / 'MC_RSH_AI_Speech*.xlsx'))
    #     ar = [r for r in ar if 'Identified' not in r]
    #     ar= [r for r in ar if 'Interested' not in r]
    #     remove_date = list(set(["_".join(r.split(sep="_")[:-2])+'*' for r in ar]))
    #     for r in remove_date:
    #         files = glob.glob(r)
    #         if len(files) > 1:
    #             #get_most_recent(r, archive_path,ts=True)
    #             assert len(files) <= 2, 'Pile up of parent study files. Please confirm it is as intended.'
    #             a, b = get_most_recent(Path(r), archive_path, sep="_", dt_ind=[-2,-1], dt_format="%Y%m%d%H%M", data_ext='.xlsx')

    reports = glob_paths([input_path, archive_path], 'MC_RSH_AI_Speech*.xlsx')
    #reports = [str(Path(r).name) for r in reports]
    reports = [r for r in reports if 'Identified' not in r]
    reports = [r for r in reports if 'Interested' not in r]
     
    parentstudy_table = generate_parentstudy(reports, out_path, archive_path, code=code, load_existing=load_existing)

    return parentstudy_table

def pop_parentstudy(df, input_path, output_path,load_existing, report_archive, code=''):
    #temp = df.loc[(pd.isnull(df['ParentStudy']) | pd.isnull(df['Site']))]
    #mrns = temp['MRN'].values


    parentstudies = load_parentstudy(input_path, output_path, load_existing, report_archive, code=code)
    
    studies = parentstudies['studies']
    sites = parentstudies['sites']
    if not all(s in df.columns for s in studies):
        mrns = df
    else:
        temp = [s for s in studies if s in df.columns]
        temp.extend(['MRN','ParentStudy','Site'])
        mrns = df[temp]
        null_mask = mrns.isnull().any(axis=1)
        mrns = mrns.loc[null_mask]

    mrns = mrns['MRN'].values

    temp_df = None
    for i in range(len(mrns)):
        m = mrns[i]
        in_study = {'MRN': [m]}

        if m in parentstudies:
            pids = parentstudies[m]
            s = list(set([sites[p] for p in pids]))
            if len(pids) > 1:
                in_study['ParentStudy'] = [None]
            else:
                in_study['ParentStudy'] = pids

            if len(s) == 1:
                in_study['Site'] = [s[0]]
            else:
                in_study['Site'] = [None]
                print(f'Participant {m} is listed in parent studies from multiple sites. Please check and manually insert site if you would like this participant to be included.')

            for s in studies:
                in_study[s] = [(s in pids)]
        else:
            curr_m = df.loc[df['MRN'] == m]
            curr_s = parentstudies['sites']
            if not pd.isnull(curr_m['ParentStudy'].values[0]):
                curr_id = curr_m['ParentStudy'].values[0]
                in_study['ParentStudy'] = [curr_id]
                
                if curr_id in curr_s:
                    in_study['Site'] = [curr_s[curr_id]]
                elif not pd.isnull(curr_m['Site'].values[0]):
                    in_study['Site'] = [curr_m['Site'].values[0]]
                else:
                    print(f'{curr_id} is not associated with a site. Potentially, there is no report associated with this study in the input folder.')
                    in_study['Site'] = [None]
                
                if curr_id in studies:
                    for s in studies:
                        in_study[s] = [(s == curr_id)]
                else:
                    for s in studies:
                        in_study[s] = [False]
            else: 
                in_study['ParentStudy'] = [None]
                in_study['Site'] =[None]
                for s in studies:
                    in_study[s] = [False]

        if temp_df is None:
            temp_df = pd.DataFrame(in_study)
        else:
            add_df = pd.DataFrame(in_study)
            temp_df = pd.concat([temp_df, add_df])

    df = df.merge(temp_df, how = 'left', on = 'MRN')
    mcols = [d for d in df.columns.values if '_x' in d]
    mcols = [d.replace('_x','') for d in mcols]

    df = merge_xy(df, 'ParentStudy', x_first=True)
    mcols.remove('ParentStudy')

    #HARDCODED
    df.loc[df['MRN'] == 14233229.0, 'Site_x'] = 'FLA'
    df.loc[df['MRN'] == 14233229.0, 'Site_y'] = 'FLA'

    #special merge for 'Site' as well? want a check that for none null, that both sites are the same, otherwise keep the first value? Don't overwrite site
    subset = df.loc[~(pd.isnull(df['Site_x'])) & ~(pd.isnull(df['Site_y']))]
    subset = subset.loc[subset['Site_x'] != subset['Site_y']]
    if not subset.empty:
        multi_m = subset['MRN'].to_list()
        print(f'The following participants are in studies from multiple sites. Please check and determine which site they should be in manually: {multi_m}')
        df.loc[df['Site_x'] != df['Site_y'], 'Site_x'] = None 
        df.loc[df['Site_x'] != df['Site_y'], 'Site_y'] = None 

    df = merge_xy(df, 'Site', x_first=True)
    mcols.remove('Site')

    for c in mcols:
        df = merge_xy(df, c, x_first=False) 
    #df = df.drop('parentstudynameid', axis=1)
    return df

def clean_consent(data):
    """
    Clean consent questions of extraneous characters

    :param data: dataframe to clean
    :return data: cleaned dataframe
    """
    qs = ['I permit Mayo Clinic to give my speech samples and the impression from the speech pathologist to researchers at business partners such as Google:',
          'I agree to provide additional recordings during my participation in the study. I understand that even if I say yes now, I can change my mind when asked in the future.',
          'I permit Mayo Clinic to use my speech samples and impressions from speech pathologists in other research projects in the future.',
          'I permit Mayo Clinic to use the information and tools that result from this study, including from my speech samples, for commercial purposes:',
          'I permit Mayo Clinic to give my speech samples and the impression from the speech pathologist to researchers at other institutions:']

    questions = data['Question'].values
    new_qs = questions.copy()

    for q in qs:
        pattern = re.compile(q)
        for i in range(len(questions)):
            curr = questions[i]
            if bool(re.search(pattern, curr)):
                new_qs[i] = q

    data['Question'] = new_qs

    data['LastName'] = [s.split(",")[0] for s in data['Name'].values]
    data['FirstName'] = [s.split(",")[1].split(" ")[1] for s in data['Name'].values]
    #data['MC Number'] = data['MC Number'].astype('string')

    ### edit column names - remove spaces,add qualtrics in front of name, select only relevant columns
    # cols = data.columns.values
    # cols = ["".join(c.split(" ")) for c in cols]
    # cols[cols.index('MCNumber')] = 'MRN'
    # cols[cols.index('Participant#')] = 'ParticipantNo'
    # cols[cols.index('Study-Participant#')] = 'StudyParticipantNo'
    # cols[cols.index('parentstudynameid')] = 'ParentStudy'

    # data.columns = cols

    # data = data[['MRN', 'FirstName', 'LastName', 'ParentStudy','Site', 'ParticipantNo', 'StudyParticipantNo', 'Version', 'VersionDate','IRBDocumentName','PTraxDocumentName','DateSigned','Response','Question']]
    # ##other thing, add parentstudyname id and such based on master
    return data

### Data specific main functions
def read_mc_rsh_ai(to_decrypt, decrypt_date, to_filter, code):
    """
    Decrypt and rename columns from MC_RSH_AI xlsx files

    :param to_decrypt: list of str paths to files to decrypt
    :param decrypt_date: list of dates extracted from the files to be used as the Initial Date

    :return to_bind: list of pandas dataframes from the decrypted and read in xlsx files
    """
    to_bind = []
    for i in range(len(to_decrypt)):
        f = to_decrypt[i]
        data = decrypt(f, code=code)

        #get epic status
        f = str(f)
        status = f.split("_")[-4:-2]
        if status[0] == 'Speech':
            status= [status[-1]]
        status = " ".join(status).lower()
        data['EpicStatus'] = status

        #rename columns to not have spaces
        cols = data.columns.values
        cols = ["".join(c.translate(str.maketrans('','', string.punctuation)).split(" ")) for c in cols]
        cols[cols.index('PatientPreferredFirstName')] = "FirstName"
        cols[cols.index('PtEmailAddress')] = "EmailAddressEpic"
        data.columns = cols

        data['LastName'] = data['LastName'].apply(str.lower)
        data['FirstName'] = data['FirstName'].apply(str.lower)
        data['EmailAddressEpic'] = data['EmailAddressEpic'].replace(np.nan,'').apply(str.lower)
        #set initial date to the file date
        data['EpicDate'] = decrypt_date[i]
        data['InitialDate'] = decrypt_date[i]
        data['InitialFile'] = str(to_decrypt[i].name)
        data['RecentDate'] = decrypt_date[i]
        data['RecentFile'] = str(to_decrypt[i].name)

        #data.update(qualtrics_mrn)
        nan_mrns = data.loc[np.isnan(data['MRN'])]
        if not nan_mrns.empty:
            to_drop = nan_mrns.index.to_list()
            data = data.drop(to_drop, axis=0)
            print('NaN MRN values existed in table. These values were dropped from the database.')

        data = data.drop_duplicates() 
        data = filter_df(data, to_filter, 'EmailAddressEpic') #are any of the duplicates typos in first/last/email? are any of them people we want to exclude?

        if len(data['MRN'].values) != len(list(set(data['MRN'].values))):
            raise Exception('Duplicate MRNs within a sheet - was not handled by dropping duplicates or filtering')
        
        to_bind.append(data)
    return to_bind

def bind_mc_rsh_ai(to_bind):
    """
    Bind MC_RSH_AI files

    :param to_bind: list of pandas dataframes from the decrypted and read in xlsx files
    :return bind: merged dataframes
    """
    bind = pd.concat(to_bind)
    bind = bind.reset_index(drop=True)
    #total duplicates should have been handled when reading in each individual file
    #any remaining duplicate MRNs should only be because of EpicStatus. 
    bind = drop_by_estatus(bind)
    if len(bind['MRN'].values) != len(list(set(bind['MRN'].values))):
        raise Exception('Duplicate MRNs within a sheet - was not handled by dropping duplicates or filtering')
        
    return bind
    
def ptrax(tl,tl_date, to_filter):
    """
    Read in ptrax file

    :param tl: string file name for ptrax file
    :param tl_date: datetime of file
    :param to_filter: dataframe of participants to exclude
    :return data: read in and filtered ptrax data
    """
    #read in and edit column names
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
    cols[cols.index('EmailAddress')] = 'EmailAddressPtrax'
    cols[cols.index('parentstudynameid')] = 'ParentStudy'

    data.columns = cols
    data = data[['MRN','LastName','FirstName','EmailAddressPtrax','PtraxStatus','PtraxDate','PtraxReason','PtraxComment','ParentStudy','Site','ParticipantNo','StudyParticipantNo','PendingActions','AccruedDate','Gender','DateofBirth','Age','AgeatConsent','DeceasedDate','Ethnicity','Race','RecruitmentOption']]
    data['LastName'] = data['LastName'].apply(str.lower)
    data['FirstName'] = data['FirstName'].apply(str.lower)
    data['EmailAddressPtrax'] = data['EmailAddressPtrax'].replace(np.nan, '').apply(str.lower)
    data['PendingActions'] = data['PendingActions'].replace(np.nan, '')
    data['MRN'] = data[['MRN']].astype(int)
    data['InitialDate'] = tl_date
    data['InitialFile'] = str(tl.name)
    data['RecentDate'] = tl_date
    data['RecentFile'] = str(tl.name)
    
    #data.update(qualtrics_mrn)
    nan_mrns = data.loc[np.isnan(data['MRN'])]
    if not nan_mrns.empty:
        to_drop = nan_mrns.index.to_list()
        data = data.drop(to_drop, axis=0)
        print('NaN MRN values existed in table. These values were dropped from the database.')

    # fix PtraxStatus based on pending actions
    pending = data['PendingActions'].values
    status = data['PtraxStatus'].values
    for i in range(len(pending)):
        p = pending[i]
        if p == 'Pending Consent' or p == 'Reconsent may be required':
            status[i] = 'Consent Prepared'
    data['PtraxStatus'] = status

    data = data.drop_duplicates() #drop any exact duplicates
    data = filter_df(data, to_filter, 'EmailAddressPtrax') #filter 
    #if anything is left after excluding, drop based on PendingActions
    data = drop_by_action(data)

    if len(data['MRN'].values) != len(list(set(data['MRN'].values))):
        raise Exception('Duplicate MRNs within a sheet - was not handled by dropping duplicates or filtering')
    
    return data

def qualtrics(q, q_date, to_filter, qualtrics_mrn):
    """
    Read in qualtrics data
    :param q: string file name for qualtrics file
    :param q_date: date of the file
    :param to_filter: dataframe of participants to exclude
    :param qualtrics_mrn: dataframe with MRN lookup table for qualtrics participants without an MRN
    :return data: read in and filtered qualtrics data
    """
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        data = pd.read_excel(q, engine='openpyxl')

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

    status, date = get_qualtrics_status(data)
    data['QualtricsDate'] = date
    data['QualtricsStatus'] = status
    data['InitialDate'] = q_date
    data['InitialFile'] = str(q.name)
    data['RecentDate'] = q_date
    data['RecentFile'] = str(q.name)
    
    data = data[['MRN','LastName','FirstName','EmailAddress','InitialDate','InitialFile', 'RecentDate','RecentFile', 'QualtricsStatus','QualtricsDate','QualtricsLink', 'QualtricsSent','QualtricsOpened','QualtricsStarted','QualtricsFinished','QualtricsUnsubscribed']]

    data['LastName'] = data['LastName'].apply(str.lower)
    data['FirstName'] = data['FirstName'].apply(str.lower)
    data['EmailAddress'] = data['EmailAddress'].apply(str.lower)

    data = data.merge(qualtrics_mrn, on=['LastName','FirstName','EmailAddress'], how='left')
    if 'MRN_x' in data.columns:
        data = merge_xy(data, 'MRN')
    
    data = data.reset_index(drop=True)
    #handle duplicates
    data = data.drop_duplicates()
    data = filter_df(data, to_filter)
    data = drop_by_qstatus(data)

    cols = data.columns.to_list()
    cols[cols.index('EmailAddress')] = 'EmailAddressQualtrics'
    data.columns = cols

    #data.update(qualtrics_mrn)
    nan_mrns = data.loc[np.isnan(data['MRN'])]
    if not nan_mrns.empty:
        to_drop = nan_mrns.index.to_list()
        data = data.drop(to_drop, axis=0)
        print('NaN MRN values existed in table. These values were dropped from the database.')

    if len(data['MRN'].values) != len(list(set(data['MRN'].values))):
        raise Exception('Duplicate MRNs within a sheet - was not handled by dropping duplicates or filtering')
    
    return data

def update_master(master, data, to_filter):
    """
    Update the master database

    :param master: dataframe, master database
    :param data: dataframe, read-in files
    :param to_filter: dataframe of participants to exclude

    :return master_xy: dataframe, updated master database
    """
    date_cols = [d for d in master.columns.to_list() if 'Date' in d]
    date_cols.remove('InitialDate')
    date_cols.remove('RecentDate')
    for d in date_cols:
        master = convert_todatetime(master, d, dt_format='%Y%m%d')
    full_cols = ['InitialDate','RecentDate','QualtricsSent','QualtricsOpened','QualtricsStarted','QualtricsFinished','QualtricsUnsubscribed']
    for d in full_cols:
        master = convert_todatetime(master, d, dt_format='%Y.%m.%d.%H.%M')
    master = filter_df(master, to_filter, 'EmailAddressEpic') #have to filter once for each
    master = filter_df(master, to_filter, 'EmailAddressPtrax')
    master = filter_df(master, to_filter, 'EmailAddressQualtrics')

    master_xy = pd.merge(master, data, how='outer',on=['MRN'])

    mcols = [d for d in master_xy.columns.values if '_x' in d]
    mcols = [d.replace('_x','') for d in mcols]

    ## MERGING
    if 'ParentStudy' in mcols:
        master_xy = merge_xy(master_xy, 'ParentStudy', x_first=True)
        mcols.remove('ParentStudy')
    # Don't overwrite parentstudyname - new name though
    #TODO: parent studyyyyyyy
    #master_xy = merge_xy(master_xy, 'ParentStudy', x_first=True)
    #mcols.remove('ParentStudy')

    # Dates - initial, take earliest, recent, take most recent
    master_xy = merge_by_date(master_xy, 'InitialFile', ['InitialDate_x','InitialDate_y'], late=False)
    master_xy = merge_by_date(master_xy, 'RecentFile', ['RecentDate_x','RecentDate_y'], late=True)
    master_xy = merge_date(master_xy, 'InitialDate', by='early')
    master_xy = merge_date(master_xy, 'RecentDate', by='late')

    mcols.remove('InitialFile')
    mcols.remove('RecentFile')
    mcols.remove('InitialDate')
    mcols.remove('RecentDate')

    ## Epic Status merge
    master_xy = merge_interested(master_xy)
    mcols.remove('EpicStatus')

    # All other dates update based on if they aren't blank in the new one
    for c in mcols:
        master_xy = merge_xy(master_xy, c, False)

    ### CONVERT DATE COLS BACK TO STRINGS
    for d in date_cols:
        master_xy = convert_tostring(master_xy, d, dt_format='%Y%m%d')
    for d in full_cols:
        master_xy = convert_tostring(master_xy, d, dt_format='%Y.%m.%d.%H.%M')

    return master_xy

### MAIN FUNCTION
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--py_path", default=None)
    parser.add_argument("--input_path", default="./uploaded_sheets")
    parser.add_argument("--input_archive", default="./uploaded_sheets/archive")
    parser.add_argument("--output_path", default="./output_sheets")
    parser.add_argument("--output_archive",default="./output_sheets/archive")
    #parser.add_argument("--report_paths", nargs="+", default=['/Volumes/AI_Research/Speech/ParticipantTracking/uploaded_sheets','/Volumes/AI_Research/Speech/ParticipantTracking/uploaded_sheets/archive','/Volumes/AI_Research/Speech/ParticipantTracking/uploaded_sheets/archive/old_sheets'], help="specify all full directory paths that may contain sheets")
    parser.add_argument("--to_filter", default="filter.csv")
    parser.add_argument("--qualtrics_mrn", default='./code/qualtrics_mrn/qualtrics_mrn.csv')
    parser.add_argument("--load_existing", type=ast.literal_eval, default=True)
    parser.add_argument("--decrypt_code",default='0726', help='specify code for decrypting file as a string')
    args = parser.parse_args()

    # set python path
    if args.py_path is None or args.py_path == '':
        #set path to parent directory of current path
        py_path = Path(__file__).absolute()
        args.py_path = py_path.parents[1]
    else:
        args.py_path = Path(args.py_path).absolute()
    
    os.chdir(args.py_path)


    #get files in the proper path format for the system
    args.input_path = Path(args.input_path).absolute()
    args.input_archive = Path(args.input_archive).absolute()
    args.output_path = Path(args.output_path).absolute()
    args.output_archive = Path(args.output_archive).absolute()
    args.to_filter = Path(args.to_filter).absolute()
    args.qualtrics_mrn = Path(args.qualtrics_mrn).absolute()

    # assert input path exists
    assert args.input_path.exists(), f'Input path must exist. Please confirm this was the desired path: {args.input_path}'
    print(f'Combining files in {args.input_path}')
    # make sure other paths exist
    if not args.output_path.exists():
        os.makedirs(args.output_path)
    if not args.input_archive.exists():
        os.makedirs(args.input_archive)
    if not args.output_archive.exists():
        os.makedirs(args.output_archive)

    # read-in filter csv
    assert '.csv' in str(args.to_filter), f'{args.to_filter} is not a csv. Must be a csv file.'
    to_filter = pd.read_csv(args.to_filter)

    # (1) read in files to merge - will ALWAYS grab the most recent file
        
    ## MC_RSH_AI
    idfd, idfd_date = get_most_recent(pat=args.input_path / 'MC_RSH_AI_Speech_Identified*.xlsx', sep="_",
                                      dt_ind=[-2,-1], dt_format='%Y%m%d%H%M', data_ext='.xlsx')

    inr, inr_date = get_most_recent(pat=args.input_path / 'MC_RSH_AI_Speech_Interested*.xlsx', sep="_",
                                      dt_ind=[-2,-1], dt_format='%Y%m%d%H%M', data_ext='.xlsx')

    n_int, n_int_date = get_most_recent(pat=args.input_path / 'MC_RSH_AI_Speech_Not_Interested*.xlsx', sep="_",
                                       dt_ind=[-2,-1], dt_format='%Y%m%d%H%M', data_ext='.xlsx')

    assert idfd !='', 'Error finding Identified sheet in input directory. Please check that the files have the correct format (MC_RSH_AI_Identified*YYYYMMDD_HHMM.xlsx).'
    assert inr !='', 'Error finding Interested sheet in input directory. Please check that the files have the correct format (MC_RSH_AI_Interested*YYYYMMDD_HHMM.xlsx).'
    assert n_int !='', 'Error finding Not Interested sheet in input directory. Please check that the files have the correct format (MC_RSH_AI_Not_Interested*YYYYMMDD_HHMM.xlsx).'

    ## PTRAX
    tl, tl_date = get_most_recent(pat= args.input_path / '[0-9]*trackingLog*.xlsx', sep="_",
                                dt_ind=-1, dt_format='%Y%m%d%H%M', data_ext='.xlsx')

    assert tl != '', 'Error finding Ptrax tracking log file in input directory. Please check that the files have the correct format ([0-9]*trackingLog_YYYYMMDDHHMM.xlsx)'

    ## QUALTRICS
    q, q_date = get_most_recent(pat=args.input_path / 'dashboard-export*.xlsx', sep="-",
                                dt_ind=[2,3,5,6,7], dt_format='%H%M%Y%m%d', data_ext='.xlsx')

    q2, q_date2 = get_most_recent(pat= args.input_path / 'qualtrics_tracking*.xlsx', sep="_",
                                    dt_ind=-1, dt_format='%Y%m%d', data_ext='.xlsx') #########################################Hugo changed sep to '_' 20231208

    if q != '' and q2 != '':
        if q_date2 > q_date:
            q = q2
            q_date = q_date2
    elif q == '' and q2 != '':
        q = q2
        q_date = q_date2
    else:
        assert q != '', 'Error finding qualtrics file in input directory. Please confirm the qualtrics files are either in the form (dashboard-export-HH-MM-*-YYYY-MM-DD.xlsx) or (qualtrics_tracking_YYYYMMDD.xlsx)'

    #briefly check which file is the most recent
    date = [idfd_date, inr_date, n_int_date, tl_date]
    max_date_ind = date.index(max(date))

    # (2) MC_RSH_AI read-in and prepare for merging
    #handle selecting files to decrypt
    to_decrypt = [n_int, inr, idfd]
    decrypt_date = [n_int_date, inr_date, idfd_date]
    to_bind = read_mc_rsh_ai(to_decrypt, decrypt_date, to_filter, args.decrypt_code)
    data_mra = bind_mc_rsh_ai(to_bind) #this dataframe contains basic info + epic status and initial date from the MOST RECENT mc rsh ai files

     #check if these files are the most recent, and if yes, store that file as curr_values
    if max_date_ind < 3:
        curr_values = to_bind[max_date_ind]

    print('MC RSH AI files read in')

    # (3) Ptrax - prep for merging
    data_pt = ptrax(tl, tl_date, to_filter)

    #check if this file is the most recent, and if yes, store that file as curr_values
    if max_date_ind == 3:
        curr_values = data_pt

    print('Tracking log read in')

    # (4) Qualtrics - prep for merging
    qualtrics_mrn = pd.read_csv(args.qualtrics_mrn)
    data_q = qualtrics(q, q_date, to_filter, qualtrics_mrn)

    print('Qualtrics read in')

    # (5) Bind sheets
    data = merge_sheets(data_mra, data_pt, ['EpicDate','PtraxDate']) #first compare epic date vs. ptrax date and keep whichever is the most recent
    data = merge_sheets(data, data_q, ['RecentDate_x','QualtricsDate']) #then compare the most recent date of file vs the qualtrics date to prioritize non-qualtrics

    data['EpicStatus'] = data['EpicStatus'].replace(np.nan, 'new interested')

    print('Sheets merged')


    # (6) Update the master database
    master_file, master_date = get_most_recent(pat = args.output_path / 'master_database*.csv', sep="_",
                                               dt_ind=-1, dt_format='%Y%m%d-%H%M%S', data_ext='.csv')
    master = pd.read_csv(master_file) #deal with dates
    cols = master.columns.to_list()
    cols[cols.index('parentstudynameid')] = 'ParentStudy'
    master.columns = cols
    master = update_master(master, data, to_filter)
    
    print('Master updated')

    # (7) fill in empty parentstudies
    master = pop_parentstudy(master, args.input_path, args.output_path, args.load_existing, args.input_archive, args.decrypt_code)
    #I have a better idea for how to access this, 
    print('Parent study populated')
    cols = master.columns.to_list()
    cols[cols.index('ParentStudy')] = 'parentstudynameid'
    master.columns = cols

    #master = update_master(master, data, to_filter)

    #(8) save output files
    current_date = datetime.today().strftime("%Y%m%d-%H%M")

    #to_archive = glob.glob(str(args.output_path / 'master_database*.csv'))
    #archive(to_archive, args.output_archive)

    out_path = args.output_path / f'master_database_{current_date}.csv'
    master.to_csv(out_path, index=False)
    print('Master database saved')

    #save consent questions, archive old
    #to_archive = glob.glob(str(args.output_path / 'consent_questions*.csv'))
    #archive(to_archive, args.output_archive)

    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        data = pd.read_excel(tl, sheet_name='ConsentQuestions', engine='openpyxl')

    data = clean_consent(data)
    out_path = args.output_path / f'consent_questions_{current_date}.csv'
    data.to_csv(out_path, index=False)
    print('Consent questions saved')

     #save new interested, archive old
    #to_archive = glob.glob(str(args.output_path / 'NewInterested*.csv'))
    #archive(to_archive, args.output_archive)

    
    new_interested = master.loc[(master['EpicStatus'] == 'interested') & (pd.isnull(master['PtraxStatus'])) & (pd.isnull(master['QualtricsStatus'])) & ~(pd.isnull(master['Site']))] #to be new interested, thye must have null ptrax and qualtrics status, have epic status as interested, and have a Site value
    #TODO: get most recent email address as well?
    new_interested = new_interested[['MRN','FirstName','LastName','EmailAddressEpic', 'EmailAddressPtrax', 'EmailAddress', 'Site']]
    skipped_new =master.loc[(master['EpicStatus'] == 'interested') & (pd.isnull(master['PtraxStatus'])) & (pd.isnull(master['QualtricsStatus'])) & (pd.isnull(master['Site']))] #to be new interested, thye must have null ptrax and qualtrics status, have epic status as interested, and have a Site value
    
    if not skipped_new.empty:
        print('Some participants were excluded from NewInterested because they did not have a value for site. Please check the master database for these potential cases.')
        #to_archive = glob.glob(str(args.output_path / 'missingsite*.csv'))
        #archive(to_archive, args.output_archive)
        out_path = args.output_path / f'missingsite_{current_date}.csv'
        skipped_new.to_csv(out_path, index=False)
        

    out_path = args.output_path / f'NewInterested_{current_date}.csv'
    new_interested.to_csv(out_path, index=False)
    print('New Interested saved')
    
    



if __name__ == '__main__':
    main()

"""
Populate master database

Author(s): Daniela Wiepert
Last Modified: 12/30/2023
"""
#IMPORTS
#built-in
import argparse
import glob
import io
import json
import os
import pickle
import string
import warnings

from datetime import datetime

#third-party
import pandas as pd
import numpy as np

from lookup_tables import decrypt, glob_paths

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

def get_min_max_date(mrn_dates):
    """
    Get the initial date and most recent date / file names for a type of sheet
    (i.e. initial/most recent for all identified MC_RSH_AI or all tracking log)
    """
    files = mrn_dates['files']
    dates = mrn_dates['dates']

    if files == [] and dates == []:
        return None, None, None, None

    min_ind = dates.index(min(dates))
    max_ind = dates.index(max(dates))

    initial_date = dates[min_ind]
    initial_file = files[min_ind]
    recent_date = dates[max_ind]
    recent_file = files[max_ind]

    return initial_date, initial_file, recent_date, recent_file

def get_absolute_min_max(mrn_dates):
    """
    Get the absolute min max dates/files across all sheet types
    """
    #exclude qualtrics
    initial = None
    initial_f = None
    recent = None
    recent_f = None


    for k in mrn_dates:
        #if k != 'qualtrics':
        idd, iff, rd, rf = get_min_max_date(mrn_dates[k])

        if initial == None:
            initial = idd
            initial_f = iff
        else:
            if not idd is None: #only update if there is sth to update with
                if idd < initial:
                    initial = idd
                    initial_f = iff

        if recent == None and k != 'qualtrics': #we only care about qualtrics if it is the initial one
            recent = rd
            recent_f = rf
        elif not rd is None and k!='qualtrics':
                if rd > recent:
                    recent = rd
                    recent_f = rf
        
    if recent == None: #if after everything, recent is still none
        recent = initial
        recent_f = initial_f


    return initial, initial_f, recent, recent_f

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


def earliest_qdate(same, inds):
    """
    Determine earliest QualtricsDate

    :param same: dataframe with duplicated participants
    :param inds: inds to find earliest date from
    :return ind: ind of earliest date
    """
    dates = []
    for i in inds:
        date = same.loc[same.index==i]['QualtricsDate'].values[0]
        dates.append(date)

    ind = dates.index(min(dates))
    return ind

def drop_by_qstatus(data):
    """
    Drop duplicate participants based on their qualtrics status. Survey Finished > Email Opened > Email Sent

    :param data: dataframe pre-dropping
    :return data: dataframe with dropped duplicates
    """
    dups = np.where(data['EmailAddress'].duplicated())[0]
    to_drop = []
    for d in dups:
        same = data.loc[data['EmailAddress'] == data['EmailAddress'].to_list()[d]]
        #### check 2) is any of the status 'survey finished?'
        indices = same.index.to_list()
        s = same.loc[same['QualtricsStatus'] == 'Survey Finished'].index.to_list()
        o = same.loc[same['QualtricsStatus'] == 'Email Opened'].index.to_list()
        if s != []:
            #find earliest one
            ind = earliest_qdate(same,s)
            del indices[ind]
            to_drop.extend(indices)
        elif o != []:
            ind = earliest_qdate(same,o)
            del indices[ind]
            to_drop.extend(indices)
        else:
            ind = earliest_qdate(same,indices)
            del indices[ind]
            to_drop.extend(indices)

    data = data.drop(index=to_drop, axis=0)
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


def generate_parentstudy(reports, out_path,  code='', dt_ind=-2, dt_format='%Y%m%d', data_ext ='.xlsx', sep="_"):
    parentstudy = {}
    studies = []
    sites_dict = {}
    for r in reports:
        if isinstance(dt_ind, int):
            date = r.split(sep)[dt_ind].replace(data_ext,"")

        elif isinstance(dt_ind, list):
            dsplit = r.split(sep)
            to_join = []
            for i in dt_ind:
                to_join.append(dsplit[i])
            date = "".join(to_join)
            date = date.replace(data_ext, "")
        
        split_name = r.replace(os.path.join(os.path.dirname(r),'MC_RSH_AI_Speech_'),'').split(sep="_")
        study_id = "_".join(split_name[:-2]).lower()
        if 'jax' in study_id:
            site = 'FLA'
        else:
            site = 'RST'

        data = decrypt(r, code=code)
        parentstudy[r] = {'studyid': study_id, 'data': data, 'date': date, 'dt_format': dt_format, 'site':site}
        if study_id not in studies:
            studies.append(study_id)
            sites_dict[study_id] = site
    
    
    mrn_parentstudy = {}
    mrn_parentstudy['studies'] = studies
    mrn_parentstudy['files'] = list(parentstudy.keys())
    mrn_parentstudy['sites'] = sites_dict

    for k in parentstudy:
        data = parentstudy[k]['data']
        mrns = data['MRN'].values
        sid = parentstudy[k]['studyid']
        site = parentstudy[k]['site']

        if isinstance(mrns[0], str):
            mrns = [float(c.translate(str.maketrans('','', string.punctuation))) for c in mrns]
        
        assert isinstance(mrns[0], float)
        
        for m in mrns:

            if m not in mrn_parentstudy:
                mrn_parentstudy[m] = {'studyid':[sid], 'site':[site]}
            else:
                temp = mrn_parentstudy[m]
                temp_sid = temp['studyid']
                if sid not in temp_sid:
                    temp_sid.append(sid)

                temp_site = temp['site']
                if site not in temp_site:
                    temp_site.append(site)

                mrn_parentstudy[m] = {'studyid': temp_sid, 'site': temp_site}

    dbfile = open(os.path.join(out_path, 'parentstudy_lookup.pkl'), 'wb')
    # source, destination
    pickle.dump(mrn_parentstudy, dbfile)                    
    dbfile.close()
    return mrn_parentstudy

def load_parentstudy(report_paths, out_path, code=''):
    reports = glob_paths(report_paths, 'MC_RSH_AI_Speech*.xlsx')
    reports = [r for r in reports if 'Identified' not in r]
    reports = [r for r in reports if 'Interested' not in r]

    if os.path.exists(os.path.join(out_path, 'parentstudy_lookup.pkl')):
        dbfile = open(os.path.join(out_path,'parentstudy_lookup.pkl'), 'rb')
        parentstudy_table = pickle.load(dbfile)
        dbfile.close()

        if not all([r in parentstudy_table['files'] for r in reports]):
             parentstudy_table = generate_parentstudy(reports, out_path, code=code)

    else:
        parentstudy_table = generate_parentstudy(reports, out_path, code=code)
    
    return parentstudy_table

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

#### POPULATE DATAFRAME

def pop_basic_info(mrn_dates, lookup, linked_participants):
    """
    Get basic info (name/email address) from the EARLIEST file for a patient
    Compare to the most recent file for a patient
    """
    mrns = list(mrn_dates.keys())
    initials = []
    initial_files = []
    recents = []
    recent_files = []
    first = []
    last = []
    email = []

    for i in range(len(mrns)):
        m = mrns[i]
        il, iff, r, rff = get_absolute_min_max(mrn_dates[m])
        initials.append(il) #initial date
        initial_files.append(iff) #initial file
        recents.append(r)
        recent_files.append(rff)


        ### GET NAME & EMAIL FROM LINKED ONE? CHECK IF MORE THAN 1
        data = linked_participants.loc[linked_participants['MRN']==m]
        if len(data) > 1:
            print('pause')

        if not data.empty:
            first.append(data['FirstName'].values[0])
            last.append(data['LastName'].values[0])
            email.append(data['EmailAddress'].values[0])
        
        else:
            data_r = lookup[rff]['data']
            first.append(data_r.loc[data_r['MRN']==m]['FirstName'].values[0])
            last.append(data_r.loc[data_r['MRN']==m]['LastName'].values[0])
            email.append(data_r.loc[data_r['MRN']==m]['EmailAddress'].values[0])

        
        ### QUESTION - should we ever change name & email based on updated files...or should we instead manually do this with the link one? or have that dropped in too?
        #first.append(data['FirstName'].values[0])
        #l3ast.append(data['LastName'].values[0])
        #email.append(data['EmailAddress'].values[0])

        # data_i = lookup[iff]['data']
        # eai = data_i.loc[data_i['MRN'] == float(m)]['EmailAddress'].values[0]
        # email.append(eai)
        # data_r = lookup[rff]['data']
        # #compare initial and most recent names/email address. DON'T UPDATE TO MOST RECENT IF MOST RECENT IS QUALTRICS

        # # FIRST NAME
        # fni = data_i.loc[data_i['MRN'] == float(m)]['FirstName'].values[0]
        # fnr = data_r.loc[data_r['MRN'] == float(m)]['FirstName'].values[0]

        # ## ANOTHER QUICK FIRST NAME CHECK:
        #     #find most recent TRACKING LOG

        # ilp, iffp, rp, rffp = get_min_max_date(mrn_dates[m]['ptrax'])
        # if rffp is not None:
        #     #ptrax first names are always the most accurate (FULL NAMES)
        #     data_p = lookup[rffp]['data']
        #     fnp = data_p.loc[data_p['MRN'] == float(m)]['FirstName'].values[0]
        #     first.append(fnp)
        # else:
        #     # CAN'T DO ANYTHING ABOUT NICKNAMES
        #     if fni!=fnr:
        #         first.append(fnr) #if they are different, use the most recent (assuming most recent is most up to date) 
        #     else:
        #         first.append(fni) #if they are the same, arbitrarily use the first one


        # lni = data_i.loc[data_i['MRN'] == float(m)]['LastName'].values[0]
        # lnr = data_r.loc[data_r['MRN'] == float(m)]['LastName'].values[0]
        # if lni!=lnr:
        #     last.append(lnr)
        # else:
        #     last.append(lni)


        # eai = data_i.loc[data_i['MRN'] == float(m)]['EmailAddress'].values[0]
        # ear = data_r.loc[data_r['MRN'] == float(m)]['EmailAddress'].values[0]
        # if eai!=ear:
        #     email.append(ear)
        # else:
        #     email.append(eai)
    
    ## TODO: DEAL WITH CONVERTING DATES
    return pd.DataFrame({'MRN':mrns, 'FirstName':first, 'LastName':last, 'EmailAddress':email, 'InitialDate': initials, 'InitialFile': initial_files, 'RecentDate': recents, 'RecentFile': recent_files})

def pop_mc_rsh_ai(df, mrn_dates, lookup):
    """
    Get the most recent epic status for a patient
    """

    mrns = df['MRN'].values
    epic_status = []
    epic_date = []
    email = []

    for i in range(len(mrns)):
        m = mrns[i]

        d_mrn = df.loc[df['MRN'] == m]
        temp = lookup[d_mrn['RecentFile'].values[0]]['data']
        temp = temp.loc[temp['MRN']==m]

        email.append(temp['EmailAddress'].values[0])

        _, _, ninr, ninrf = get_min_max_date(mrn_dates[m]['not_interested'])

        if ninrf is not None:
            #if there is a file with not interested for this patient, then they are not interested
            epic_status.append('not interested')
            epic_date.append(ninr)
            continue

        _, _, inrr, inrf = get_min_max_date(mrn_dates[m]['interested'])

        if inrf is not None:
            #if there is a file with interested and they don't have a file with not interested, then they are interested
            epic_status.append('interested')
            epic_date.append(inrr)
            continue


        _, _, idrr,idrf = get_min_max_date(mrn_dates[m]['identified'])
        if idrf is not None:
            #if there is a file with identified, and they haven't been moved to interested/not interested yet, then they are interested
            epic_status.append('identified')
            epic_date.append(idrr)
            continue

        #FINALLY, if they aren't on any of those, they are considered new interested
        epic_status.append('new interested')
        epic_date.append(np.datetime64('NaT'))
    
    df['EpicStatus'] = epic_status
    df['EmailAddressEpic'] = email
    df['EpicDate'] = epic_date
    return df

def pop_ptrax(df, mrn_dates, lookup):
    """
    Populate all the Ptrax information from the most recent ptrax file
    """
    mrns = df['MRN'].values

## TODO: DEAL WITH DUPLICATES, DEAL W DATES
    temp_df = None
    for i in range(len(mrns)):
        m = mrns[i]
        #m_ind = df.loc[df['MRN'] == m].index.values[0]
        _, _, _, rf = get_min_max_date(mrn_dates[m]['ptrax'])
        if rf is not None:
            data = lookup[rf]['data']
            data['MRN'] = data['MRN'].astype(np.float64)
            data = data.loc[data['MRN'] == m]
            data = data[['MRN', 'EmailAddress', 'PtraxStatus','PtraxDate','PtraxReason','PtraxComment','parentstudynameid','Site','ParticipantNo','StudyParticipantNo','PendingActions','AccruedDate','Gender','DateofBirth','Age','AgeatConsent','DeceasedDate','Ethnicity','Race','RecruitmentOption']]

            cols = data.columns.to_list()
            cols[cols.index('EmailAddress')] = 'EmailAddressPtrax'
            cols[cols.index('parentstudynameid')] = 'ParentStudy'
            data.columns = cols

            ## TODO: set some type of flag if there are still more than 1 after the whole thing?
            #data['MRN'] = data['MRN'].astype(np.float64)

            # fix PtraxStatus based on pending actions
            pending = data['PendingActions'].values
            status = data['PtraxStatus'].values
            for i in range(len(pending)):
                p = pending[i]
                if p == 'Pending Consent' or p == 'Reconsent may be required':
                    status[i] = 'Consent Prepared'

            data['PtraxStatus'] = status

            data = data.drop_duplicates()
            data = drop_by_action(data)

            if len(data) > 1:
                print('pause')

            if temp_df is None:
                temp_df = data
            else:
                temp_df = pd.concat([temp_df, data])
            # data = data.to_dict('list')
            # for k in data:
            #     df.at[m_ind,k] = data[k]
            
            # #df = df.merge(data, how='left', on='MRN')
    df = df.merge(temp_df, how = 'left', on = 'MRN')
    return df



def pop_qualtrics(df, mrn_dates, lookup):
    """
    Populate all the Ptrax information from the most recent ptrax file
    """
    mrns = df['MRN'].values
    temp_df = None

## TODO: DEAL WITH DUPLICATES, DEAL W DATES, DEAL WITH OTHER WEIRD QUALTRICS THINGS!!
    for i in range(len(mrns)):
        m = mrns[i]
        _, _, _, rf = get_min_max_date(mrn_dates[m]['qualtrics'])
        if rf is not None:
            data = lookup[rf]['data']
            data['MRN'] = data['MRN'].astype(np.float64)
            data = data.loc[data['MRN'] == m]

            status, date = get_qualtrics_status(data)
            data['QualtricsDate'] = date
            data['QualtricsStatus'] = status

            data = data.drop_duplicates()
            data = drop_by_qstatus(data)

            data = data[['MRN', 'EmailAddress', 'QualtricsDate','QualtricsStatus','QualtricsLink', 'QualtricsSent','QualtricsOpened','QualtricsStarted','QualtricsFinished','QualtricsUnsubscribed']]
            
            cols = data.columns.to_list()
            cols[cols.index('EmailAddress')] = 'EmailAddressQualtrics'
            data.columns = cols
            if len(data) > 1:
                print('pause')

            if temp_df is None:
                temp_df = data
            else:
                temp_df = pd.concat([temp_df, data])
            # data = data.to_dict('list')
            # for k in data:
            #     df.at[m_ind,k] = data[k]
            
            # #df = df.merge(data, how='left', on='MRN')
    df = df.merge(temp_df, how = 'left', on = 'MRN')
    return df

def pop_parentstudy(df, report_path, output_path, code=''):

    parentstudies = load_parentstudy(report_path, output_path, code=code)

    studies = parentstudies['studies']
    temp = [s for s in studies]
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
            m_dict = parentstudies[m]
            pids = m_dict['studyid']
            sites = m_dict['site']
            if len(pids) > 1:
                in_study['ParentStudy'] = [None]
            else:
                in_study['ParentStudy'] = pids

            if all(i == sites[0] for i in sites):
                in_study['Site'] = [sites[0]]
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
    df = merge_xy(df, 'ParentStudy', x_first=True)

    subset = df.loc[~(pd.isnull(df['Site_x'])) & ~(pd.isnull(df['Site_y']))]
    subset = subset.loc[subset['Site_x'] != subset['Site_y']]
    if not subset.empty:
        multi_m = subset['MRN'].to_list()
        print(f'The following participants are in studies from multiple sites. Please check and determine which site they should be in manually: {multi_m}')
        df.loc[df['Site_x'] != df['Site_y'], 'Site_x'] = None 
        df.loc[df['Site_x'] != df['Site_y'], 'Site_y'] = None 

    df = merge_xy(df, 'Site', x_first=True)
    return df
    

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_path", default='/Volumes/AI_Research/Speech/ParticipantTracking/testing/new_master')
    parser.add_argument("--report_paths", nargs="+", default=['/Volumes/AI_Research/Speech/ParticipantTracking/uploaded_sheets','/Volumes/AI_Research/Speech/ParticipantTracking/uploaded_sheets/archive','/Volumes/AI_Research/Speech/ParticipantTracking/uploaded_sheets/archive/old_sheets'], help="specify all full directory paths that may contain sheets")
    parser.add_argument("--link_path", default="/Volumes/AI_Research/Speech/ParticipantTracking/testing/new_master/participants-export-20231227.csv")
    parser.add_argument("--decrypt_code", default='', help="specify the decryption code as a string")
    args = parser.parse_args()

    dbfile = open(os.path.join(args.output_path,'lookup_table.pkl'), 'rb')
    lookup_table = pickle.load(dbfile)
    dbfile.close()

    dbfile = open(os.path.join(args.output_path,'mrn_dates.pkl'), 'rb')
    mrn_dates = pickle.load(dbfile)
    dbfile.close()

    lookup = {}
    for k in lookup_table:
        temp = lookup_table[k]
        for t in temp:
            temp2 = temp[t]
            temp2['type'] = k
            temp[t] = temp2

        lookup.update(temp)

    linked_participants = pd.read_csv(args.link_path)
    new_mrn = []
    for m in linked_participants['MRN'].values:
        if isinstance(m, str):
            new_mrn.append(float(m.translate(str.maketrans('','', string.punctuation))))
        else:
           # print('pause')
            new_mrn.append(float(m))
    linked_participants['MRN'] = new_mrn
    ### TODO: GO THROUGH FOR ANY DUPLICATES!!! COULD STILL HAVE DUPLICATES? HOW?
    ### TODO: CONVERT DATETIME COLUMNS TO STRINGS AGAIN
    
    #TODO: figure out initial dates better bc they're WRONG FOR PTRAX
    df = pop_basic_info(mrn_dates, lookup, linked_participants)

    df = pop_mc_rsh_ai(df, mrn_dates, lookup)

    df = pop_ptrax(df, mrn_dates, lookup) #there are def some duplicates in this process, figure it out

    df = pop_qualtrics(df, mrn_dates, lookup)

    ### NOTE: this code assumes that the parentstudy lookup already exists OR that if it doesn't
    df = pop_parentstudy(df, args.report_paths, args.output_path, args.decrypt_code)

    ### TODO: pause statements to error raising

    full_date = ['InitialDate','RecentDate','QualtricsSent','QualtricsOpened','QualtricsStarted','QualtricsFinished','QualtricsUnsubscribed']
    for d in full_date:
       df = convert_tostring(df, d, dt_format="%Y.%m.%d.%H.%M")
    ymd_dates = df.filter(like='Date').columns.to_list()
    ymd_dates.remove('InitialDate')
    ymd_dates.remove('RecentDate')
    for d in ymd_dates:
       df = convert_tostring(df, d, dt_format="%Y%m%d")

    cols = df.columns.to_list()
    cols[cols.index('ParentStudy')] = 'parentstudynameid'
    df.columns = cols
    
     #(10) save output fies
    current_date = datetime.today().strftime("%Y%m%d-%H%M")


    out_path = os.path.join(args.output_path,f'master_database_{current_date}.csv')
    #check the dupe emails - are they listed as interested?
    df.to_csv(out_path, index=False)
    print('Master database saved')



if __name__ == '__main__':
    main()
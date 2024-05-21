import argparse
import shutil
from pathlib import Path
import os
from combine_sheets_v5 import get_most_recent
import glob
import re


def archive(files, archive_path):
    """
    archive files that are not needed

    :param files: list of files to archive
    :param archive_path: path to move files to

    :return: None
    """
    for f in files:
        f = Path(f)
        dst_path = archive_path / f.name
        if f.exists():
            shutil.move(f, dst_path)




def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--py_path", default=None)
    parser.add_argument("--input_path", default="./uploaded_sheets")
    parser.add_argument("--input_archive", default="./uploaded_sheets/archive")
    parser.add_argument("--output_path", default="./output_sheets")
    parser.add_argument("--output_archive",default="./output_sheets/archive")
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

    inputs = glob.glob(str(args.input_path / '*'))
    outputs = glob.glob(str(args.output_path / '*'))

    files_i = [f for f in inputs if ('.csv' in f or '.xlsx' in f)]
    files_o = [f for f in outputs if ('.csv' in f or '.xlsx' in f)]
    
    #inputs
    idfd, idfd_date = get_most_recent(pat=args.input_path / 'MC_RSH_AI_Speech_Identified*.xlsx', sep="_",
                                      dt_ind=[-2,-1], dt_format='%Y%m%d%H%M', data_ext='.xlsx')

    inr, inr_date = get_most_recent(pat=args.input_path / 'MC_RSH_AI_Speech_Interested*.xlsx', sep="_",
                                      dt_ind=[-2,-1], dt_format='%Y%m%d%H%M', data_ext='.xlsx')

    n_int, n_int_date = get_most_recent(pat=args.input_path / 'MC_RSH_AI_Speech_Not_Interested*.xlsx', sep="_",
                                       dt_ind=[-2,-1], dt_format='%Y%m%d%H%M', data_ext='.xlsx')
    tl, tl_date = get_most_recent(pat= args.input_path / '[0-9]*trackingLog*.xlsx', sep="_",
                                dt_ind=-1, dt_format='%Y%m%d%H%M', data_ext='.xlsx')
    
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


    files_i.remove(str(idfd))
    files_i.remove(str(inr))
    files_i.remove(str(n_int))
    files_i.remove(str(tl))
    files_i.remove(str(q))


    #PARENT STUDIES
    ar = glob.glob(str(args.input_path / 'MC_RSH_AI_Speech*.xlsx'))
    ar = [r for r in ar if 'Identified' not in r]
    ar= [r for r in ar if 'Interested' not in r]
    ar = [r for r in ar if str(args.input_archive.name) not in r]
    for r in ar:
        a, _ = get_most_recent(r,sep="_", dt_ind=[-2,-1], dt_format="%Y%m%d%H%M", data_ext='.xlsx' )
        files_i.remove(str(a))


    #OUTPUTS
    master_file, master_date = get_most_recent(pat = args.output_path / 'master_database*.csv', sep="_",
                                               dt_ind=-1, dt_format='%Y%m%d-%H%M%S', data_ext='.csv')

    files_o.remove(str(master_file))

    cq, _ = get_most_recent(pat = str(args.output_path / 'consent_questions*.csv'), sep="_",
                                               dt_ind=-1, dt_format='%Y%m%d-%H%M%S', data_ext='.csv')
    ni, _ = get_most_recent(pat = str(args.output_path / 'NewInterested*.csv'), sep="_",
                                               dt_ind=-1, dt_format='%Y%m%d-%H%M%S', data_ext='.csv')
    ms, _ = get_most_recent(pat = str(args.output_path / 'missingsite*.csv'), sep="_",
                                               dt_ind=-1, dt_format='%Y%m%d-%H%M%S', data_ext='.csv')

    files_o.remove(str(cq))
    files_o.remove(str(ni))
    files_o.remove(str(ms))
    
    archive(files_i,args.input_archive)
    archive(files_o, args.output_archive)
    print('pause')

if __name__ == '__main__':
    main()
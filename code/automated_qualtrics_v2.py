'''
Automated qualtrics email sending for NewInterested participants

Author(s): Daniela Wiepert
Last Modified: 03/22/2024
'''
### IMPORTS
#built-in
import argparse
import ast
import glob
import os
import string

from pathlib import Path

from datetime import datetime

#third-party
import numpy as np
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

#local
from combine_sheets_v4 import get_most_recent, archive


def open_survey(browser, headless):
    """
    Open the CHROME driver in headless form (doesn't show window).
    TODO: set up to work with other formats

    :param browser: string indicating which browser to use (chrome, edge)
    :return driver: opened survey object
    """
    if browser == "chrome":
        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument("--headless=new")
        #options.headless = headless
        driver = webdriver.Chrome(options=options)
    if browser == "edge":
        options = webdriver.EdgeOptions()
        if headless:
            options.add_argument("--headless=new")
        driver = webdriver.Edge(options=options)
    #driver.get('https://surveys.mayoclinic.org/jfe/form/SV_5sVEtc1ptYkdbkW')
    driver.get('https://surveys.mayoclinic.org/jfe/form/SV_bjD7eVnox5iucCy') #NEW SURVEY

    return driver

def quit_survey(driver):
    """
    Quit the survey post-running

    :param driver: opened survey object
    """
    driver.quit()

def survey_actions(driver, first, last, email, mrn, site):
    """
    Add a participant to the survey

    :param driver: opened survey object
    :param first: first name of participant as a string
    :param last: last name of participant as a string
    :param email: VALID email of participant as a string
    :param mrn: mrn number AS A STRING
    :return: None, completes the actions on the page
    """
    actions = ActionChains(driver)
    #wait = WebDriverWait(driver, 30)
    actions.pause(15).perform()
    # 1) select add participant
    #element11 = wait.until(EC.presence_of_element_located((By.NAME, 'QR~QID1')))
    next = driver.find_element(By.NAME, 'NextButton')
    element11 = driver.find_element(By.NAME, 'QR~QID1')
    # click add participant button, wait 1 second click next button, weight 3 seconds for it to load
    #TODO: have the retrying option in case it takes too long? rather than pausing for three seconds?
    actions.click(element11).pause(.5).click(next).pause(3).perform()

    #next = wait.until(EC.element)
    #next = driver.find_element(By.NAME, 'NextButton')
    #click(next).pause(3).perform()

    # 2) enter participant information
    #element21 = wait.until(EC.presence_of_element_located((By.NAME, 'QR~QID2~1~TEXT')))
    element21 = driver.find_element(By.NAME, 'QR~QID2~1~TEXT')
    element21.send_keys(first.capitalize()) #type first name in box

    element22 = driver.find_element(By.NAME, 'QR~QID2~2~TEXT')
    element22.send_keys(last.capitalize()) #type last name in box

    element23 = driver.find_element(By.NAME, 'QR~QID2~3~TEXT')
    element23.send_keys(email) #type email in box

    element24 = driver.find_element(By.NAME, 'QR~QID2~4~TEXT')
    element24.send_keys(mrn) #type mrn in box

    element24 = driver.find_element(By.NAME, 'QR~QID2~5~TEXT')
    element24.send_keys(site) #type mrn in box

    next = driver.find_element(By.NAME, 'NextButton')

    #after inputting the text, wait a momemnt then click next and pause for the page to load
    #TODO: other options rather than giving it only 3 seconds to load
    #actions.pause(.5).click(next).perform()
    actions.pause(.5).click(next).pause(3).perform()

    # 3) CLICK TO GO BACK RATHER THAN QUITTING THE DRIVER
    #element3 = wait.until(EC.presence_of_element_located((By.XPATH,"/html/body/div[3]/div/form/div/div[2]/div[1]/div[2]/div[1]/div/div/a")))
    element3 = driver.find_element(By.XPATH,"/html/body/div[3]/div/form/div/div[2]/div[1]/div[2]/div[1]/div/div/a")
    # click the element and pause to load
    #TODO: other options rather than giving it only 3 seconds to load
    actions.click(element3).perform()

def valid_email(email):

    allowed_prefix = set(string.ascii_lowercase + string.ascii_uppercase + string.digits + '.' + '_' + '-')
    allowed_domain = set(string.ascii_lowercase + string.ascii_uppercase + string.digits + '-')

    if not '@' in email:
        return False

    split_email = email.split("@")
    prefix = split_email[0]
    if not set(prefix).issubset(allowed_prefix):
        return False

    if prefix == '':
        return False

    domain = split_email[1]
    if not '.' in domain:
        return False

    split_domain = domain.split(".")
    if split_domain[0] == '':
        return False
    if len(split_domain) != 2:
        return False #only allowed one . in domain

    if not set(split_domain[0]).issubset(allowed_domain):
        return False

    if len(split_domain[1]) < 2:
        return False

    return True

def send_newinterested(new_interested, master, browser, headless, testing_mode):
    """
    Send email to all participants on new interested list and edit their qualtrics status in the master database.
    """
    if not testing_mode:
        driver = open_survey(browser, headless)
   
    skipped = []
    for index,row  in new_interested.iterrows():
        mrn = str(int(row['MRN'])) #check type
        first = row['FirstName']
        last = row['LastName']
        email = row['EmailAddress']
        site = row['Site']
        if pd.isnull(site):
            print('Participant does not have a value for site. Skipping participant')
            skipped.append(index)
            continue
        
        if not valid_email(email):
            print(f'{email} is not a valid email address. Skipping participant.')
            skipped.append(index)
            continue

        if not testing_mode:
            try:
                survey_actions(driver, first, last, email, mrn, site)
            except:
                print('Error in Survey')
                skipped.append(index)
                continue


            #find in master
            master_row = master.loc[(master['FirstName'] == row['FirstName']) & (master['LastName'] == row['LastName']) & (master['EmailAddress'] == row['EmailAddress']) & (master['MRN'] == row['MRN'])]
            master_ind = master_row.index.values[0]
            master.at[master_ind, 'QualtricsStatus'] = 'Email Sent'

    if not testing_mode:
        try:
            quit_survey(driver)
        except:
            print('Error quitting survey')
    return master, skipped

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--py_path", default=None)
    parser.add_argument("--output_path", default="./output_sheets")
    parser.add_argument("--output_archive",default="./output_sheets/archive")
    parser.add_argument("--browser", default="chrome")
    parser.add_argument("--headless", type=ast.literal_eval, default=False)
    parser.add_argument("--testing_mode", type=ast.literal_eval, default=True)
    args = parser.parse_args()

    if args.py_path is None or args.py_path == '':
        #set path to parent directory of current path
        py_path = Path(__file__).absolute()
        args.py_path = py_path.parents[1]
    else:
        args.py_path = Path(args.py_path)
    
    os.chdir(args.py_path)

    args.output_path = Path(args.output_path).absolute()
    args.output_archive = Path(args.output_archive).absolute()

    assert args.browser in ['chrome', 'edge'], f'{args.browser} is an incompatible browser type.'

    m, master_date = get_most_recent(pat = args.output_path / 'master_database*.csv', archive_path = args.output_archive, sep="_",
                                                dt_ind=-1, dt_format='%Y%m%d-%H%M%S', data_ext='.csv')
    master = pd.read_csv(m)

    ni_df = master.loc[(master['EpicStatus'] == 'interested') & (pd.isnull(master['PtraxStatus'])) & (pd.isnull(master['QualtricsStatus'])) & ~(pd.isnull(master['Site']))] #to be new interested, thye must have null ptrax and qualtrics status, have epic status as interested, and have a Site value
    #TODO: get most recent email address as well?
    ni_df = ni_df[['MRN','FirstName','LastName','EmailAddressEpic', 'EmailAddressPtrax', 'EmailAddress', 'Site']]
    ni_df = ni_df.reset_index()


    #CHECK THAT NEW_INTERESTED IS NOT EMPTY
    if not ni_df.empty:
        print(f"{len(ni_df.index.values)} participants in NewInterested")
        #CHECK THAT ALL NEW INTERESTED EMAILS ARE VALID EMAILS
        master, skipped = send_newinterested(ni_df, master, args.browser, args.headless, args.testing_mode)

        #check that all new interested are now Email Sent
        test = ni_df.merge(master, on=['FirstName','LastName','MRN','EmailAddress'], how='left')
        test = test.drop(skipped)
        test = test['QualtricsStatus'].values

        if test.size != 0:
            assert len(set(test)) == 1 and list(set(test))[0] == 'Email Sent', 'Not all new interested'

            #for saving new master, other one goes into archive
            current_date = datetime.today().strftime("%Y%m%d-%H%M")

            try:
                to_archive = glob.glob(str(args.output_path / 'master_database*.csv'))
                archive(to_archive, args.output_archive)
            except:
                print('Error thrown while trying to archive master database')

            out_path = args.output_path / f'master_database_{current_date}.csv'
            master.to_csv(out_path, index=False)
            print('Master database saved')

            new_interested = master.loc[(master['EpicStatus'] == 'interested') & (pd.isnull(master['PtraxStatus'])) & (pd.isnull(master['QualtricsStatus'])) & ~(pd.isnull(master['Site']))] #to be new interested, thye must have null ptrax and qualtrics status, have epic status as interested, and have a Site value
            #TODO: get most recent email address as well?
            new_interested = new_interested[['MRN','FirstName','LastName','EmailAddressEpic', 'EmailAddressPtrax', 'EmailAddress', 'Site']]
                
            out_path =  args.output_path / f'NewInterested_{current_date}.csv'
            new_interested.to_csv(out_path, index=False)
            print('New Interested saved')
        
        else:
            print('All participants skipped.')

    else:
        print('No participants in NewInterested.')

if __name__ == '__main__':
    main()

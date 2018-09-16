import pandas as pd
import glob
import os
import numpy as np
import re
from Merge_Records import merge_customer_files
## Sort out the files with no or bad email addresses
## df = merge_customer_files()
def shuffle_files(df):
    
    '''
    This function is to perform some validation checks against the Emails within the dataframe
    '''

    df['Email'] = df['Email'].str.upper()
    no_email_df = df[pd.isna(df['Email'])]
    df_with_email = df[pd.notna(df['Email'])]

 ## find bad email

    df_with_email['email_helper'] = df_with_email['Email'].str.contains('[^@]+@[^@]+\.[^@]+')
    bad_email_df = df_with_email[df_with_email['email_helper'] == False]
    df_with_email.drop(columns=["email_helper"], inplace=True)


    df_bad_emails = pd.concat([bad_email_df,no_email_df])
    df_bad_emails.drop(columns=['email_helper'], inplace=True)
    df_bad_emails.drop_duplicates(keep='first', inplace=True)

    df_bad_emails.to_pickle('./DataFiles/df_with_bad_emails.pickle') #push this to bad_emails jupyter notebook to monitor
    

    return df_with_email

if __name__ == '__main__':
    pass
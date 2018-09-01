# Clean Data

import pandas as pd
import re
import numpy as np

combined_data_df = pd.read_csv('./DataFiles/combined_data2018-08-31.csv')

## Find and drop bad email addresses

combined_data_df['email_helper'] = combined_data_df['Email'].str.contains('[^@]+@[^@]+\.[^@]+')
combined_data_df = combined_data_df[combined_data_df['email_helper'] == True]
combined_data_df.drop(columns=["email_helper"], inplace=True)

## Normalize phone numbers when possible

combined_data_df['PhoneNumber'].fillna('', inplace=True)


# def align_phonenumbers(combined_data_df=combined_data_df):
    
#     '''
#     Applies reg to PhoneNumber list to clean up inconsistent formats.
#     '''
    
#     phonePattern = re.compile(r'(\d{3})\D*(\d{3})\D*(\d{4})\D*(\d*)$')
#     PhoneNumber_list = combined_data_df.PhoneNumber.tolist()
#     pn = []
#     for number in PhoneNumber_list:
#             try:
#                 patterned_group =phonePattern.search(number).groups()
#                 pn.append("".join(patterned_group))
#             except AttributeError: 
#                 pn.append(number)
#     return pn

# f = align_phonenumbers()
# combined_data_df['PhoneNumber'] = np.asarray(f)

# LOAD Store Code Data



store_codes_df = pd.read_csv(r"./DataFiles/StoreCodes.csv", dtype={'Store ID':int,
                                                                                                         'MarketNumber':int})

## Merge with Combined DF Master

combined_data_df = pd.merge(left=combined_data_df, right= store_codes_df, how='left', left_on ="StoreCode", right_on="Store ID")

combined_data_df.to_csv('./DataFiles/combined_data_master.csv', index=False)

print('Completed:  2 - Clean Data -','combined data master done' )

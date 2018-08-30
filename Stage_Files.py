# Stage Files - Append Combined Lookup
import warnings
import pandas as pd
import numpy as np
import glob
from pathlib import Path

#shush warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

#File path variables

recent_sales_file = next(Path('.\DataFiles\SalesTransactionExport').glob('*.csv'))
combined_data_file =Path('.\DataFiles\combined_data2018-08-28.csv')


## LOAD combined_data_master

combined_data_df = pd.read_csv(combined_data_file)

## LOAD Sales Transaction Export

recent_sales_data_df = pd.read_csv(recent_sales_file,index_col=None, header=0, low_memory=False,
                 thousands=',', dtype={'StoreCode':object}, usecols = ['AccountNumber','StoreCode','StoreName',
                                                                        'SaleDate', 'Email','PhoneNumber'])

recent_sales_data_df.SaleDate.max()


## TRANSFORM Sales Transaction Export to match combined data format

# Upper_case Email Column
recent_sales_data_df['Email'] = recent_sales_data_df.Email.str.upper()

#Turn SalesDate Column into DateTime type
recent_sales_data_df["SaleDate"] = pd.to_datetime(recent_sales_data_df.SaleDate)


# group recent_sales data by Email, StoreCode, PhoneNumber and MAX of SalesDate
recent_sales_data_df_grouped =recent_sales_data_df.groupby(["Email","StoreCode"])
recent_sales_data_df_grouped = recent_sales_data_df_grouped['SaleDate'].agg([np.max])
recent_sales_data_df_grouped.reset_index(inplace=True)

#shuffle columns
recent_sales_data_df_grouped = recent_sales_data_df_grouped[['Email', 'StoreCode','amax']]


## Append Sales Transaction data to Combined data

# Append Data
updated_combined_data_df = combined_data_df.append(recent_sales_data_df_grouped)

#Check size for append validity
recent_sales_data_df_grouped.size + combined_data_df.size == updated_combined_data_df.size

updated_combined_data_df[updated_combined_data_df.Email == 'JENNYGRICHARDSON@GMAIL.COM' ]

# Solve for data duplicates
updated_combined_data_df['amax'] = updated_combined_data_df['amax'].apply(lambda x: str(x))
updated_combined_data_df.sort_values(by=['amax'], ascending=False, inplace=True)
updated_combined_data_df.drop_duplicates(subset=['Email'], keep='first', inplace=True)
updated_combined_data_df['amax'] = updated_combined_data_df['amax'].apply(lambda x: str(x).split(' ')[0])

## Export Updated Data

# Export to csv - no index
combined_data_filename = ''.join(['./DataFiles/','combined_data',updated_combined_data_df.amax.max(),'.csv'])
updated_combined_data_df.to_csv(combined_data_filename, index=False)

print('completed: 1 - Stage_Files ', combined_data_filename)
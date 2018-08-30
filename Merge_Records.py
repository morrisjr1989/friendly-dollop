import pandas as pd
import glob
import os
import numpy as np
from pathlib import Path



def merge_customer_files():
    folder_exportcustomers = Path(".\DataFiles\ExportCustomers")
    # Create an empty list

    frames_ExportCustomerAll = []
    frames_ExportCustomer = []

    for file in os.listdir(folder_exportcustomers):
        store_code = file.split("_",1)[0]
        file_type  = file.split('_',1)[1]
        file_path = Path.joinpath(folder_exportcustomers, file)
        
        
        if file == "merge_records.py": pass
        
        
        elif file_type == "Export Customer All.csv":
            df = pd.read_csv(file_path, usecols=['AccountNumber','CustomerName', 'Email','FBPlanName', 'FBOutstandingRewards', 'LastName', 'FirstName'], dtype = {'Email': object,'AccountNumber':object})
            df['MarketCode'] = np.asarray(store_code)
            frames_ExportCustomerAll.append(df)
            #df.to_pickle(store_code+"_"+"ExportCustomerAll.pickle")
        
        elif file_type == 'Export Customers.csv':
            df = pd.read_csv(file_path, usecols=['AccountNumber', 'FirstName','LastName', 'AddressLine', 'City', 'State', 'ZipCode', 'Email', 'Memo'], dtype= {'AccountNumber':object,'ZipCode':object,'PhoneNumber':object,'Memo':object})
            df['MarketCode'] = np.asarray(store_code)
            frames_ExportCustomer.append(df)
            #df.to_pickle(store_code+"_"+"ExportCustomers.pickle")     

    print(frames_ExportCustomer, frames_ExportCustomerAll)
    ExportCustomerAll_df = pd.concat(frames_ExportCustomerAll)      
    ExportCustomer_df = pd.concat(frames_ExportCustomer)                                                                                                                                                                                                      

    ExportCustomerAll_df['CustomerID'] = ExportCustomerAll_df['AccountNumber'] +'_'+ ExportCustomerAll_df['Email']+'_'+ExportCustomerAll_df['LastName']+'_'+ExportCustomerAll_df['FirstName']
    ExportCustomer_df['CustomerID'] = ExportCustomer_df['AccountNumber'] +'_'+ ExportCustomer_df['Email']+'_'+ExportCustomer_df['LastName']+'_'+ExportCustomer_df['FirstName']

    ExportCustomerAll_df.set_index('CustomerID', inplace=True)
    ExportCustomer_df.set_index('CustomerID', inplace=True)

    export_customer_master = pd.merge(left=ExportCustomer_df, right=ExportCustomerAll_df, how='left')
    export_customer_master.to_pickle('.\DataFiles\export_customer_master.pickle')
    return export_customer_master

if __name__ == '__main__':
    pass

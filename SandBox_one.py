#!/usr/bin/python
import pyodbc
import pandas as pd
import os
import configparser

server = 'LAPTOP-E3PNBOI6'
database = 'KATAS_TESTS'
username = 'demipt2'
password = 'peregrintook'
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD=' + password, autocommit=False)
cursor = cnxn.cursor()



#Sample select query
cursor.execute("select * from dbo.RESULT;")
row = cursor.fetchone()
while row:
    print(row[0])
    row = cursor.fetchone()

#if __name__ == '__main__':

# declaring variables
    upload_dt = str(input("Enter upload date in format DDMMYYYY: "))
    working_dir = "C:\\Users\\stasr\\OneDrive\\Data_Engineering\\Final_Project\\"
    processed_dir = working_dir + "archive\\"

    passp_working_file = "passport_blacklist_" + upload_dt +".xlsx"

"""
    import jaydebeapi
conn = jaydebeapi.connect('oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:demipt2/peregrintook@de-oracle.chronosavant.ru:1521/deorale',
['demipt2', 'peregrintook'],
'/home/demipt2/ojdbc8.jar')
"""
#blck_list_df = pd.read_excel((working_dir + working_file), sheet_name= "blacklist", header=0, index_col=None)
blck_list_df = pd.read_excel(working_dir + passp_working_file)
print (blck_list_df.shape)
print (blck_list_df.columns)
print(blck_list_df)

os.system("move "+working_dir + passp_working_file+" "+processed_dir + passp_working_file + ".backup")











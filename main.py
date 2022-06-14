#!/usr/bin/python

"""Script algorithm
Essentials are gruped to four categories and caputered to DWH sequentually:
[1] Tables of Relational DBs containing dimensional data. In DWH such data is stored in SCD2 format [Clients; Accounts; Cards]
[2] Tables of Relational DBs containing transactional data. (Not applicable in this partucular case)
[3] Files (.xlsx, csv, etc.) containing dimensional data. In DWH such data is stored in SCD2 format [Terminals]
[4] Files (.xlsx, csv, etc.) containing transactional data. Suuch data is stored directly to detailed level (staging is passed), 
    SCD2 is not applied, deletions are not traced as do not occur in transactional data [Black List passports; Transactions]

MetaData update 

Commit
"""
#---------------------prerequisits and setup
import pandas as pd
import jaydebeapi
import time
import os
import subprocess
from configparser import ConfigParser

upld_dt = str(input("Enter date of upload run using format DDMMYYYY: " ))

from py_scripts.func_clients_upload     import clients_upload
from py_scripts.func_accounts_upload    import accounts_upload
from py_scripts.func_cards_upload       import cards_upload

from py_scripts.func_files_upload       import blck_list_upload
from py_scripts.func_files_upload       import trans_upload
from py_scripts.func_files_upload       import term_upload

def file_archieving(working_dir, working_file):
    #for Linux
    subprocess.call(["cd", working_dir], shell=True)
    subprocess.call(str("mv " + working_file + " " + processed_dir + working_file+".backup"), shell=True, cwd = working_dir)
    
    #for Windows
    #https://stackoverflow.com/questions/21804935/how-to-use-the-mv-command-in-python-with-subprocess    
    #os.system("move "+working_dir + passp_working_file+" "+processed_dir + passp_working_file + ".backup")
    
def main():
    #---------------------declaring viariables
    launch_time = time.time()  
    conn = jaydebeapi.connect(
    'oracle.jdbc.driver.OracleDriver',
    'jdbc:oracle:thin:demipt2/peregrintook@de-oracle.chronosavant.ru:1521/deoracle',
    ['demipt2','peregrintook'],
    '/home/demipt2/ojdbc8.jar'
    )
    conn.jconn.setAutoCommit(False)
    
    config = ConfigParser()                         # instantiate
    configFilePath = r'/home/demipt2/rusn/config/config.ini'
    config.read(configFilePath)       # parse existing file
    working_dir = config.get('Linux_sys', 'working_dir' )
    processed_dir = config.get('Linux_sys', 'processed_dir' )
    passp_working_file = config.get('Files_data', 'passp_working_file') + upld_dt + config.get('Files_data', 'passp_file_ext')
    trans_working_file = config.get('Files_data', 'trans_working_file') + upld_dt + config.get('Files_data', 'trans_file_ext')
    term_working_file = config.get('Files_data',  'term_working_file' ) + upld_dt + config.get('Files_data', 'term_file_ext')
    
    rep_sqls = [ '/home/demipt2/rusn/sql_scripts/black_list_transactions.sql', '/home/demipt2/rusn/sql_scripts/diffrnt_cities_60_mins.sql', '/home/demipt2/rusn/sql_scripts/amount_enumeration.sql' ]
    #---------------------------------------------------------------ETL process 
    #-------------------------------- Dimensional tables
    clients_upload(conn, upld_dt)
    accounts_upload(conn, upld_dt)
    cards_upload(conn, upld_dt)

    #-------------------------------- Dimensional files
    term_upload(conn, upld_dt, working_dir, term_working_file)
    file_archieving(working_dir, term_working_file)

    #-------------------------------- Transactional files (captured directly to staging, no deletions occur in transactional data, thus deletions are not tracked) 
    blck_list_upload(conn, upld_dt, working_dir, passp_working_file)
    file_archieving(working_dir, passp_working_file)

    trans_upload(conn, upld_dt, working_dir, trans_working_file)
    file_archieving(working_dir, trans_working_file)
    
    #---------------------------------------------------------------Fraud alarms (reports generation) 

    for rep_sql in rep_sqls:
        with open(rep_sql,'r') as rep_query: 
            sql_statement=rep_query.read()
            sql_statement = sql_statement.replace(':upld_dt', upld_dt)
            print(sql_statement)
            rep_query.close()

        with conn.cursor() as curs:
            curs.execute(sql_statement)
            time.sleep(3)
    
    with conn.cursor() as curs:
        curs.execute(""" update demipt2.RUSN_REP_FRAUD set REPORT_DT = to_date( '""" + upld_dt + """' , 'DDMMYYYY') where REPORT_DT is null """ ) 
        time.sleep(1)
    conn.commit()  

    #------------------------------------------CLOSING 
    conn.close()  
    print("Script run time: " + str((time.time() - launch_time))) 

if __name__ == "__main__":
    main()

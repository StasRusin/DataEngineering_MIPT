#!/usr/bin/python
#---------------------prerequsits setup
import pandas as pd
import jaydebeapi

conn = jaydebeapi.connect(
'oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:demipt2/peregrintook@de-oracle.chronosavant.ru:1521/deoracle',
['demipt2','peregrintook'],
"C:/Users/stasr/PycharmProjects/DEMIPT2_RUSN_FinalProject/ojdbc8.jar"
#'/home/demipt2/ojdbc8.jar'
)
conn.jconn.setAutoCommit(False)
curs = conn.cursor()

#---------------------declaring viariables
working_dir = "/home/demipt2/rusn/fin_proj/"                                         #server version
#working_dir = "C:/Users/stasr/OneDrive/Data_Engineering/Final_Project/"                                         #local version
pass_working_file = "passport_blacklist_" + "01032021" +".xlsx"
term_working_file = "terminals_" + "04032021" +".xlsx"
trans_workin_file = "transactions_" + "03042021" +".txt"
#processed_dir = working_dir + "archive"                                             # is this variable needed here??? files move will be exec$

# ----------------------------------------------------------upload itself
#---------------------black_list_passports  TO BE DEBUGGED !!!!!

blck_list_df = pd.read_excel((working_dir + pass_working_file), sheet_name= "blacklist", header=0, index_col=None)
print (blck_list_df.shape)
blck_list_df
curs.executemany( "insert into demipt2.RUSN_STG_FACT_PSSPRT_BLCKLST (ENTRY_DT, PASSPORT_NUM) values (to_date('?','YYYY-MM-DD HH24:MI:SS'), ?)", blck_list_df.values.tolist() )
conn.commit

#---------------------terminals
"""
term_df = pd.read_excel((working_dir + term_working_file), sheet_name= "terminals", header=0, index_col=None)
print (term_df.shape)
term_df
curs.executemany( "insert into demipt2.RUSN_STG_TERMINALS  (TERMINAL_ID, TERMINAL_TYPO, TERMINAL_CITY, TERMINAL_ADDRESS) VALUES (?, ?, ?, ?)",$
conn.commit
"""
#---------------------transactions

dataset = pd.read_csv( working_dir + term_working_file, delimiter=";", decimal=',')
print(dataset)

sql='INSERT INTO gnl.tbl_deneme VALUES(:1,:2,:3,:4,:5,:6)'
df_list = dataset.values.tolist()
n = 0
for i in dataset.iterrows():
    cursor.execute(sql,df_list[n])
    n += 1

con.commit


#---------------------CLOSING 
curs.close()
conn.close()
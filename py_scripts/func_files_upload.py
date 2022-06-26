#!/usr/bin/python
import pandas as pd
import jaydebeapi
import time
import os

def blck_list_upload(conn, upld_dt, working_dir, passp_working_file):    
    # conn = jaydebeapi.connect(
    # 'oracle.jdbc.driver.OracleDriver',
    # 'jdbc:oracle:thin:demipt2/peregrintook@de-oracle.chronosavant.ru:1521/deoracle',
    # ['demipt2','peregrintook'],
    # '/home/demipt2/ojdbc8.jar'
    # )
    curs = conn.cursor()
    launch_time = time.time()  
    
    # -------------------------------------------------------- I Cleaning Staging tables
    curs.execute("delete from RUSN_STG_PSSPRT_BLCKLST")
    
    #--------------------------------------------------------- II(i) capturing files to staging 
    #---------------------black_list_passports
    blck_list_df = pd.read_excel((working_dir + passp_working_file), sheet_name= "blacklist", header=0, index_col=None)
    #print(blck_list_df.shape)
    blck_list_df['date'] = blck_list_df['date'].astype(str)
    curs.executemany( "insert into DEMIPT2.RUSN_STG_PSSPRT_BLCKLST (ENTRY_DT, PASSPORT_NUM) values (to_date(?, 'YYYY-MM-DD'), ?) ", blck_list_df.values.tolist() )
    conn.commit    
    time.sleep(2)   
    
    #-------------------- IV Detailed level processing is made in one step as this is transactional table. SCD2 format is not applied for the same reason	
    #BLCK_LIST_PASSPORTS
    sql_blcl_lst_to_dwh="""
    insert into RUSN_DWH_FACT_PSSPRT_BLCKLST (PASSPORT_NUM, ENTRY_DT) 
                                (select replace(PASSPORT_NUM, CHR(32), ''), ENTRY_DT
                                    from RUSN_STG_PSSPRT_BLCKLST stg                            
                                    where stg.ENTRY_DT > (select last_update from rusn_meta where table_name='BLCK_PASSP') 
                                )
    """
    curs.execute(sql_blcl_lst_to_dwh)
    time.sleep(2)

    #-----------------------------VI  Updating MetaData table, updating timestamp of most recent captured record		
    curs.execute(""" update demipt2.RUSN_META set LAST_UPDATE = to_date( '""" + upld_dt + """' , 'DDMMYYYY') where TABLE_NAME='BLCK_PASSP' """ ) 

    #--------------------- VII  commit upload
    conn.commit    
    #---------------------CLOSING 
    print("Black list passports upload run time is: " + str(time.time() - launch_time) + " sec.") 
    
def trans_upload(conn, upld_dt, working_dir, trans_working_file): 
    conn = jaydebeapi.connect(
    'oracle.jdbc.driver.OracleDriver',
    'jdbc:oracle:thin:demipt2/peregrintook@de-oracle.chronosavant.ru:1521/deoracle',
    ['demipt2','peregrintook'],
    '/home/demipt2/ojdbc8.jar'
    )
    curs = conn.cursor()
    launch_time = time.time()  
    
    trans_dataset = pd.read_csv( working_dir + trans_working_file, delimiter=";", decimal=',')
    #print(trans_dataset)
    sql="insert into DEMIPT2.RUSN_DWH_FACT_TRANSACTIONS(TRANS_ID, TRANS_DATE,  amnt, CARD_NUM, OPER_TYPE,oper_result, terminal) values (:1, to_date(:2, 'YYYY-MM-DD HH24:MI:SS'), :3, :4, :5, :6, :7)"

    trans_df = trans_dataset.values.tolist()
    n = 0
    for i in trans_dataset.iterrows():
        #print( sql ) 
        curs.execute(sql,trans_df[n])
        n += 1

    time.sleep(2)
        #-----------------------------VI  Updating MetaData table, updating timestamp of most recent captured record		
    curs.execute(""" update demipt2.RUSN_META set LAST_UPDATE = to_date( '""" + upld_dt + """' , 'DDMMYYYY') where TABLE_NAME='TRANSACTIONS' """ ) 

    #--------------------- VII  commit upload
    conn.commit    
    #---------------------CLOSING 
    print("Transactions upload run time is: " + str(time.time() - launch_time) + " sec.") 

def term_upload(conn, upld_dt, working_dir, term_working_file): 
    conn = jaydebeapi.connect(
    'oracle.jdbc.driver.OracleDriver',
    'jdbc:oracle:thin:demipt2/peregrintook@de-oracle.chronosavant.ru:1521/deoracle',
    ['demipt2','peregrintook'],
    '/home/demipt2/ojdbc8.jar'
    )
    curs = conn.cursor()
    launch_time = time.time()  
    
    # -------------------------------------------------------- I Cleaning Staging tables
    curs.execute("delete from RUSN_STG_TERMINALS")
    #--------------------------------------------------------- II(i) capturing files to staging 
    #---------------------terminals    
    term_df = pd.read_excel((working_dir + term_working_file), sheet_name= "terminals", header=0, index_col=None)
    #print (term_df.shape)
    curs.executemany( "insert into demipt2.RUSN_STG_TERMINALS  (TERMINAL_ID, TERMINAL_TYPO, TERMINAL_CITY, TERMINAL_ADDRESS) VALUES (?, ?, ?, ?)", term_df.values.tolist() )
    time.sleep(2)
    #-------------------- IV Detailed level processing is made in two steps (SCD2 format)	
    #TERMINALS additions
    sql_term_adds_to_dwh_step_one="""
     insert into RUSN_DWH_DIM_TERMINALS_HIST
         (TERMINAL_ID, TERMINAL_TYPO, TERMINAL_CITY, TERMINAL_ADDRESS, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG) 
                     (select stg.TERMINAL_ID, stg.TERMINAL_TYPO, stg.TERMINAL_CITY, stg.TERMINAL_ADDRESS, to_date('""" + upld_dt + """' , 'DDMMYYYY'), to_date('31-12-2999', 'DD-MM-YYYY'), 'F'
                     from RUSN_STG_TERMINALS stg
                     left join RUSN_DWH_DIM_TERMINALS_HIST dwh on stg.TERMINAL_ID = dwh.TERMINAL_ID
                     where dwh.TERMINAL_ID is null)
     """
    curs.execute(sql_term_adds_to_dwh_step_one)
    time.sleep(2)
    ##TERMINALS amendments 
    ##closing current version {Step 1 out of 2}
    sql_term_amend_clsg_curr_vers="""
    update RUSN_DWH_DIM_TERMINALS_HIST set EFFECTIVE_TO=to_date('""" + upld_dt + """ ', 'DDMMYYYY') - interval '1' day where EFFECTIVE_TO=date'2999-12-31' and TERMINAL_ID in 
                (select dwh.TERMINAL_ID
                from RUSN_STG_TERMINALS stg
                left join RUSN_DWH_DIM_TERMINALS_HIST dwh on stg.TERMINAL_ID = dwh.TERMINAL_ID and dwh.EFFECTIVE_TO = date'2999-12-31'
                where dwh.TERMINAL_ID is not null and ( 1 = 0 
                                                    or dwh.TERMINAL_TYPO <> stg.TERMINAL_TYPO or (stg.TERMINAL_TYPO  is null and dwh.TERMINAL_TYPO  is not null ) or (stg.TERMINAL_TYPO  is not null and dwh.TERMINAL_TYPO  is null ) 
                                                    or dwh.TERMINAL_CITY <> stg.TERMINAL_CITY or (stg.TERMINAL_CITY  is null and dwh.TERMINAL_CITY  is not null ) or (stg.TERMINAL_CITY  is not null and dwh.TERMINAL_CITY  is null)                                                            
                                                    or dwh.TERMINAL_ADDRESS <> stg.TERMINAL_ADDRESS or (stg.TERMINAL_ADDRESS  is null and dwh.TERMINAL_ADDRESS  is not null ) or (stg.TERMINAL_ADDRESS  is not null and dwh.TERMINAL_ADDRESS  is null )                                                             
                                                                                                                )
                )
    """                
    curs.execute(sql_term_amend_clsg_curr_vers)
    time.sleep(2)
    
    #opening new version {Step 2 out of 2}
    sql_term_adds_to_dwh_step_two="""
    insert into RUSN_DWH_DIM_TERMINALS_HIST
        (TERMINAL_ID, TERMINAL_TYPO, TERMINAL_CITY, TERMINAL_ADDRESS, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG) 
                    (select stg.TERMINAL_ID, stg.TERMINAL_TYPO, stg.TERMINAL_CITY, stg.TERMINAL_ADDRESS, to_date('""" + upld_dt + """' , 'DDMMYYYY'), to_date('31-12-2999', 'DD-MM-YYYY'), 'F'
                    from RUSN_STG_TERMINALS stg
                    left join RUSN_DWH_DIM_TERMINALS_HIST dwh on stg.TERMINAL_ID = dwh.TERMINAL_ID and dwh.EFFECTIVE_TO = to_date('""" + upld_dt + """ ', 'DDMMYYYY') - interval '1' day
                    where dwh.TERMINAL_ID is not null and ( 1 = 0 
                                                    or dwh.TERMINAL_TYPO <> stg.TERMINAL_TYPO or (stg.TERMINAL_TYPO  is null and dwh.TERMINAL_TYPO  is not null ) or (stg.TERMINAL_TYPO  is not null and dwh.TERMINAL_TYPO  is null ) 
                                                    or dwh.TERMINAL_CITY <> stg.TERMINAL_CITY or (stg.TERMINAL_CITY  is null and dwh.TERMINAL_CITY  is not null ) or (stg.TERMINAL_CITY  is not null and dwh.TERMINAL_CITY  is null)                                                            
                                                    or dwh.TERMINAL_ADDRESS <> stg.TERMINAL_ADDRESS or (stg.TERMINAL_ADDRESS  is null and dwh.TERMINAL_ADDRESS  is not null ) or (stg.TERMINAL_ADDRESS  is not null and dwh.TERMINAL_ADDRESS  is null )                                                             
                                                           )
                    )
    """ 
    curs.execute(sql_term_adds_to_dwh_step_two)
    time.sleep(2)
    
    #TERMINALS deletions 
    #closing current version {Step 1 out of 2}
    sql_term_del_step_one="""
    update RUSN_DWH_DIM_TERMINALS_HIST dwh set EFFECTIVE_TO = to_date('""" + upld_dt + """' , 'DDMMYYYY') - interval '1' day
    where dwh.TERMINAL_ID in
            (select dwh.TERMINAL_ID
            from RUSN_DWH_DIM_TERMINALS_HIST dwh
            left join RUSN_STG_TERMINALS stg on stg.TERMINAL_ID = dwh.TERMINAL_ID 
            where 
                        dwh.EFFECTIVE_TO = date'2999-12-31' and
                        dwh.DELETED_FLG='F' and
                        stg.TERMINAL_ID is null
            )"""
    curs.execute(sql_term_del_step_one)
    time.sleep(2)
    
    #opening new version {Step 2 out of 2}
    sql_term_del_step_two="""
    insert into RUSN_DWH_DIM_TERMINALS_HIST
        (TERMINAL_ID, TERMINAL_TYPO, TERMINAL_CITY, TERMINAL_ADDRESS, EFFECTIVE_FROM, EFFECTIVE_TO, DELETED_FLG) 
            (select dwh.TERMINAL_ID, dwh.TERMINAL_TYPO, dwh.TERMINAL_CITY, dwh.TERMINAL_ADDRESS, to_date('""" + upld_dt + """' , 'DDMMYYYY'), to_date('31-12-2999', 'DD-MM-YYYY'), 'T'
            from RUSN_DWH_DIM_TERMINALS_HIST dwh
            left join RUSN_STG_TERMINALS stg on stg.TERMINAL_ID = dwh.TERMINAL_ID 
            where stg.TERMINAL_ID is null
            )"""
    curs.execute(sql_term_del_step_two)
    time.sleep(2)         
    #-----------------------------VI  Updating MetaData table, updating timestamp of most recent captured record		
    curs.execute(""" update demipt2.RUSN_META set LAST_UPDATE = to_date( '""" + upld_dt + """' , 'DDMMYYYY') where TABLE_NAME='TERMINALS' """ ) 

    #--------------------- VII  commit upload
    conn.commit    
    #---------------------CLOSING 
    print("Terminals upload run time is: " + str(time.time() - launch_time) + " sec.") 
    
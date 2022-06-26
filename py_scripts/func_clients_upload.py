#!/usr/bin/python
import pandas as pd
import jaydebeapi
import time
import os

def clients_upload (conn, upld_dt):
    curs = conn.cursor()
    launch_time = time.time() 
    # -------------------------------------------------------- I Cleaning Staging tables
    curs.execute("""delete from RUSN_STG_CLIENTS """)
    curs.execute("""delete from RUSN_STG_CLIENTS_DEL """)

    ##-- II capturing CLIENTS to staging

    curs.execute("""insert into demipt2.RUSN_STG_CLIENTS stg (CLIENT_ID, LAST_NAME, FIRST_NAME, PATRONYMIC, DATE_OF_BIRTH, PASSPORT_NUM, PASSPORT_VALID_TO, PHONE, CREATE_DT, UPDATE_DT) 
                        (select CLIENT_ID, LAST_NAME, FIRST_NAME, PATRONYMIC, DATE_OF_BIRTH, PASSPORT_NUM, PASSPORT_VALID_TO, PHONE, CREATE_DT, UPDATE_DT 
                                from BANK.CLIENTS 
                                where coalesce(update_dt, create_dt) > 
                                                (select coalesce(last_update, to_date('1899-12-31', 'YYYY-MM-DD') ) from RUSN_META where table_name='BANK.CLIENT'))"""	
                )	
    time.sleep(2)       

    #-- III   Capturing exisiting lines  into separate table to track DELETIONS. IDs only to be catured to minimize traffic and processing time !!!!
    #/*Capturing is made in two SEPARATE queries insert + update , do NOT combine these two into single query!!! as load might start before before midnight and finish after midnight, 
    #making upload_date inconsistent*/
    curs.execute(""" insert into demipt2.RUSN_STG_CLIENTS_DEL (CLIENT_ID)
                    (select CLIENT_ID from BANK.CLIENTS) """ )
    time.sleep(1)
    curs.execute(""" update demipt2.RUSN_STG_CLIENTS_DEL set upload_date = to_date( '""" + upld_dt +"""' , 'DDMMYYYY' ) where upload_date is null """)
    time.sleep(1)
    #-------------------- IV Detailed level processing: processing changes	from captured lines selecting those that already exist in staging	
    #-----------------------------IV(i) step 1; closing current version
    # CLIENTS
    sql_cl_change="""
    merge into demipt2.RUSN_DWH_DIM_CLIENTS_HIST dwh1
    using  (
                    select 
                             stg.CLIENT_ID,-- as ID_SOURCE
                              stg.UPDATE_DT --as EFFECTIVE_TO
                            from demipt2.RUSN_STG_CLIENTS stg 
                            left join DEMIPT2.RUSN_DWH_DIM_CLIENTS_HIST dwh on stg.CLIENT_ID = dwh.CLIENT_ID /*and stg.UPDATE_DT = (select max(upload_date) from demipt2.RUSN_STG_CLIENTS)*/
                            where dwh.CLIENT_ID is not null and ( 1 = 0 
                                            or dwh.FIRST_NAME <> stg.FIRST_NAME or (stg.FIRST_NAME is null and dwh.FIRST_NAME is not null ) or ( stg.FIRST_NAME is not null and dwh.FIRST_NAME is null ) 
                                            or dwh.LAST_NAME  <> stg.LAST_NAME  or (stg.LAST_NAME  is null and dwh.LAST_NAME  is not null ) or ( stg.LAST_NAME  is not null and dwh.LAST_NAME  is null ) 
                                            or dwh.PATRONYMIC  <> stg.PATRONYMIC  or (stg.PATRONYMIC  is null and dwh.PATRONYMIC  is not null ) or ( stg.PATRONYMIC  is not null and dwh.PATRONYMIC  is null ) 
                                            or dwh.DATE_OF_BIRTH  <> stg.DATE_OF_BIRTH  or (stg.DATE_OF_BIRTH  is null and dwh.DATE_OF_BIRTH  is not null ) or ( stg.DATE_OF_BIRTH  is not null and dwh.DATE_OF_BIRTH  is null ) 
                                            or dwh.PASSPORT_NUM  <> stg.PASSPORT_NUM  or (stg.PASSPORT_NUM  is null and dwh.PASSPORT_NUM  is not null ) or ( stg.PASSPORT_NUM  is not null and dwh.PASSPORT_NUM  is null ) 
                                            or dwh.PASSPORT_VALID_TO  <> stg.PASSPORT_VALID_TO  or (stg.PASSPORT_VALID_TO  is null and dwh.PASSPORT_VALID_TO  is not null ) or ( stg.PASSPORT_VALID_TO  is not null and dwh.PASSPORT_VALID_TO  is null ) 
                                            or dwh.PHONE  <> stg.PHONE  or (stg.PHONE  is null and dwh.PHONE  is not null ) or ( stg.PHONE  is not null and dwh.PHONE  is null ) 
                                                                                                )
                                                                                         and  dwh.EFFECTIVE_TO=date'2999-12-31'      
                            ) qaz
    on (dwh1.CLIENT_ID=qaz.CLIENT_ID)
    when matched then update set dwh1.EFFECTIVE_TO = qaz.update_dt - interval '1' day where EFFECTIVE_TO=date'2999-12-31'"""
    curs.execute(sql_cl_change)
    time.sleep(2)
    #-----------------------------IV(ii) step 2; opening new version for both amended or added lines. Common process is used for amended and inserted lines 
    # CLIENTS
    sql_cl_add="""
    insert into demipt2.RUSN_DWH_DIM_CLIENTS_HIST (
    CLIENT_ID, 
    FIRST_NAME, 
    LAST_NAME, 
    PATRONYMIC, 
    DATE_OF_BIRTH, 
    PASSPORT_NUM, 
    PHONE, 
    EFFECTIVE_FROM, 
    EFFECTIVE_TO, 
    DELETED_FLG, 
    PASSPORT_VALID_TO) 
                (select 
                    stg.CLIENT_ID, 
                    stg.FIRST_NAME, 
                    stg.LAST_NAME, 
                    stg.PATRONYMIC, 
                    stg.DATE_OF_BIRTH, 
                    stg.PASSPORT_NUM, 
                    stg.PHONE, 
                    coalesce(stg.UPDATE_DT, stg.CREATE_DT),
                    date'2999-12-31',
                    'F',
                    stg.PASSPORT_VALID_TO
                from RUSN_STG_CLIENTS stg                            
                where coalesce(stg.UPDATE_DT, stg.CREATE_DT) > (select last_update from rusn_meta where table_name='BANK.CLIENT') 
                ) 
    """
    curs.execute(sql_cl_add)
    time.sleep(2)

    #-----------------------------V   Processing DELETIONS (using special tables)
    #-----------------------------V(i) step 1; closing current version
    # CLIENTS
    sql_cl_del="""
    merge into demipt2.RUSN_DWH_DIM_CLIENTS_HIST dwh1
    using (
        select dwh.CLIENT_ID
        from RUSN_DWH_DIM_CLIENTS_HIST dwh
        left join RUSN_STG_CLIENTS_DEL stg_del on dwh.CLIENT_ID = stg_del.CLIENT_ID
        where stg_del.CLIENT_ID is null
                ) qaz 
    on (qaz.CLIENT_ID = dwh1.CLIENT_ID)
    when matched  then update set dwh1.EFFECTIVE_TO = (select last_update from rusn_meta where table_name='BANK.CLIENT')  
    """
    curs.execute(sql_cl_del)
    time.sleep(2)
    #-----------------------------V(ii) step 2; opening new version and setting deleted flag =Y for it
    # CLIENTS
    sql_cl_del_nv="""
    insert into demipt2.RUSN_DWH_DIM_CLIENTS_HIST (CLIENT_ID,  FIRST_NAME,  LAST_NAME,  PATRONYMIC,  DATE_OF_BIRTH,  
                                           PASSPORT_NUM,  PHONE,  EFFECTIVE_FROM,  EFFECTIVE_TO,  DELETED_FLG,  PASSPORT_VALID_TO) 
          (select    CLIENT_ID, 
                     FIRST_NAME, 
                     LAST_NAME, 
                     PATRONYMIC, 
                     DATE_OF_BIRTH, 
                     PASSPORT_NUM, 
                     PHONE, 
                     (select last_update  + interval '1' day from rusn_meta where table_name='BANK.CLIENT'),
                     date'2999-12-31', 
                     'Y',
                     PASSPORT_VALID_TO 
            from RUSN_DWH_DIM_CLIENTS_HIST dwh1
            where dwh1.CLIENT_ID in (
                                     select dwh.CLIENT_ID
                                     from RUSN_DWH_DIM_CLIENTS_HIST dwh
                                     left join RUSN_STG_CLIENTS_DEL stg_del on dwh.CLIENT_ID = stg_del.CLIENT_ID
                                     where stg_del.CLIENT_ID is null  
                                     )                                
            )     
    """
    curs.execute(sql_cl_del_nv)
    time.sleep(2)

    #-----------------------------VI  Updating MetaData table, updating timestamp of most recent captured record		
    curs.execute("""update demipt2.RUSN_META set LAST_UPDATE = to_date( '""" + upld_dt + """' , 'DDMMYYYY') where TABLE_NAME='BANK.CLIENT'	""" ) 

    #-----------------------------VII  commit upload
    conn.commit    

    #---------------------CLOSING 
    curs.close()
    #conn.close()  
    print("Clients upload run time is: " + str(time.time() - launch_time) + " sec.") 
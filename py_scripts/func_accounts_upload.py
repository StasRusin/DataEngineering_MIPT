#!/usr/bin/python
import pandas as pd
import jaydebeapi
import time
import os


def accounts_upload(conn, upld_dt):
    curs = conn.cursor()
    launch_time = time.time()
    # -------------------------------------------------------- I Cleaning Staging tables
    curs.execute("""delete from RUSN_STG_ACCOUNTS """)
    curs.execute("""delete from RUSN_STG_ACCOUNTS_DEL """)

    ##-- II capturing ACCOUNTS to staging

    curs.execute("""insert into demipt2.RUSN_STG_ACCOUNTS (ACCOUNT,  VALID_TO,  CLIENT,  CREATE_DT,  UPDATE_DT)
                    (select ACCOUNT,  VALID_TO,  CLIENT,  CREATE_DT,  UPDATE_DT  
                    from BANK.ACCOUNTS 
                                where coalesce(update_dt, create_dt) > 
                                                (select coalesce(last_update, to_date('1899-12-31', 'YYYY-MM-DD') ) from RUSN_META where table_name='BANK.ACCOUNTS'))"""
                 )
    time.sleep(2)

    # -- III   Capturing exisiting lines  into separate table to track DELETIONS. IDs only to be catured to minimize traffic and processing time !!!!
    # /*Capturing is made in two SEPARATE queries insert + update , do NOT combine these two into single query!!! as load might start before before midnight and finish after midnight,
    # making upload_date inconsistent*/
    curs.execute(""" insert into demipt2.RUSN_STG_ACCOUNTS_DEL (ACCOUNT)    
                    (select ACCOUNT from BANK.ACCOUNTS) """)
    time.sleep(1)
    curs.execute(
        """ update demipt2.RUSN_STG_ACCOUNTS_DEL set upload_date = to_date( '""" + upld_dt + """' , 'DDMMYYYY' ) where upload_date is null """)
    time.sleep(1)
    # -------------------- IV Detailed level processing: processing changes	from captured lines selecting those that already exist in staging
    # -----------------------------IV(i) step 1; closing current version
    # ACCOUNTS
    sql_acc_change = """
    merge into DEMIPT2.RUSN_DWH_DIM_ACCOUNTS_HIST dwh1
    using  (
                    select 
                             stg.ACCOUNT , -- as ACCOUNT_NUM
                              stg.UPDATE_DT --as EFFECTIVE_TO
                            from demipt2.RUSN_STG_ACCOUNTS stg 
                            left join RUSN_DWH_DIM_ACCOUNTS_HIST dwh on stg.ACCOUNT  = dwh.ACCOUNT_NUM  /* and stg.UPDATE_DT = (select max(upload_date) from demipt2.RUSN_STG_ACCOUNTS)*/
                            where dwh.ACCOUNT_NUM is not null and ( 1 = 0 
                                        or dwh.VALID_TO  <> stg.VALID_TO  or (stg.VALID_TO  is null and dwh.VALID_TO  is not null ) or ( stg.VALID_TO  is not null and dwh.VALID_TO  is null ) 
                                        or dwh.CLIENT  <> stg.CLIENT  or (stg.CLIENT  is null and dwh.CLIENT  is not null ) or ( stg.CLIENT  is not null and dwh.CLIENT  is null ) 
                                                                                                )
                                                                                         and  dwh.EFFECTIVE_TO=date'2999-12-31'      
                            ) qaz
    on (dwh1.ACCOUNT_NUM=qaz.ACCOUNT )
    when matched then update set dwh1.EFFECTIVE_TO = qaz.update_dt - interval '1' day where EFFECTIVE_TO=date'2999-12-31' """
    curs.execute(sql_acc_change)
    time.sleep(2)
    # -----------------------------IV(ii) step 2; opening new version for both amended or added lines. Common process is used for amended and inserted lines
    # ACCOUNTS
    sql_acc_add = """
    insert into RUSN_DWH_DIM_ACCOUNTS_HIST (
    ACCOUNT_NUM, 
    VALID_TO, 
    CLIENT, 
    EFFECTIVE_FROM, 
    EFFECTIVE_TO, 
    DELETED_FLG) 
                (select 
                    stg.ACCOUNT, 
                    stg.VALID_TO, 
                    stg.CLIENT, 
                    coalesce(stg.UPDATE_DT, stg.CREATE_DT),
                    date'2999-12-31',
                    'F'
                from RUSN_STG_ACCOUNTS stg                            
                where coalesce(stg.UPDATE_DT, stg.CREATE_DT) > (select last_update from rusn_meta where table_name='BANK.ACCOUNTS')
                   ) 
    """
    curs.execute(sql_acc_add)
    time.sleep(2)

    # -----------------------------V   Processing DELETIONS (using special tables)
    # -----------------------------V(i) step 1; closing current version
    # ACCOUNTS
    sql_acc_del = """
    merge into RUSN_DWH_DIM_ACCOUNTS_HIST dwh1
    using (
        select dwh.ACCOUNT_NUM
        from RUSN_DWH_DIM_ACCOUNTS_HIST dwh
        left join RUSN_STG_ACCOUNTS_DEL stg_del on dwh.ACCOUNT_NUM = stg_del.ACCOUNT
        where stg_del.ACCOUNT is null
                ) qaz 
    on ( qaz.ACCOUNT_NUM = dwh1.ACCOUNT_NUM )
    when matched  then update set dwh1.EFFECTIVE_TO = (select last_update from rusn_meta where table_name='BANK.ACCOUNT')        
    """
    curs.execute(sql_acc_del)
    time.sleep(2)
    # -----------------------------V(ii) step 2; opening new version and setting deleted flag =Y for it
    # ACCOUNTS
    sql_acc_del_nv = """
    insert into RUSN_DWH_DIM_ACCOUNTS_HIST (ACCOUNT_NUM,	VALID_TO,	CLIENT,	EFFECTIVE_FROM,	EFFECTIVE_TO,	DELETED_FLG  ) 
          (select    
                    ACCOUNT_NUM,
                    VALID_TO,
                    CLIENT,
                    (select last_update  + interval '1' day from rusn_meta where table_name='BANK.ACCOUNTS'),
                    date'2999-12-31', 
                    'Y'

            from RUSN_DWH_DIM_ACCOUNTS_HIST dwh1
            where dwh1.ACCOUNT_NUM in (
                                     select dwh.ACCOUNT_NUM
                                     from RUSN_DWH_DIM_ACCOUNTS_HIST dwh
                                     left join RUSN_STG_ACCOUNTS_DEL stg_del on dwh.ACCOUNT_NUM = stg_del.ACCOUNT
                                     where stg_del.ACCOUNT is null  
                                     )                                
            ) 
    """
    curs.execute(sql_acc_del_nv)
    time.sleep(2)

    # -----------------------------VI  Updating MetaData table, updating timestamp of most recent captured record
    curs.execute(
        """ update demipt2.RUSN_META set LAST_UPDATE = to_date( '""" + upld_dt + """' , 'DDMMYYYY') where TABLE_NAME='BANK.ACCOUNTS' """)

    # --------------------- VII  commit upload
    conn.commit

    # ---------------------CLOSING
    curs.close()
    # conn.close()
    print("Accounts upload run time is: " + str(time.time() - launch_time) + " sec.")
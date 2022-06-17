#!/usr/bin/python
import pandas as pd
import jaydebeapi
import time
import os

def cards_upload(conn, upld_dt):
    curs = conn.cursor()
    launch_time = time.time()  
    # -------------------------------------------------------- I Cleaning Staging tables
    curs.execute("""delete from RUSN_STG_CARDS """)
    curs.execute("""delete from RUSN_STG_CARDS_DEL """)

    ##-- II capturing CARDS to staging
    curs.execute("""		
    insert into demipt2.RUSN_STG_CARDS  (CARD_NUM,  ACCOUNT,  CREATE_DT,  UPDATE_DT)
                    (select CARD_NUM,  ACCOUNT,  CREATE_DT,  UPDATE_DT  
                    from BANK.CARDS
                        where coalesce(update_dt, create_dt) > 
                                (select coalesce (last_update , to_date('1899-12-31', 'DDMMYYYY') ) from RUSN_META where table_name = 'BANK.CARDS')     
                )  
    """)            
    time.sleep(2)   

    #-- III   Capturing exisiting lines  into separate table to track DELETIONS. IDs only to be catured to minimize traffic and processing time !!!!
    #/*Capturing is made in two SEPARATE queries insert + update , do NOT combine these two into single query!!! as load might start before before midnight and finish after midnight, 
    #making upload_date inconsistent*/
    curs.execute(""" insert into demipt2.RUSN_STG_CARDS_DEL (CARD_NUM)
                    (select CARD_NUM from BANK.CARDS) """ )
    time.sleep(1)
    curs.execute(""" update demipt2.RUSN_STG_CARDS_DEL set upload_date = to_date( '""" + upld_dt +"""' , 'DDMMYYYY' ) where upload_date is null """)
    time.sleep(1)
    #-------------------- IV Detailed level processing: processing changes	from captured lines selecting those that already exist in staging	
    #-----------------------------IV(i) step 1; closing current version
    # CARDS
    sql_cards_change="""
    merge into DEMIPT2.RUSN_DWH_DIM_CARDS_HIST dwh1
    using  (
                    select 
                             stg.CARD_NUM ,-- as ID_SOURCE
                              stg.UPDATE_DT --as EFFECTIVE_TO
                            from demipt2.RUSN_STG_CARDS stg 
                            left join DEMIPT2.RUSN_DWH_DIM_CARDS_HIST dwh on stg.CARD_NUM  = dwh.CARD_NUM /*and stg.UPDATE_DT = (select max(upload_date) from demipt2.RUSN_STG_ACCOUNTS)*/
                            where dwh.CARD_NUM is not null and ( 1 = 0 
                                    or dwh.ACCOUNT_NUM <> stg.ACCOUNT 
                                    or (stg.ACCOUNT  is null and dwh.ACCOUNT_NUM  is not null ) 
                                    or (stg.ACCOUNT  is not null and dwh.ACCOUNT_NUM  is null ) 
                                                                                                )
                                                     and  dwh.EFFECTIVE_TO=date'2999-12-31'      
                            ) qaz
    on (dwh1.CARD_NUM = qaz.CARD_NUM )
    when matched then update set dwh1.EFFECTIVE_TO = qaz.update_dt - interval '1' day where EFFECTIVE_TO=date'2999-12-31' """
    curs.execute(sql_cards_change)
    time.sleep(2)
    #-----------------------------IV(ii) step 2; opening new version for both amended or added lines. Common process is used for amended and inserted lines 
    # CARDS
    sql_cadrs_add="""
    insert into RUSN_DWH_DIM_CARDS_HIST (
                    CARD_NUM, 
                    ACCOUNT_NUM, 
                    EFFECTIVE_FROM, 
                    EFFECTIVE_TO, 
                    DELETED_FLG) 
                                (select 
                    stg.CARD_NUM, 
                    stg.ACCOUNT, 
                    coalesce(stg.UPDATE_DT, stg.CREATE_DT),
                    date'2999-12-31',
                    'F'
                from RUSN_STG_CARDS stg                            
                where coalesce(stg.UPDATE_DT, stg.CREATE_DT) > (select last_update from rusn_meta where table_name='BANK.CARDS') 
                               ) 
    """
    curs.execute(sql_cadrs_add)
    time.sleep(2)  
                
    #-----------------------------V   Processing DELETIONS (using special tables)
    #-----------------------------V(i) step 1; closing current version
    # ACCOUNTS
    # CARDS
    sql_cards_del="""
    merge into RUSN_DWH_DIM_CARDS_HIST dwh1
    using (
        select dwh.CARD_NUM 
        from RUSN_DWH_DIM_CARDS_HIST dwh
        left join RUSN_STG_CARDS_DEL stg_del on dwh.CARD_NUM  = stg_del.CARD_NUM
        where stg_del.CARD_NUM is null
                ) qaz 
    on ( qaz.CARD_NUM = dwh1.CARD_NUM )
    when matched  then update set dwh1.EFFECTIVE_TO = (select last_update from rusn_meta where table_name='BANK.CARDS')       
    """
    curs.execute(sql_cards_del)
    time.sleep(2)
    #-----------------------------V(ii) step 2; opening new version and setting deleted flag =Y for it
    # CARDS
    sql_card_del_nv="""
    insert into RUSN_DWH_DIM_CARDS_HIST (CARD_NUM,	ACCOUNT_NUM,	EFFECTIVE_FROM,	EFFECTIVE_TO,	DELETED_FLG ) 
          (select    CARD_NUM	,
                     ACCOUNT_NUM	,
                     (select last_update  + interval '1' day from rusn_meta where table_name='BANK.CARDS'),
                     date'2999-12-31', 
                     'Y'
                     
            from RUSN_DWH_DIM_CARDS_HIST dwh1
            where dwh1.CARD_NUM in (
                                     select dwh.CARD_NUM
                                     from RUSN_DWH_DIM_CARDS_HIST dwh
                                     left join RUSN_STG_CARDS_DEL stg_del on dwh.CARD_NUM = stg_del.CARD_NUM
                                     where stg_del.CARD_NUM is null  
                                     )                                
            )   
    """
    curs.execute(sql_card_del_nv)
    time.sleep(2)

    #-----------------------------VI  Updating MetaData table, updating timestamp of most recent captured record		
    curs.execute(""" update demipt2.RUSN_META set LAST_UPDATE = to_date( '""" + upld_dt + """' , 'DDMMYYYY') where TABLE_NAME='BANK.CARDS' """ ) 


    #--------------------- VII  commit upload
    conn.commit    

    #---------------------CLOSING 
    curs.close()
    #conn.close()  
    print("Cards upload run time is: " + str(time.time() - launch_time) + " sec.") 
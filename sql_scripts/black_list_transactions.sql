--reporting (only transactions of customers with passports from black list)
insert into  RUSN_REP_FRAUD (EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE/*, REPORT_DT*/)
        (select trans.TRANS_DATE as event_dt, 
        clt.passport_num as passport,
        clt.FIRST_NAME || ' ' || clt.PATRONYMIC || ' ' || clt.LAST_NAME as FIO,
        clt.PHONE, 
        'Black list client' as event_type
		from RUSN_DWH_FACT_TRANSACTIONS trans
        left join RUSN_DWH_DIM_CARDS_HIST crd on crd.card_num=trans.card_num and 
                                                                                trans.TRANS_DATE between crd.effective_from and crd.effective_to
        left join RUSN_DWH_DIM_ACCOUNTS_HIST acct on acct.account_num=crd.account_num and 
                                                                                         trans.TRANS_DATE between acct.effective_from and acct.effective_to
        left join RUSN_DWH_DIM_CLIENTS_HIST clt on clt.client_id=acct.client and
                                                                                 trans.TRANS_DATE between clt.effective_from and clt.effective_to 
        left join  (select clt.client_id, clt.passport_num, blcl.ENTRY_DT
                         from RUSN_DWH_DIM_CLIENTS_HIST clt 
                         left join RUSN_DWH_FACT_PSSPRT_BLCKLST blcl on  (blcl.passport_num =
                                            (substr(clt.passport_num, 1, 4) || substr(clt.passport_num, instr(clt.passport_num, ' ', 1, 1)+1 , (length(clt.passport_num)-instr(clt.passport_num, ' ', 1, 1)+1)) )
                                                                         )   
                         where blcl.passport_num is not null) qaz on qaz.client_id = clt.client_id
		where qaz.client_id is not null 
        and trans.TRANS_DATE >=qaz.ENTRY_DT 
        and trans.oper_result = 'SUCCESS' 
		and trans.TRANS_DATE between to_timestamp(':upld_dt 00:00:00', 'DDMMYYYY HH24:MI:SS')  and to_timestamp(':upld_dt 23:59:59', 'DDMMYYYY HH24:MI:SS') 
		
		union ALL
		
		select 
        trans.TRANS_DATE as event_dt, 
        clt.passport_num as passport,
        clt.FIRST_NAME || ' ' || clt.PATRONYMIC || ' ' || clt.LAST_NAME as FIO,
        clt.PHONE, 
        'Expired passports transactions' as event_type

        from RUSN_DWH_FACT_TRANSACTIONS trans
        left join RUSN_DWH_DIM_CARDS_HIST crd on crd.card_num=trans.card_num and 
                                                                                trans.TRANS_DATE between crd.effective_from and crd.effective_to
        left join RUSN_DWH_DIM_ACCOUNTS_HIST acct on acct.account_num=crd.account_num and 
                                                                                         trans.TRANS_DATE between acct.effective_from and acct.effective_to
        left join RUSN_DWH_DIM_CLIENTS_HIST clt on clt.client_id=acct.client and
                                                                                 trans.TRANS_DATE between clt.effective_from and clt.effective_to  
        where trans.TRANS_DATE > clt.passport_valid_to
        and trans.oper_result = 'SUCCESS' 
		and trans.TRANS_DATE between to_timestamp(':upld_dt 00:00:00', 'DDMMYYYY HH24:MI:SS')  and to_timestamp(':upld_dt 23:59:59', 'DDMMYYYY HH24:MI:SS') 
		)
			
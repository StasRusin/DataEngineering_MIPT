insert into  RUSN_REP_FRAUD (EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE)
(
select 
        --trans1.TRANS_DATE as event_dt, 
        trans.TRANS_DATE as event_dt, 
        --trans.trans_id, trans1.trans_id,
        clt.passport_num as passport,
        clt.FIRST_NAME || ' ' || clt.PATRONYMIC || ' ' || clt.LAST_NAME as FIO,
        clt.PHONE, 
        'Diff cities within 1 hour' as event_type/*,
        date'2021-03-01' as report_dt*/
        --termn.terminal_city,
        --termn1.terminal_city
		--select count (trans1.TRANS_DATE)
        from RUSN_DWH_FACT_TRANSACTIONS trans
        inner join RUSN_DWH_FACT_TRANSACTIONS trans1 on    trans.card_num=trans1.card_num and 
                                                                       (trans1.TRANS_DATE - trans.TRANS_DATE ) between interval '0' MINUTE and interval '60' MINUTE 
        left join RUSN_DWH_DIM_TERMINALS_HIST termn on termn.TERMINAL_ID = trans.TERMINAL and trans.trans_date between termn.effective_from and termn.effective_to
        left join RUSN_DWH_DIM_TERMINALS_HIST termn1 on termn1.TERMINAL_ID = trans1.TERMINAL and trans1.trans_date between termn1.effective_from and termn1.effective_to
        left join RUSN_DWH_DIM_CARDS_HIST crd on crd.card_num=trans.card_num and 
                                                                                trans.TRANS_DATE between crd.effective_from and crd.effective_to
        left join RUSN_DWH_DIM_ACCOUNTS_HIST acct on acct.account_num=crd.account_num and 
                                                                                         trans.TRANS_DATE between acct.effective_from and acct.effective_to
        left join RUSN_DWH_DIM_CLIENTS_HIST clt on clt.client_id=acct.client and
                                                                                 trans.TRANS_DATE between clt.effective_from and clt.effective_to  
        where termn1.TERMINAL_CITY  <> termn.TERMINAL_CITY
		and trans.TRANS_DATE between to_timestamp(':upld_dt 00:00:00', 'DDMMYYYY HH24:MI:SS')  and to_timestamp(':upld_dt 23:59:59', 'DDMMYYYY HH24:MI:SS') 
)

insert into  RUSN_REP_FRAUD (EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE)
(select TRANS_DATE, passport, FIO, PHONE, event_type 
from (
            select trans.TRANS_DATE, clt.passport_num as passport, clt.FIRST_NAME || ' ' || clt.PATRONYMIC || ' ' || clt.LAST_NAME as FIO, clt.PHONE, 'Amount Enumeration' as event_type
						, trans.OPER_RESULT
                        , lag(trans.OPER_RESULT, 1) over (partition by clt.client_id order by trans.TRANS_DATE) as prev_trans_resl_1
                        , lag(trans.OPER_RESULT, 2) over (partition by clt.client_id order by trans.TRANS_DATE) as prev_trans_resl_2
                        , lag(trans.OPER_RESULT, 3) over (partition by clt.client_id order by trans.TRANS_DATE) as prev_trans_resl_3
                        , trans.AMNT
                        , lag(trans.AMNT, 1) over (partition by clt.client_id order by trans.TRANS_DATE) as prev_trans_amnt_1
                        , lag(trans.AMNT, 2) over (partition by clt.client_id order by trans.TRANS_DATE) as prev_trans_amnt_2
                        , lag(trans.AMNT, 3) over (partition by clt.client_id order by trans.TRANS_DATE) as prev_trans_amnt_3
                  
                        , lag(trans.TRANS_DATE, 4) over (partition by clt.client_id order by trans.TRANS_DATE) as prev_four_trans
                        
            from RUSN_DWH_FACT_TRANSACTIONS trans
                    left join RUSN_DWH_DIM_CARDS_HIST crd on crd.card_num=trans.card_num and 
                                                                                            trans.TRANS_DATE between crd.effective_from and crd.effective_to
                    left join RUSN_DWH_DIM_ACCOUNTS_HIST acct on acct.account_num=crd.account_num and 
                                                                                                     trans.TRANS_DATE between acct.effective_from and acct.effective_to
                    left join RUSN_DWH_DIM_CLIENTS_HIST clt on clt.client_id=acct.client and
                                                                                             trans.TRANS_DATE between clt.effective_from and clt.effective_to 
        )  where OPER_RESULT='SUCCESS' and  prev_trans_resl_1='REJECT ' and  prev_trans_resl_2='REJECT ' and  prev_trans_resl_3='REJECT ' 
                and prev_trans_amnt_3 > prev_trans_amnt_2 and prev_trans_amnt_2 > prev_trans_amnt_1 and prev_trans_amnt_1 > AMNT
                and (trans_date - prev_four_trans) <= interval '20' minute
	 and TRANS_DATE between to_timestamp(':upld_dt 00:00:00', 'DDMMYYYY HH24:MI:SS')  and to_timestamp(':upld_dt 23:59:59', 'DDMMYYYY HH24:MI:SS') 
)     

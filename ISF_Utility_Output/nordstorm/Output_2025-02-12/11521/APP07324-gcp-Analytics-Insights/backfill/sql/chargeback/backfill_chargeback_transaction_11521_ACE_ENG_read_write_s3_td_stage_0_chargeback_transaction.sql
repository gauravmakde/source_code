SET QUERY_BAND = 'App_ID=APP09301;
     DAG_ID=chargeback_transaction_11521_ACE_ENG;
     Task_Name=chargeback_transaction;'
     FOR SESSION VOLATILE;
/*
Table: T2DL_DAS_TRUST_ENGINE_PROD.chargeback_load
       T2DL_DAS_TRUST_ENGINE_PROD.chargeback_transaction
Owner: Rujira Achawanantakun
Modified: 2024-09-18
- Loading chargeback data to T2 tables
- Joining chargeback with NAP transaction, and load it to T2 tables
*/
-- create non-duplicated chargeback data
create multiset volatile table chargeback_tab as (
    select
        case_id,
        loss_type_cd,
        loss_type,
        ft_reported_date,
        case_open_date,
        fraud_tag_date,
        transaction_date,
        ft_tran_id,
        trxn_amt,
        reference23_nr,
        industry_transaction_id,
        authorization_cd,
        mail_phone_indicator_cd,
        pos_entry_mode_cd,
        merchant_account_id,
        merchant_nm,
        merch_cat_code,
        merchant_postal_cd,
        CURRENT_TIMESTAMP as dw_sys_load_tmstp
	from T2DL_DAS_TRUST_ENGINE_PROD.chargeback_load cl
	where not exists (select 1
                      from T2DL_DAS_TRUST_ENGINE_PROD.chargeback t
                      where t.case_id = cl.case_id
                        and t.ft_tran_id = cl.ft_tran_id
                    )
) with data on commit preserve rows
;

-- add zero padding to loss_type_cd that have one digit number
update chargeback_tab
set loss_type_cd = LPAD(TRIM(loss_type_cd), 2, '0')
where REGEXP_SIMILAR(loss_type_cd, '^[0-9]') = 1
;

-- prod data lab: insert chargeback data from a staging table to the chargeback table
insert into T2DL_DAS_TRUST_ENGINE_PROD.chargeback
    select *
    from chargeback_tab
    QUALIFY ROW_NUMBER()
    OVER (PARTITION BY case_id, ft_tran_id
          ORDER BY fraud_tag_date
         ) = 1
;

-- get all sale transactions within the last 90 days
create multiset volatile table transaction_tab as (
    select
       cast(rttf.global_tran_id as char(13)) as global_tran_id,
        rttf.business_day_date,
        psrdfv.entry_mode,
        psrdfv.token_value,
        pstmf.settlement_result_received_id,
        psrdfv.network_reference_number,
        psrdfv.visa_transaction_id,
        sd.business_unit_desc,
        sd.store_address_city,
        sd.store_address_state,
        sd.store_postal_code,
        rtdf.intent_store_num,
        rthf.ringing_store_num,
        rthf.register_num,
        rthf.tran_num,
        rtdf.line_item_seq_num,
        rtdf.merch_dept_num,
        rthf.online_shopper_id,
        rthf.deterministic_profile_id,
        case
             when rttf.tender_type_code = 'cash'
              then 'cash'
             when rttf.tender_type_code = 'affirm'
              then 'affirm'
             when rttf.tender_type_code in ('paypal','pp')
              then 'paypal'
             when rttf.tender_type_code = 'check'
              then 'check'
             when rttf.card_type_code = 'nc'
              and rttf.card_subtype_code in ('md','nd')
              then 'nordstrom debit'
             when rttf.card_type_code = 'nv'
              and rttf.card_subtype_code in ('tp','tv','rv')
              or rttf.card_type_code = 'nv'
              then 'nordstrom visa'
             when rttf.card_type_code = 'nc'
              and rttf.card_subtype_code in ('rt','tr','rr')
              then 'nordstrom retail'
             when rttf.card_type_code = 'nb'
              then 'nordstrom corporate'
             when rttf.tender_type_code = 'nordstrom_note'
              or rttf.card_subtype_code in ('nn')
              then 'nordstrom note'
             when rttf.tender_type_code = 'gift card'
              or rttf.card_subtype_code in ('gc')
              then 'gift card'
             when rttf.card_type_code = 'vc'
              then 'visa'
             when rttf.card_type_code = 'mc'
              then 'mastercard'
             when rttf.card_type_code = 'ds'
              then 'discover'
             when rttf.card_type_code = 'ae'
              then 'amex'
             when rttf.card_type_code = 'jc'
              then 'jcb'
             when rttf.card_type_code = 'dc'
              then 'diners'
             when rttf.card_type_code = 'ia'
              then 'interact debit'
            else 'other'
        end as tender,
        rttf.tran_type_code,
        rttf.tender_item_entry_method_code as tender_entry_method_code,
        case
            when rttf.tender_item_entry_method_code = '0'
             then 'no entry method'
            when rttf.tender_item_entry_method_code = '1'
             then 'scanned'
            when rttf.tender_item_entry_method_code in ('2', '10')
             then 'swiped'
            when rttf.tender_item_entry_method_code = '3'
             then 'handkeyed'
            when rttf.tender_item_entry_method_code = '4'
             then 'qr code nordstrom txt'
            when rttf.tender_item_entry_method_code in ('5', '7')
             then 'card free shopping'
            when rttf.tender_item_entry_method_code = '6'
             then 'card on file'
            when rttf.tender_item_entry_method_code = '8'
             then 'dipped (chip card read)'
            when rttf.tender_item_entry_method_code = '9'
             then 'tapped'
            when rttf.tender_item_entry_method_code = '11'
             then 'qr code in app'
            when rttf.tender_item_entry_method_code = '12'
             then 'wallet (applepay)'
            else rttf.tender_item_entry_method_code
        end as entry_method_desc,
        rtdf.line_item_order_type,
        rttf.tender_item_usd_amt,
        rtdf.commission_slsprsn_num,
        rtdf.employee_discount_flag,
        rtdf.employee_discount_num
    from prd_nap_usr_vws.payment_settlement_transaction_match_fact pstmf
        inner join prd_nap_usr_vws.payment_settlement_result_details_fact_vw psrdfv
            on pstmf.settlement_result_received_id = psrdfv.settlement_result_received_id
        inner join prd_nap_usr_vws.retail_tran_tender_fact rttf
            on pstmf.global_transaction_id = rttf.global_tran_id
            and pstmf.ertm_tender_item_sequence = rttf.tender_item_seq_num
        inner join prd_nap_usr_vws.retail_tran_hdr_fact rthf
            on rttf.global_tran_id  = rthf.global_tran_id
        inner join prd_nap_usr_vws.retail_tran_detail_fact rtdf
            on rttf.global_tran_id  = rtdf.global_tran_id
            and rttf.tender_item_seq_num  = rtdf.line_item_seq_num
        inner join prd_nap_usr_vws.store_dim sd
            on rthf.ringing_store_num = sd.store_num
    where
        rttf.tender_item_usd_amt <> 0
        and rttf.tran_latest_version_ind = 'y'
        and rttf.tran_type_code = 'SALE'
        and sd.store_country_code = 'US'
        and rtdf.line_net_usd_amt <> 0
        and rttf.business_day_date between (select min(fraud_tag_date) - 90 from chargeback_tab) and (select max(fraud_tag_date) from chargeback_tab)
) with data on commit preserve rows
;

-- join chargeback and transaction
insert into T2DL_DAS_TRUST_ENGINE_PROD.chargeback_transaction
    select distinct
        b.case_id,
        b.loss_type_cd,
        b.loss_type,
        b.ft_reported_date,
        b.case_open_date,
        b.fraud_tag_date,
        b.transaction_date,
        b.ft_tran_id,
        b.trxn_amt,
        b.reference23_nr,
        b.industry_transaction_id,
        b.authorization_cd,
        b.mail_phone_indicator_cd,
        b.pos_entry_mode_cd,
        b.merchant_account_id,
        b.merchant_nm,
        b.merch_cat_code,
        b.merchant_postal_cd,
        a.global_tran_id,
        a.business_day_date,
        a.settlement_result_received_id,
        a.network_reference_number,
        a.visa_transaction_id,
        a.online_shopper_id,
        a.deterministic_profile_id,
        a.token_value,
        a.tender,
        a.tender_item_usd_amt,
        a.tender_entry_method_code,
        a.entry_mode,
        a.entry_method_desc,
        a.line_item_order_type,
        a.business_unit_desc,
        a.store_address_city,
        a.store_address_state,
        a.store_postal_code,
        a.intent_store_num,
        a.ringing_store_num,
        a.register_num,
        a.tran_num,
        a.line_item_seq_num,
        a.merch_dept_num,
        a.commission_slsprsn_num,
        a.employee_discount_flag,
        a.employee_discount_num,
        CURRENT_TIMESTAMP as dw_sys_load_tmstp
    from transaction_tab a
    inner join chargeback_tab b
        on a.network_reference_number = b.reference23_nr
        and a.network_reference_number is not null
        and b.reference23_nr is not null
    where not exists (select 1
                      from T2DL_DAS_TRUST_ENGINE_PROD.chargeback_transaction t
                      where t.case_id = b.case_id
                        and t.ft_tran_id = b.ft_tran_id
                )
;

-- drop staging table
drop table T2DL_DAS_TRUST_ENGINE_PROD.chargeback_load;

SET QUERY_BAND = NONE FOR SESSION;

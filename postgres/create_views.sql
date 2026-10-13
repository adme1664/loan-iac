CREATE OR REPLACE VIEW commercialloan.deal_view AS
SELECT
    deal.borrowing_agmt_id AS deal_no,
    deal.deal_id,
    -- Calculate latest timestamp across all related entities
    GREATEST(deal.sys_ins_ts, COALESCE(deal.sys_upd_ts, deal.sys_ins_ts)) AS latest_sys_ts,
    -- Build complete JSON object with nested related entities
    json_build_object(
            'dealNo', deal.borrowing_agmt_id,
            'deal', json_build_object(
                    'dealId', deal.deal_id,
                    'borrowingAgreementId', deal.borrowing_agmt_id,
                    'borrowingAgreementName', deal.borrowing_agmt_name,
                    'effectiveDate', deal.effctv_dt,
                    'classificationCode', deal.classification_cd,
                    'nextReviewDate', deal.next_review_dt
                    ),
            'creditFacilities', COALESCE(
                    (SELECT json_agg(
                                    json_build_object(
                                            'creditFacilityId', credit_facility.credit_facility_id,
                                            'creditFacilityNo', credit_facility.credit_facility_no,
                                            'borrowingAgreementId', credit_facility.borrowing_agmt_id,
                                            'creditFacilityName', credit_facility.credit_facility_name,
                                            'statusCode', credit_facility.status_cd,
                                            'statusCodeSource', credit_facility.status_cd_src,
                                            'productCode', credit_facility.product_cd,
                                            'lenderRoleCode', credit_facility.lender_role_cd,
                                            'effectiveDate', credit_facility.effctv_dt,
                                            'expiryDate', credit_facility.expiry_dt,
                                            'maturityDate', credit_facility.maturity_dt,
                                            'currency', credit_facility.currency,
                                            'agreementRelations', COALESCE(
                                                    (SELECT json_agg(
                                                                    json_build_object(
                                                                            'agreementRelationId', agreement_relation.agreement_relation_id,
                                                                            'creditFacilityId', agreement_relation.credit_facility_id,
                                                                            'agreementId', agreement_relation.agmt_id
                                                                    )
                                                            )
                                                     FROM commercialloan.tblr_agreement_relation agreement_relation
                                                     WHERE agreement_relation.credit_facility_id = credit_facility.credit_facility_id),
                                                    '[]'::json
                                                                  )
                                    )
                            )
                     FROM commercialloan.tbl_credit_facility credit_facility
                     WHERE credit_facility.borrowing_agmt_id = deal.borrowing_agmt_id),
                    '[]'::json
                                ),
            'parties', COALESCE(
                    (SELECT json_agg(
                                    json_build_object(
                                            'partyId', party.cf_party_id,
                                            'dealId', party.deal_id,
                                            'unbankId', party.unbnk_id,
                                            'memberIdNo', party.member_id_no
                                    )
                            )
                     FROM commercialloan.tbl_cf_party party
                     WHERE party.deal_id = deal.deal_id),
                    '[]'::json
                       )
    ) AS deal_json
FROM commercialloan.tbl_deal deal;

-- =====================================================================
-- View for Kafka Publishing - Agreement Bundle with JSON
-- Aggregates agreement with related entities (balance, party, interest rate, payment schedule)
-- =====================================================================

CREATE OR REPLACE VIEW commercialloan.loan_view AS
SELECT
    agreement.agreement_no,
    agreement.agreement_id,
    -- Calculate latest timestamp across all related entities
    GREATEST(agreement.sys_ins_ts, COALESCE(agreement.sys_upd_ts, agreement.sys_ins_ts)) AS latest_sys_ts,
    -- Build complete JSON object with nested related entities
    json_build_object(
        'agreementNo', agreement.agreement_no,
        'agreementType',agreement.agreement_type,
        'agreement', json_build_object(
            'agreementId', agreement.agreement_id,
            'agreementNo', agreement.agreement_no,
            'creditFacilityNo', agreement.credit_facility_no,
            'agreementType', agreement.agreement_type,
            'productCode', agreement.product_cd,
            'statusCodeSoure', agreement.status_cd_src,
            'statusCd', agreement.status_cd,
            'unbnkLetterIssuerInd', agreement.unbnk_letter_issuer_ind,
            'originalAmt', agreement.original_amt,
            'effctvDt', agreement.effctv_dt,
            'expiryDt', agreement.expiry_dt,
            'maturityDt', agreement.maturity_dt,
            'currency', agreement.currency,
            'sourceSystem', agreement.source_system,
            'deletedInd', agreement.deleted_ind
        ),
        'balances', COALESCE(
            (SELECT json_agg(
                json_build_object(
                    'balanceId', balance.balance_id,
                    'agreementId', balance.agreement_id,
                    'balanceType', balance.balance_type,
                    'creditAmnt', balance.credit_amnt,
                    'debitAmnt', balance.debit_amnt,
                    'balanceDate', balance.balance_date
                )
            )
            FROM commercialloan.tbl_balance balance
            WHERE balance.agreement_id = agreement.agreement_id),
            '[]'::json
        ),
        'interestRates', COALESCE(
            (SELECT json_agg(
                json_build_object(
                    'interestRateId', interest_rate.interest_rate_id,
                    'agreementId', interest_rate.agreement_id,
                    'interestRateType', interest_rate.interest_rate_type,
                    'interestRate', interest_rate.interest_rate,
                    'baseRate', interest_rate.base_rate,
                    'spreadRate', interest_rate.spread_rate,
                    'effectiveDate', interest_rate.effective_dt
                )
            )
            FROM commercialloan.tbl_interest_rate interest_rate
            WHERE interest_rate.agreement_id = agreement.agreement_id),
            '[]'::json
        ),
        'paymentSchedules', COALESCE(
            (SELECT json_agg(
                json_build_object(
                    'paymentScheduleId', payment_schedule.payment_schedule_id,
                    'agreementId', payment_schedule.agreement_id,
                    'paymentDate', payment_schedule.payment_date,
                    'paymentAmnt', payment_schedule.payment_amnt,
                    'businessDate', payment_schedule.schedule_date
                )
            )
            FROM commercialloan.tbl_payment_schedule payment_schedule
            WHERE payment_schedule.agreement_id = agreement.agreement_id),
            '[]'::json
        ),
        'party', (
            SELECT json_build_object(
                'partyId', party.party_id,
                'agreementId', party.agreement_id,
                'unbnkId', party.unbnk_id,
                'memberIdNo', party.member_id_no
            )
            FROM commercialloan.tbl_party party
            WHERE party.agreement_id = agreement.agreement_id
            LIMIT 1
        )
    ) AS agreement_json
FROM commercialloan.tbl_agreement agreement where agreement.agreement_type='LOAN';

CREATE OR REPLACE VIEW commercialloan.loc_view AS
SELECT
    agreement.agreement_no,
    agreement.agreement_id,
    -- Calculate latest timestamp across all related entities
    GREATEST(agreement.sys_ins_ts, COALESCE(agreement.sys_upd_ts, agreement.sys_ins_ts)) AS latest_sys_ts,
    -- Build complete JSON object with nested related entities
    json_build_object(
            'agreementNo', agreement.agreement_no,
            'agreementType',agreement.agreement_type,
            'agreement', json_build_object(
                    'agreementId', agreement.agreement_id,
                    'agreementNo', agreement.agreement_no,
                    'creditFacilityNo', agreement.credit_facility_no,
                    'agreementType', agreement.agreement_type,
                    'productCode', agreement.product_cd,
                    'statusCodeSoure', agreement.status_cd_src,
                    'statusCd', agreement.status_cd,
                    'unbnkLetterIssuerInd', agreement.unbnk_letter_issuer_ind,
                    'originalAmt', agreement.original_amt,
                    'effctvDt', agreement.effctv_dt,
                    'expiryDt', agreement.expiry_dt,
                    'maturityDt', agreement.maturity_dt,
                    'currency', agreement.currency,
                    'sourceSystem', agreement.source_system,
                    'deletedInd', agreement.deleted_ind
                         ),
            'balances', COALESCE(
                    (SELECT json_agg(
                                    json_build_object(
                                            'balanceId', balance.balance_id,
                                            'agreementId', balance.agreement_id,
                                            'balanceType', balance.balance_type,
                                            'creditAmnt', balance.credit_amnt,
                                            'debitAmnt', balance.debit_amnt,
                                            'balanceDate', balance.balance_date
                                    )
                            )
                     FROM commercialloan.tbl_balance balance
                     WHERE balance.agreement_id = agreement.agreement_id),
                    '[]'::json
                        ),
            'interestRates', COALESCE(
                    (SELECT json_agg(
                                    json_build_object(
                                            'interestRateId', interest_rate.interest_rate_id,
                                            'agreementId', interest_rate.agreement_id,
                                            'interestRateType', interest_rate.interest_rate_type,
                                            'interestRate', interest_rate.interest_rate,
                                            'baseRate', interest_rate.base_rate,
                                            'spreadRate', interest_rate.spread_rate,
                                            'effectiveDate', interest_rate.effective_dt
                                    )
                            )
                     FROM commercialloan.tbl_interest_rate interest_rate
                     WHERE interest_rate.agreement_id = agreement.agreement_id),
                    '[]'::json
                             ),
            'paymentSchedules', COALESCE(
                    (SELECT json_agg(
                                    json_build_object(
                                            'paymentScheduleId', payment_schedule.payment_schedule_id,
                                            'agreementId', payment_schedule.agreement_id,
                                            'paymentDate', payment_schedule.payment_date,
                                            'paymentAmnt', payment_schedule.payment_amnt,
                                            'businessDate', payment_schedule.schedule_date
                                    )
                            )
                     FROM commercialloan.tbl_payment_schedule payment_schedule
                     WHERE payment_schedule.agreement_id = agreement.agreement_id),
                    '[]'::json
                                ),
            'party', (
                SELECT json_build_object(
                               'partyId', party.party_id,
                               'agreementId', party.agreement_id,
                               'unbnkId', party.unbnk_id,
                               'memberIdNo', party.member_id_no
                       )
                FROM commercialloan.tbl_party party
                WHERE party.agreement_id = agreement.agreement_id
                LIMIT 1
            )
    ) AS agreement_json
FROM commercialloan.tbl_agreement agreement where agreement.agreement_type='LINE_OF_CREDIT';

CREATE OR REPLACE VIEW commercialloan.letter_view AS
SELECT
    agreement.agreement_no,
    agreement.agreement_id,
    -- Calculate latest timestamp across all related entities
    GREATEST(agreement.sys_ins_ts, COALESCE(agreement.sys_upd_ts, agreement.sys_ins_ts)) AS latest_sys_ts,
    -- Build complete JSON object with nested related entities
    json_build_object(
            'agreementNo', agreement.agreement_no,
            'agreementType',agreement.agreement_type,
            'agreement', json_build_object(
                    'agreementId', agreement.agreement_id,
                    'agreementNo', agreement.agreement_no,
                    'creditFacilityNo', agreement.credit_facility_no,
                    'agreementType', agreement.agreement_type,
                    'productCode', agreement.product_cd,
                    'statusCodeSoure', agreement.status_cd_src,
                    'statusCd', agreement.status_cd,
                    'unbnkLetterIssuerInd', agreement.unbnk_letter_issuer_ind,
                    'originalAmt', agreement.original_amt,
                    'effctvDt', agreement.effctv_dt,
                    'expiryDt', agreement.expiry_dt,
                    'maturityDt', agreement.maturity_dt,
                    'currency', agreement.currency,
                    'sourceSystem', agreement.source_system,
                    'deletedInd', agreement.deleted_ind
                         ),
            'balances', COALESCE(
                    (SELECT json_agg(
                                    json_build_object(
                                            'balanceId', balance.balance_id,
                                            'agreementId', balance.agreement_id,
                                            'balanceType', balance.balance_type,
                                            'creditAmnt', balance.credit_amnt,
                                            'debitAmnt', balance.debit_amnt,
                                            'balanceDate', balance.balance_date
                                    )
                            )
                     FROM commercialloan.tbl_balance balance
                     WHERE balance.agreement_id = agreement.agreement_id),
                    '[]'::json
                        ),
            'interestRates', COALESCE(
                    (SELECT json_agg(
                                    json_build_object(
                                            'interestRateId', interest_rate.interest_rate_id,
                                            'agreementId', interest_rate.agreement_id,
                                            'interestRateType', interest_rate.interest_rate_type,
                                            'interestRate', interest_rate.interest_rate,
                                            'baseRate', interest_rate.base_rate,
                                            'spreadRate', interest_rate.spread_rate,
                                            'effectiveDate', interest_rate.effective_dt
                                    )
                            )
                     FROM commercialloan.tbl_interest_rate interest_rate
                     WHERE interest_rate.agreement_id = agreement.agreement_id),
                    '[]'::json
                             ),
            'paymentSchedules', COALESCE(
                    (SELECT json_agg(
                                    json_build_object(
                                            'paymentScheduleId', payment_schedule.payment_schedule_id,
                                            'agreementId', payment_schedule.agreement_id,
                                            'paymentDate', payment_schedule.payment_date,
                                            'paymentAmnt', payment_schedule.payment_amnt,
                                            'businessDate', payment_schedule.schedule_date
                                    )
                            )
                     FROM commercialloan.tbl_payment_schedule payment_schedule
                     WHERE payment_schedule.agreement_id = agreement.agreement_id),
                    '[]'::json
                                ),
            'party', (
                SELECT json_build_object(
                               'partyId', party.party_id,
                               'agreementId', party.agreement_id,
                               'unbnkId', party.unbnk_id,
                               'memberIdNo', party.member_id_no
                       )
                FROM commercialloan.tbl_party party
                WHERE party.agreement_id = agreement.agreement_id
                LIMIT 1
            )
    ) AS letter_json
FROM commercialloan.tbl_agreement agreement where agreement.agreement_type='GUARANTY_LETTER';



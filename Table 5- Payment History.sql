CREATE
OR REPLACE TABLE analytics.credit.VM_R6T6A_Trade_Line_Payment_Summary AS
SELECT
    CONCAT(
        tl.OPEN_DATE,
        '_',
        tl.SUBSCRIBER_CODE,
        '_',
        tl.DURATION
    ) AS trade_line_key,
    tl.fbbid,
    tl.OPEN_DATE,
    tl.SUBSCRIBER_CODE,
    tl.DURATION,
    tl.CREATED_TIME,
    tl.ACCOUNT_TYPE,
    tl.payment_type,
    tl.subscriber_name,
    tl.creditor_name,
    tl.account_status,
    tl.last_modified_time,
    tl.current_balance_amount_range,
    tl.past_due_amount_range,
    tl.number_of_months_in_history,
    tl.num_delinquencies_last_30_days,
    tl.num_delinquencies_last_60_days,
    tl.num_delinquencies_last_90_days,
    tl.derogatory_counter,
    tl.most_recent_delinquency_date,
    tl.second_recent_delinquency_date,
    tl.payment_profile
FROM
    CDC_V2.CREDIT_REPORT.TRADE_LINES tl
ORDER BY
    3,
    4,
    5,
    6 desc
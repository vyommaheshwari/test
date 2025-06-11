-- Observations & Comments:
-- 1. Credit reports are fetched multiple times. Created a partition on fbbid x created_time, meaning the latest created_time would be the latest credit report pull
-- 2. Currently, we cannot track the progression of an individual tradeline over time (e.g., payment history or status updates). Each credit report pull generates a new tradeline ID, hence currently unable to link entries without a consistent identifier. Although the TRADE_LINES table includes an account number field, it is encrypted (account_number_encrypted) and therefore unusable for this purpose.

-- 3. To work around this, we are generating a temporary unique key by combining open_date, subscriber_code, and duration. This allows us to approximate a unique identifier for each tradeline and trace its changes over time. However, this method may produce duplicates in cases where the same customer has multiple loans from the same subscriber on the same date. This is a temporary solution until a reliable account number becomes available.
-- 4. No field found to get account name

CREATE
OR REPLACE TABLE analytics.credit.VM_R6T6_Trade_Line_Data_Summary AS
SELECT
    CONCAT(
        tl.OPEN_DATE,
        '_',
        tl.SUBSCRIBER_CODE,
        '_',
        tl.DURATION
    ) AS trade_line_key,
    tl.fbbid,
    CONCAT(
        bd.first_name,
        ' ',
        bd.last_name
    ) AS PG_Name,
    tl.OPEN_DATE,
    tl.SUBSCRIBER_CODE,
    tl.DURATION,
    tl.CREATED_TIME,
    tl.LIMIT_AMOUNT_RANGE,
    tl.ACCOUNT_TYPE,
    tl.original_amount_range,
    tl.is_closed,
    epd.account_condition AS status,
    tl.current_balance_amount_range,
    (
        (
            CASE
                WHEN tl.LIMIT_AMOUNT_RANGE LIKE '>%' THEN CAST(SUBSTRING(tl.LIMIT_AMOUNT_RANGE, 2) AS NUMERIC)
                ELSE tl.LIMIT_AMOUNT_RANGE
            END - tl.CURRENT_BALANCE_AMOUNT_RANGE
        ) / CASE
            WHEN tl.LIMIT_AMOUNT_RANGE LIKE '>%' THEN CAST(SUBSTRING(tl.LIMIT_AMOUNT_RANGE, 2) AS NUMERIC)
            ELSE tl.LIMIT_AMOUNT_RANGE
        END
    ) * 100 AS credit_limit_usage,
    tl.current_balance_date,
    tl.MONTHLY_PAYMENT_AMOUNT,
    tl.HIGH_BALANCE_AMOUNT_RANGE,
    tl.monthly_payment_type as Terms_of_Payment,
    tl.ECOA,
    tl.account_number_encrypted
FROM
    CDC_V2.CREDIT_REPORT.TRADE_LINES tl
LEFT JOIN CDC_V2.CREDIT_REPORT.ENHANCED_PAYMENT_DATA epd 
ON tl.fbbid = epd.fbbid
AND tl.id = epd.id

LEFT JOIN CDC_V2.CREDIT_REPORT.basic_details bd 
ON tl.fbbid = bd.fbbid

ORDER BY
    3,
    4,
    5,
    6 desc
;
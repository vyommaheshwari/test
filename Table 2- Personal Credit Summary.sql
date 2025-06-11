-- Observations & Comments:
-- 1. Credit reports are fetched multiple times. Created a partition on fbbid x created_time, meaning the latest created_time would be the latest credit report pull, will have the same fetch_job_id and the number of fetch_job_id are total trades of a customer/fbbid.
-- 2. For "Accounts paid in full", assumed that an account would be completely paid off when both current balance and charge off amount are 0/null.
-- 3. To confirm if we should be using "Open" accounts only to get balances(real estate, installment etc.)

CREATE OR REPLACE TABLE analytics.credit.VM_R5T5_Personal_Credit_Summary AS

WITH aggregated_trades AS (
    SELECT
        tl.fbbid,
        tl.created_time,
        tl.fetch_job_id,
        CONCAT(
        bd.first_name,
        ' ',
        bd.last_name
    ) AS PG_Name,
        COUNT(*) AS total_trades,
        COUNT_IF(is_closed = 'FALSE') AS current_trades,
        SUM(num_delinquencies_last_30_days) AS delinquencies_30_day,
        SUM(num_delinquencies_last_60_days) AS delinquencies_60_day,
        SUM(num_delinquencies_last_90_days) AS delinquencies_90_day,
        SUM(monthly_payment_amount) AS monthly_payment_amt,
        SUM(past_due_amount_range) AS past_due_amount,
        COUNT(
    CASE
        WHEN (current_balance_amount_range = 0 OR current_balance_amount_range IS NULL)
         AND (charge_off_amount_range = 0 OR charge_off_amount_range IS NULL)
        THEN 1
    END
) AS accounts_paid_in_full,
        SUM(CASE WHEN is_closed='FALSE' THEN CURRENT_BALANCE_AMOUNT_RANGE ELSE 0 END) AS current_account_balance,
        -- negative trades
        SUM(derogatory_counter) AS total_neg_trades,
        SUM(
            CASE
                WHEN is_closed = 'FALSE' THEN derogatory_counter
                ELSE 0
            END
        ) AS current_neg_trades,
        -- real_estate balance
        SUM(
            CASE
                WHEN account_type ILIKE '%Real Estate%'
                OR account_type ILIKE '%Mortgage%' THEN current_balance_amount_range
                ELSE 0
            END
        ) AS real_estate_balance,
        -- revolving balance
        SUM(
            CASE 
                WHEN payment_type='Revolving' 
                THEN current_balance_amount_range
                ELSE 0
            END
        ) AS revolving_balance,
        -- installment balance
        SUM(
            CASE
                WHEN payment_type = 'Installment' THEN current_balance_amount_range
                ELSE 0
            END
        ) AS installment_balance,
        --cc balance
        SUM(
            CASE
                WHEN account_type ILIKE '%Credit Card%' THEN current_balance_amount_range
                ELSE 0
            END
        ) AS cc_balance,
        -- non cc balance
        SUM(
            CASE
                WHEN payment_type = 'Revolving' THEN current_balance_amount_range
                ELSE 0
            END
        ) - SUM(
            CASE
                WHEN account_type ILIKE '%Credit Card%' THEN current_balance_amount_range
                ELSE 0
            END
        ) AS non_cc_balance,
        -- credit limit
        SUM(
            CASE
                WHEN payment_type = 'Revolving' THEN CASE
                    WHEN limit_amount_range LIKE '>%' THEN CAST(SUBSTRING(limit_amount_range, 2) AS NUMERIC)
                    ELSE limit_amount_range::NUMERIC
                END
                ELSE 0
            END
        ) AS credit_limit,
        -- % of revolving balances available to draw
CASE 
    WHEN SUM(
        CASE WHEN payment_type = 'Revolving' THEN current_balance_amount_range ELSE 0 END
    ) > 0 THEN
        (
            SUM(
                CASE
                    WHEN payment_type = 'Revolving' THEN 
                        CASE 
                            WHEN limit_amount_range LIKE '>%' THEN CAST(SUBSTRING(limit_amount_range, 2) AS NUMERIC)
                            ELSE limit_amount_range::NUMERIC
                        END
                    ELSE 0
                END
            )
            -
            SUM(
                CASE WHEN payment_type = 'Revolving' THEN current_balance_amount_range ELSE 0 END
            )
        )
        /
        SUM(
            CASE WHEN payment_type = 'Revolving' THEN current_balance_amount_range 
        ELSE 0 END
        )
    ELSE 0
END AS revolving_available_pct,

-- % of revolving CC balances available to draw
CASE 
    WHEN SUM(
        CASE 
            WHEN payment_type = 'Revolving' AND account_type ILIKE '%Credit Card%' 
            THEN current_balance_amount_range 
            ELSE 0 
        END
    ) > 0 THEN
        (
            SUM(
                CASE
                    WHEN payment_type = 'Revolving' AND account_type ILIKE '%Credit Card%' THEN 
                        CASE 
                            WHEN limit_amount_range LIKE '>%' THEN CAST(SUBSTRING(limit_amount_range, 2) AS NUMERIC)
                            ELSE limit_amount_range::NUMERIC
                        END
                    ELSE 0
                END
            )
            -
            SUM(
                CASE 
                    WHEN payment_type = 'Revolving' AND account_type ILIKE '%Credit Card%' 
                    THEN current_balance_amount_range 
                    ELSE 0 
                END
            )
        )
        /
        SUM(
            CASE 
                WHEN payment_type = 'Revolving' AND account_type ILIKE '%Credit Card%' 
                THEN current_balance_amount_range 
                ELSE 0 
            END
        )
    ELSE 0
END AS cc_available_percentage,
COUNT(DISTINCT CASE 
        WHEN inq.created_time >= DATEADD(MONTH, -6, CURRENT_DATE) THEN inq.fetch_job_id 
        END) AS inquiries_last_6_months,
    COUNT(DISTINCT CASE 
        WHEN inq.created_time >= DATEADD(MONTH, -24, CURRENT_DATE) THEN inq.fetch_job_id 
        END) AS inquiries_last_24_months
        
    FROM
        cdc_v2.credit_report.trade_lines tl
    LEFT JOIN cdc_v2.credit_report.inquiries inq
        ON tl.fbbid=inq.fbbid 
        -- AND tl.id=inq.id
    LEFT JOIN cdc_v2.credit_report.basic_details bd
    ON tl.fbbid=bd.fbbid
    
    GROUP BY
        tl.fbbid,
        tl.created_time,
        tl.fetch_job_id,
        bd.first_name,
        bd.last_name
)
SELECT
    fbbid,
    created_time,
    fetch_job_id,
    PG_Name,
    total_trades,
    current_trades,
    delinquencies_30_day,
    delinquencies_60_day,
    delinquencies_90_day,
    total_neg_trades,
    current_neg_trades,
    real_estate_balance,
    revolving_balance,
    installment_balance,
    cc_balance,
    non_cc_balance,
    credit_limit,
    current_account_balance,
    revolving_available_pct,
    cc_available_percentage,
    accounts_paid_in_full,
    inquiries_last_6_months,
    inquiries_last_24_months
    
FROM
    aggregated_trades
ORDER BY
    fbbid,
    fetch_job_id,
    created_time desc;
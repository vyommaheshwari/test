CREATE OR REPLACE TABLE analytics.credit.VM_R4T4_Vantage_and_FICO_Scores AS

SELECT 
    cs.fbbid,
    cs.created_time,
    CONCAT(
        bd.first_name,
        ' ',
        bd.last_name
    ) AS PG_Name,
    cs.fetch_job_id,
    cs.model,
    cs.score,
    cs.score_percentile,
    cs.score_factor_1_text,
    cs.score_factor_2_text,
    cs.score_factor_3_text,
    cs.score_factor_4_text,
    cs.score_factor_5_text,
FROM CDC_V2.CREDIT_REPORT.CREDIT_SCORES cs
LEFT JOIN CDC_V2.CREDIT_REPORT.BASIC_DETAILS bd
ON cs.fbbid=bd.fbbid
ORDER BY cs.fbbid, model, cs.created_time desc
;
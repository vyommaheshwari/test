SELECT
    pr.fbbid,
    pr.created_time,
    pr.fetch_job_id,
    CONCAT(
        bd.first_name,
        ' ',
        bd.last_name
    ) AS PG_Name,
    pr.type,
    pr.status,
    pr.court,
    pr.filing_date,
    pr.reference_number,
    pr.amount,
    pr.consumer_comment,
    pr.credit_report_account_id
    
FROM 
    CDC_V2.CREDIT_REPORT.public_records pr

LEFT JOIN CDC_V2.CREDIT_REPORT.BASIC_DETAILS bd
    ON pr.fbbid=bd.fbbid

ORDER BY
    1,
    2,
    3

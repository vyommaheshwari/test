CREATE
OR REPLACE TABLE analytics.credit.VM_R7T7B_Inquiry_Data AS

SELECT
    inq.fbbid,
    CONCAT(
        bd.first_name,
        ' ',
        bd.last_name
    ) AS PG_Name,
    inq.created_time,
    inq.inquirer_name,
    inq.date,
    inq.purpose_type,
FROM
    cdc_v2.credit_report.inquiries inq

LEFT JOIN CDC_V2.CREDIT_REPORT.BASIC_DETAILS bd
ON inq.fbbid=bd.fbbid

ORDER BY
    fbbid,
    created_time,
    inq.date
;
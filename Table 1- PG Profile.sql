CREATE OR REPLACE TABLE analytics.credit.VM_R2T1_Experian_Personal_Information AS

SELECT 
    fbbid, 
    first_name, 
    last_name, 
    full_street_address, 
    city, 
    state, 
    zip_code 
FROM CDC_V2.CREDIT_REPORT.basic_details
ORDER BY fbbid;
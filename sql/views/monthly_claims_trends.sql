CREATE OR REPLACE VIEW monthly_claims_trends AS
SELECT 
    d.year,
    d.month,
    d.month_name,
    COUNT(f.claim_key) as monthly_claims,
    ROUND(SUM(f.charge_amount), 2) as monthly_charges,
    ROUND(SUM(f.allowed_amount), 2) as monthly_allowed,
    ROUND(AVG(f.charge_amount), 2) as avg_claim_amount,
    ROUND(AVG(f.reimbursement_rate), 4) as avg_reimbursement
FROM fact_claims f
JOIN dim_date d ON f.received_date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;
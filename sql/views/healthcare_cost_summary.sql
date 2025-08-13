CREATE OR REPLACE VIEW healthcare_cost_summary AS
SELECT 
    COUNT(*) as total_claims,
    SUM(charge_amount) as total_charges,
    SUM(allowed_amount) as total_allowed,
    SUM(cost_savings_amount) as total_savings,
    AVG(reimbursement_rate) as avg_reimbursement_rate,
    ROUND(SUM(cost_savings_amount) / SUM(charge_amount) * 100, 2) as savings_percentage
FROM fact_claims;
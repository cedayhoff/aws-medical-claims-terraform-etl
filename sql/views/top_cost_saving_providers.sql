CREATE OR REPLACE VIEW top_cost_saving_providers AS
SELECT 
    p.provider_name,
    COUNT(f.claim_key) as total_claims,
    ROUND(SUM(f.cost_savings_amount), 2) as total_cost_savings,
    ROUND(AVG(f.cost_savings_amount), 2) as avg_savings_per_claim,
    ROUND(AVG(f.reimbursement_rate), 4) as avg_reimbursement_rate,
    ROUND(SUM(f.charge_amount), 2) as total_charges,
    ROUND(SUM(f.allowed_amount), 2) as total_allowed
FROM fact_claims f
JOIN dim_provider p ON f.provider_key = p.provider_key  
GROUP BY p.provider_name
HAVING COUNT(f.claim_key) >= 10
ORDER BY total_cost_savings DESC;
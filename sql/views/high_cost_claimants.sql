CREATE OR REPLACE VIEW high_cost_claimants AS
SELECT 
    c.claimant_id,
    COUNT(f.claim_key) as total_claims,
    ROUND(SUM(f.charge_amount), 2) as total_charges,
    ROUND(SUM(f.allowed_amount), 2) as total_allowed,
    ROUND(AVG(f.charge_amount), 2) as avg_charge_per_claim,
    ROUND(SUM(f.cost_savings_amount), 2) as total_savings_generated,
    ROUND(AVG(f.reimbursement_rate), 4) as avg_reimbursement_rate,
    
    -- Risk groupings
    CASE 
        WHEN SUM(f.charge_amount) >= 10000 THEN 'High Risk'
        WHEN SUM(f.charge_amount) >= 5000 THEN 'Medium Risk'  
        WHEN SUM(f.charge_amount) >= 1000 THEN 'Low Risk'
        ELSE 'Very Low Risk'
    END as risk_category
    
FROM fact_claims f
JOIN dim_claimant c ON f.claimant_key = c.claimant_key
GROUP BY c.claimant_id
HAVING SUM(f.charge_amount) > 1000
ORDER BY total_charges DESC;
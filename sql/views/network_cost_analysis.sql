CREATE OR REPLACE VIEW network_cost_analysis AS
SELECT 
    CASE WHEN in_network THEN 'In-Network' ELSE 'Out-of-Network' END as network_status,
    COUNT(*) as claim_count,
    ROUND(AVG(charge_amount), 2) as avg_charge,
    ROUND(AVG(allowed_amount), 2) as avg_allowed, 
    ROUND(AVG(reimbursement_rate), 4) as avg_reimbursement_rate,
    ROUND(SUM(cost_savings_amount), 2) as total_savings,
    ROUND(AVG(cost_savings_amount), 2) as avg_savings_per_claim
FROM fact_claims 
GROUP BY in_network
ORDER BY avg_savings_per_claim DESC;
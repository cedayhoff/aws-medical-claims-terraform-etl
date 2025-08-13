CREATE OR REPLACE VIEW claims_amount_distribution AS
SELECT 
    CASE 
        WHEN charge_amount = 0 THEN '$0 (Preventive/Admin)'
        WHEN charge_amount <= 100 THEN '$1-$100'
        WHEN charge_amount <= 500 THEN '$101-$500' 
        WHEN charge_amount <= 1000 THEN '$501-$1,000'
        WHEN charge_amount <= 5000 THEN '$1,001-$5,000'
        ELSE '$5,000+'
    END as amount_range,
    
    COUNT(*) as claim_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_claims), 1) as percentage,
    ROUND(AVG(reimbursement_rate), 4) as avg_reimbursement_rate,
    ROUND(SUM(cost_savings_amount), 2) as total_savings,
    ROUND(AVG(cost_savings_amount), 2) as avg_savings_per_claim
    
FROM fact_claims
GROUP BY 
    CASE 
        WHEN charge_amount = 0 THEN '$0 (Preventive/Admin)'
        WHEN charge_amount <= 100 THEN '$1-$100'
        WHEN charge_amount <= 500 THEN '$101-$500' 
        WHEN charge_amount <= 1000 THEN '$501-$1,000'
        WHEN charge_amount <= 5000 THEN '$1,001-$5,000'
        ELSE '$5,000+'
    END
ORDER BY MIN(charge_amount);
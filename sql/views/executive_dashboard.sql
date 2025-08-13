CREATE OR REPLACE VIEW executive_dashboard AS
SELECT 
    -- Volume Metrics
    (SELECT COUNT(*) FROM fact_claims) as total_claims,
    (SELECT COUNT(DISTINCT claimant_key) FROM fact_claims) as unique_claimants,  
    (SELECT COUNT(DISTINCT provider_key) FROM fact_claims) as unique_providers,
    
    -- Financial Metrics
    (SELECT ROUND(SUM(charge_amount), 2) FROM fact_claims) as total_charges,
    (SELECT ROUND(SUM(allowed_amount), 2) FROM fact_claims) as total_allowed,
    (SELECT ROUND(SUM(cost_savings_amount), 2) FROM fact_claims) as total_cost_savings,
    (SELECT ROUND(AVG(reimbursement_rate), 4) FROM fact_claims) as avg_reimbursement_rate,
    
    -- Efficiency Metrics
    (SELECT ROUND(SUM(cost_savings_amount) / SUM(charge_amount) * 100, 2) FROM fact_claims) as overall_savings_rate,
    (SELECT ROUND(AVG(CASE WHEN in_network THEN 1.0 ELSE 0.0 END) * 100, 1) FROM fact_claims) as in_network_percentage,
    
    -- Cost Per Claim
    (SELECT ROUND(AVG(charge_amount), 2) FROM fact_claims) as avg_charge_per_claim,
    (SELECT ROUND(AVG(allowed_amount), 2) FROM fact_claims) as avg_allowed_per_claim,
    (SELECT ROUND(AVG(cost_savings_amount), 2) FROM fact_claims) as avg_savings_per_claim;
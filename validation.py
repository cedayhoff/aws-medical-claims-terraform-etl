import pandas as pd
import numpy as np

df = pd.read_csv('medical_claims_data.csv')

print("Total rows in CSV:", len(df))


# Validate key metrics
total_claims = len(df)
print("\n=== VALIDATION RESULTS ===")
print(f"Total Claims: {total_claims}")

# Calculate financial totals
total_charges = df['CHARGE_AMT'].sum()
total_allowed = df['ALLOWED_AMT'].sum()
total_savings = total_charges - total_allowed
savings_rate = (total_savings / total_charges) * 100

print(f"Total Charges: ${total_charges:,.2f}")
print(f"Total Allowed: ${total_allowed:,.2f}")
print(f"Total Savings: ${total_savings:,.2f}")
print(f"Savings Rate: {savings_rate:.1f}%")

# Count unique patients
unique_patients = df['CLAIMANT_ID'].nunique()
print(f"\nUnique Patients: {unique_patients}")

# Count unique providers (excluding null values)
unique_providers = df['SERVICE_PROVIDER'].dropna().nunique()
print(f"Unique Providers: {unique_providers}")

# Network status analysis
in_network = df['OI_IN_NETWORK'].str.upper().eq('Y').sum()
out_network = total_claims - in_network
in_network_rate = (in_network / total_claims) * 100

print(f"\nIn-Network Claims: {in_network}")
print(f"Out-of-Network Claims: {out_network}")
print(f"In-Network Rate: {in_network_rate:.1f}%")

# Average calculations
print("\n=== AVERAGES ===")
print(f"Average Charge per Claim: ${total_charges / total_claims:.2f}")
print(f"Average Allowed per Claim: ${total_allowed / total_claims:.2f}")
print(f"Average Savings per Claim: ${total_savings / total_claims:.2f}")
print(f"Average Claims per Patient: {total_claims / unique_patients:.1f}")
print(f"Average Claims per Provider: {total_claims / unique_providers:.1f}")

# Reimbursement rate calculation
avg_reimbursement_rate = (total_allowed / total_charges) * 100
print(f"Average Reimbursement Rate: {avg_reimbursement_rate:.2f}%")

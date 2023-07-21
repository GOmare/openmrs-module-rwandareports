-- $BEGIN

-- Dimensions
CALL sp_mamba_dim_admission;
CALL sp_mamba_dim_beneficiary;
CALL sp_mamba_dim_bill_payment;
CALL sp_mamba_dim_billable_service;
CALL sp_mamba_dim_consommation;
CALL sp_mamba_dim_department;
CALL sp_mamba_dim_facility_service_price;
CALL sp_mamba_dim_global_bill;
CALL sp_mamba_dim_hop_service;
CALL sp_mamba_dim_insurance_rate;
CALL sp_mamba_dim_insurance;
CALL sp_mamba_dim_insurance_bill;
CALL sp_mamba_dim_insurance_policy;
CALL sp_mamba_dim_paid_service_bill;
CALL sp_mamba_dim_patient_bill;
CALL sp_mamba_dim_patient_service_bill;
CALL sp_mamba_dim_service_category;
CALL sp_mamba_dim_third_party_bill;
CALL sp_mamba_dim_thirdparty;

-- Facts
CALL sp_mamba_fact_patient_service_bill;

-- $END
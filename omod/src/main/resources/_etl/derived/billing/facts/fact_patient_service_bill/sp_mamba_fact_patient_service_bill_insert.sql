-- $BEGIN

INSERT INTO mamba_fact_patient_service_bill(admission_date, closing_date, beneficiary_name, household_head_name,
                                            family_code, beneficiary_level, card_number, company_name, age, birth_date,
                                            gender, doctor_name, service_bill_quantity, service_bill_unit_price,
                                            insurance_id, hop_service_id, global_bill_id, hop_service_name)

SELECT -- DATE_FORMAT(gb.created_date, '%d/%m/%Y') AS admission_date,
       DATE(gb.created_date) AS admission_date,
       DATE(gb.closing_date) AS closing_date,
       bps.person_name_long  AS beneficiary_name,
       ben.owner_name        AS household_head_name,
       ben.owner_code        AS family_code,
       ben.level             AS beneficiary_level,
       isp.insurance_card_no AS card_number,
       ben.company           AS company_name,
       bps.age               AS age,
       DATE(bps.birthdate)   AS birth_date,
       bps.gender            AS gender,
       gb.closed_by_name     AS doctor_name,
       psb.quantity          AS service_bill_quantity,
       psb.unit_price        AS service_bill_unit_price,
       ins.insurance_id      AS insurance_id,
       psb.service_id        AS hop_service_id,
       gb.global_bill_id     AS global_bill_id,
       hp.name               AS hop_service_name

FROM mamba_dim_patient_service_bill psb
         INNER JOIN mamba_dim_consommation cons ON psb.consommation_id = cons.consommation_id
         INNER JOIN mamba_dim_global_bill gb on cons.global_bill_id = gb.global_bill_id
         INNER JOIN mamba_dim_beneficiary ben on cons.beneficiary_id = ben.beneficiary_id
         INNER JOIN mamba_dim_insurance_policy isp on ben.insurance_policy_id = isp.insurance_policy_id
         INNER JOIN mamba_dim_insurance ins ON ins.insurance_id = isp.insurance_id
         INNER JOIN mamba_dim_person bps ON bps.person_id = ben.patient_id
         INNER JOIN mamba_dim_hop_service hp on hp.service_id = psb.service_id

WHERE gb.closed = 1
  AND psb.voided = 0
-- GROUP BY cons.global_bill_id
-- HAVING MIN(cons.consommation_id)
ORDER BY gb.closing_date ASC
;
-- $END
-- $BEGIN

CREATE TABLE IF NOT EXISTS mamba_dim_consommation
(
    id                  INT      NOT NULL AUTO_INCREMENT,
    consommation_id     INT      NOT NULL,
    global_bill_id      INT      NULL,
    department_id       INT      NULL,
    beneficiary_id      INT      NOT NULL,
    patient_bill_id     INT      NOT NULL,
    insurance_bill_id   INT      NULL,
    third_party_bill_id INT      NULL,
    created_date        DATETIME NOT NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_consommation_consommation_id_index
    ON mamba_dim_consommation (consommation_id);

CREATE INDEX mamba_dim_consommation_global_bill_id_index
    ON mamba_dim_consommation (global_bill_id);

CREATE INDEX mamba_dim_consommation_department_id_index
    ON mamba_dim_consommation (department_id);

CREATE INDEX mamba_dim_consommation_beneficiary_id_index
    ON mamba_dim_consommation (beneficiary_id);

CREATE INDEX mamba_dim_consommation_patient_bill_id_index
    ON mamba_dim_consommation (patient_bill_id);

CREATE INDEX mamba_dim_consommation_insurance_bill_id_index
    ON mamba_dim_consommation (insurance_bill_id);

CREATE INDEX mamba_dim_consommation_third_party_bill_id_index
    ON mamba_dim_consommation (third_party_bill_id);

-- $END
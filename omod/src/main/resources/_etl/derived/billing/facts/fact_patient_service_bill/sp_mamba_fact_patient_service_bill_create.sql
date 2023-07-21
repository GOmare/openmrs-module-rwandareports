-- $BEGIN

CREATE TABLE mamba_fact_patient_service_bill
(
    id                      INT            NOT NULL AUTO_INCREMENT,
    admission_date          DATE           NOT NULL,
    closing_date            DATE           NULL,
    beneficiary_name        TEXT           NULL,
    household_head_name     VARCHAR(255)   NULL,
    family_code             VARCHAR(255)   NULL,
    beneficiary_level       INT            NULL,
    card_number             VARCHAR(255)   NULL,
    company_name            VARCHAR(255)   NULL,
    age                     INT            NULL,
    birth_date              DATE           NULL,
    gender                  CHAR(1)        NULL,
    doctor_name             VARCHAR(255)   NULL,
    service_bill_quantity   DECIMAL(20, 2) NULL,
    service_bill_unit_price DECIMAL(20, 2) NOT NULL,

    insurance_id            INT            NOT NULL,
    hop_service_id          INT            NULL,
    global_bill_id          INT            NOT NULL,
    hop_service_name        VARCHAR(50)    NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_fact_patient_service_bill_insurance_id_index
    ON mamba_fact_patient_service_bill (insurance_id);

CREATE INDEX mamba_fact_patient_service_bill_global_bill_id_index
    ON mamba_fact_patient_service_bill (global_bill_id);

CREATE INDEX mamba_fact_patient_service_bill_hop_service_id_index
    ON mamba_fact_patient_service_bill (hop_service_id);

CREATE INDEX mamba_fact_patient_service_bill_closing_date_index
    ON mamba_fact_patient_service_bill (closing_date);

CREATE INDEX mamba_fact_patient_service_bill_admission_date_index
    ON mamba_fact_patient_service_bill (admission_date);

-- $END


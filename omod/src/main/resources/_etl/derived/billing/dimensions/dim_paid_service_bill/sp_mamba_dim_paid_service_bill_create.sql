-- $BEGIN

CREATE TABLE mamba_dim_paid_service_bill
(
    id                      INT      NOT NULL AUTO_INCREMENT,
    paid_service_bill_id    INT      NOT NULL,
    bill_payment_id         int      not null,
    patient_service_bill_id int      not null,
    paid_quantity           decimal  not null,
    voided                  smallint not null,
    created_date            datetime not null,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_paid_service_bill_paid_service_bill_id_index
    ON mamba_dim_paid_service_bill (paid_service_bill_id);

CREATE INDEX mamba_dim_paid_service_bill_bill_payment_id_index
    ON mamba_dim_paid_service_bill (bill_payment_id);

CREATE INDEX mamba_dim_paid_service_bill_patient_service_bill_id_index
    ON mamba_dim_paid_service_bill (patient_service_bill_id);

CREATE INDEX mamba_dim_paid_service_bill_voided_index
    ON mamba_dim_paid_service_bill (voided);

-- $END
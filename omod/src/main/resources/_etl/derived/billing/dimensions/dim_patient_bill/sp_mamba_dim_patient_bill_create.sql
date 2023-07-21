-- $BEGIN

CREATE TABLE mamba_dim_patient_bill
(
    id              INT            NOT NULL AUTO_INCREMENT,
    patient_bill_id INT            NOT NULL,
    amount          decimal(20, 2) not null,
    is_paid         smallint       null,
    status          varchar(150)   null,
    created_date    datetime       null,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_patient_bill_patient_bill_id_index
    ON mamba_dim_patient_bill (patient_bill_id);

-- $END
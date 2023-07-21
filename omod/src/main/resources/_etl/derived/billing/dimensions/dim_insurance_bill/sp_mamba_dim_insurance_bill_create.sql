-- $BEGIN

CREATE TABLE mamba_dim_insurance_bill
(
    id                INT      NOT NULL AUTO_INCREMENT,
    insurance_bill_id INT      NOT NULL,
    amount            decimal  not null,
    created_date      datetime not null,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_insurance_bill_insurance_bill_id_index
    ON mamba_dim_insurance_bill (insurance_bill_id);

-- $END
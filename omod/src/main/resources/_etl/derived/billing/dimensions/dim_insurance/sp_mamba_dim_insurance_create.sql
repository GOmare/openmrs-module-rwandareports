-- $BEGIN

CREATE TABLE mamba_dim_insurance
(
    id                              INT          NOT NULL AUTO_INCREMENT,
    insurance_id                    INT          NOT NULL,
    current_insurance_rate          FLOAT        NULL,
    current_insurance_rate_flat_fee FLOAT        NULL,
    concept_id                      int          null,
    category                        varchar(150) not null,
    name                            varchar(50)  not null,
    address                         varchar(150) null,
    phone                           varchar(100) null,
    created_date                    date         not null,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_insurance_insurance_id_index
    ON mamba_dim_insurance (insurance_id);

CREATE INDEX mamba_dim_insurance_concept_id_index
    ON mamba_dim_insurance (concept_id);

CREATE INDEX mamba_dim_insurance_category_index
    ON mamba_dim_insurance (category);

-- $END
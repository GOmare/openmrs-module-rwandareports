-- $BEGIN

CREATE TABLE IF NOT EXISTS mamba_dim_admission
(
    id                  INT          NOT NULL AUTO_INCREMENT,
    admission_id        INT          NOT NULL,
    insurance_policy_id int          not null,
    is_admitted         tinyint(1)   not null,
    admission_date      datetime     not null,
    discharging_date    datetime     null,
    discharged_by       int          null,
    disease_type        varchar(100) null,
    admission_type      tinyint(1)   null,
    created_date        datetime     not null,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_admission_admission_id_index
    ON mamba_dim_admission (admission_id);

CREATE INDEX mamba_dim_admission_insurance_policy_id_index
    ON mamba_dim_admission (insurance_policy_id);

-- $END
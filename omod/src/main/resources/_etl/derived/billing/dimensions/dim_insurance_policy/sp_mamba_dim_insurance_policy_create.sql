-- $BEGIN

CREATE TABLE mamba_dim_insurance_policy
(
    id                  INT          NOT NULL AUTO_INCREMENT,
    insurance_policy_id INT          NOT NULL,
    insurance_id        int          not null,
    third_party_id      int          null,
    insurance_card_no   varchar(250) null,
    owner               int          not null,
    coverage_start_date date         not null,
    expiration_date     date         null,
    created_date        datetime     not null,

    constraint mamba_dim_insurance_policy_insurance_card_no_UNIQUE
        unique (insurance_card_no),

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_insurance_policy_insurance_policy_id_index
    ON mamba_dim_insurance_policy (insurance_policy_id);

CREATE INDEX mamba_dim_insurance_policy_insurance_card_no_index
    ON mamba_dim_insurance_policy (insurance_card_no);

CREATE INDEX mamba_dim_insurance_policy_owner_index
    ON mamba_dim_insurance_policy (owner);

CREATE INDEX mamba_dim_insurance_policy_coverage_start_date_index
    ON mamba_dim_insurance_policy (coverage_start_date);

CREATE INDEX mamba_dim_insurance_policy_expiration_date_index
    ON mamba_dim_insurance_policy (expiration_date);

CREATE INDEX mamba_dim_insurance_policy_insurance_id_index
    ON mamba_dim_insurance_policy (insurance_id);

CREATE INDEX mamba_dim_insurance_policy_third_party_id_index
    ON mamba_dim_insurance_policy (third_party_id);

-- $END

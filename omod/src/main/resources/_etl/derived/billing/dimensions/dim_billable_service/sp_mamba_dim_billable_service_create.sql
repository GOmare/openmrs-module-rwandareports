-- $BEGIN

CREATE TABLE IF NOT EXISTS mamba_dim_billable_service
(
    id                        INT            NOT NULL AUTO_INCREMENT,
    billable_service_id       INT            NOT NULL,
    insurance_id              int            null,
    facility_service_price_id int            not null,
    service_category_id       int            null,
    maxima_to_pay             decimal(20, 2) null,
    start_date                date           not null,
    end_date                  date           null,
    created_date              datetime       not null,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_billable_service_billable_service_id_index
    ON mamba_dim_billable_service (billable_service_id);

CREATE INDEX mamba_dim_billable_service_insurance_id_index
    ON mamba_dim_billable_service (insurance_id);

CREATE INDEX mamba_dim_billable_service_service_category_id_index
    ON mamba_dim_billable_service (service_category_id);

CREATE INDEX mamba_dim_billable_service_facility_service_price_id_index
    ON mamba_dim_billable_service (facility_service_price_id);

-- $END
-- $BEGIN

CREATE TABLE mamba_dim_facility_service_price
(
    id                        INT            NOT NULL AUTO_INCREMENT,
    facility_service_price_id INT            NOT NULL,
    location_id               int            not null,
    concept_id                int            null,
    name                      varchar(150)   not null,
    short_name                varchar(100)   null,
    description               varchar(250)   null,
    category                  varchar(150)   null,
    full_price                decimal(20, 2) not null,
    start_date                date           not null,
    end_date                  date           null,
    item_type                 tinyint(1)     null,
    hide_item                 tinyint(1)     null,
    created_date              date           not null,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_facility_service_price_facility_service_price_id_index
    ON mamba_dim_facility_service_price (facility_service_price_id);

CREATE INDEX mamba_dim_facility_service_price_concept_id_index
    ON mamba_dim_facility_service_price (concept_id);

CREATE INDEX mamba_dim_facility_service_price_location_id_index
    ON mamba_dim_facility_service_price (location_id);

-- $END
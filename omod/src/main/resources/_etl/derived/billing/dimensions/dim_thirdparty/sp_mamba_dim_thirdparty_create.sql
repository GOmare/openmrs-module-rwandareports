-- $BEGIN

CREATE TABLE mamba_dim_third_party
(
    id             INT          NOT NULL AUTO_INCREMENT,
    third_party_id INT          NOT NULL,
    name           VARCHAR(150) NOT NULL,
    rate           FLOAT        NOT NULL,
    created_date   DATE         NOT NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_third_party_third_party_id_index
    ON mamba_dim_third_party (third_party_id);

-- $END
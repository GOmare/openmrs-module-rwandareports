-- $BEGIN

CREATE TABLE IF NOT EXISTS mamba_dim_department
(
    id            INT         NOT NULL AUTO_INCREMENT,
    department_id INT         NOT NULL,
    name          varchar(50) null,
    description   varchar(50) null,
    created_date  datetime    not null,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_department_department_id_index
    ON mamba_dim_department (department_id);

-- $END
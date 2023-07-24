
        
    
        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_calculate_agegroup  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP FUNCTION IF EXISTS fn_mamba_calculate_agegroup;

~
CREATE FUNCTION fn_mamba_calculate_agegroup(age INT) RETURNS VARCHAR(15)
    DETERMINISTIC
BEGIN
    DECLARE agegroup VARCHAR(15);
    IF (age < 1) THEN
        SET agegroup = '<1';
    ELSEIF age between 1 and 4 THEN
        SET agegroup = '1-4';
    ELSEIF age between 5 and 9 THEN
        SET agegroup = '5-9';
    ELSEIF age between 10 and 14 THEN
        SET agegroup = '10-14';
    ELSEIF age between 15 and 19 THEN
        SET agegroup = '15-19';
    ELSEIF age between 20 and 24 THEN
        SET agegroup = '20-24';
    ELSEIF age between 25 and 29 THEN
        SET agegroup = '25-29';
    ELSEIF age between 30 and 34 THEN
        SET agegroup = '30-34';
    ELSEIF age between 35 and 39 THEN
        SET agegroup = '35-39';
    ELSEIF age between 40 and 44 THEN
        SET agegroup = '40-44';
    ELSEIF age between 45 and 49 THEN
        SET agegroup = '45-49';
    ELSEIF age between 50 and 54 THEN
        SET agegroup = '50-54';
    ELSEIF age between 55 and 59 THEN
        SET agegroup = '55-59';
    ELSEIF age between 60 and 64 THEN
        SET agegroup = '60-64';
    ELSE
        SET agegroup = '65+';
    END IF;

    RETURN (agegroup);
END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_get_obs_value_column  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP FUNCTION IF EXISTS fn_mamba_get_obs_value_column;

~
CREATE FUNCTION fn_mamba_get_obs_value_column(conceptDatatype VARCHAR(20)) RETURNS VARCHAR(20)
    DETERMINISTIC
BEGIN
    DECLARE obsValueColumn VARCHAR(20);
    IF (conceptDatatype = 'Text' OR conceptDatatype = 'Coded' OR conceptDatatype = 'N/A' OR
        conceptDatatype = 'Boolean') THEN
        SET obsValueColumn = 'obs_value_text';
    ELSEIF conceptDatatype = 'Date' OR conceptDatatype = 'Datetime' THEN
        SET obsValueColumn = 'obs_value_datetime';
    ELSEIF conceptDatatype = 'Numeric' THEN
        SET obsValueColumn = 'obs_value_numeric';
    END IF;

    RETURN (obsValueColumn);
END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  fn_mamba_age_calculator  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP FUNCTION IF EXISTS fn_mamba_age_calculator;

~
CREATE FUNCTION fn_mamba_age_calculator (birthdate DATE,deathDate DATE) RETURNS  Integer
    DETERMINISTIC
BEGIN
    DECLARE onDate DATE;
    DECLARE today DATE;
    DECLARE bday DATE;
    DECLARE age INT;
    DECLARE todaysMonth INT;
    DECLARE bdayMonth INT;
    DECLARE todaysDay INT;
    DECLARE bdayDay INT;

    SET onDate = NULL ;

    IF birthdate IS NULL THEN
        RETURN NULL;
    ELSE
        SET today = CURDATE();

        IF onDate IS NOT NULL THEN
            SET today = onDate;
        END IF;

        IF deathDate IS NOT NULL AND today > deathDate THEN
            SET today = deathDate;
        END IF;

        SET bday = birthdate;
        SET age = YEAR(today) - YEAR(bday);
        SET todaysMonth = MONTH(today);
        SET bdayMonth = MONTH(bday);
        SET todaysDay = DAY(today);
        SET bdayDay = DAY(bday);

        IF todaysMonth < bdayMonth THEN
            SET age = age - 1;
        ELSEIF todaysMonth = bdayMonth AND todaysDay < bdayDay THEN
            SET age = age - 1;
        END IF;

        RETURN age;
    END IF;
END;




        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_xf_system_drop_all_functions_in_schema  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_xf_system_drop_all_stored_functions_in_schema;

~
CREATE PROCEDURE sp_xf_system_drop_all_stored_functions_in_schema(
    IN database_name CHAR(255) CHARACTER SET UTF8MB4
)
BEGIN
    DELETE FROM `mysql`.`proc` WHERE `type` = 'FUNCTION' AND `db` = database_name; -- works in mysql before v.8

END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_xf_system_drop_all_stored_procedures_in_schema  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_xf_system_drop_all_stored_procedures_in_schema;

~
CREATE PROCEDURE sp_xf_system_drop_all_stored_procedures_in_schema(
    IN database_name CHAR(255) CHARACTER SET UTF8MB4
)
BEGIN

    DELETE FROM `mysql`.`proc` WHERE `type` = 'PROCEDURE' AND `db` = database_name; -- works in mysql before v.8

END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_xf_system_drop_all_objects_in_schema  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_xf_system_drop_all_objects_in_schema;

~
CREATE PROCEDURE sp_xf_system_drop_all_objects_in_schema(
    IN database_name CHAR(255) CHARACTER SET UTF8MB4
)
BEGIN

    CALL sp_xf_system_drop_all_stored_functions_in_schema(database_name);
    CALL sp_xf_system_drop_all_stored_procedures_in_schema(database_name);
    CALL sp_xf_system_drop_all_tables_in_schema(database_name);
    # CALL sp_xf_system_drop_all_views_in_schema (database_name);

END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_xf_system_drop_all_tables_in_schema  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_xf_system_drop_all_tables_in_schema;

-- CREATE PROCEDURE sp_xf_system_drop_all_tables_in_schema(IN database_name CHAR(255) CHARACTER SET UTF8MB4)
~
CREATE PROCEDURE sp_xf_system_drop_all_tables_in_schema()
BEGIN

    DECLARE tables_count INT;

    SET @database_name = (SELECT DATABASE());

    SELECT COUNT(1)
    INTO tables_count
    FROM information_schema.tables
    WHERE TABLE_TYPE = 'BASE TABLE'
      AND TABLE_SCHEMA = @database_name;

    IF tables_count > 0 THEN

        SET session group_concat_max_len = 20000;

        SET @tbls = (SELECT GROUP_CONCAT(@database_name, '.', TABLE_NAME SEPARATOR ', ')
                     FROM information_schema.tables
                     WHERE TABLE_TYPE = 'BASE TABLE'
                       AND TABLE_SCHEMA = @database_name
                       AND TABLE_NAME REGEXP '^(mamba_|dim_|fact_|flat_)');

        IF (@tbls IS NOT NULL) THEN

            SET @drop_tables = CONCAT('DROP TABLE IF EXISTS ', @tbls);

            SET foreign_key_checks = 0; -- Remove check, so we don't have to drop tables in the correct order, or care if they exist or not.
            PREPARE drop_tbls FROM @drop_tables;
            EXECUTE drop_tbls;
            DEALLOCATE PREPARE drop_tbls;
            SET foreign_key_checks = 1;

        END IF;

    END IF;

END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_etl_execute  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_etl_execute;

~
CREATE PROCEDURE sp_mamba_etl_execute()
BEGIN
    DECLARE error_message VARCHAR(255) DEFAULT 'OK';
    DECLARE error_code CHAR(5) DEFAULT '00000';

    DECLARE start_time bigint;
    DECLARE end_time bigint;
    DECLARE start_date_time DATETIME;
    DECLARE end_date_time DATETIME;

    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
        BEGIN
            GET DIAGNOSTICS CONDITION 1
                error_code = RETURNED_SQLSTATE,
                error_message = MESSAGE_TEXT;

            -- SET @sql = CONCAT('SIGNAL SQLSTATE ''', error_code, ''' SET MESSAGE_TEXT = ''', error_message, '''');
            -- SET @sql = CONCAT('SET @signal = ''', @sql, '''');

            -- SET @sql = CONCAT('SIGNAL SQLSTATE ''', error_code, ''' SET MESSAGE_TEXT = ''', error_message, '''');
            -- PREPARE stmt FROM @sql;
            -- EXECUTE stmt;
            -- DEALLOCATE PREPARE stmt;

            INSERT INTO zzmamba_etl_tracker (initial_run_date,
                                             start_date,
                                             end_date,
                                             time_taken_microsec,
                                             completion_status,
                                             success_or_error_message,
                                             next_run_date)
            SELECT NOW(),
                   start_date_time,
                   NOW(),
                   (((UNIX_TIMESTAMP(NOW()) * 1000000 + MICROSECOND(NOW(6))) - @start_time) / 1000),
                   'ERROR',
                   (CONCAT(error_code, ' : ', error_message)),
                   NOW() + 5;
        END;

    -- Fix start time in microseconds
    SET start_date_time = NOW();
    SET @start_time = (UNIX_TIMESTAMP(NOW()) * 1000000 + MICROSECOND(NOW(6)));

    CALL sp_mamba_data_processing_etl();

    -- Fix end time in microseconds
    SET end_date_time = NOW();
    SET @end_time = (UNIX_TIMESTAMP(NOW()) * 1000000 + MICROSECOND(NOW(6)));

    -- Result
    SET @time_taken = (@end_time - @start_time) / 1000;
    SELECT @time_taken;


    INSERT INTO zzmamba_etl_tracker (initial_run_date,
                                     start_date,
                                     end_date,
                                     time_taken_microsec,
                                     completion_status,
                                     success_or_error_message,
                                     next_run_date)
    SELECT NOW(), start_date_time, end_date_time, @time_taken, 'SUCCESS', 'OK', NOW() + 5;

END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_table_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_flat_encounter_table_create;

~
CREATE PROCEDURE sp_mamba_flat_encounter_table_create(
    IN flat_encounter_table_name VARCHAR(255) CHARSET UTF8MB4
)
BEGIN

    SET session group_concat_max_len = 20000;
    SET @column_labels := NULL;

    SET @drop_table = CONCAT('DROP TABLE IF EXISTS `', flat_encounter_table_name, '`');

    SELECT GROUP_CONCAT(column_label SEPARATOR ' TEXT, ')
    INTO @column_labels
    FROM mamba_dim_concept_metadata
    WHERE flat_table_name = flat_encounter_table_name
      AND concept_datatype IS NOT NULL;

    IF @column_labels IS NULL THEN
        SET @create_table = CONCAT(
                'CREATE TABLE `', flat_encounter_table_name, '` (encounter_id INT NOT NULL, client_id INT NOT NULL, encounter_datetime DATETIME NOT NULL);');
    ELSE
        SET @create_table = CONCAT(
                'CREATE TABLE `', flat_encounter_table_name, '` (encounter_id INT NOT NULL, client_id INT NOT NULL, encounter_datetime DATETIME NOT NULL, ', @column_labels,
                ' TEXT);');
    END IF;


    PREPARE deletetb FROM @drop_table;
    PREPARE createtb FROM @create_table;

    EXECUTE deletetb;
    EXECUTE createtb;

    DEALLOCATE PREPARE deletetb;
    DEALLOCATE PREPARE createtb;

END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_table_create_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Flatten all Encounters given in Config folder

DROP PROCEDURE IF EXISTS sp_mamba_flat_encounter_table_create_all;

~
CREATE PROCEDURE sp_mamba_flat_encounter_table_create_all()
BEGIN

    DECLARE tbl_name CHAR(50) CHARACTER SET UTF8MB4;

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_flat_tables CURSOR FOR
        SELECT DISTINCT(flat_table_name) FROM mamba_dim_concept_metadata;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cursor_flat_tables;
    computations_loop:
    LOOP
        FETCH cursor_flat_tables INTO tbl_name;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        CALL sp_mamba_flat_encounter_table_create(tbl_name);

    END LOOP computations_loop;
    CLOSE cursor_flat_tables;

END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_table_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_flat_encounter_table_insert;

~
CREATE PROCEDURE sp_mamba_flat_encounter_table_insert(
    IN flat_encounter_table_name CHAR(255) CHARACTER SET UTF8MB4
)
BEGIN

    SET session group_concat_max_len = 20000;
    SET @tbl_name = flat_encounter_table_name;

    SET @old_sql = (SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ', ')
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = @tbl_name
                      AND TABLE_SCHEMA = Database());

    SELECT
        GROUP_CONCAT(DISTINCT
            CONCAT(' MAX(CASE WHEN column_label = ''', column_label, ''' THEN ',
                fn_mamba_get_obs_value_column(concept_datatype), ' END) ', column_label)
            ORDER BY id ASC)
    INTO @column_labels
    FROM mamba_dim_concept_metadata
    WHERE flat_table_name = @tbl_name;

    SET @insert_stmt = CONCAT(
            'INSERT INTO `', @tbl_name, '` SELECT eo.encounter_id, eo.person_id, eo.encounter_datetime, ',
            @column_labels, '
            FROM mamba_z_encounter_obs eo
                INNER JOIN mamba_dim_concept_metadata cm
                ON IF(cm.concept_answer_obs=1, cm.concept_uuid=eo.obs_value_coded_uuid, cm.concept_uuid=eo.obs_question_uuid)
            WHERE cm.flat_table_name = ''', @tbl_name, '''
            AND eo.encounter_type_uuid = cm.encounter_type_uuid
            GROUP BY eo.encounter_id, eo.person_id, eo.encounter_datetime;');

    PREPARE inserttbl FROM @insert_stmt;
    EXECUTE inserttbl;
    DEALLOCATE PREPARE inserttbl;

END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_flat_encounter_table_insert_all  ----------------------------
-- ---------------------------------------------------------------------------------------------

-- Flatten all Encounters given in Config folder

DROP PROCEDURE IF EXISTS sp_mamba_flat_encounter_table_insert_all;

~
CREATE PROCEDURE sp_mamba_flat_encounter_table_insert_all()
BEGIN

    DECLARE tbl_name CHAR(50) CHARACTER SET UTF8MB4;

    DECLARE done INT DEFAULT FALSE;

    DECLARE cursor_flat_tables CURSOR FOR
        SELECT DISTINCT(flat_table_name) FROM mamba_dim_concept_metadata;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cursor_flat_tables;
    computations_loop:
    LOOP
        FETCH cursor_flat_tables INTO tbl_name;

        IF done THEN
            LEAVE computations_loop;
        END IF;

        CALL sp_mamba_flat_encounter_table_insert(tbl_name);

    END LOOP computations_loop;
    CLOSE cursor_flat_tables;

END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_multiselect_values_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS `sp_mamba_multiselect_values_update`;

~
CREATE PROCEDURE `sp_mamba_multiselect_values_update`(
    IN table_to_update CHAR(100) CHARACTER SET UTF8MB4,
    IN column_names TEXT CHARACTER SET UTF8MB4,
    IN value_yes CHAR(100) CHARACTER SET UTF8MB4,
    IN value_no CHAR(100) CHARACTER SET UTF8MB4
)
BEGIN

    SET @table_columns = column_names;
    SET @start_pos = 1;
    SET @comma_pos = locate(',', @table_columns);
    SET @end_loop = 0;

    SET @column_label = '';

    REPEAT
        IF @comma_pos > 0 THEN
            SET @column_label = substring(@table_columns, @start_pos, @comma_pos - @start_pos);
            SET @end_loop = 0;
        ELSE
            SET @column_label = substring(@table_columns, @start_pos);
            SET @end_loop = 1;
        END IF;

        -- UPDATE fact_hts SET @column_label=IF(@column_label IS NULL OR '', new_value_if_false, new_value_if_true);

        SET @update_sql = CONCAT(
                'UPDATE ', table_to_update, ' SET ', @column_label, '= IF(', @column_label, ' IS NOT NULL, ''',
                value_yes, ''', ''', value_no, ''');');
        PREPARE stmt FROM @update_sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        IF @end_loop = 0 THEN
            SET @table_columns = substring(@table_columns, @comma_pos + 1);
            SET @comma_pos = locate(',', @table_columns);
        END IF;
    UNTIL @end_loop = 1
        END REPEAT;

END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_extract_report_metadata  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_extract_report_metadata;

~
CREATE PROCEDURE sp_mamba_extract_report_metadata(
    IN report_data MEDIUMTEXT CHARACTER SET UTF8MB4,
    IN metadata_table VARCHAR(255) CHARSET UTF8MB4
)
BEGIN

    SET session group_concat_max_len = 20000;

    SELECT JSON_EXTRACT(report_data, '$.flat_report_metadata') INTO @report_array;
    SELECT JSON_LENGTH(@report_array) INTO @report_array_len;

    SET @report_count = 0;
    WHILE @report_count < @report_array_len
        DO

            SELECT JSON_EXTRACT(@report_array, CONCAT('$[', @report_count, ']')) INTO @report;
            SELECT JSON_EXTRACT(@report, '$.report_name') INTO @report_name;
            SELECT JSON_EXTRACT(@report, '$.flat_table_name') INTO @flat_table_name;
            SELECT JSON_EXTRACT(@report, '$.encounter_type_uuid') INTO @encounter_type;
            SELECT JSON_EXTRACT(@report, '$.concepts_locale') INTO @concepts_locale;
            SELECT JSON_EXTRACT(@report, '$.table_columns') INTO @column_array;

            SELECT JSON_KEYS(@column_array) INTO @column_keys_array;
            SELECT JSON_LENGTH(@column_keys_array) INTO @column_keys_array_len;
            SET @col_count = 0;
            WHILE @col_count < @column_keys_array_len
                DO
                    SELECT JSON_EXTRACT(@column_keys_array, CONCAT('$[', @col_count, ']')) INTO @field_name;
                    SELECT JSON_EXTRACT(@column_array, CONCAT('$.', @field_name)) INTO @concept_uuid;

                    SET @tbl_name = '';
                    INSERT INTO mamba_dim_concept_metadata
                        (
                            report_name,
                            flat_table_name,
                            encounter_type_uuid,
                            column_label,
                            concept_uuid,
                            concepts_locale
                        )
                    VALUES (JSON_UNQUOTE(@report_name),
                            JSON_UNQUOTE(@flat_table_name),
                            JSON_UNQUOTE(@encounter_type),
                            JSON_UNQUOTE(@field_name),
                            JSON_UNQUOTE(@concept_uuid),
                            JSON_UNQUOTE(@concepts_locale));

                    SET @col_count = @col_count + 1;
                END WHILE;

            SET @report_count = @report_count + 1;
        END WHILE;

END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_load_agegroup  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_load_agegroup;

~
CREATE PROCEDURE sp_mamba_load_agegroup()
BEGIN
    DECLARE age INT DEFAULT 0;
    WHILE age <= 120
        DO
            INSERT INTO mamba_dim_agegroup(age, datim_agegroup, normal_agegroup)
            VALUES (age, fn_mamba_calculate_agegroup(age), IF(age < 15, '<15', '15+'));
            SET age = age + 1;
        END WHILE;
END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_location_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_location_create;

~
CREATE PROCEDURE sp_mamba_dim_location_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_location
(
    id              INT          NOT NULL AUTO_INCREMENT,
    location_id     INT          NOT NULL,
    name            VARCHAR(255) NOT NULL,
    description     VARCHAR(255) NULL,
    city_village    VARCHAR(255) NULL,
    state_province  VARCHAR(255) NULL,
    postal_code     VARCHAR(50)  NULL,
    country         VARCHAR(50)  NULL,
    latitude        VARCHAR(50)  NULL,
    longitude       VARCHAR(50)  NULL,
    county_district VARCHAR(255) NULL,
    address1        VARCHAR(255) NULL,
    address2        VARCHAR(255) NULL,
    address3        VARCHAR(255) NULL,
    address4        VARCHAR(255) NULL,
    address5        VARCHAR(255) NULL,
    address6        VARCHAR(255) NULL,
    address7        VARCHAR(255) NULL,
    address8        VARCHAR(255) NULL,
    address9        VARCHAR(255) NULL,
    address10       VARCHAR(255) NULL,
    address11       VARCHAR(255) NULL,
    address12       VARCHAR(255) NULL,
    address13       VARCHAR(255) NULL,
    address14       VARCHAR(255) NULL,
    address15       VARCHAR(255) NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_location_location_id_index
    ON mamba_dim_location (location_id);

CREATE INDEX mamba_dim_location_name_index
    ON mamba_dim_location (name);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_location_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_location_insert;

~
CREATE PROCEDURE sp_mamba_dim_location_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_location (location_id,
                                name,
                                description,
                                city_village,
                                state_province,
                                postal_code,
                                country,
                                latitude,
                                longitude,
                                county_district,
                                address1,
                                address2,
                                address3,
                                address4,
                                address5,
                                address6,
                                address7,
                                address8,
                                address9,
                                address10,
                                address11,
                                address12,
                                address13,
                                address14,
                                address15)
SELECT location_id,
       name,
       description,
       city_village,
       state_province,
       postal_code,
       country,
       latitude,
       longitude,
       county_district,
       address1,
       address2,
       address3,
       address4,
       address5,
       address6,
       address7,
       address8,
       address9,
       address10,
       address11,
       address12,
       address13,
       address14,
       address15
FROM location;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_location_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_location_update;

~
CREATE PROCEDURE sp_mamba_dim_location_update()
BEGIN
-- $BEGIN

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_location  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_location;

~
CREATE PROCEDURE sp_mamba_dim_location()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_location_create();
CALL sp_mamba_dim_location_insert();
CALL sp_mamba_dim_location_update();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_type_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_type_create;

~
CREATE PROCEDURE sp_mamba_dim_patient_identifier_type_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_patient_identifier_type
(
    id                         INT         NOT NULL AUTO_INCREMENT,
    patient_identifier_type_id INT         NOT NULL,
    name                       VARCHAR(50) NOT NULL,
    description                TEXT        NULL,
    uuid                       CHAR(38)    NOT NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_patient_identifier_type_id_index
    ON mamba_dim_patient_identifier_type (patient_identifier_type_id);

CREATE INDEX mamba_dim_patient_identifier_type_name_index
    ON mamba_dim_patient_identifier_type (name);

CREATE INDEX mamba_dim_patient_identifier_type_uuid_index
    ON mamba_dim_patient_identifier_type (uuid);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_type_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_type_insert;

~
CREATE PROCEDURE sp_mamba_dim_patient_identifier_type_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_patient_identifier_type (patient_identifier_type_id,
                                               name,
                                               description,
                                               uuid)
SELECT patient_identifier_type_id,
       name,
       description,
       uuid
FROM patient_identifier_type;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_type_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_type_update;

~
CREATE PROCEDURE sp_mamba_dim_patient_identifier_type_update()
BEGIN
-- $BEGIN

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_type  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_type;

~
CREATE PROCEDURE sp_mamba_dim_patient_identifier_type()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_patient_identifier_type_create();
CALL sp_mamba_dim_patient_identifier_type_insert();
CALL sp_mamba_dim_patient_identifier_type_update();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_datatype_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_datatype_create;

~
CREATE PROCEDURE sp_mamba_dim_concept_datatype_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_concept_datatype
(
    id                  INT          NOT NULL AUTO_INCREMENT,
    concept_datatype_id INT          NOT NULL,
    datatype_name       VARCHAR(255) NOT NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_concept_datatype_concept_datatype_id_index
    ON mamba_dim_concept_datatype (concept_datatype_id);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_datatype_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_datatype_insert;

~
CREATE PROCEDURE sp_mamba_dim_concept_datatype_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_concept_datatype (concept_datatype_id,
                                        datatype_name)
SELECT dt.concept_datatype_id AS concept_datatype_id,
       dt.name                AS datatype_name
FROM concept_datatype dt;
-- WHERE dt.retired = 0;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_datatype  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_datatype;

~
CREATE PROCEDURE sp_mamba_dim_concept_datatype()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_concept_datatype_create();
CALL sp_mamba_dim_concept_datatype_insert();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_create;

~
CREATE PROCEDURE sp_mamba_dim_concept_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_concept
(
    id          INT          NOT NULL AUTO_INCREMENT,
    concept_id  INT          NOT NULL,
    uuid        CHAR(38)     NOT NULL,
    datatype_id INT NOT NULL, -- make it a FK
    datatype    VARCHAR(100) NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_concept_concept_id_index
    ON mamba_dim_concept (concept_id);

CREATE INDEX mamba_dim_concept_uuid_index
    ON mamba_dim_concept (uuid);

CREATE INDEX mamba_dim_concept_datatype_id_index
    ON mamba_dim_concept (datatype_id);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_insert;

~
CREATE PROCEDURE sp_mamba_dim_concept_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_concept (uuid,
                               concept_id,
                               datatype_id)
SELECT c.uuid        AS uuid,
       c.concept_id  AS concept_id,
       c.datatype_id AS datatype_id
FROM concept c;
-- WHERE c.retired = 0;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_update;

~
CREATE PROCEDURE sp_mamba_dim_concept_update()
BEGIN
-- $BEGIN

UPDATE mamba_dim_concept c
    INNER JOIN mamba_dim_concept_datatype dt
    ON c.datatype_id = dt.concept_datatype_id
SET c.datatype = dt.datatype_name
WHERE c.id > 0;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept;

~
CREATE PROCEDURE sp_mamba_dim_concept()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_concept_create();
CALL sp_mamba_dim_concept_insert();
CALL sp_mamba_dim_concept_update();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_answer_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_answer_create;

~
CREATE PROCEDURE sp_mamba_dim_concept_answer_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_concept_answer
(
    id                INT NOT NULL AUTO_INCREMENT,
    concept_answer_id INT NOT NULL,
    concept_id        INT NOT NULL,
    answer_concept    INT,
    answer_drug       INT,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_concept_answer_concept_answer_id_index
    ON mamba_dim_concept_answer (concept_answer_id);

CREATE INDEX mamba_dim_concept_answer_concept_id_index
    ON mamba_dim_concept_answer (concept_id);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_answer_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_answer_insert;

~
CREATE PROCEDURE sp_mamba_dim_concept_answer_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_concept_answer (concept_answer_id,
                                      concept_id,
                                      answer_concept,
                                      answer_drug)
SELECT ca.concept_answer_id AS concept_answer_id,
       ca.concept_id        AS concept_id,
       ca.answer_concept    AS answer_concept,
       ca.answer_drug       AS answer_drug
FROM concept_answer ca;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_answer  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_answer;

~
CREATE PROCEDURE sp_mamba_dim_concept_answer()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_concept_answer_create();
CALL sp_mamba_dim_concept_answer_insert();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_name_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_name_create;

~
CREATE PROCEDURE sp_mamba_dim_concept_name_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_concept_name
(
    id                INT          NOT NULL AUTO_INCREMENT,
    concept_name_id   INT          NOT NULL,
    concept_id        INT,
    name              VARCHAR(255) NOT NULL,
    locale            VARCHAR(50)  NOT NULL,
    locale_preferred  TINYINT,
    concept_name_type VARCHAR(255),

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_concept_name_concept_name_id_index
    ON mamba_dim_concept_name (concept_name_id);

CREATE INDEX mamba_dim_concept_name_concept_id_index
    ON mamba_dim_concept_name (concept_id);

CREATE INDEX mamba_dim_concept_name_concept_name_type_index
    ON mamba_dim_concept_name (concept_name_type);

CREATE INDEX mamba_dim_concept_name_locale_index
    ON mamba_dim_concept_name (locale);

CREATE INDEX mamba_dim_concept_name_locale_preferred_index
    ON mamba_dim_concept_name (locale_preferred);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_name_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_name_insert;

~
CREATE PROCEDURE sp_mamba_dim_concept_name_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_concept_name (concept_name_id,
                                    concept_id,
                                    name,
                                    locale,
                                    locale_preferred,
                                    concept_name_type)
SELECT cn.concept_name_id,
       cn.concept_id,
       cn.name,
       cn.locale,
       cn.locale_preferred,
       cn.concept_name_type
FROM concept_name cn
 WHERE cn.locale = 'en'
  AND cn.locale_preferred = 1
    AND cn.voided = 0;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_name  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_name;

~
CREATE PROCEDURE sp_mamba_dim_concept_name()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_concept_name_create();
CALL sp_mamba_dim_concept_name_insert();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_type_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type_create;

~
CREATE PROCEDURE sp_mamba_dim_encounter_type_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_encounter_type
(
    id                INT      NOT NULL AUTO_INCREMENT,
    encounter_type_id INT      NOT NULL,
    uuid              CHAR(38) NOT NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_encounter_type_encounter_type_id_index
    ON mamba_dim_encounter_type (encounter_type_id);

CREATE INDEX mamba_dim_encounter_type_uuid_index
    ON mamba_dim_encounter_type (uuid);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_type_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type_insert;

~
CREATE PROCEDURE sp_mamba_dim_encounter_type_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_encounter_type (encounter_type_id,
                                      uuid)
SELECT et.encounter_type_id,
       et.uuid
FROM encounter_type et;
-- WHERE et.retired = 0;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_type  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_type;

~
CREATE PROCEDURE sp_mamba_dim_encounter_type()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_encounter_type_create();
CALL sp_mamba_dim_encounter_type_insert();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_create;

~
CREATE PROCEDURE sp_mamba_dim_encounter_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_encounter
(
    id                  INT        NOT NULL AUTO_INCREMENT,
    encounter_id        INT        NOT NULL,
    uuid                CHAR(38)   NOT NULL,
    encounter_type      INT        NOT NULL,
    encounter_type_uuid CHAR(38)   NULL,
    patient_id          INT        NOT NULL,
    encounter_datetime  DATETIME   NOT NULL,
    date_created        DATETIME   NOT NULL,
    voided              TINYINT NOT NULL,
    visit_id            INT        NULL,

    CONSTRAINT encounter_encounter_id_index
        UNIQUE (encounter_id),

    CONSTRAINT encounter_uuid_index
        UNIQUE (uuid),

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_encounter_encounter_id_index
    ON mamba_dim_encounter (encounter_id);

CREATE INDEX mamba_dim_encounter_encounter_type_index
    ON mamba_dim_encounter (encounter_type);

CREATE INDEX mamba_dim_encounter_uuid_index
    ON mamba_dim_encounter (uuid);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_insert;

~
CREATE PROCEDURE sp_mamba_dim_encounter_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_encounter (encounter_id,
                                 uuid,
                                 encounter_type,
                                 encounter_type_uuid,
                                 patient_id,
                                 encounter_datetime,
                                 date_created,
                                 voided,
                                 visit_id)
SELECT e.encounter_id,
       e.uuid,
       e.encounter_type,
       et.uuid,
       e.patient_id,
       e.encounter_datetime,
       e.date_created,
       e.voided,
       e.visit_id
FROM encounter e
         INNER JOIN mamba_dim_encounter_type et
                    ON e.encounter_type = et.encounter_type_id
WHERE et.uuid
          IN (SELECT DISTINCT(md.encounter_type_uuid)
              FROM mamba_dim_concept_metadata md);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter_update;

~
CREATE PROCEDURE sp_mamba_dim_encounter_update()
BEGIN
-- $BEGIN

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_encounter  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_encounter;

~
CREATE PROCEDURE sp_mamba_dim_encounter()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_encounter_create();
CALL sp_mamba_dim_encounter_insert();
CALL sp_mamba_dim_encounter_update();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_metadata_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_metadata_create;

~
CREATE PROCEDURE sp_mamba_dim_concept_metadata_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_concept_metadata
(
    id                  INT          NOT NULL AUTO_INCREMENT,
    concept_id          INT          NULL,
    concept_uuid        CHAR(38)     NOT NULL,
    concept_name        VARCHAR(255) NULL,
    concepts_locale     VARCHAR(20)  NOT NULL,
    column_number       INT,
    column_label        VARCHAR(50)  NOT NULL,
    concept_datatype    VARCHAR(255) NULL,
    concept_answer_obs  TINYINT      NOT NULL DEFAULT 0,
    report_name         VARCHAR(255) NOT NULL,
    flat_table_name     VARCHAR(255) NULL,
    encounter_type_uuid CHAR(38)     NOT NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_concept_metadata_concept_id_index
    ON mamba_dim_concept_metadata (concept_id);

CREATE INDEX mamba_dim_concept_metadata_concept_uuid_index
    ON mamba_dim_concept_metadata (concept_uuid);

CREATE INDEX mamba_dim_concept_metadata_encounter_type_uuid_index
    ON mamba_dim_concept_metadata (encounter_type_uuid);

CREATE INDEX mamba_dim_concept_metadata_concepts_locale_index
    ON mamba_dim_concept_metadata (concepts_locale);

-- ALTER TABLE `mamba_dim_concept_metadata`
--     ADD COLUMN `encounter_type_id` INT NULL AFTER `output_table_name`,
--     ADD CONSTRAINT `fk_encounter_type_id`
--         FOREIGN KEY (`encounter_type_id`) REFERENCES `mamba_dim_encounter_type` (`encounter_type_id`);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_metadata_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_metadata_insert;

~
CREATE PROCEDURE sp_mamba_dim_concept_metadata_insert()
BEGIN
  -- $BEGIN

  SET @report_data = '{"flat_report_metadata":[
  {
  "report_name": "NCD Diabetes Report",
  "flat_table_name": "mamba_flat_encounter_diabetes",
  "encounter_type_uuid": "6bbe8864-a005-4183-bd14-9497883d9655",
  "concepts_locale": "en",
  "table_columns": {
    "x-ray_chest": "3ccc6186-26fe-102b-80cb-0017a47871b2",
    "hemoglobin": "3ccc7158-26fe-102b-80cb-0017a47871b2",
    "total_protein": "3cd3bd5a-26fe-102b-80cb-0017a47871b2",
    "platelets": "3cd3d038-26fe-102b-80cb-0017a47871b2",
    "serum_creatinine": "3cd4374e-26fe-102b-80cb-0017a47871b2",
    "urinary_albumin": "3cd49d88-26fe-102b-80cb-0017a47871b2",
    "blood_urea_nitrogen": "3cd4aa12-26fe-102b-80cb-0017a47871b2",
    "serum_glucose": "3cd4e194-26fe-102b-80cb-0017a47871b2",
    "allergy_to_drug": "3cd66cda-26fe-102b-80cb-0017a47871b2",
    "total_cholesterol": "3cd68c7e-26fe-102b-80cb-0017a47871b2",
    "highdensity_lipoprotein_cholesterol": "3cd68e18-26fe-102b-80cb-0017a47871b2",
    "lowdensity_lipoprotein_cholesterol": "3cd68fa8-26fe-102b-80cb-0017a47871b2",
    "triglycerides": "3cd69138-26fe-102b-80cb-0017a47871b2",
    "hematocrit": "3cd69a98-26fe-102b-80cb-0017a47871b2",
    "lymphocytes": "3cd6a402-26fe-102b-80cb-0017a47871b2",
    "neutrophils": "3cd6a592-26fe-102b-80cb-0017a47871b2",
    "monocytes": "3cd6a722-26fe-102b-80cb-0017a47871b2",
    "eosinophils": "3cd6a8b2-26fe-102b-80cb-0017a47871b2",
    "basophils": "3cd6aa38-26fe-102b-80cb-0017a47871b2",
    "general_exam_findings": "0adeea3a-15f5-102d-96e4-000c29c2a5d7",
    "heent_exam_findings": "3cd75b86-26fe-102b-80cb-0017a47871b2",
    "cardiac_exam_findings": "3cd75e9c-26fe-102b-80cb-0017a47871b2",
    "abdominal_exam_findings": "3cd76054-26fe-102b-80cb-0017a47871b2",
    "urogenital_exam_findings": "3cd761ee-26fe-102b-80cb-0017a47871b2",
    "extremity_exam_findings": "3cd7637e-26fe-102b-80cb-0017a47871b2",
    "serum_sodium": "3cd76b58-26fe-102b-80cb-0017a47871b2",
    "serum_potassium": "3cd76ce8-26fe-102b-80cb-0017a47871b2",
    "medication_history": "3cd9478e-26fe-102b-80cb-0017a47871b2",
    "name_of_health_care_provider": "3cda02e6-26fe-102b-80cb-0017a47871b2",
    "telephone_number": "3cda3d7e-26fe-102b-80cb-0017a47871b2",
    "current_oi_or_comorbidity": "0ae23a5a-15f5-102d-96e4-000c29c2a5d7",
    "comments_at_conclusion_of_examination": "3cdc5938-26fe-102b-80cb-0017a47871b2",
    "creatinine_clearance": "3cdc609a-26fe-102b-80cb-0017a47871b2",
    "body_mass_index,_measured": "3ce14da8-26fe-102b-80cb-0017a47871b2",
    "result_of_hiv_test": "3ce17cec-26fe-102b-80cb-0017a47871b2",
    "other_lab_test_name": "3ce1c90e-26fe-102b-80cb-0017a47871b2",
    "other_lab_test_result": "3ce1ca8a-26fe-102b-80cb-0017a47871b2",
    "current_complaints_or_symptoms": "3ce2b170-26fe-102b-80cb-0017a47871b2",
    "cardiac_medication": "4dde0454-edd2-4e61-b86b-4283b482f453",
    "other_general_exam_findings": "8d8597b4-0b54-4bc1-a35c-fa06d80e7a2b",
    "lung_exam_findings": "0aee5fba-15f5-102d-96e4-000c29c2a5d7",
    "patients_fluid_management": "5c5755df-3d1b-4ae2-a465-31dc05f49ddd",
    "drug_frequency_coded": "71ffb8ee-382e-4ad0-9b42-0b665f81aaf9",
    "echocardiogram_comment": "5c052224-aeaf-4a17-8dca-8255eef79644",
    "cardiac_medication_construct": "cd0f1b4b-8045-4482-837d-448d51815d26",
    "morning_dose_in_milligrams": "f032b306-4d4b-4b02-8335-3cce084f30a6",
    "noon_dose_in_milligrams": "41abbe03-04da-4b5d-8223-0a249245dcf1",
    "night_dose_in_milligrams": "305c266e-2035-4ef2-ab44-b4e70756998d",
    "type_of_referring_clinic_or_hospital": "c3e1d8d4-3040-49dd-ad66-c0928d912941",
    "allergy_comment": "38b0118c-1cf4-40a9-a508-15c48d9586ac",
    "chronic_care_diagnosis": "bb7e04d8-3355-4fe8-9c87-98642eafab93",
    "hypoglycemia": "641f4fe3-cac2-46c4-aa94-c8b6d05e9407",
    "ddb_echocardiograph_result": "75b3b477-a8ab-4eba-9912-c8bca53b0bbf",
    "heart_failure_diagnosis": "e6bb1491-43b5-46b8-ba55-bd1ad188123c",
    "neurological_exam_findings": "edd2cae1-99e7-4219-b5f9-fa512a69fed2",
    "systolic_blood_pressure": "3ce934fa-26fe-102b-80cb-0017a47871b2",
    "diastolic_blood_pressure": "3ce93694-26fe-102b-80cb-0017a47871b2",
    "pulse": "3ce93824-26fe-102b-80cb-0017a47871b2",
    "temperature_c": "3ce939d2-26fe-102b-80cb-0017a47871b2",
    "weight_kg": "3ce93b62-26fe-102b-80cb-0017a47871b2",
    "height_cm": "3ce93cf2-26fe-102b-80cb-0017a47871b2",
    "blood_oxygen_saturation": "3ce9401c-26fe-102b-80cb-0017a47871b2",
    "return_visit_date": "3ce94df0-26fe-102b-80cb-0017a47871b2",
    "respiratory_rate": "3ceb11f8-26fe-102b-80cb-0017a47871b2",
    "previous_medical_history": "bc3862d8-6825-4878-8801-f1e7b0790071",
    "time_units": "f1904502-319d-4681-9030-e642111e7ce2",
    "alat_result": "fd826b26-0343-41d6-b51c-81f3b75e388c",
    "asat_result": "68ba31fa-e2ae-45c1-8d6d-f116c27f190e",
    "location_of_lymphadenopathy": "9da47553-eaf7-4c28-90b3-414f8eebffd9",
    "type_of_diabetes_diagnosis": "d66e5df3-bc91-41ea-9592-0f199fbc589e",
    "diabetes_medications": "fa9b8b64-7081-41ec-9018-a8ea51464d70",
    "clinical_diagnosis_for_asthma": "e8f25c6e-9491-4ca3-9d31-7df9cb3d9ed9",
    "sensation_in_left_foot": "19afbd01-c1d0-4041-8cc8-e87a7922c1df",
    "sensation_in_right_foot": "62e72ad7-869e-4eae-909c-36ec4f2b7555",
    "asthma_medications": "0c0ae909-30bf-4b06-85d0-49e08056b1ea",
    "gamma_gt": "a01f58fa-0e27-474a-8975-f282f888a31d",
    "diagnosis_or_problem,_non-coded": "970d41ce-5098-47a4-8872-4dd843c0df3f",
    "facility": "3c9331e3-d02c-4b7d-840e-59e2d8ab7dff",
    "other_diabetes_drug": "219d650a-3305-4f56-9613-147d213bc9c9",
    "other_cardiovascular_chronic_kidney_meds": "0e039736-b016-484d-8931-5ea5c0391995",
    "diagnosis_or_problem_construct": "e9efefc3-065a-4fc9-991f-63255415b4f6",
    "hba1c": "159644AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "clinican_notes": "6602a73b-b8d3-40bc-bae8-f00ff3b9cceb",
    "ubudehe_category": "b9016409-8ca6-4123-97a5-146dc93571e8",
    "family_care-givername": "14a171cd-d8e6-4326-9f29-a5ee3f8055dd",
    "family_care-giver_phone_number": "a20ccea2-8671-4bfa-9852-946266fe9ac7",
    "patients_contact_phone_owner": "d44faa5c-32e0-4715-b4af-f89664cde097",
    "other_text": "c548a633-9931-496d-9ade-4b7c971e6600",
    "duration": "222d802c-4306-4c4c-866f-c4ca5d46d4a7",
    "duration_with_units_set": "2b6ac755-70a2-47a6-bba2-db1bc63457e3",
    "surgicalhistory": "6f035d26-aab4-4a54-ae3b-6c1db01cd120",
    "obstestrical": "9ea8c0c8-9a4f-4c1d-9962-c31c7abc62dc",
    "patient_medical_history": "9090e8f8-54cd-4ed0-b994-1130afb384bd",
    "patient_chronic_disease_history": "e20ab8c4-bd53-4f9f-b2e1-04fe7cb667ea",
    "other_disease": "f9e8e946-d177-4ca9-8004-779ae6abe9df",
    "familymember_suffered_chronic_disease": "f6268042-d8b6-4814-8abe-c4a8023eb058",
    "familymember_chronic_diseases": "bb523be9-a82a-4d3b-82b9-30f00726848c",
    "chest_thorax_exam_findings": "d261d79b-71a1-42b5-9393-18dac01a5955",
    "other_medical_findings": "b192c81d-5e89-40ed-a71d-ff03c1e380c0",
    "hypertension_stages": "5d31abca-d671-4e93-aaa0-5b383ff7d8ad",
    "treatment_appoach": "0bbba919-97d0-496e-8b7d-787090773e3c",
    "morning_dose_in_units": "9325d70e-705a-45fd-bed2-31e815d6b2db",
    "noon_dose_in_units": "a260b953-ac39-4745-bbc9-9c6fb45f3821",
    "evening_dose_in_units": "41a50d09-a434-41ee-ab3a-220367316056",
    "bedtime_dose_in_units": "f1ca287b-a329-4764-8ccb-a27927cef16c",
    "diabetes_treatment_construct": "5511ae49-91c1-43ab-aa76-49e38ba02146",
    "other_asthma_drug": "16fae208-4dea-4e32-9233-238be6eac56c",
    "other_hf_htn_ckd_drugs": "ec641732-c28f-4fd6-9d9e-019fadcf799b",
    "compliance": "7c9f7be9-3b1b-44b6-a00f-8738a99a7431",
    "ophthalmology_service": "b9d90af3-e9e8-4316-b2c7-673938076252",
    "bed_time_dose_in_milligrams": "ef7581c0-18f7-40a6-acb9-1e31e1c325f8",
    "food_insecurity": "55475803-90e6-4fda-b2aa-6874e563da4b",
    "part_of_the_day": "d7dff675-50d2-4a2b-88c3-dab4150dfc7b",
    "exit_ncd_program": "bedc579c-cfdc-4a94-9b13-112ce21145f0",
    "physical_pain_score": "8f0f2330-d296-4b7f-bc7e-561328227721",
    "spiritual_pain_score": "ef0c2cc9-0eb0-41f4-9a0d-34f1b3b17593",
    "psychological_pain_score": "6e429c48-cdfa-4dfd-9267-90131c2b1e12",
    "other_pain_drugs": "b20f4e61-94d3-4b31-955d-36e4df61a710",
    "drug_prescription_construct": "f7505d6c-fa17-4279-822d-8914b2e9d352",
    "pain_score_comments": "7c4fb4d3-5e09-49ed-bd44-190ca9a75a55",
    "glucose_test": "b8de5533-e263-49fe-bb3f-62298365e268",
    "telephone_group": "5ab398c2-5185-4f66-9f18-6772e26e82ac"
  }
}]}';

  CALL sp_mamba_extract_report_metadata(@report_data, 'mamba_dim_concept_metadata');

  -- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_metadata_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_metadata_update;

~
CREATE PROCEDURE sp_mamba_dim_concept_metadata_update()
BEGIN
-- $BEGIN

-- Update the Concept datatypes, concept_name and concept_id based on given locale
UPDATE mamba_dim_concept_metadata md
    INNER JOIN mamba_dim_concept c
    ON md.concept_uuid = c.uuid
    INNER JOIN mamba_dim_concept_name cn
    ON c.concept_id = cn.concept_id
SET md.concept_datatype = c.datatype,
    md.concept_id       = c.concept_id,
    md.concept_name     = cn.name
WHERE md.id > 0
  AND cn.locale = md.concepts_locale
  AND cn.locale_preferred = 1 and  cn.locale = 'en';
-- Use locale preferred or Fully specified name

-- Update to True if this field is an obs answer to an obs Question
UPDATE mamba_dim_concept_metadata md
    INNER JOIN mamba_dim_concept_answer ca
    ON md.concept_id = ca.answer_concept
SET md.concept_answer_obs = 1
WHERE md.id > 0;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_concept_metadata  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_concept_metadata;

~
CREATE PROCEDURE sp_mamba_dim_concept_metadata()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_concept_metadata_create();
CALL sp_mamba_dim_concept_metadata_insert();
CALL sp_mamba_dim_concept_metadata_update();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_create;

~
CREATE PROCEDURE sp_mamba_dim_person_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_person
(
    id                  INT          NOT NULL AUTO_INCREMENT,
    person_id           INT          NOT NULL,
    birthdate           DATE         NULL,
    birthdate_estimated TINYINT      NOT NULL,
    age                 INT          NULL,
    dead                TINYINT      NOT NULL,
    death_date          DATETIME     NULL,
    deathdate_estimated TINYINT      NOT NULL,
    gender              VARCHAR(255) NULL,
    date_created        DATETIME     NOT NULL,
    person_name_short   VARCHAR(255) NULL,
    person_name_long    TEXT         NULL,
    uuid                CHAR(38)     NOT NULL,
    voided              TINYINT      NOT NULL,

    PRIMARY KEY (id)
) CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_person_person_id_index
    ON mamba_dim_person (person_id);

CREATE INDEX mamba_dim_person_uuid_index
    ON mamba_dim_person (uuid);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_insert;

~
CREATE PROCEDURE sp_mamba_dim_person_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_person
<<<<<<< HEAD
(person_id,
 birthdate,
 birthdate_estimated,
 age,
 dead,
 death_date,
 deathdate_estimated,
 gender,
 date_created,
 person_name_short,
 person_name_long,
 uuid,
 voided)

SELECT psn.person_id,
       psn.birthdate,
       psn.birthdate_estimated,
       fn_mamba_age_calculator(birthdate, death_date)               AS age,
       psn.dead,
       psn.death_date,
       psn.deathdate_estimated,
       psn.gender,
       psn.date_created,
       CONCAT_WS(' ', prefix, given_name, middle_name, family_name) AS person_name_short,
       CONCAT_WS(' ', prefix, given_name, middle_name, family_name_prefix, family_name, family_name2,
                 family_name_suffix, degree)
                                                                    AS person_name_long,
       psn.uuid,
       psn.voided
FROM person psn
         INNER JOIN person_name pn
                    on psn.person_id = pn.person_id
where pn.preferred=1;
=======
    (
        person_id,
        birthdate,
        birthdate_estimated,
        age,
        dead,
        death_date,
        deathdate_estimated,
        gender,
        date_created,
        uuid,
        voided
    )

    SELECT psn.person_id,
           psn.birthdate,
           psn.birthdate_estimated,
           fn_mamba_age_calculator(birthdate,death_date) AS age,
           psn.dead,
           psn.death_date,
           psn.deathdate_estimated,
           psn.gender,
           psn.date_created,
           psn.uuid,
           psn.voided
    FROM person psn;
>>>>>>> bf27a34 (etl diabetes)

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_update;

~
CREATE PROCEDURE sp_mamba_dim_person_update()
BEGIN
-- $BEGIN
UPDATE mamba_dim_person dp
    INNER JOIN person psn  on psn.person_id = dp.person_id
    INNER JOIN  person_name pn on psn.person_id = pn.person_id
    SET   person_name_short = CONCAT_WS(' ',prefix,given_name,middle_name,family_name),
        person_name_long = CONCAT_WS(' ',prefix,given_name, middle_name,family_name_prefix, family_name,family_name2,family_name_suffix, degree)
;
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_person;

~
CREATE PROCEDURE sp_mamba_dim_person()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_person_create();
CALL sp_mamba_dim_person_insert();
CALL sp_mamba_dim_person_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_create;

~
CREATE PROCEDURE sp_mamba_dim_patient_identifier_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_patient_identifier
(
    id                    INT         NOT NULL AUTO_INCREMENT,
    patient_identifier_id INT,
    patient_id            INT         NOT NULL,
    identifier            VARCHAR(50) NOT NULL,
    identifier_type       INT         NOT NULL,
    preferred             TINYINT     NOT NULL,
    location_id           INT         NULL,
    date_created          DATETIME    NOT NULL,
    uuid                  CHAR(38)    NOT NULL,
    voided                TINYINT     NOT NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_patient_identifier_patient_identifier_id_index
    ON mamba_dim_patient_identifier (patient_identifier_id);

CREATE INDEX mamba_dim_patient_identifier_patient_id_index
    ON mamba_dim_patient_identifier (patient_id);

CREATE INDEX mamba_dim_patient_identifier_identifier_index
    ON mamba_dim_patient_identifier (identifier);

CREATE INDEX mamba_dim_patient_identifier_identifier_type_index
    ON mamba_dim_patient_identifier (identifier_type);

CREATE INDEX mamba_dim_patient_identifier_uuid_index
    ON mamba_dim_patient_identifier (uuid);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_insert;

~
CREATE PROCEDURE sp_mamba_dim_patient_identifier_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_patient_identifier (patient_id,
                                          identifier,
                                          identifier_type,
                                          preferred,
                                          location_id,
                                          date_created,
                                          uuid,
                                          voided)
SELECT patient_id,
       identifier,
       identifier_type,
       preferred,
       location_id,
       date_created,
       uuid,
       voided
FROM patient_identifier;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier_update;

~
CREATE PROCEDURE sp_mamba_dim_patient_identifier_update()
BEGIN
-- $BEGIN

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_identifier  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_identifier;

~
CREATE PROCEDURE sp_mamba_dim_patient_identifier()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_patient_identifier_create();
CALL sp_mamba_dim_patient_identifier_insert();
CALL sp_mamba_dim_patient_identifier_update();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_name_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_name_create;

~
CREATE PROCEDURE sp_mamba_dim_person_name_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_person_name
(
    id                 INT         NOT NULL AUTO_INCREMENT,
    person_name_id     INT         NOT NULL,
    person_id          INT         NOT NULL,
    preferred          TINYINT  NOT NULL,
    prefix             VARCHAR(50) NULL,
    given_name         VARCHAR(50) NULL,
    middle_name        VARCHAR(50) NULL,
    family_name_prefix VARCHAR(50) NULL,
    family_name        VARCHAR(50) NULL,
    family_name2       VARCHAR(50) NULL,
    family_name_suffix VARCHAR(50) NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_person_name_person_name_id_index
    ON mamba_dim_person_name (person_name_id);

CREATE INDEX mamba_dim_person_name_person_id_index
    ON mamba_dim_person_name (person_id);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_name_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_name_insert;

~
CREATE PROCEDURE sp_mamba_dim_person_name_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_person_name
    (
        person_name_id,
        person_id,
        preferred,
        prefix,
        given_name,
        middle_name,
        family_name_prefix,
        family_name,
        family_name2,
        family_name_suffix
    )
    SELECT
        pn.person_name_id,
        pn.person_id,
        pn.preferred,
        pn.prefix,
        pn.given_name,
        pn.middle_name,
        pn.family_name_prefix,
        pn.family_name,
        pn.family_name2,
        pn.family_name_suffix
    FROM
        person_name pn;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_name  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_name;

~
CREATE PROCEDURE sp_mamba_dim_person_name()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_person_name_create();
CALL sp_mamba_dim_person_name_insert();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_address_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_address_create;

~
CREATE PROCEDURE sp_mamba_dim_person_address_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_person_address
(
    id                INT          NOT NULL AUTO_INCREMENT,
    person_address_id INT          NOT NULL,
    person_id         INT          NULL,
    preferred         TINYINT      NOT NULL,
    address1          VARCHAR(255) NULL,
    address2          VARCHAR(255) NULL,
    address3          VARCHAR(255) NULL,
    address4          VARCHAR(255) NULL,
    address5          VARCHAR(255) NULL,
    address6          VARCHAR(255) NULL,
    city_village      VARCHAR(255) NULL,
    county_district   VARCHAR(255) NULL,
    state_province    VARCHAR(255) NULL,
    postal_code       VARCHAR(50)  NULL,
    country           VARCHAR(50)  NULL,
    latitude          VARCHAR(50)  NULL,
    longitude         VARCHAR(50)  NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_person_address_person_address_id_index
    ON mamba_dim_person_address (person_address_id);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_address_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_address_insert;

~
CREATE PROCEDURE sp_mamba_dim_person_address_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_person_address (person_address_id,
                                      person_id,
                                      preferred,
                                      address1,
                                      address2,
                                      address3,
                                      address4,
                                      address5,
                                      address6,
                                      city_village,
                                      county_district,
                                      state_province,
                                      postal_code,
                                      country,
                                      latitude,
                                      longitude)
SELECT person_address_id,
       person_id,
       preferred,
       address1,
       address2,
       address3,
       address4,
       address5,
       address6,
       city_village,
       county_district,
       state_province,
       postal_code,
       country,
       latitude,
       longitude
FROM person_address;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_person_address  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_person_address;

~
CREATE PROCEDURE sp_mamba_dim_person_address()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_person_address_create();
CALL sp_mamba_dim_person_address_insert();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_user_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_user_create;

~
CREATE PROCEDURE sp_mamba_dim_user_create()
BEGIN
-- $BEGIN
    CREATE TABLE mamba_dim_users
    (
        id            INT          NOT NULL AUTO_INCREMENT,
        user_id       INT          NOT NULL,
        system_id     VARCHAR(50)  NOT NULL,
        username      VARCHAR(50)  NULL,
        creator       INT          NOT NULL,
        date_created  DATETIME     NOT NULL,
        changed_by    INT          NULL,
        date_changed  DATETIME     NULL,
        person_id     INT          NOT NULL,
        retired       TINYINT(1)   NOT NULL,
        retired_by    INT          NULL,
        date_retired  DATETIME     NULL,
        retire_reason VARCHAR(255) NULL,
        uuid          CHAR(38)     NOT NULL,
        email         VARCHAR(255) NULL,

        PRIMARY KEY (id)
    )
        CHARSET = UTF8MB4;

    CREATE INDEX mamba_dim_users_user_id_index
        ON mamba_dim_users (user_id);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_user_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_user_insert;

~
CREATE PROCEDURE sp_mamba_dim_user_insert()
BEGIN
-- $BEGIN
    INSERT INTO mamba_dim_users
        (
            user_id,
            system_id,
            username,
            creator,
            date_created,
            changed_by,
            date_changed,
            person_id,
            retired,
            retired_by,
            date_retired,
            retire_reason,
            uuid,
            email
        )
        SELECT
            user_id,
            system_id,
            username,
            creator,
            date_created,
            changed_by,
            date_changed,
            person_id,
            retired,
            retired_by,
            date_retired,
            retire_reason,
            uuid,
            email
        FROM users c;
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_user_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_user_update;

~
CREATE PROCEDURE sp_mamba_dim_user_update()
BEGIN
-- $BEGIN

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_user  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_user;

~
CREATE PROCEDURE sp_mamba_dim_user()
BEGIN
-- $BEGIN
    CALL sp_mamba_dim_user_create();
    CALL sp_mamba_dim_user_insert();
    CALL sp_mamba_dim_user_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_agegroup_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_agegroup_create;

~
CREATE PROCEDURE sp_mamba_dim_agegroup_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_agegroup
(
    id              INT         NOT NULL AUTO_INCREMENT,
    age             INT         NULL,
    datim_agegroup  VARCHAR(50) NULL,
    datim_age_val   INT         NULL,
    normal_agegroup VARCHAR(50) NULL,
    normal_age_val   INT        NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_agegroup_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_agegroup_insert;

~
CREATE PROCEDURE sp_mamba_dim_agegroup_insert()
BEGIN
-- $BEGIN

-- Enter unknown dimension value (in case a person's date of birth is unknown)
CALL sp_mamba_load_agegroup();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_agegroup_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_agegroup_update;

~
CREATE PROCEDURE sp_mamba_dim_agegroup_update()
BEGIN
-- $BEGIN

-- update age_value b
UPDATE mamba_dim_agegroup a
SET datim_age_val =
    CASE
        WHEN a.datim_agegroup = '<1' THEN 1
        WHEN a.datim_agegroup = '1-4' THEN 2
        WHEN a.datim_agegroup = '5-9' THEN 3
        WHEN a.datim_agegroup = '10-14' THEN 4
        WHEN a.datim_agegroup = '15-19' THEN 5
        WHEN a.datim_agegroup = '20-24' THEN 6
        WHEN a.datim_agegroup = '25-29' THEN 7
        WHEN a.datim_agegroup = '30-34' THEN 8
        WHEN a.datim_agegroup = '35-39' THEN 9
        WHEN a.datim_agegroup = '40-44' THEN 10
        WHEN a.datim_agegroup = '45-49' THEN 11
        WHEN a.datim_agegroup = '50-54' THEN 12
        WHEN a.datim_agegroup = '55-59' THEN 13
        WHEN a.datim_agegroup = '60-64' THEN 14
        WHEN a.datim_agegroup = '65+' THEN 15
    END
WHERE a.datim_agegroup IS NOT NULL;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_agegroup  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_agegroup;

~
CREATE PROCEDURE sp_mamba_dim_agegroup()
BEGIN
-- $BEGIN

CALL sp_mamba_dim_agegroup_create();
CALL sp_mamba_dim_agegroup_insert();
CALL sp_mamba_dim_agegroup_update();
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_z_encounter_obs_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_z_encounter_obs_create;

~
CREATE PROCEDURE sp_mamba_z_encounter_obs_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_z_encounter_obs
(
    id                      INT           NOT NULL AUTO_INCREMENT,
    encounter_id            INT           NULL,
    person_id               INT           NOT NULL,
    encounter_datetime      DATETIME      NOT NULL,
    obs_datetime            DATETIME      NOT NULL,
    obs_question_concept_id INT DEFAULT 0 NOT NULL,
    obs_value_text          TEXT          NULL,
    obs_value_numeric       DOUBLE        NULL,
    obs_value_coded         INT           NULL,
    obs_value_datetime      DATETIME      NULL,
    obs_value_complex       VARCHAR(1000) NULL,
    obs_value_drug          INT           NULL,
    obs_question_uuid       CHAR(38),
    obs_answer_uuid         CHAR(38),
    obs_value_coded_uuid    CHAR(38),
    encounter_type_uuid     CHAR(38),
    status                  VARCHAR(16)   NOT NULL,
    voided                  TINYINT       NOT NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_z_encounter_obs_encounter_id_type_uuid_person_id_index
    ON mamba_z_encounter_obs (encounter_id, encounter_type_uuid, person_id);

CREATE INDEX mamba_z_encounter_obs_encounter_type_uuid_index
    ON mamba_z_encounter_obs (encounter_type_uuid);

CREATE INDEX mamba_z_encounter_obs_question_concept_id_index
    ON mamba_z_encounter_obs (obs_question_concept_id);

CREATE INDEX mamba_z_encounter_obs_value_coded_index
    ON mamba_z_encounter_obs (obs_value_coded);

CREATE INDEX mamba_z_encounter_obs_value_coded_uuid_index
    ON mamba_z_encounter_obs (obs_value_coded_uuid);

CREATE INDEX mamba_z_encounter_obs_question_uuid_index
    ON mamba_z_encounter_obs (obs_question_uuid);

CREATE INDEX mamba_z_encounter_obs_status_index
    ON mamba_z_encounter_obs (status);

CREATE INDEX mamba_z_encounter_obs_voided_index
    ON mamba_z_encounter_obs (voided);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_z_encounter_obs_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_z_encounter_obs_insert;

~
CREATE PROCEDURE sp_mamba_z_encounter_obs_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_z_encounter_obs
    (
        encounter_id,
        person_id,
        obs_datetime,
        encounter_datetime,
        encounter_type_uuid,
        obs_question_concept_id,
        obs_value_text,
        obs_value_numeric,
        obs_value_coded,
        obs_value_datetime,
        obs_value_complex,
        obs_value_drug,
        obs_question_uuid,
        obs_answer_uuid,
        obs_value_coded_uuid,
        status,
        voided
    )
    SELECT o.encounter_id,
           o.person_id,
           o.obs_datetime,
           e.encounter_datetime,
           e.encounter_type_uuid,
           o.concept_id     AS obs_question_concept_id,
           o.value_text     AS obs_value_text,
           o.value_numeric  AS obs_value_numeric,
           o.value_coded    AS obs_value_coded,
           o.value_datetime AS obs_value_datetime,
           o.value_complex  AS obs_value_complex,
           o.value_drug     AS obs_value_drug,
           NULL             AS obs_question_uuid,
           NULL             AS obs_answer_uuid,
           NULL             AS obs_value_coded_uuid,
           o.status,
           o.voided
    FROM obs o
             INNER JOIN mamba_dim_encounter e
                        ON o.encounter_id = e.encounter_id
    WHERE o.encounter_id IS NOT NULL;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_z_encounter_obs_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_z_encounter_obs_update;

~
CREATE PROCEDURE sp_mamba_z_encounter_obs_update()
BEGIN
-- $BEGIN

-- update obs question UUIDs
UPDATE mamba_z_encounter_obs z
    INNER JOIN mamba_dim_concept_metadata md
    ON z.obs_question_concept_id = md.concept_id
SET z.obs_question_uuid = md.concept_uuid
WHERE TRUE;

-- update obs_value_coded (UUIDs & Concept value names)
UPDATE mamba_z_encounter_obs z
    INNER JOIN mamba_dim_concept_name cn
    ON z.obs_value_coded = cn.concept_id
    INNER JOIN mamba_dim_concept c
    ON z.obs_value_coded = c.concept_id
SET z.obs_value_text       = cn.name,
    z.obs_value_coded_uuid = c.uuid
WHERE z.obs_value_coded IS NOT NULL;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_z_encounter_obs  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_z_encounter_obs;

~
CREATE PROCEDURE sp_mamba_z_encounter_obs()
BEGIN
-- $BEGIN

CALL sp_mamba_z_encounter_obs_create();
CALL sp_mamba_z_encounter_obs_insert();
CALL sp_mamba_z_encounter_obs_update();

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_data_processing_flatten  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_data_processing_flatten;

~
CREATE PROCEDURE sp_mamba_data_processing_flatten()
BEGIN
-- $BEGIN
-- CALL sp_xf_system_drop_all_tables_in_schema($target_database);
CALL sp_xf_system_drop_all_tables_in_schema();

CALL sp_mamba_dim_location;

CALL sp_mamba_dim_patient_identifier_type;

CALL sp_mamba_dim_concept_datatype;

CALL sp_mamba_dim_concept_answer;

CALL sp_mamba_dim_concept_name;

CALL sp_mamba_dim_concept;

CALL sp_mamba_dim_concept_metadata;

CALL sp_mamba_dim_encounter_type;

CALL sp_mamba_dim_encounter;

CALL sp_mamba_dim_person;

CALL sp_mamba_dim_person_name;

CALL sp_mamba_dim_person_address;

CALL sp_mamba_dim_user;

CALL sp_mamba_dim_patient_identifier;

CALL sp_mamba_dim_agegroup;

CALL sp_mamba_z_encounter_obs;

CALL sp_mamba_flat_encounter_table_create_all;

CALL sp_mamba_flat_encounter_table_insert_all;
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_data_processing_derived_billing  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_data_processing_derived_billing;

~
CREATE PROCEDURE sp_mamba_data_processing_derived_billing()
BEGIN
-- $BEGIN

-- Dimensions
CALL sp_mamba_dim_admission;
CALL sp_mamba_dim_beneficiary;
CALL sp_mamba_dim_bill_payment;
CALL sp_mamba_dim_billable_service;
CALL sp_mamba_dim_consommation;
CALL sp_mamba_dim_department;
CALL sp_mamba_dim_facility_service_price;
CALL sp_mamba_dim_global_bill;
CALL sp_mamba_dim_hop_service;
CALL sp_mamba_dim_insurance_rate;
CALL sp_mamba_dim_insurance;
CALL sp_mamba_dim_insurance_bill;
CALL sp_mamba_dim_insurance_policy;
CALL sp_mamba_dim_paid_service_bill;
CALL sp_mamba_dim_patient_bill;
CALL sp_mamba_dim_patient_service_bill;
CALL sp_mamba_dim_service_category;
CALL sp_mamba_dim_third_party_bill;
CALL sp_mamba_dim_thirdparty;

-- Facts
CALL sp_mamba_fact_patient_service_bill;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_data_processing_etl  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_data_processing_etl;

~
CREATE PROCEDURE sp_mamba_data_processing_etl()
BEGIN
-- $BEGIN
-- add base folder SP here --

-- Flatten the tables first
CALL sp_mamba_data_processing_flatten();

-- Call the ETL process
CALL sp_mamba_data_processing_derived_billing();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_thirdparty_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_thirdparty_create;

~
CREATE PROCEDURE sp_mamba_dim_thirdparty_create()
BEGIN
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
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_thirdparty_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_thirdparty_insert;

~
CREATE PROCEDURE sp_mamba_dim_thirdparty_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_third_party (third_party_id,
                                   name,
                                   rate,
                                   created_date)
SELECT third_party_id,
       name,
       rate,
       created_date
FROM moh_bill_third_party;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_thirdparty_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_thirdparty_update;

~
CREATE PROCEDURE sp_mamba_dim_thirdparty_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_thirdparty  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_thirdparty;

~
CREATE PROCEDURE sp_mamba_dim_thirdparty()
BEGIN
-- $BEGIN
CALL sp_mamba_dim_thirdparty_create();
CALL sp_mamba_dim_thirdparty_insert();
CALL sp_mamba_dim_thirdparty_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_department_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_department_create;

~
CREATE PROCEDURE sp_mamba_dim_department_create()
BEGIN
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
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_department_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_department_insert;

~
CREATE PROCEDURE sp_mamba_dim_department_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_department (department_id,
                                  name,
                                  description,
                                  created_date)
SELECT department_id,
       name,
       description,
       created_date
FROM moh_bill_department;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_department_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_department_update;

~
CREATE PROCEDURE sp_mamba_dim_department_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_department  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_department;

~
CREATE PROCEDURE sp_mamba_dim_department()
BEGIN
-- $BEGIN
CALL sp_mamba_dim_department_create();
CALL sp_mamba_dim_department_insert();
CALL sp_mamba_dim_department_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_hop_service_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_hop_service_create;

~
CREATE PROCEDURE sp_mamba_dim_hop_service_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_hop_service
(
    id           INT         NOT NULL AUTO_INCREMENT,
    service_id   INT         NOT NULL,
    name         varchar(50) null,
    description  varchar(50) null,
    created_date datetime    not null,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_hop_service_service_id_index
    ON mamba_dim_hop_service (service_id);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_hop_service_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_hop_service_insert;

~
CREATE PROCEDURE sp_mamba_dim_hop_service_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_hop_service (service_id,
                                   name,
                                   description,
                                   created_date)
SELECT service_id,
       name,
       description,
       created_date
FROM moh_bill_hop_service;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_hop_service_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_hop_service_update;

~
CREATE PROCEDURE sp_mamba_dim_hop_service_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_hop_service  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_hop_service;

~
CREATE PROCEDURE sp_mamba_dim_hop_service()
BEGIN
-- $BEGIN
CALL sp_mamba_dim_hop_service_create();
CALL sp_mamba_dim_hop_service_insert();
CALL sp_mamba_dim_hop_service_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_insurance_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_insurance_create;

~
CREATE PROCEDURE sp_mamba_dim_insurance_create()
BEGIN
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
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_insurance_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_insurance_insert;

~
CREATE PROCEDURE sp_mamba_dim_insurance_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_insurance (insurance_id,
                                 concept_id,
                                 category,
                                 name,
                                 address,
                                 phone,
                                 created_date)
SELECT insurance_id,
       concept_id,
       category,
       name,
       address,
       phone,
       created_date
FROM moh_bill_insurance;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_insurance_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_insurance_update;

~
CREATE PROCEDURE sp_mamba_dim_insurance_update()
BEGIN
-- $BEGIN

-- Update the current insurance rate for this insurance
UPDATE mamba_dim_insurance ins
SET ins.current_insurance_rate = COALESCE(
        (SELECT rate
         FROM mamba_dim_insurance_rate ir
         WHERE ir.insurance_id = ins.insurance_id
           AND (ir.retire_date IS NULL OR ir.retire_date > NOW())
         ORDER BY ir.retire_date ASC
         LIMIT 1),
        0 -- Default value when no active rate is found (you can change this to any default value)
    );

-- Update flat_rate as well -- TODO: combine this update into one update with upper update
UPDATE mamba_dim_insurance ins
SET ins.current_insurance_rate_flat_fee = COALESCE(
        (SELECT flatFee
         FROM mamba_dim_insurance_rate ir
         WHERE ir.insurance_id = ins.insurance_id
           AND (ir.retire_date IS NULL OR ir.retire_date > NOW())
         ORDER BY ir.retire_date ASC
         LIMIT 1),
        0
    );

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_insurance  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_insurance;

~
CREATE PROCEDURE sp_mamba_dim_insurance()
BEGIN
-- $BEGIN
CALL sp_mamba_dim_insurance_create();
CALL sp_mamba_dim_insurance_insert();
CALL sp_mamba_dim_insurance_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_insurance_rate_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_insurance_rate_create;

~
CREATE PROCEDURE sp_mamba_dim_insurance_rate_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_insurance_rate
(
    id                INT            NOT NULL AUTO_INCREMENT,
    insurance_rate_id INT            NOT NULL,
    insurance_id      int            not null,
    rate              float          not null,
    flatFee           decimal(20, 2) null,
    start_date        date           not null,
    end_date          date           null,
    created_date      date           not null,
    retired           smallint       not null,
    retire_date       date           null,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_insurance_rate_insurance_rate_id_index
    ON mamba_dim_insurance_rate (insurance_rate_id);

CREATE INDEX mamba_dim_insurance_rate_insurance_id_index
    ON mamba_dim_insurance_rate (insurance_id);

CREATE INDEX mamba_dim_insurance_rate_insurance_retired_index
    ON mamba_dim_insurance_rate (retired);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_insurance_rate_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_insurance_rate_insert;

~
CREATE PROCEDURE sp_mamba_dim_insurance_rate_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_insurance_rate (insurance_rate_id,
                                      insurance_id,
                                      rate,
                                      flatFee,
                                      start_date,
                                      end_date,
                                      created_date,
                                      retired,
                                      retire_date)
SELECT insurance_rate_id,
       insurance_id,
       rate,
       flatFee,
       start_date,
       end_date,
       created_date,
       retired,
       retire_date
FROM moh_bill_insurance_rate;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_insurance_rate_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_insurance_rate_update;

~
CREATE PROCEDURE sp_mamba_dim_insurance_rate_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_insurance_rate  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_insurance_rate;

~
CREATE PROCEDURE sp_mamba_dim_insurance_rate()
BEGIN
-- $BEGIN
CALL sp_mamba_dim_insurance_rate_create();
CALL sp_mamba_dim_insurance_rate_insert();
CALL sp_mamba_dim_insurance_rate_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_service_category_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_service_category_create;

~
CREATE PROCEDURE sp_mamba_dim_service_category_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_service_category
(
    id                  INT          NOT NULL AUTO_INCREMENT,
    service_category_id INT          NOT NULL,
    insurance_id        int          not null,
    department_id       int          null,
    service_id          int          null,
    name                varchar(150) not null,
    description         varchar(250) null,
    price               decimal      null,
    created_date        datetime     not null,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_service_category_service_category_id_index
    ON mamba_dim_service_category (service_category_id);

CREATE INDEX mamba_dim_service_category_insurance_id_index
    ON mamba_dim_service_category (insurance_id);

CREATE INDEX mamba_dim_service_category_department_id_index
    ON mamba_dim_service_category (department_id);

CREATE INDEX mamba_dim_service_category_service_id_index
    ON mamba_dim_service_category (service_id);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_service_category_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_service_category_insert;

~
CREATE PROCEDURE sp_mamba_dim_service_category_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_service_category (service_category_id,
                                        insurance_id,
                                        department_id,
                                        service_id,
                                        name,
                                        description,
                                        price,
                                        created_date)
SELECT service_category_id,
       insurance_id,
       department_id,
       service_id,
       name,
       description,
       price,
       created_date
FROM moh_bill_service_category;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_service_category_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_service_category_update;

~
CREATE PROCEDURE sp_mamba_dim_service_category_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_service_category  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_service_category;

~
CREATE PROCEDURE sp_mamba_dim_service_category()
BEGIN
-- $BEGIN
CALL sp_mamba_dim_service_category_create();
CALL sp_mamba_dim_service_category_insert();
CALL sp_mamba_dim_service_category_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_insurance_policy_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_insurance_policy_create;

~
CREATE PROCEDURE sp_mamba_dim_insurance_policy_create()
BEGIN
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
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_insurance_policy_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_insurance_policy_insert;

~
CREATE PROCEDURE sp_mamba_dim_insurance_policy_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_insurance_policy (insurance_policy_id,
                                        insurance_id,
                                        third_party_id,
                                        insurance_card_no,
                                        owner,
                                        coverage_start_date,
                                        expiration_date,
                                        created_date)
SELECT insurance_policy_id,
       insurance_id,
       third_party_id,
       insurance_card_no,
       owner,
       coverage_start_date,
       expiration_date,
       created_date
FROM moh_bill_insurance_policy;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_insurance_policy_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_insurance_policy_update;

~
CREATE PROCEDURE sp_mamba_dim_insurance_policy_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_insurance_policy  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_insurance_policy;

~
CREATE PROCEDURE sp_mamba_dim_insurance_policy()
BEGIN
-- $BEGIN
CALL sp_mamba_dim_insurance_policy_create();
CALL sp_mamba_dim_insurance_policy_insert();
CALL sp_mamba_dim_insurance_policy_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_beneficiary_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_beneficiary_create;

~
CREATE PROCEDURE sp_mamba_dim_beneficiary_create()
BEGIN
-- $BEGIN

CREATE TABLE IF NOT EXISTS mamba_dim_beneficiary
(
    id                  INT          NOT NULL AUTO_INCREMENT,
    beneficiary_id      INT          NOT NULL,
    patient_id          INT          NOT NULL,
    insurance_policy_id INT          NOT NULL,
    policy_id_number    VARCHAR(250) NULL,
    created_date        DATE         NOT NULL,
    creator             INT          NOT NULL,
    owner_name          VARCHAR(150) NULL,
    owner_code          VARCHAR(150) NULL,
    level               INT          NULL,
    company             VARCHAR(100) NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_beneficiary_beneficiary_id_index
    ON mamba_dim_beneficiary (beneficiary_id);

CREATE INDEX mamba_dim_beneficiary_patient_id_index
    ON mamba_dim_beneficiary (patient_id);

CREATE INDEX mamba_dim_beneficiary_insurance_policy_id_index
    ON mamba_dim_beneficiary (insurance_policy_id);

CREATE INDEX mamba_dim_beneficiary_policy_id_number_index
    ON mamba_dim_beneficiary (policy_id_number);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_beneficiary_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_beneficiary_insert;

~
CREATE PROCEDURE sp_mamba_dim_beneficiary_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_beneficiary (beneficiary_id,
                                   patient_id,
                                   insurance_policy_id,
                                   policy_id_number,
                                   created_date,
                                   creator,
                                   owner_name,
                                   owner_code,
                                   level,
                                   company)
SELECT beneficiary_id,
       patient_id,
       insurance_policy_id,
       policy_id_number,
       created_date,
       creator,
       owner_name,
       owner_code,
       level,
       company
FROM moh_bill_beneficiary;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_beneficiary_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_beneficiary_update;

~
CREATE PROCEDURE sp_mamba_dim_beneficiary_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_beneficiary  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_beneficiary;

~
CREATE PROCEDURE sp_mamba_dim_beneficiary()
BEGIN
-- $BEGIN
CALL sp_mamba_dim_beneficiary_create();
CALL sp_mamba_dim_beneficiary_insert();
CALL sp_mamba_dim_beneficiary_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_admission_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_admission_create;

~
CREATE PROCEDURE sp_mamba_dim_admission_create()
BEGIN
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
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_admission_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_admission_insert;

~
CREATE PROCEDURE sp_mamba_dim_admission_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_admission (admission_id,
                                 insurance_policy_id,
                                 is_admitted,
                                 admission_date,
                                 discharging_date,
                                 discharged_by,
                                 disease_type,
                                 admission_type,
                                 created_date)
SELECT admission_id,
       insurance_policy_id,
       is_admitted,
       admission_date,
       discharging_date,
       discharged_by,
       disease_type,
       admission_type,
       created_date
FROM moh_bill_admission;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_admission_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_admission_update;

~
CREATE PROCEDURE sp_mamba_dim_admission_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_admission  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_admission;

~
CREATE PROCEDURE sp_mamba_dim_admission()
BEGIN
-- $BEGIN
CALL sp_mamba_dim_admission_create();
CALL sp_mamba_dim_admission_insert();
CALL sp_mamba_dim_admission_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_facility_service_price_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_facility_service_price_create;

~
CREATE PROCEDURE sp_mamba_dim_facility_service_price_create()
BEGIN
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
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_facility_service_price_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_facility_service_price_insert;

~
CREATE PROCEDURE sp_mamba_dim_facility_service_price_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_facility_service_price (facility_service_price_id,
                                              location_id,
                                              concept_id,
                                              name,
                                              short_name,
                                              description,
                                              category,
                                              full_price,
                                              start_date,
                                              end_date,
                                              item_type,
                                              hide_item,
                                              created_date)
SELECT facility_service_price_id,
       location_id,
       concept_id,
       name,
       short_name,
       description,
       category,
       full_price,
       start_date,
       end_date,
       item_type,
       hide_item,
       created_date
FROM moh_bill_facility_service_price;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_facility_service_price_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_facility_service_price_update;

~
CREATE PROCEDURE sp_mamba_dim_facility_service_price_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_facility_service_price  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_facility_service_price;

~
CREATE PROCEDURE sp_mamba_dim_facility_service_price()
BEGIN
-- $BEGIN
CALL sp_mamba_dim_facility_service_price_create();
CALL sp_mamba_dim_facility_service_price_insert();
CALL sp_mamba_dim_facility_service_price_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_billable_service_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_billable_service_create;

~
CREATE PROCEDURE sp_mamba_dim_billable_service_create()
BEGIN
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
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_billable_service_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_billable_service_insert;

~
CREATE PROCEDURE sp_mamba_dim_billable_service_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_billable_service (billable_service_id,
                                        insurance_id,
                                        facility_service_price_id,
                                        service_category_id,
                                        maxima_to_pay,
                                        start_date,
                                        end_date,
                                        created_date)
SELECT billable_service_id,
       insurance_id,
       facility_service_price_id,
       service_category_id,
       maxima_to_pay,
       start_date,
       end_date,
       created_date
FROM moh_bill_billable_service;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_billable_service_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_billable_service_update;

~
CREATE PROCEDURE sp_mamba_dim_billable_service_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_billable_service  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_billable_service;

~
CREATE PROCEDURE sp_mamba_dim_billable_service()
BEGIN
-- $BEGIN
CALL sp_mamba_dim_billable_service_create();
CALL sp_mamba_dim_billable_service_insert();
CALL sp_mamba_dim_billable_service_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_insurance_bill_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_insurance_bill_create;

~
CREATE PROCEDURE sp_mamba_dim_insurance_bill_create()
BEGIN
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
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_insurance_bill_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_insurance_bill_insert;

~
CREATE PROCEDURE sp_mamba_dim_insurance_bill_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_insurance_bill (insurance_bill_id,
                                      amount,
                                      created_date)
SELECT insurance_bill_id,
       amount,
       created_date
FROM moh_bill_insurance_bill;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_insurance_bill_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_insurance_bill_update;

~
CREATE PROCEDURE sp_mamba_dim_insurance_bill_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_insurance_bill  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_insurance_bill;

~
CREATE PROCEDURE sp_mamba_dim_insurance_bill()
BEGIN
-- $BEGIN
CALL sp_mamba_dim_insurance_bill_create();
CALL sp_mamba_dim_insurance_bill_insert();
CALL sp_mamba_dim_insurance_bill_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_third_party_bill_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_third_party_bill_create;

~
CREATE PROCEDURE sp_mamba_dim_third_party_bill_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_third_party_bill
(
    id                  INT      NOT NULL AUTO_INCREMENT,
    third_party_bill_id INT      NOT NULL,
    amount              decimal  not null,
    created_date        datetime not null,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_third_party_bill_third_party_bill_id_index
    ON mamba_dim_third_party_bill (third_party_bill_id);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_third_party_bill_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_third_party_bill_insert;

~
CREATE PROCEDURE sp_mamba_dim_third_party_bill_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_third_party_bill (third_party_bill_id,
                                        amount,
                                        created_date)
SELECT third_party_bill_id,
       amount,
       created_date
FROM moh_bill_third_party_bill;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_third_party_bill_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_third_party_bill_update;

~
CREATE PROCEDURE sp_mamba_dim_third_party_bill_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_third_party_bill  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_third_party_bill;

~
CREATE PROCEDURE sp_mamba_dim_third_party_bill()
BEGIN
-- $BEGIN
CALL sp_mamba_dim_third_party_bill_create();
CALL sp_mamba_dim_third_party_bill_insert();
CALL sp_mamba_dim_third_party_bill_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_global_bill_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_global_bill_create;

~
CREATE PROCEDURE sp_mamba_dim_global_bill_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_global_bill
(
    id              INT          NOT NULL AUTO_INCREMENT,
    global_bill_id  INT          NOT NULL,
    admission_id    int          not null,
    insurance_id    int          null,
    bill_identifier varchar(250) not null,
    global_amount   decimal      not null,
    closing_date    datetime     null,
    closed          TINYINT(1)   not null,
    closed_by_id    int          null,
    closed_by_name  varchar(255) null,
    closed_reason   varchar(150) null,
    edited_by       int          null,
    edit_reason     varchar(150) null,
    created_date    datetime     not null,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_global_bill_global_bill_id_index
    ON mamba_dim_global_bill (global_bill_id);

CREATE INDEX mamba_dim_global_bill_admission_id_index
    ON mamba_dim_global_bill (admission_id);

CREATE INDEX mamba_dim_global_bill_insurance_id_index
    ON mamba_dim_global_bill (insurance_id);

CREATE INDEX mamba_dim_global_bill_closed_index
    ON mamba_dim_global_bill (closed);

CREATE INDEX mamba_dim_global_bill_closed_by_id_index
    ON mamba_dim_global_bill (closed_by_id);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_global_bill_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_global_bill_insert;

~
CREATE PROCEDURE sp_mamba_dim_global_bill_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_global_bill (global_bill_id,
                                   admission_id,
                                   insurance_id,
                                   bill_identifier,
                                   global_amount,
                                   closing_date,
                                   closed,
                                   closed_by_id,
                                   closed_reason,
                                   edited_by,
                                   edit_reason,
                                   created_date)
SELECT global_bill_id,
       admission_id,
       insurance_id,
       bill_identifier,
       global_amount,
       closing_date,
       closed,
       closed_by as closed_by_id,
       closed_reason,
       edited_by,
       edit_reason,
       created_date
FROM moh_bill_global_bill;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_global_bill_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_global_bill_update;

~
CREATE PROCEDURE sp_mamba_dim_global_bill_update()
BEGIN
-- $BEGIN

-- update the user who closed this bill - in this case it is a doctor
UPDATE mamba_dim_global_bill gb
    INNER JOIN mamba_dim_users u ON u.user_id = gb.closed_by_id
    INNER JOIN mamba_dim_person_name psn ON psn.person_id = u.person_id
SET gb.closed_by_name = CONCAT(psn.family_name, ' ', psn.given_name)
WHERE gb.closed = 1;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_global_bill  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_global_bill;

~
CREATE PROCEDURE sp_mamba_dim_global_bill()
BEGIN
-- $BEGIN
CALL sp_mamba_dim_global_bill_create();
CALL sp_mamba_dim_global_bill_insert();
CALL sp_mamba_dim_global_bill_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_bill_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_bill_create;

~
CREATE PROCEDURE sp_mamba_dim_patient_bill_create()
BEGIN
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
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_bill_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_bill_insert;

~
CREATE PROCEDURE sp_mamba_dim_patient_bill_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_patient_bill (patient_bill_id,
                                    amount,
                                    is_paid,
                                    status)
SELECT patient_bill_id,
       amount,
       is_paid,
       status
FROM moh_bill_patient_bill;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_bill_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_bill_update;

~
CREATE PROCEDURE sp_mamba_dim_patient_bill_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_bill  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_bill;

~
CREATE PROCEDURE sp_mamba_dim_patient_bill()
BEGIN
-- $BEGIN
CALL sp_mamba_dim_patient_bill_create();
CALL sp_mamba_dim_patient_bill_insert();
CALL sp_mamba_dim_patient_bill_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_consommation_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_consommation_create;

~
CREATE PROCEDURE sp_mamba_dim_consommation_create()
BEGIN
-- $BEGIN

CREATE TABLE IF NOT EXISTS mamba_dim_consommation
(
    id                  INT      NOT NULL AUTO_INCREMENT,
    consommation_id     INT      NOT NULL,
    global_bill_id      INT      NULL,
    department_id       INT      NULL,
    beneficiary_id      INT      NOT NULL,
    patient_bill_id     INT      NOT NULL,
    insurance_bill_id   INT      NULL,
    third_party_bill_id INT      NULL,
    created_date        DATETIME NOT NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_consommation_consommation_id_index
    ON mamba_dim_consommation (consommation_id);

CREATE INDEX mamba_dim_consommation_global_bill_id_index
    ON mamba_dim_consommation (global_bill_id);

CREATE INDEX mamba_dim_consommation_department_id_index
    ON mamba_dim_consommation (department_id);

CREATE INDEX mamba_dim_consommation_beneficiary_id_index
    ON mamba_dim_consommation (beneficiary_id);

CREATE INDEX mamba_dim_consommation_patient_bill_id_index
    ON mamba_dim_consommation (patient_bill_id);

CREATE INDEX mamba_dim_consommation_insurance_bill_id_index
    ON mamba_dim_consommation (insurance_bill_id);

CREATE INDEX mamba_dim_consommation_third_party_bill_id_index
    ON mamba_dim_consommation (third_party_bill_id);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_consommation_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_consommation_insert;

~
CREATE PROCEDURE sp_mamba_dim_consommation_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_consommation (consommation_id,
                                    global_bill_id,
                                    department_id,
                                    beneficiary_id,
                                    patient_bill_id,
                                    insurance_bill_id,
                                    third_party_bill_id,
                                    created_date)
SELECT consommation_id,
       global_bill_id,
       department_id,
       beneficiary_id,
       patient_bill_id,
       insurance_bill_id,
       third_party_bill_id,
       created_date
FROM moh_bill_consommation;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_consommation_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_consommation_update;

~
CREATE PROCEDURE sp_mamba_dim_consommation_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_consommation  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_consommation;

~
CREATE PROCEDURE sp_mamba_dim_consommation()
BEGIN
-- $BEGIN
CALL sp_mamba_dim_consommation_create();
CALL sp_mamba_dim_consommation_insert();
CALL sp_mamba_dim_consommation_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_service_bill_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_service_bill_create;

~
CREATE PROCEDURE sp_mamba_dim_patient_service_bill_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_dim_patient_service_bill
(
    id                        INT            NOT NULL AUTO_INCREMENT,
    patient_service_bill_id   INT            NOT NULL,
    consommation_id           int            not null,
    billable_service_id       int            null,
    service_id                int            null,
    service_date              date           not null,
    unit_price                decimal(20, 2) not null,
    quantity                  decimal(20, 2) null,
    paid_quantity             decimal(20, 2) null,
    service_other             varchar(100)   null,
    service_other_description varchar(250)   null,
    is_paid                   smallint       not null,
    drug_frequency            varchar(255)   null,
    item_type                 tinyint(1)     null,
    voided                    smallint       not null,
    created_date              datetime       null,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_patient_service_bill_patient_service_bill_id_index
    ON mamba_dim_patient_service_bill (patient_service_bill_id);

CREATE INDEX mamba_dim_patient_service_bill_consommation_id_index
    ON mamba_dim_patient_service_bill (consommation_id);

CREATE INDEX mamba_dim_patient_service_bill_billable_service_id_index
    ON mamba_dim_patient_service_bill (billable_service_id);

CREATE INDEX mamba_dim_patient_service_bill_service_id_index
    ON mamba_dim_patient_service_bill (service_id);

CREATE INDEX mamba_dim_patient_service_bill_voided_index
    ON mamba_dim_patient_service_bill (voided);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_service_bill_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_service_bill_insert;

~
CREATE PROCEDURE sp_mamba_dim_patient_service_bill_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_patient_service_bill (patient_service_bill_id,
                                            consommation_id,
                                            billable_service_id,
                                            service_id,
                                            service_date,
                                            unit_price,
                                            quantity,
                                            paid_quantity,
                                            service_other,
                                            service_other_description,
                                            is_paid,
                                            drug_frequency,
                                            item_type,
                                            voided,
                                            created_date)

SELECT patient_service_bill_id,
       consommation_id,
       billable_service_id,
       service_id,
       service_date,
       unit_price,
       quantity,
       paid_quantity,
       service_other,
       service_other_description,
       is_paid,
       drug_frequency,
       item_type,
       voided,
       created_date
FROM moh_bill_patient_service_bill;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_service_bill_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_service_bill_update;

~
CREATE PROCEDURE sp_mamba_dim_patient_service_bill_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_patient_service_bill  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_patient_service_bill;

~
CREATE PROCEDURE sp_mamba_dim_patient_service_bill()
BEGIN
-- $BEGIN
CALL sp_mamba_dim_patient_service_bill_create();
CALL sp_mamba_dim_patient_service_bill_insert();
CALL sp_mamba_dim_patient_service_bill_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_bill_payment_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_bill_payment_create;

~
CREATE PROCEDURE sp_mamba_dim_bill_payment_create()
BEGIN
-- $BEGIN

CREATE TABLE IF NOT EXISTS mamba_dim_bill_payment
(
    id              INT            NOT NULL AUTO_INCREMENT,
    bill_payment_id INT            NOT NULL,
    patient_bill_id int            not null,
    amount_paid     decimal(20, 2) not null,
    date_received   datetime       null,
    collector       int            not null,
    created_date    datetime       not null,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_dim_bill_payment_bill_payment_id_index
    ON mamba_dim_bill_payment (bill_payment_id);

CREATE INDEX mamba_dim_bill_payment_patient_bill_id_index
    ON mamba_dim_bill_payment (patient_bill_id);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_bill_payment_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_bill_payment_insert;

~
CREATE PROCEDURE sp_mamba_dim_bill_payment_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_bill_payment (bill_payment_id,
                                    patient_bill_id,
                                    amount_paid,
                                    date_received,
                                    collector,
                                    created_date)
SELECT bill_payment_id,
       patient_bill_id,
       amount_paid,
       date_received,
       collector,
       created_date
FROM moh_bill_payment;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_bill_payment_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_bill_payment_update;

~
CREATE PROCEDURE sp_mamba_dim_bill_payment_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_bill_payment  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_bill_payment;

~
CREATE PROCEDURE sp_mamba_dim_bill_payment()
BEGIN
-- $BEGIN
CALL sp_mamba_dim_bill_payment_create();
CALL sp_mamba_dim_bill_payment_insert();
CALL sp_mamba_dim_bill_payment_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_paid_service_bill_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_paid_service_bill_create;

~
CREATE PROCEDURE sp_mamba_dim_paid_service_bill_create()
BEGIN
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
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_paid_service_bill_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_paid_service_bill_insert;

~
CREATE PROCEDURE sp_mamba_dim_paid_service_bill_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_dim_paid_service_bill (paid_service_bill_id,
                                         bill_payment_id,
                                         patient_service_bill_id,
                                         paid_quantity,
                                         voided,
                                         created_date)
SELECT paid_service_bill_id,
       bill_payment_id,
       patient_service_bill_id,
       paid_quantity,
       voided,
       created_date
FROM moh_bill_paid_service_bill;

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_paid_service_bill_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_paid_service_bill_update;

~
CREATE PROCEDURE sp_mamba_dim_paid_service_bill_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_dim_paid_service_bill  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_dim_paid_service_bill;

~
CREATE PROCEDURE sp_mamba_dim_paid_service_bill()
BEGIN
-- $BEGIN
CALL sp_mamba_dim_paid_service_bill_create();
CALL sp_mamba_dim_paid_service_bill_insert();
CALL sp_mamba_dim_paid_service_bill_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_patient_service_bill_create  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_patient_service_bill_create;

~
CREATE PROCEDURE sp_mamba_fact_patient_service_bill_create()
BEGIN
-- $BEGIN

CREATE TABLE mamba_fact_patient_service_bill
(
    id                      INT            NOT NULL AUTO_INCREMENT,
    admission_date          DATE           NOT NULL,
    closing_date            DATE           NULL,
    beneficiary_name        TEXT           NULL,
    household_head_name     VARCHAR(255)   NULL,
    family_code             VARCHAR(255)   NULL,
    beneficiary_level       INT            NULL,
    card_number             VARCHAR(255)   NULL,
    company_name            VARCHAR(255)   NULL,
    age                     INT            NULL,
    birth_date              DATE           NULL,
    gender                  CHAR(1)        NULL,
    doctor_name             VARCHAR(255)   NULL,
    service_bill_quantity   DECIMAL(20, 2) NULL,
    service_bill_unit_price DECIMAL(20, 2) NOT NULL,

    insurance_id            INT            NOT NULL,
    hop_service_id          INT            NULL,
    global_bill_id          INT            NOT NULL,
    hop_service_name        VARCHAR(50)    NULL,

    PRIMARY KEY (id)
)
    CHARSET = UTF8MB4;

CREATE INDEX mamba_fact_patient_service_bill_insurance_id_index
    ON mamba_fact_patient_service_bill (insurance_id);

CREATE INDEX mamba_fact_patient_service_bill_global_bill_id_index
    ON mamba_fact_patient_service_bill (global_bill_id);

CREATE INDEX mamba_fact_patient_service_bill_hop_service_id_index
    ON mamba_fact_patient_service_bill (hop_service_id);

CREATE INDEX mamba_fact_patient_service_bill_closing_date_index
    ON mamba_fact_patient_service_bill (closing_date);

CREATE INDEX mamba_fact_patient_service_bill_admission_date_index
    ON mamba_fact_patient_service_bill (admission_date);

-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_patient_service_bill_insert  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_patient_service_bill_insert;

~
CREATE PROCEDURE sp_mamba_fact_patient_service_bill_insert()
BEGIN
-- $BEGIN

INSERT INTO mamba_fact_patient_service_bill(admission_date, closing_date, beneficiary_name, household_head_name,
                                            family_code, beneficiary_level, card_number, company_name, age, birth_date,
                                            gender, doctor_name, service_bill_quantity, service_bill_unit_price,
                                            insurance_id, hop_service_id, global_bill_id, hop_service_name)

SELECT -- DATE_FORMAT(gb.created_date, '%d/%m/%Y') AS admission_date,
       DATE(gb.created_date) AS admission_date,
       DATE(gb.closing_date) AS closing_date,
       bps.person_name_long  AS beneficiary_name,
       ben.owner_name        AS household_head_name,
       ben.owner_code        AS family_code,
       ben.level             AS beneficiary_level,
       isp.insurance_card_no AS card_number,
       ben.company           AS company_name,
       bps.age               AS age,
       DATE(bps.birthdate)   AS birth_date,
       bps.gender            AS gender,
       gb.closed_by_name     AS doctor_name,
       psb.quantity          AS service_bill_quantity,
       psb.unit_price        AS service_bill_unit_price,
       ins.insurance_id      AS insurance_id,
       psb.service_id        AS hop_service_id,
       gb.global_bill_id     AS global_bill_id,
       hp.name               AS hop_service_name

FROM mamba_dim_patient_service_bill psb
         INNER JOIN mamba_dim_consommation cons ON psb.consommation_id = cons.consommation_id
         INNER JOIN mamba_dim_global_bill gb on cons.global_bill_id = gb.global_bill_id
         INNER JOIN mamba_dim_beneficiary ben on cons.beneficiary_id = ben.beneficiary_id
         INNER JOIN mamba_dim_insurance_policy isp on ben.insurance_policy_id = isp.insurance_policy_id
         INNER JOIN mamba_dim_insurance ins ON ins.insurance_id = isp.insurance_id
         INNER JOIN mamba_dim_person bps ON bps.person_id = ben.patient_id
         INNER JOIN mamba_dim_hop_service hp on hp.service_id = psb.service_id

WHERE gb.closed = 1
  AND psb.voided = 0
-- GROUP BY cons.global_bill_id
-- HAVING MIN(cons.consommation_id)
ORDER BY gb.closing_date ASC
;
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_patient_service_bill_update  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_patient_service_bill_update;

~
CREATE PROCEDURE sp_mamba_fact_patient_service_bill_update()
BEGIN
-- $BEGIN
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_patient_service_bill_query  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_patient_service_bill_query;

~
CREATE PROCEDURE sp_mamba_fact_patient_service_bill_query(
    IN insurance_id INT,
    IN start_date DATE,
    IN end_date DATE)

BEGIN

    SELECT *
    FROM mamba_fact_patient_service_bill bill
    WHERE bill.insurance_id = insurance_id
      AND bill.admission_date BETWEEN start_date AND end_date;

END~



        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_fact_patient_service_bill  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_fact_patient_service_bill;

~
CREATE PROCEDURE sp_mamba_fact_patient_service_bill()
BEGIN
-- $BEGIN
CALL sp_mamba_fact_patient_service_bill_create();
CALL sp_mamba_fact_patient_service_bill_insert();
CALL sp_mamba_fact_patient_service_bill_update();
-- $END
END~


        
-- ---------------------------------------------------------------------------------------------
-- ----------------------  sp_mamba_data_processing_derived_billing  ----------------------------
-- ---------------------------------------------------------------------------------------------


DROP PROCEDURE IF EXISTS sp_mamba_data_processing_derived_billing;

~
CREATE PROCEDURE sp_mamba_data_processing_derived_billing()
BEGIN
-- $BEGIN

-- Dimensions
CALL sp_mamba_dim_admission;
CALL sp_mamba_dim_beneficiary;
CALL sp_mamba_dim_bill_payment;
CALL sp_mamba_dim_billable_service;
CALL sp_mamba_dim_consommation;
CALL sp_mamba_dim_department;
CALL sp_mamba_dim_facility_service_price;
CALL sp_mamba_dim_global_bill;
CALL sp_mamba_dim_hop_service;
CALL sp_mamba_dim_insurance_rate;
CALL sp_mamba_dim_insurance;
CALL sp_mamba_dim_insurance_bill;
CALL sp_mamba_dim_insurance_policy;
CALL sp_mamba_dim_paid_service_bill;
CALL sp_mamba_dim_patient_bill;
CALL sp_mamba_dim_patient_service_bill;
CALL sp_mamba_dim_service_category;
CALL sp_mamba_dim_third_party_bill;
CALL sp_mamba_dim_thirdparty;

-- Facts
CALL sp_mamba_fact_patient_service_bill;

-- $END
END~



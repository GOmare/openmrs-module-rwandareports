DELIMITER //

DROP PROCEDURE IF EXISTS sp_mamba_fact_patient_service_bill_query;

CREATE PROCEDURE sp_mamba_fact_patient_service_bill_query(
    IN insurance_id INT,
    IN start_date DATE,
    IN end_date DATE)

BEGIN

    SELECT *
    FROM mamba_fact_patient_service_bill bill
    WHERE bill.insurance_id = insurance_id
      AND bill.admission_date BETWEEN start_date AND end_date;

END //

DELIMITER ;
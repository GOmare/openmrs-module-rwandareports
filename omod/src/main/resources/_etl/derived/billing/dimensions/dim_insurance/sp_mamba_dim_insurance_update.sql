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
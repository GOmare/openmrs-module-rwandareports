-- $BEGIN

-- update the user who closed this bill - in this case it is a doctor
UPDATE mamba_dim_global_bill gb
    INNER JOIN mamba_dim_users u ON u.user_id = gb.closed_by_id
    INNER JOIN mamba_dim_person_name psn ON psn.person_id = u.person_id
SET gb.closed_by_name = CONCAT(psn.family_name, ' ', psn.given_name)
WHERE gb.closed = 1;

-- $END
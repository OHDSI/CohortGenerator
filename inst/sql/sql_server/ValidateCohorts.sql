SELECT
   cohort_definition_id,
   MAX(CASE WHEN validation_setting = 'overlapping eras' THEN stat_count END) AS overlapping_eras_count,
   MAX(CASE WHEN validation_setting = 'invalid dates' THEN stat_count END) AS invalid_date_count,
   MAX(CASE WHEN validation_setting = 'duplicate entries' THEN stat_count END) AS duplicate_count,
   MAX(CASE WHEN validation_setting = 'outside_observation_count' THEN stat_count END) AS outside_observation_count
FROM (
   SELECT
    c1.cohort_definition_id,
    -- 1. Number of overlapping eras per cohort_definition_id
    SUM(
        CASE
            WHEN c1.cohort_definition_id = c2.cohort_definition_id
                AND c1.subject_id = c2.subject_id
                -- must be distinct rows
                AND (c1.cohort_start_date != c2.cohort_start_date and c1.cohort_end_date != c2.cohort_end_date)
                -- it doesn't matter when the second cohort ends, it just can't start within another era
                AND c1.cohort_start_date BETWEEN c2.cohort_start_date AND c2.cohort_end_date
            THEN 1
            ELSE 0
        END
    ) AS stat_count,
    'overlapping eras' as validation_setting
    FROM @cohort_database_schema.@cohort_table c1
    LEFT JOIN  @cohort_database_schema.@cohort_table c2  ON  c1.cohort_definition_id = c2.cohort_definition_id  AND c1.subject_id = c2.subject_id

    WHERE c2.cohort_definition_id IS NOT NULL
    {@cohort_ids != ''} ? {AND c1.cohort_definition_id IN (@cohort_ids)}
    GROUP BY  c1.cohort_definition_id

    UNION

    SELECT
    c1.cohort_definition_id,
  -- 2. Number of entries that have a start date after the end date or NULL dates
    SUM(
        CASE
            WHEN
                c1.cohort_start_date > c1.cohort_end_date
                OR c1.cohort_start_date IS NULL
                OR c1.cohort_end_date IS NULL
            THEN 1
            ELSE 0
        END
    ) AS stat_count,
    'invalid dates' as validation_setting

    FROM @cohort_database_schema.@cohort_table c1

    WHERE 1 = 1
    {@cohort_ids != ''} ? {AND c1.cohort_definition_id IN (@cohort_ids)}
    GROUP BY  c1.cohort_definition_id

  -- 3. Number of duplicate entries per cohort_definition_id
  UNION

  SELECT
    c1.cohort_definition_id,
    COUNT(*) - COUNT(DISTINCT CONCAT(c1.subject_id, c1.cohort_start_date, c1.cohort_end_date)) AS stat_count,
    'duplicate entries' as validation_setting
    FROM @cohort_database_schema.@cohort_table c1

  WHERE 1 = 1
  {@cohort_ids != ''} ? {AND c1.cohort_definition_id IN (@cohort_ids)}
  GROUP BY  c1.cohort_definition_id

  -- 4. Number of entries lying outside observation periods
    UNION

  SELECT
     c1.cohort_definition_id,
    SUM(
        CASE WHEN op.person_id IS NULL
            OR c1.cohort_start_date < op.observation_period_start_date
            OR c1.cohort_end_date > op.observation_period_end_date
            THEN 1
            ELSE 0
        END
    ) AS stat_count,
    'outside_observation_count' as validation_setting

    FROM @cohort_database_schema.@cohort_table c1
    -- Join to observation period for outside observation check
    LEFT JOIN  @cdm_database_schema.observation_period op ON  c1.subject_id = op.person_id AND (
        c1.cohort_start_date BETWEEN op.observation_period_start_date AND op.observation_period_end_date
        OR  c1.cohort_end_date BETWEEN op.observation_period_start_date AND op.observation_period_end_date
    )
    WHERE 1 = 1
    {@cohort_ids != ''} ? {AND c1.cohort_definition_id IN (@cohort_ids)}
    GROUP BY  c1.cohort_definition_id
) stat_sq

GROUP BY cohort_definition_id
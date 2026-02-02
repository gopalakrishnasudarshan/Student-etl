CREATE OR REPLACE VIEW v_grade_by_failures AS
SELECT
    subject,
    CASE
        WHEN failures >= 3 THEN '3+'
        ELSE failures::text
        END AS failures_bucket,
    COUNT(*) AS student_count,
    ROUND(AVG(g3)::numeric, 2) AS avg_g3,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY g3) AS median_g3,
    MIN(g3) AS min_g3,
    MAX(g3) AS max_g3
FROM uci_students
GROUP BY subject, failures_bucket
ORDER BY subject, failures_bucket;

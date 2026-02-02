CREATE OR REPLACE VIEW v_absences_buckets AS
SELECT
    subject,
    CASE
        WHEN absences = 0 THEN '0'
        WHEN absences BETWEEN 1 AND 5 THEN '1-5'
        WHEN absences BETWEEN 6 AND 10 THEN '6-10'
        WHEN absences BETWEEN 11 AND 20 THEN '11-20'
        ELSE '21+'
        END AS absences_bucket,
    COUNT(*) AS student_count,
    ROUND(AVG(g3)::numeric, 2) AS avg_g3,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY g3) AS median_g3
FROM uci_students
GROUP BY subject, absences_bucket
ORDER BY subject, absences_bucket;

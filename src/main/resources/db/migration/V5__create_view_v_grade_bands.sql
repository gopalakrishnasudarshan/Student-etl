CREATE OR REPLACE VIEW v_grade_bands AS
SELECT
    subject,
    CASE
        WHEN g3 BETWEEN 0 AND 5  THEN '0-5'
        WHEN g3 BETWEEN 6 AND 9  THEN '6-9'
        WHEN g3 BETWEEN 10 AND 12 THEN '10-12'
        WHEN g3 BETWEEN 13 AND 15 THEN '13-15'
        WHEN g3 BETWEEN 16 AND 20 THEN '16-20'
        ELSE 'unknown'
        END AS grade_band,
    COUNT(*) AS student_count,
    ROUND(AVG(g3)::numeric, 2) AS avg_g3
FROM uci_students
GROUP BY subject, grade_band
ORDER BY subject, grade_band;

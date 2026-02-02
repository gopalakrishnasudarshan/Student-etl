CREATE OR REPLACE VIEW v_grade_by_sex AS
SELECT
    subject,
    sex,
    COUNT(*) AS student_count,
    ROUND(AVG(g3)::numeric, 2) AS avg_g3,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY g3) AS median_g3,
    MIN(g3) AS min_g3,
    MAX(g3) AS max_g3
FROM uci_students
GROUP BY subject, sex
ORDER BY subject, sex;

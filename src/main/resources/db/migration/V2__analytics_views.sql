CREATE OR REPLACE VIEW v_subject_Summary AS
SELECT
    subject,
    COUNT(*) AS n,
    AVG(g3) AS avg_grade,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY g3) AS median_grade,
    MIN(g3) AS min_grade,
    MAX(g3) AS max_grade
FROM uci_students
GROUP BY subject;
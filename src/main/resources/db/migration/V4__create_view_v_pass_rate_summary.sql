CREATE OR REPLACE VIEW v_pass_rate_summary AS
SELECT
    subject,
    COUNT(*) AS student_count,
    SUM(CASE WHEN g3 >= 10 THEN 1 ELSE 0 END) AS passed_count,
    ROUND(
            (SUM(CASE WHEN g3 >= 10 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0)) * 100
        , 2) AS pass_rate_pct,
    ROUND(AVG(g3)::numeric, 2) AS avg_g3,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY g3) AS median_g3,
    MIN(g3) AS min_g3,
    MAX(g3) AS max_g3
FROM uci_students
GROUP BY subject;

CREATE OR REPLACE VIEW v_g1_g2_g3_progression AS
SELECT
    subject,
    ROUND(AVG(g2 - g1)::numeric, 2) AS avg_delta_g2_g1,
    ROUND(AVG(g3 - g2)::numeric, 2) AS avg_delta_g3_g2,
    ROUND(AVG(g3 - g1)::numeric, 2) AS avg_delta_g3_g1,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (g3 - g1)) AS median_delta_g3_g1
FROM uci_students
GROUP BY subject;

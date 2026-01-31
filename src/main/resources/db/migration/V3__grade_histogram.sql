CREATE OR REPLACE VIEW v_grade_histogram AS
SELECT
    subject,
    width_bucket(g3, 0, 20, 10) AS bucket,
    COUNT(*) AS count
FROM uci_students
GROUP BY subject, bucket
ORDER BY subject, bucket;

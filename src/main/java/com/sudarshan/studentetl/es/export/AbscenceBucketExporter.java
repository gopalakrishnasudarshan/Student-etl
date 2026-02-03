package com.sudarshan.studentetl.es.export;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class AbscenceBucketExporter {

    private static final String VIEW_NAME = "v_absences_buckets";

    private final JdbcTemplate jdbcTemplate;
    private final ElasticsearchClient es;

    public AbscenceBucketExporter(JdbcTemplate jdbcTemplate, ElasticsearchClient es) {
        this.jdbcTemplate = jdbcTemplate;
        this.es = es;
    }

    public long export(String indexName, String exportRunId, long etlRunId) throws IOException {

        List<Map<String, Object>> rows = jdbcTemplate.queryForList("""
            select
              subject,
              absences_bucket,
              student_count,
              avg_g3,
              median_g3
            from v_absences_buckets
            order by subject, absences_bucket
            """);

        if (rows.isEmpty()) return 0;

        Instant exportedAt = Instant.now();
        List<BulkOperation> ops = new ArrayList<>(rows.size());

        for (Map<String, Object> r : rows) {
            String subject = (String) r.get("subject");
            String bucket = (String) r.get("absences_bucket");

            String id = subject + "|" + bucket;

            Map<String, Object> doc = Map.of(
                    "view_name", VIEW_NAME,
                    "export_run_id", exportRunId,
                    "exported_at", exportedAt.toString(),
                    "etl_run_id", etlRunId,

                    "subject", subject,
                    "absences_bucket", bucket,
                    "student_count", toLong(r.get("student_count")),
                    "avg_g3", toDouble(r.get("avg_g3")),
                    "median_g3", toDouble(r.get("median_g3"))
            );

            ops.add(BulkOperation.of(op -> op
                    .index(i -> i
                            .index(indexName)
                            .id(id)
                            .document(doc)
                    )
            ));
        }

        BulkRequest bulkRequest = BulkRequest.of(b -> b.operations(ops));
        var resp = es.bulk(bulkRequest);

        if (resp.errors()) {
            throw new IllegalStateException("Bulk indexing had errors: " + resp.items());
        }

        return rows.size();
    }

    private static long toLong(Object v) {
        if (v == null) return 0L;
        if (v instanceof Number n) return n.longValue();
        return Long.parseLong(v.toString());
    }

    private static double toDouble(Object v) {
        if (v == null) return 0.0;
        if (v instanceof Number n) return n.doubleValue();
        return Double.parseDouble(v.toString());
    }
}

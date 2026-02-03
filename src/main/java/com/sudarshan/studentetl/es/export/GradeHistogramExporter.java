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
public class GradeHistogramExporter {

    private static final String VIEW_NAME = "v_grade_histogram";

    private final JdbcTemplate jdbcTemplate;
    private final ElasticsearchClient es;

    public GradeHistogramExporter(JdbcTemplate jdbcTemplate, ElasticsearchClient es) {
        this.jdbcTemplate = jdbcTemplate;
        this.es = es;
    }

    public long export(String indexName, String exportRunId, long etlRunId) throws IOException {

        List<Map<String, Object>> rows = jdbcTemplate.queryForList("""
            select
              subject,
              bucket,
              count
            from v_grade_histogram
            order by subject, bucket
            """);

        if (rows.isEmpty()) return 0;

        Instant exportedAt = Instant.now();
        List<BulkOperation> ops = new ArrayList<>(rows.size());

        for (Map<String, Object> r : rows) {
            String subject = (String) r.get("subject");
            int bucket = toInt(r.get("bucket"));

            String id = subject + "|" + bucket;

            Map<String, Object> doc = Map.of(
                    "view_name", VIEW_NAME,
                    "export_run_id", exportRunId,
                    "exported_at", exportedAt.toString(),
                    "etl_run_id", etlRunId,

                    "subject", subject,
                    "bucket", bucket,
                    "count", toLong(r.get("count"))
            );

            ops.add(BulkOperation.of(op -> op
                    .index(i -> i.index(indexName).id(id).document(doc))
            ));
        }

        var resp = es.bulk(BulkRequest.of(b -> b.operations(ops)));
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

    private static int toInt(Object v) {
        if (v == null) return 0;
        if (v instanceof Number n) return n.intValue();
        return Integer.parseInt(v.toString());
    }
}

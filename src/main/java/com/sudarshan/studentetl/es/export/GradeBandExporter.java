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
public class GradeBandExporter {

    private static  final String VIEW_NAME = "v_grade_bands";
    private final JdbcTemplate jdbcTemplate;
    private final ElasticsearchClient es;

    public GradeBandExporter(JdbcTemplate jdbcTemplate, ElasticsearchClient es) {
        this.jdbcTemplate = jdbcTemplate;
        this.es = es;
    }

    public long export(String indexName, String exportRunId, long etlRunId) throws IOException {

        List<Map<String , Object >> rows = jdbcTemplate.queryForList("""
            select
              subject,
              grade_band,
              student_count,
              avg_g3
            from v_grade_bands
            order by subject, grade_band
            """);

        if(rows.isEmpty()) return 0;

        Instant exportedAt = Instant.now();

        List<BulkOperation> ops = new ArrayList<>(rows.size());

        for (Map<String, Object> r : rows) {
            String subject = (String) r.get("subject");
            String gradeBand = (String) r.get("grade_band");

            String id = subject + "|" + gradeBand;


            Map<String, Object> doc = Map.of(
                    "view_name", VIEW_NAME,
                    "export_run_id", exportRunId,
                    "exported_at", exportedAt.toString(),
                    "etl_run_id", etlRunId,

                    "subject", subject,
                    "grade_band", gradeBand,
                    "student_count", toLong(r.get("student_count")),
                    "avg_g3", toDouble(r.get("avg_g3"))
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


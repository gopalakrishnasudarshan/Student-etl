package com.sudarshan.studentetl.es.export;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class GradeByStudytimeExporter {

    private static final String VIEW_NAME = "v_grade_by_studytime";

    private final JdbcTemplate jdbcTemplate;
    private final ElasticsearchClient es;

    public GradeByStudytimeExporter(JdbcTemplate jdbcTemplate, ElasticsearchClient es) {
        this.jdbcTemplate = jdbcTemplate;
        this.es = es;
    }

    public long export(String indexName, String exportRunId, long etlRunId) throws IOException {

        List<Map<String, Object>> rows = jdbcTemplate.queryForList("""
                select
                  subject,
                  studytime,
                  student_count,
                  avg_g3,
                  median_g3,
                  min_g3,
                  max_g3
                from v_grade_by_studytime
                order by subject, studytime
                """);

        if (rows.isEmpty()) return 0;

        Instant exportedAt = Instant.now();
        List<BulkOperation> ops = new ArrayList<>(rows.size());

        for (Map<String, Object> r : rows) {
            String subject = (String) r.get("subject");
            int studytime = toInt(r.get("studytime"));

            String id = subject + "|" + studytime;

            Map<String, Object> doc = new LinkedHashMap<>();


            doc.put("view_name", VIEW_NAME);
            doc.put("export_run_id", exportRunId);
            doc.put("exported_at", exportedAt.toString());
            doc.put("etl_run_id", etlRunId);


            doc.put("subject", subject);
            doc.put("studytime", studytime);


            doc.put("student_count", toLong(r.get("student_count")));
            doc.put("avg_g3", toDouble(r.get("avg_g3")));
            doc.put("median_g3", toDouble(r.get("median_g3")));
            doc.put("min_g3", toInt(r.get("min_g3")));
            doc.put("max_g3", toInt(r.get("max_g3")));

            ops.add(BulkOperation.of(op -> op.index(i -> i.index(indexName).id(id).document(doc))));
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

    private static double toDouble(Object v) {
        if (v == null) return 0.0;
        if (v instanceof BigDecimal bd) return bd.doubleValue();
        if (v instanceof Number n) return n.doubleValue();
        return Double.parseDouble(v.toString());
    }


}

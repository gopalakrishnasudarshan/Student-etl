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
public class G1G2G3ProgressionExporter {

    private static final String VIEW_NAME = "v_g1_g2_g3_progression";

    private final JdbcTemplate jdbcTemplate;
    private final ElasticsearchClient es;

    public G1G2G3ProgressionExporter(JdbcTemplate jdbcTemplate, ElasticsearchClient es) {
        this.jdbcTemplate = jdbcTemplate;
        this.es = es;
    }

    public long export(String indexName, String exportRunId, long etlRunId) throws IOException {

        List<Map<String, Object>> rows = jdbcTemplate.queryForList("""
                select
                  subject,
                  avg_delta_g2_g1,
                  avg_delta_g3_g2,
                  avg_delta_g3_g1,
                  median_delta_g3_g1
                from v_g1_g2_g3_progression
                order by subject
                """);

        if (rows.isEmpty()) return 0;

        Instant exportedAt = Instant.now();
        List<BulkOperation> ops = new ArrayList<>(rows.size());

        for (Map<String, Object> r : rows) {
            String subject = (String) r.get("subject");
            String id = subject; // deterministic _id (one row per subject)

            Map<String, Object> doc = new LinkedHashMap<>();

            doc.put("view_name", VIEW_NAME);
            doc.put("export_run_id", exportRunId);
            doc.put("exported_at", exportedAt.toString());
            doc.put("etl_run_id", etlRunId);

            doc.put("subject", subject);

            doc.put("avg_delta_g2_g1", toDouble(r.get("avg_delta_g2_g1")));
            doc.put("avg_delta_g3_g2", toDouble(r.get("avg_delta_g3_g2")));
            doc.put("avg_delta_g3_g1", toDouble(r.get("avg_delta_g3_g1")));
            doc.put("median_delta_g3_g1", toDouble(r.get("median_delta_g3_g1")));

            ops.add(BulkOperation.of(op -> op.index(i -> i.index(indexName).id(id).document(doc))));
        }

        var resp = es.bulk(BulkRequest.of(b -> b.operations(ops)));
        if (resp.errors()) {
            throw new IllegalStateException("Bulk indexing had errors: " + resp.items());
        }

        return rows.size();
    }

    private static double toDouble(Object v) {
        if (v == null) return 0.0;
        if (v instanceof BigDecimal bd) return bd.doubleValue();
        if (v instanceof Number n) return n.doubleValue();
        return Double.parseDouble(v.toString());
    }


}

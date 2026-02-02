package com.sudarshan.studentetl.es.export;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import com.sudarshan.studentetl.es.indices.PassRateSummaryIndexManager;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class PassRateSummaryExporter {

    private static final String VIEW_NAME = "v_pass_rate_summary";
    private final JdbcTemplate jdbcTemplate;
    private final ElasticsearchClient es;
    private final PassRateSummaryIndexManager indexManager;

    public PassRateSummaryExporter(JdbcTemplate jdbcTemplate, ElasticsearchClient es, PassRateSummaryIndexManager indexManager) {
        this.jdbcTemplate = jdbcTemplate;
        this.es = es;
        this.indexManager = indexManager;
    }

    public long export(String indexPrefix, String exportRunId, long etlRunId) throws IOException {

        String indexName = indexManager.indexName(indexPrefix);
        indexManager.ensureIndexExists(indexName);

        List<Map<String, Object>> rows = jdbcTemplate.queryForList("""
            
              select
              subject,
              student_count,
              passed_count,
              (student_count - passed_count) as failed_count,
              pass_rate_pct
                            from
                v_pass_rate_summary
            order by subject
             
            """);


        if(rows.isEmpty()) return 0;

        Instant exportedAt = Instant.now();

        List<BulkOperation> ops = new ArrayList<>(rows.size());

        for(Map<String, Object> r: rows){
            String subject = (String) r.get("subject");

            Map<String, Object> doc = Map.of(
                    "view_name", VIEW_NAME,
                    "export_run_id", exportRunId,
                    "exported_at", Instant.now().toString(),

                    "etl_run_id", etlRunId,

                    "subject", subject,
                    "n", toLong(r.get("n")),
                    "n_pass", toLong(r.get("n_pass")),
                    "n_fail", toLong(r.get("n_fail")),
                    "pass_rate_pct", toDouble(r.get("pass_rate_pct"))
            );

            ops.add(BulkOperation.of(op -> op
                    .index(i -> i
                            .index(indexName)
                            .id(subject) // deterministic _id
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

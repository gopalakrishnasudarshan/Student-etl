package com.sudarshan.studentetl.es.export;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

@Service
public class SubjectSummaryExporter {

    private final JdbcTemplate jdbcTemplate;
    private final ElasticsearchClient es;

    public SubjectSummaryExporter(JdbcTemplate jdbcTemplate, ElasticsearchClient es) {
        this.jdbcTemplate = jdbcTemplate;
        this.es = es;
    }

    public int export(String indexName, String exportRunId, Long etlRunId) throws IOException {
        String sql = "SELECT subject, n, avg_grade, median_grade, min_grade, max_grade FROM v_subject_summary";

        List<Map<String, Object>> rows = jdbcTemplate.query(sql, (rs,rowNum) -> {
            Map<String, Object> doc = new LinkedHashMap<>();
            doc.put("view_name", "v_subject_summary");
            doc.put("export_run_id", exportRunId);
            doc.put("exported_at", Instant.now().toString());
            if (etlRunId != null) doc.put("etl_run_id", etlRunId);

            doc.put("subject", rs.getString("subject"));
            doc.put("n", rs.getLong("n"));
            doc.put("avg_grade", rs.getBigDecimal("avg_grade") == null ? null : rs.getBigDecimal("avg_grade").doubleValue());
            doc.put("median_grade", rs.getDouble("median_grade"));
            doc.put("min_grade", rs.getInt("min_grade"));
            doc.put("max_grade", rs.getInt("max_grade"));
            return doc;
        });

        List<BulkOperation> ops = new ArrayList<>();
        for(Map<String, Object> doc : rows)
        {
            String id = Objects.toString(doc.get("subject"));

            ops.add(BulkOperation.of(op -> op
                    .index(idx -> idx
                            .index(indexName)
                            .id(id)
                            .document(doc)
                    )
            ));

        }

        BulkRequest bulk = BulkRequest.of(b -> b.operations(ops));
        var resp = es.bulk(bulk);

        if (resp.errors()) {
            throw new IllegalStateException("Bulk export had errors; first error: " +
                    resp.items().stream()
                            .filter(i -> i.error() != null)
                            .findFirst()
                            .map(i -> i.error().reason())
                            .orElse("unknown"));
        }
        return rows.size();

    }
}

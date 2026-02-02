package com.sudarshan.studentetl.es.indices;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class SubjectSummaryIndexManager {

    private final ElasticsearchClient es;

    public SubjectSummaryIndexManager(ElasticsearchClient client) {
        this.es = client;

    }


    public String indexName(String prefix){
        return prefix+"v_subject_summary";
    }

    public void ensureIndexExists(String indexName) throws IOException {
        boolean exists = es.indices().exists(ExistsRequest.of(b -> b.index(indexName))).value();

        if(exists) return;

        es.indices().create(CreateIndexRequest.of(b -> b
                .index(indexName)
                .mappings(m -> m
                        .properties("view_name", p -> p.keyword(k -> k))
                        .properties("export_run_id", p -> p.keyword(k -> k))
                        .properties("exported_at", p -> p.date(d -> d))
                        .properties("etl_run_id", p -> p.long_(l -> l))

                        .properties("subject", p -> p.keyword(k -> k))
                        .properties("n", p -> p.long_(l -> l))
                        .properties("avg_grade", p -> p.double_(d -> d))
                        .properties("median_grade", p -> p.double_(d -> d))
                        .properties("min_grade", p -> p.integer(i -> i))
                        .properties("max_grade", p -> p.integer(i -> i))
                )
        ));


    }

}

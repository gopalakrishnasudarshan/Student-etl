package com.sudarshan.studentetl.es;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import co.elastic.clients.elasticsearch.ElasticsearchClient;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Component
@ConditionalOnProperty(name = "elasticsearch.ping.enabled", havingValue = "true")
public class ElasticsearchPingRunner implements CommandLineRunner {


    private final ElasticsearchClient client;

    private static final Logger log = LogManager.getLogger(ElasticsearchPingRunner.class);

    public ElasticsearchPingRunner(ElasticsearchClient client) {

        this.client = client;
    }
    @Override
    public void run(String... args) throws Exception {

        var info = client.info();
        log.info("ElasticsearchPingRunner info: {} {}", info.clusterName(), info.version().number());


    }
}

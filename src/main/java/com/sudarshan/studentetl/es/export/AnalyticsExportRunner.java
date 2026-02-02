package com.sudarshan.studentetl.es.export;


import com.sudarshan.studentetl.es.indices.PassRateSummaryIndexManager;
import com.sudarshan.studentetl.es.indices.SubjectSummaryIndexManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;

@Component
@ConditionalOnProperty(name = "export.enabled", havingValue = "true")
public class AnalyticsExportRunner implements CommandLineRunner {

    private static final Logger log = LogManager.getLogger(AnalyticsExportRunner.class);

    private final SubjectSummaryIndexManager indexManager;
    private final SubjectSummaryExporter exporter;
    private final PassRateSummaryExporter passRateSummaryExporter;
    private final PassRateSummaryIndexManager passRateSummaryIndexManager;


    @Value("${export.indexPrefix:student_analytics_}")
    private String indexPrefix;

    public AnalyticsExportRunner(SubjectSummaryIndexManager indexManager, SubjectSummaryExporter exporter, PassRateSummaryExporter passRateSummaryExporter, PassRateSummaryIndexManager passRateSummaryIndexManager) {
        this.indexManager = indexManager;
        this.exporter = exporter;
        this.passRateSummaryExporter = passRateSummaryExporter;
        this.passRateSummaryIndexManager = passRateSummaryIndexManager;
    }

    @Override
    public void run(String... args) throws Exception {

        String exportRunId = UUID.randomUUID().toString();


        String subjectIndex = indexManager.indexName(indexPrefix);
        indexManager.ensureIndexExists(subjectIndex);

        int subjectExported = exporter.export(subjectIndex, exportRunId, null);

        log.info("EXPORT DONE view=v_subject_summary index={} exported={} export_run_id={}",
                subjectIndex, subjectExported, exportRunId);


        String passRateIndex = passRateSummaryIndexManager.indexName(indexPrefix);
        passRateSummaryIndexManager.ensureIndexExists(passRateIndex);

        long passRateExported = passRateSummaryExporter.export(
                indexPrefix, exportRunId, 0L
        );

        log.info("EXPORT DONE view=v_pass_rate_summary index={} exported={} export_run_id={}",
                passRateIndex, passRateExported, exportRunId);
    }
}

package com.sudarshan.studentetl.es.export;

import com.sudarshan.studentetl.es.indices.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
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

    private final GradeBandExporter gradeBandExporter;
    private final GradeBandIndexManager gradeBandIndexManager;

    private final AbscenceBucketExporter abscenceBucketExporter;
    private  final AbsenceBucketIndexManager absenceBucketIndexManager;

    private final GradeHistogramExporter gradeHistogramExporter;
    private final GradeHistogramIndexManager gradeHistogramIndexManager;

    private final JdbcTemplate jdbcTemplate;

    @Value("${export.indexPrefix:student_analytics_}")
    private String indexPrefix;

    public AnalyticsExportRunner(
            SubjectSummaryIndexManager indexManager,
            SubjectSummaryExporter exporter,
            PassRateSummaryExporter passRateSummaryExporter,
            PassRateSummaryIndexManager passRateSummaryIndexManager,
            GradeBandExporter gradeBandExporter,
            GradeBandIndexManager gradeBandIndexManager,
            AbscenceBucketExporter abscenceBucketExporter,
            AbsenceBucketIndexManager absenceBucketIndexManager, GradeHistogramExporter gradeHistogramExporter, GradeHistogramIndexManager gradeHistogramIndexManager,
            JdbcTemplate jdbcTemplate
    ) {
        this.indexManager = indexManager;
        this.exporter = exporter;
        this.passRateSummaryExporter = passRateSummaryExporter;
        this.passRateSummaryIndexManager = passRateSummaryIndexManager;
        this.gradeBandExporter = gradeBandExporter;
        this.gradeBandIndexManager = gradeBandIndexManager;
        this.abscenceBucketExporter = abscenceBucketExporter;
        this.absenceBucketIndexManager = absenceBucketIndexManager;
        this.gradeHistogramExporter = gradeHistogramExporter;
        this.gradeHistogramIndexManager = gradeHistogramIndexManager;

        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void run(String... args) throws Exception {

        String exportRunId = UUID.randomUUID().toString();

        Long etlRunId = jdbcTemplate.queryForObject(
                "select max(id) from etl_runs where status = 'SUCCESS'",
                Long.class
        );

        if (etlRunId == null) {
            log.warn("No successful ETL run found, skipping analytics export");
            return;
        }

        String subjectIndex = indexManager.indexName(indexPrefix);
        indexManager.ensureIndexExists(subjectIndex);

        int subjectExported = exporter.export(subjectIndex, exportRunId, etlRunId);

        log.info("EXPORT DONE view=v_subject_summary index={} exported={} export_run_id={}",
                subjectIndex, subjectExported, exportRunId);

        String passRateIndex = passRateSummaryIndexManager.indexName(indexPrefix);
        passRateSummaryIndexManager.ensureIndexExists(passRateIndex);

        long passRateExported = passRateSummaryExporter.export(
                passRateIndex, exportRunId, etlRunId
        );

        log.info("EXPORT DONE view=v_pass_rate_summary index={} exported={} export_run_id={}",
                passRateIndex, passRateExported, exportRunId);

        String gradeBandsIndex = gradeBandIndexManager.indexName(indexPrefix);
        gradeBandIndexManager.ensureIndexExists(gradeBandsIndex);

        long gradeBandsExported = gradeBandExporter.export(
                gradeBandsIndex,
                exportRunId,
                etlRunId
        );

        log.info("EXPORT DONE: v_grade_bands -> index={} docs={}", gradeBandsIndex, gradeBandsExported);

        String absencesIndex = absenceBucketIndexManager.indexName(indexPrefix);
        absenceBucketIndexManager.ensureIndexExists(absencesIndex);

        long absencesExported =abscenceBucketExporter.export(
                absencesIndex,
                exportRunId,
                etlRunId
        );

        log.info("EXPORT DONE: v_absences_buckets -> index={} docs={}", absencesIndex, absencesExported);
        String gradeHistogramIndex = gradeHistogramIndexManager.indexName(indexPrefix);
        gradeHistogramIndexManager.ensureIndexExists(gradeHistogramIndex);

        long gradeHistogramExported = gradeHistogramExporter.export(
                gradeHistogramIndex,
                exportRunId,
                etlRunId
        );

        log.info("EXPORT DONE: v_grade_histogram -> index={} docs={}", gradeHistogramIndex, gradeHistogramExported);
    }









}

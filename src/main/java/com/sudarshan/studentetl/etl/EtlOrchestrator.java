package com.sudarshan.studentetl.etl;


import com.sudarshan.studentetl.entity.ETLRunEntity;
import com.sudarshan.studentetl.etl.csv.StudentCsvReader;
import com.sudarshan.studentetl.etl.csv.StudentCsvRow;
import com.sudarshan.studentetl.etl.csv.StudentCsvValidator;
import com.sudarshan.studentetl.etl.csv.StudentRowValidationResult;
import com.sudarshan.studentetl.etl.dto.UCIStudentTransformedDto;
import com.sudarshan.studentetl.etl.transform.UciStudentTransformer;
import com.sudarshan.studentetl.service.EtlRunPersistenceService;
import com.sudarshan.studentetl.service.StudentUpsertService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.IOException;
import java.time.Instant;
import java.util.List;


@Component
@ConditionalOnProperty(name = "etl.enabled", havingValue = "true")
public class EtlOrchestrator implements CommandLineRunner {

    private final StudentCsvReader csvReader;
    private final StudentCsvValidator csvValidator;
    private final EtlRunPersistenceService etlRunPersistenceService;
    private final UciStudentTransformer transformer;
    private final StudentUpsertService studentUpsertService;
    private static final Logger log = LogManager.getLogger(EtlOrchestrator.class);




    public EtlOrchestrator(StudentCsvReader csvReader, StudentCsvValidator validator, EtlRunPersistenceService etlRunPersistenceService
    ,UciStudentTransformer transformer, StudentUpsertService studentUpsertService) {

        this.csvReader = csvReader;
        this.csvValidator = validator;
        this.etlRunPersistenceService = etlRunPersistenceService;
        this.transformer = transformer;
        this.studentUpsertService = studentUpsertService;
    }


    @Override
    public void run(String... args) throws IOException {
        Instant startedAt = Instant.now();
        log.info("ETL started at {}", startedAt);

        List<StudentCsvRow> rows = csvReader.readAll();
        log.info("CSV rows read {}", rows.size());

        List<StudentRowValidationResult> results = csvValidator.validateAll(rows);

        int total = results.size();
        int valid = (int) results.stream().filter(StudentRowValidationResult::isValid).count();
        int invalid = total - valid;

        log.info("Validation completed: total={}, valid={}, invalid={}", total, valid, invalid);

        ETLRunEntity run = etlRunPersistenceService.createRunAndPersisErrors(startedAt, results);

        Long runId = run.getId();

        int insertedRows = 0;
        int updatedRows = 0;

        for (StudentRowValidationResult r : results) {
            if (!r.isValid()) continue;

            StudentCsvRow row = r.getRow();
            UCIStudentTransformedDto dto = transformer.transform(row, row.getSubject());

            var outcome = studentUpsertService.upsert(dto, runId);

            if (outcome.inserted()) insertedRows++;
            else updatedRows++;
        }
        if (insertedRows + updatedRows != valid) {
            log.warn(
                    "ETL invariant violated: valid_rows={} but inserted_rows+updated_rows={} (inserted={}, updated={})",
                    valid,
                    insertedRows + updatedRows,
                    insertedRows,
                    updatedRows
            );
        }

        ETLRunEntity finished = etlRunPersistenceService.finishRun(runId, "SUCCESS",
                total,
                valid,
                invalid,
                insertedRows,
                updatedRows
        );

        log.info(
                "ETL run completed | runId={} | status={} | total={} | valid={} | invalid={} | inserted={} | updated={}",
                finished.getId(),
                finished.getStatus(),
                finished.getTotalRows(),
                finished.getValidRows(),
                finished.getInvalidRows(),
                finished.getInsertedRows(),
                finished.getUpdatedRows()
        );

    }

}

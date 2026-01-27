package com.sudarshan.studentetl.etl;


import com.sudarshan.studentetl.entity.ETLRunEntity;
import com.sudarshan.studentetl.etl.csv.StudentCsvReader;
import com.sudarshan.studentetl.etl.csv.StudentCsvRow;
import com.sudarshan.studentetl.etl.csv.StudentCsvValidator;
import com.sudarshan.studentetl.etl.csv.StudentRowValidationResult;
import com.sudarshan.studentetl.service.EtlRunPersistenceService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

@Component
public class EtlOrchestrator implements CommandLineRunner {

    private final StudentCsvReader csvReader;
    private final StudentCsvValidator csvValidator;
    private final EtlRunPersistenceService etlRunPersistenceService;

    Instant startedAt = Instant.now();

    public EtlOrchestrator(StudentCsvReader csvReader, StudentCsvValidator validator, EtlRunPersistenceService etlRunPersistenceService) {

        this.csvReader = csvReader;
        this.csvValidator = validator;
        this.etlRunPersistenceService = etlRunPersistenceService;
    }

    @Override
    public void run(String... args) throws Exception {
        List<StudentCsvRow> rows = csvReader.readAll();
        System.out.println("CSV rows read: " + rows.size());
        System.out.println("First student_id"+ rows.get(0).getStudentId());

        List<StudentRowValidationResult> results = csvValidator.validateAll(rows);


        ETLRunEntity run = etlRunPersistenceService.persistRun(startedAt,results);
        System.out.println("ETL run saved with id= "+ run.getId()+ " status= "+run.getStatus()
        +" total= "+ run.getTotalRows() +" valid= "+run.getValidRows()+ " invalid= "+run.getInvalidRows());

//        long validCount = results.stream().filter(StudentRowValidationResult::isValid).count();
//        long invalidCount = results.size() - validCount;
//
//        System.out.println("Valid rows:" +validCount);
//        System.out.println("Invalid rows:" +invalidCount);
//
//        results.stream().filter(r -> !r.isValid()).
//                limit(10)
//                .forEach(r -> System.out.println("Row" + r.getRow().getRowNumber() + "(Student_id=" + r.getRow()
//                        .getStudentId() +"):" + String.join(";", r.getErrors())));
    }
}

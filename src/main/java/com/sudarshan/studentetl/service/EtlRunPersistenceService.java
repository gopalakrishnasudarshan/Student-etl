package com.sudarshan.studentetl.service;

import com.sudarshan.studentetl.entity.ETLRunEntity;
import com.sudarshan.studentetl.entity.EtlRunErrorEntirty;
import com.sudarshan.studentetl.etl.csv.StudentCsvRow;
import com.sudarshan.studentetl.etl.csv.StudentRowValidationResult;
import com.sudarshan.studentetl.repository.EtlRunErrorRepository;
import com.sudarshan.studentetl.repository.EtlRunRepository;
import jakarta.transaction.Transactional;
import org.springframework.stereotype.Service;
import java.time.Instant;
import java.util.List;


@Service
public class EtlRunPersistenceService {

    private final EtlRunRepository runRepository;
    private final EtlRunErrorRepository errorRepository;

    public EtlRunPersistenceService(EtlRunRepository runRepository, EtlRunErrorRepository errorRepository) {
        this.runRepository = runRepository;
        this.errorRepository = errorRepository;
    }

    @Transactional
    public ETLRunEntity createRunAndPersisErrors(Instant startedAt, List<StudentRowValidationResult> results)
    {
        ETLRunEntity run = new ETLRunEntity(startedAt, "STARTED");
        run = runRepository.save(run);

        for(StudentRowValidationResult res : results)
        {
            if(res.isValid()) continue;

            long rowNumber = res.getRow().getRowNumber();
            String studentIdLike = buildRowIdentifier(res.getRow());

            for(String msg : res.getErrors())
            {
                errorRepository.save(new EtlRunErrorEntirty(run, rowNumber,studentIdLike, msg));
            }
        }
        return run;
    }

    @Transactional
    public ETLRunEntity finishRun(long runId,
                                  String status,
                                  int totalRows,
                                  int validRows,
                                  int invalidRows,
                                  int insertedRows,
                                  int updatedRows){
        ETLRunEntity run = runRepository.findById(runId)
                .orElseThrow(() -> new IllegalStateException("ETL Run not found id = "+ runId));
        run.markFinished(Instant.now(), status, totalRows,validRows,invalidRows, insertedRows,updatedRows);
        return runRepository.save(run);


    }



    private String buildRowIdentifier(StudentCsvRow row) {
        return String.join("|",
                nullSafe(row.getSubject()),
                nullSafe(row.getSchool()),
                nullSafe(row.getSex()),
                nullSafe(row.getAge()),
                nullSafe(row.getG3())
        );

    }
    private String nullSafe(String v) {
        return v == null ? "" : v;
    }

}

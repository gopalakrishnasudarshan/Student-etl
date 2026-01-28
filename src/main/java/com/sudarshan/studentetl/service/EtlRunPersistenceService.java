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
    public ETLRunEntity persistRun(Instant startedAt, List<StudentRowValidationResult> results)
    {
        ETLRunEntity run = new ETLRunEntity(startedAt,"STARTED");
        run = runRepository.save(run);

        int total = results.size();
        int valid = (int)results.stream().filter(StudentRowValidationResult::isValid).count();
        int invalid = total - valid;

        for(StudentRowValidationResult res : results)
        {
            if(res.isValid()) continue;

            long rowNumber = res.getRow().getRowNumber();
           String studentIdLike = buildRowIdentifier(res.getRow());


                for (String msg : res.getErrors()) {
                    errorRepository.save(new EtlRunErrorEntirty(run, rowNumber, studentIdLike, msg));
                }
        }

        run.markFinished(Instant.now(),"SUCCESS", total, valid, invalid);
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

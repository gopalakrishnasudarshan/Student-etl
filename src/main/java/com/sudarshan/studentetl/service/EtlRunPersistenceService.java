package com.sudarshan.studentetl.service;

import com.sudarshan.studentetl.entity.ETLRunEntity;
import com.sudarshan.studentetl.entity.EtlRunErrorEntirty;
import com.sudarshan.studentetl.etl.csv.StudentRowValidationResult;
import com.sudarshan.studentetl.repository.EtlRunErrorRepository;
import com.sudarshan.studentetl.repository.EtlRunRepository;
import jakarta.transaction.Transactional;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.query.FluentQuery;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

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
            String studentId = res.getRow().getStudentId();

            for(String msg : res.getErrors())
            {
                errorRepository.save(new EtlRunErrorEntirty(run,rowNumber,studentId,msg));


            }
        }

        run.markFinished(Instant.now(),"SUCCESS", total, valid, invalid);
        return runRepository.save(run);

    }

}

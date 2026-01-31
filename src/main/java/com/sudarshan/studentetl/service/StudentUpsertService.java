package com.sudarshan.studentetl.service;

import com.sudarshan.studentetl.entity.StudentEntity;
import com.sudarshan.studentetl.etl.dto.UCIStudentTransformedDto;
import com.sudarshan.studentetl.repository.StudentRepository;
import jakarta.transaction.Transactional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

@Service
public class StudentUpsertService {

    private final StudentRepository studentRepository;
    private static final Logger log = LogManager.getLogger(StudentUpsertService.class);

    public StudentUpsertService(StudentRepository studentRepository) {
        this.studentRepository = studentRepository;
    }

    @Transactional
    public UpsertOutcome upsert(UCIStudentTransformedDto dto, Long runId) {

        var existing = studentRepository
                .findByStudentFingerprintAndSubject(dto.studentFingerprint(), dto.subject());

        boolean inserted = existing.isEmpty();


        StudentEntity entity = existing.orElseGet(() ->
                new StudentEntity(dto.studentFingerprint(), dto.subject())
        );


        entity.setStudentFingerprint(dto.studentFingerprint());
        entity.setSubject(dto.subject());

        entity.setSchool(dto.school());
        entity.setSex(dto.sex());
        entity.setAddress(dto.address());
        entity.setFamsize(dto.famsize());
        entity.setPstatus(dto.pstatus());
        entity.setMjob(dto.mjob());
        entity.setFjob(dto.fjob());
        entity.setReason(dto.reason());
        entity.setGuardian(dto.guardian());

        entity.setAge(dto.age());
        entity.setMedu(dto.medu());
        entity.setFedu(dto.fedu());
        entity.setTraveltime(dto.traveltime());
        entity.setStudytime(dto.studytime());
        entity.setFailures(dto.failures());

        entity.setSchoolsup(dto.schoolsup());
        entity.setFamsup(dto.famsup());
        entity.setPaid(dto.paid());
        entity.setActivities(dto.activities());
        entity.setNursery(dto.nursery());
        entity.setHigher(dto.higher());
        entity.setInternet(dto.internet());
        entity.setRomantic(dto.romantic());

        entity.setFamrel(dto.famrel());
        entity.setFreetime(dto.freetime());
        entity.setGoout(dto.goout());
        entity.setDalc(dto.dalc());
        entity.setWalc(dto.walc());
        entity.setHealth(dto.health());
        entity.setAbsences(dto.absences());
        entity.setG1(dto.g1());
        entity.setG2(dto.g2());
        entity.setG3(dto.g3());

        entity.setEtlRunId(runId);

        StudentEntity saved = studentRepository.save(entity);

        if (inserted) {
            log.info("UPSERT: INSERT for fingerprint={}, subject={}",
                    dto.studentFingerprint(), dto.subject());
        } else {
            log.info("UPSERT: UPDATE for fingerprint={}, subject={}",
                    dto.studentFingerprint(), dto.subject());
        }

        return new UpsertOutcome(saved, inserted);
    }
}

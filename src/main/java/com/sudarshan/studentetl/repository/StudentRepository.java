package com.sudarshan.studentetl.repository;

import com.sudarshan.studentetl.entity.StudentEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface StudentRepository extends JpaRepository<StudentEntity, Long> {

    Optional<StudentEntity> findByStudentFingerprintAndSubject(String StudentFingerprint, String Subject);


}

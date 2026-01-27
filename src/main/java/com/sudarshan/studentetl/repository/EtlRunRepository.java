package com.sudarshan.studentetl.repository;

import com.sudarshan.studentetl.entity.ETLRunEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EtlRunRepository extends JpaRepository<ETLRunEntity, Long> {
}

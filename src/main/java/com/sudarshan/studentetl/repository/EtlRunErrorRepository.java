package com.sudarshan.studentetl.repository;

import com.sudarshan.studentetl.entity.EtlRunErrorEntirty;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EtlRunErrorRepository extends JpaRepository<EtlRunErrorEntirty, Long> {
}

package com.sudarshan.studentetl.service;

import com.sudarshan.studentetl.entity.StudentEntity;

public record UpsertOutcome(StudentEntity entity, boolean inserted) {
}

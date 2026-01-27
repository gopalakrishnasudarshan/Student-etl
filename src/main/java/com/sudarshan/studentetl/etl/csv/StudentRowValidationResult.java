package com.sudarshan.studentetl.etl.csv;

import java.util.List;

public class StudentRowValidationResult {

    private final StudentCsvRow row ;
    private final List<String> errors;

    public StudentRowValidationResult(StudentCsvRow row, List<String> errors) {
        this.row = row;
        this.errors = errors;
    }

    public StudentCsvRow getRow() {
        return row;
    }

    public List<String> getErrors() {
        return errors;
    }

    public boolean isValid() {
        return errors.isEmpty();
    }
}

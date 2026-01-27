package com.sudarshan.studentetl.etl.csv;

import com.sudarshan.studentetl.config.EtlProperties;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;


@Component
public class StudentCsvReader {

    private EtlProperties etlProperties;

    public StudentCsvReader(EtlProperties etlProperties) {
        this.etlProperties = etlProperties;
    }

    public List<StudentCsvRow> readAll() throws IOException {
        Path csvPath = Paths.get(etlProperties.getStudentsCsvPath());

        try(Reader reader = Files.newBufferedReader(csvPath);
            CSVParser parser = CSVFormat.DEFAULT
                    .builder()
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .setTrim(true)
                    .build()
                    .parse(reader)) {

            List<StudentCsvRow> rows = new ArrayList<>();

            for(CSVRecord record : parser)
            {
                long rowNumber = record.getRecordNumber() + 1;

                StudentCsvRow row = new StudentCsvRow(
                        rowNumber,
                        record.get("student_id"),
                        record.get("first_name"),
                        record.get("last_name"),
                        record.get("email"),
                        record.get("gender"),
                        parseDate(record.get("date_of_birth")),
                        parseDate(record.get("enrollment_date")),
                        record.get("program"),
                        parseInt(record.get("semester")),
                        parseDouble(record.get("gpa")),
                        parseInt(record.get("credits")),
                        record.get("city"),
                        record.get("country")
                );

                rows.add(row);
            }
            return rows;
        }

    }
    private LocalDate parseDate(String raw) {
        if (raw == null || raw.isBlank()) return null;
        try
        {
            return LocalDate.parse(raw);
        }catch (Exception ex)
        {
            return null;
        }
    }

    private Integer parseInt(String raw) {
        if (raw == null || raw.isBlank()) return null;

        try{
            return Integer.parseInt(raw);
        }catch (Exception ex)
        {
            return null;
        }
    }

    private Double parseDouble(String raw) {
        if (raw == null || raw.isBlank()) return null;

        try{
            return Double.parseDouble(raw);

        }catch (Exception ex){
            return null;
        }
    }
}

package com.sudarshan.studentetl.etl.csv;

import com.sudarshan.studentetl.config.EtlProperties;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;


@Component
public class StudentCsvReader {

    private EtlProperties etlProperties;

    public StudentCsvReader(EtlProperties etlProperties) {
        this.etlProperties = etlProperties;
    }

    public List<StudentCsvRow> readAll() throws IOException {

        Path csvPath = Paths.get(etlProperties.getStudentsCsvPath());
        String subject = inferSubjectFromPath(csvPath);

        try(Reader reader = Files.newBufferedReader(csvPath);
            CSVParser parser = CSVFormat.DEFAULT.
                    builder()
                    .setDelimiter(';')
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .setTrim(true)
                    .build().parse(reader))
        {
            List<StudentCsvRow> rows = new ArrayList<>();

            for(CSVRecord record : parser) {

                long rowNumber = record.getRecordNumber() + 1;

                StudentCsvRow row = new StudentCsvRow(rowNumber,
                        subject,
                        getSafe(record, "school"),
                        getSafe(record, "sex"),
                        getSafe(record, "age"),
                        getSafe(record, "address"),
                        getSafe(record, "famsize"),
                        getSafe(record, "Pstatus"),
                        getSafe(record, "Medu"),
                        getSafe(record, "Fedu"),
                        getSafe(record, "Mjob"),
                        getSafe(record, "Fjob"),
                        getSafe(record, "reason"),
                        getSafe(record, "guardian"),
                        getSafe(record, "traveltime"),
                        getSafe(record, "studytime"),
                        getSafe(record, "failures"),
                        getSafe(record, "schoolsup"),
                        getSafe(record, "famsup"),
                        getSafe(record, "paid"),
                        getSafe(record, "activities"),
                        getSafe(record, "nursery"),
                        getSafe(record, "higher"),
                        getSafe(record, "internet"),
                        getSafe(record, "romantic"),
                        getSafe(record, "famrel"),
                        getSafe(record, "freetime"),
                        getSafe(record, "goout"),
                        getSafe(record, "Dalc"),
                        getSafe(record, "Walc"),
                        getSafe(record, "health"),
                        getSafe(record, "absences"),
                        getSafe(record, "G1"),
                        getSafe(record, "G2"),
                        getSafe(record, "G3")
                );

                rows.add(row);
            }
            return rows;
        }

    }

    private static String getSafe(CSVRecord record, String header)
    {
        try
        {
            String val = record.get(header);
            return val == null ? "" : val.trim();
        }catch (Exception e)
        {
            return "";
        }
    }
    private static String inferSubjectFromPath(Path csvPath)
    {
        String name = csvPath.getFileName().toString().toLowerCase();
        if(name.contains("mat")) return "MAT";
        if(name.contains("por")) return "POR";

        return "MAT";
    }

}

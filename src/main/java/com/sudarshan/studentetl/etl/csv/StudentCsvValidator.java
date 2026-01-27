package com.sudarshan.studentetl.etl.csv;


import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;


@Component
public class StudentCsvValidator {

    private static final Pattern EMAIL_PATTERN =
            Pattern.compile("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$");

        public List<StudentRowValidationResult> validateAll(List<StudentCsvRow> rows)
        {

            //avoiding duplicate student id
            Map<String, Integer> studentIdCounts = new HashMap<>();
            for(StudentCsvRow row : rows)
            {
                String id = row.getStudentId();
                if(id != null && !id.isBlank())
                {
                    studentIdCounts.put(id,studentIdCounts.getOrDefault(id,0) +1);
                }
            }

            List<StudentRowValidationResult> results = new ArrayList<>();

            for(StudentCsvRow row : rows)
            {
                List<String> errors = new ArrayList<>();

                if(isBlank(row.getStudentId())) errors.add("Student_id is missing");
                if(isBlank(row.getFirstName())) errors.add("first_name is missing");
                if(isBlank(row.getLastName())) errors.add("last_name is missing");
                if(isBlank(row.getProgram())) errors.add("program is missing");

                if(isBlank(row.getEmail()))
                {
                    errors.add("email is missing");
                } else if (!EMAIL_PATTERN.matcher(row.getEmail()).matches()) {
                    errors.add("email format is invalid");

                }

                if(row.getDateOfBirth() == null)
                {
                    errors.add("date_of_birth is invalid or missing");
                }

                if(row.getEnrollmentDate() == null)
                {
                    errors.add("enrollment_date is invalid or missing");
                }

                if(row.getSemester() == null || row.getSemester() < 1)
                {
                    errors.add("semester must be greater than equal (>=) to 1");
                }

                if(row.getCredits() == null || row.getCredits() < 0)
                {
                    errors.add("Credits must be greater than or equal (>=) to 0");
                }

                if(row.getGpa() == null)
                {
                    errors.add("gpa is missing or invalid");
                } else if (row.getGpa() <0.0 || row.getGpa() > 4.0) {
                    errors.add("gpa must be between 0.0 and 4.0 ");

                }
                if(!isBlank(row.getStudentId()) && studentIdCounts.getOrDefault(row.getStudentId(),0) > 1){
                    errors.add("Student id is duplicated in the input file");
                }

                results.add(new StudentRowValidationResult(row,errors));
            }
                return results;
        }

        private boolean isBlank(String value)
        {
            return value == null || value.isBlank();
        }


}

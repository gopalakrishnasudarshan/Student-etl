package com.sudarshan.studentetl.etl.csv;


import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;


@Component
public class StudentCsvValidator {

    private static final Set<String> SCHOOL = Set.of("GP", "MS");
    private static final Set<String> SEX = Set.of("F", "M");
    private static final Set<String> ADDRESS = Set.of("U", "R");
    private static final Set<String> FAMSIZE = Set.of("LE3", "GT3");
    private static final Set<String> PSTATUS = Set.of("T", "A");
    private static final Set<String> MJOB = Set.of("teacher", "health", "services", "at_home", "other");
    private static final Set<String> FJOB = Set.of("teacher", "health", "services", "at_home", "other");
    private static final Set<String> REASON = Set.of("home", "reputation", "course", "other");
    private static final Set<String> GUARDIAN = Set.of("mother", "father", "other");
    private static final Set<String> YESNO = Set.of("yes", "no");

    public List<StudentRowValidationResult> validateAll(List<StudentCsvRow> rows)
    {
        List<StudentRowValidationResult> results = new ArrayList<>();

        for(StudentCsvRow row : rows)
        {
            List<String> errors = new ArrayList<>();

            if (isBlank(row.getSubject()) || !(row.getSubject().equals("MAT") || row.getSubject().equals("POR"))) {
                errors.add("subject must be MAT or POR");
            }

            requireIn(errors,"school", row.getSchool(), SCHOOL);
            requireIn(errors, "sex", row.getSex(), SEX);
            requireIn(errors, "address", row.getAddress(), ADDRESS);
            requireIn(errors, "famsize", row.getFamsize(), FAMSIZE);
            requireIn(errors, "Pstatus", row.getPstatus(), PSTATUS);
            requireIn(errors, "Mjob", row.getMjob(), MJOB);
            requireIn(errors, "Fjob", row.getFjob(), FJOB);
            requireIn(errors, "reason", row.getReason(), REASON);
            requireIn(errors, "guardian", row.getGuardian(), GUARDIAN);

            requireIn(errors, "schoolsup", row.getSchoolsup(), YESNO);
            requireIn(errors, "famsup", row.getFamsup(), YESNO);
            requireIn(errors, "paid", row.getPaid(), YESNO);
            requireIn(errors, "activities", row.getActivities(), YESNO);
            requireIn(errors, "nursery", row.getNursery(), YESNO);
            requireIn(errors, "higher", row.getHigher(), YESNO);
            requireIn(errors, "internet", row.getInternet(), YESNO);
            requireIn(errors, "romantic", row.getRomantic(), YESNO);

            requireIntBetween(errors, "age", row.getAge(), 15, 22);
            requireIntBetween(errors, "Medu", row.getMedu(), 0, 4);
            requireIntBetween(errors, "Fedu", row.getFedu(), 0, 4);

            requireIntBetween(errors, "traveltime", row.getTraveltime(), 1, 4);
            requireIntBetween(errors, "studytime", row.getStudytime(), 1, 4);
            requireIntBetween(errors, "failures", row.getFailures(), 0, 4);

            requireIntBetween(errors, "famrel", row.getFamrel(), 1, 5);
            requireIntBetween(errors, "freetime", row.getFreetime(), 1, 5);
            requireIntBetween(errors, "goout", row.getGoout(), 1, 5);
            requireIntBetween(errors, "Dalc", row.getDalc(), 1, 5);
            requireIntBetween(errors, "Walc", row.getWalc(), 1, 5);
            requireIntBetween(errors, "health", row.getHealth(), 1, 5);

            requireIntBetween(errors, "absences", row.getAbsences(), 0, 93);

            requireIntBetween(errors, "G1", row.getG1(), 0, 20);
            requireIntBetween(errors, "G2", row.getG2(), 0, 20);
            requireIntBetween(errors, "G3", row.getG3(), 0, 20);

            results.add(new StudentRowValidationResult(row,errors));


        }
        return results;
    }

    private void requireIn(List<String> errors, String field, String value, Set<String> allowed) {
        if (isBlank(value)) {
            errors.add(field + " is missing");
            return;
        }
        if (!allowed.contains(value)) {
            errors.add(field + " has invalid value: " + value);
        }
    }

    private void requireIntBetween(List<String> errors, String field, String raw, int min, int max) {
        if (isBlank(raw)) {
            errors.add(field + " is missing");
            return;
        }
        try {
            int v = Integer.parseInt(raw);
            if (v < min || v > max) {
                errors.add(field + " must be between " + min + " and " + max);
            }
        } catch (Exception ex) {
            errors.add(field + " must be an integer");
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }


}

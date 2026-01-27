package com.sudarshan.studentetl.etl.csv;

import java.time.LocalDate;

public class StudentCsvRow {

    private final long rowNumber;
    private final String studentId;
    private final String firstName;
    private final String lastName;
    private final String email;
    private final String gender;
    private final LocalDate dateOfBirth;
    private final LocalDate enrollmentDate;
    private final String program;
    private final Integer semester;
    private final Double gpa;
    private final Integer credits;
    private final String city;
    private final String country;

    public StudentCsvRow(long rowNumber, String studentId, String firstName, String lastName,
                         String email, String gender, LocalDate dateOfBirth,
                         LocalDate enrollmentDate, String program, Integer semester,
                         Double gpa, Integer credits, String city, String country) {
        this.rowNumber = rowNumber;
        this.studentId = studentId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
        this.gender = gender;
        this.dateOfBirth = dateOfBirth;
        this.enrollmentDate = enrollmentDate;
        this.program = program;
        this.semester = semester;
        this.gpa = gpa;
        this.credits = credits;
        this.city = city;
        this.country = country;
    }

    public long getRowNumber() {
        return rowNumber;
    }

    public String getStudentId() {
        return studentId;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public String getEmail() {
        return email;
    }

    public String getGender() {
        return gender;
    }

    public LocalDate getDateOfBirth() {
        return dateOfBirth;
    }

    public LocalDate getEnrollmentDate() {
        return enrollmentDate;
    }

    public String getProgram() {
        return program;
    }

    public Integer getSemester() {
        return semester;
    }

    public Double getGpa() {
        return gpa;
    }

    public Integer getCredits() {
        return credits;
    }

    public String getCity() {
        return city;
    }

    public String getCountry() {
        return country;
    }
}

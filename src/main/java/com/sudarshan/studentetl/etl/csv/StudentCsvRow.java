package com.sudarshan.studentetl.etl.csv;

public class StudentCsvRow {

    private final long rowNumber;

    // We will set this in the ETL (e.g., "MAT" or "POR") based on the input file being processed
    private final String subject;

    // UCI columns (exact headers from student-mat.csv)
    private final String school;
    private final String sex;
    private final String age;
    private final String address;
    private final String famsize;
    private final String Pstatus;
    private final String Medu;
    private final String Fedu;
    private final String Mjob;
    private final String Fjob;
    private final String reason;
    private final String guardian;
    private final String traveltime;
    private final String studytime;
    private final String failures;
    private final String schoolsup;
    private final String famsup;
    private final String paid;
    private final String activities;
    private final String nursery;
    private final String higher;
    private final String internet;
    private final String romantic;
    private final String famrel;
    private final String freetime;
    private final String goout;
    private final String Dalc;
    private final String Walc;
    private final String health;
    private final String absences;
    private final String G1;
    private final String G2;
    private final String G3;

    public StudentCsvRow(
            long rowNumber,
            String subject,
            String school,
            String sex,
            String age,
            String address,
            String famsize,
            String Pstatus,
            String Medu,
            String Fedu,
            String Mjob,
            String Fjob,
            String reason,
            String guardian,
            String traveltime,
            String studytime,
            String failures,
            String schoolsup,
            String famsup,
            String paid,
            String activities,
            String nursery,
            String higher,
            String internet,
            String romantic,
            String famrel,
            String freetime,
            String goout,
            String Dalc,
            String Walc,
            String health,
            String absences,
            String G1,
            String G2,
            String G3
    ) {
        this.rowNumber = rowNumber;
        this.subject = subject;
        this.school = school;
        this.sex = sex;
        this.age = age;
        this.address = address;
        this.famsize = famsize;
        this.Pstatus = Pstatus;
        this.Medu = Medu;
        this.Fedu = Fedu;
        this.Mjob = Mjob;
        this.Fjob = Fjob;
        this.reason = reason;
        this.guardian = guardian;
        this.traveltime = traveltime;
        this.studytime = studytime;
        this.failures = failures;
        this.schoolsup = schoolsup;
        this.famsup = famsup;
        this.paid = paid;
        this.activities = activities;
        this.nursery = nursery;
        this.higher = higher;
        this.internet = internet;
        this.romantic = romantic;
        this.famrel = famrel;
        this.freetime = freetime;
        this.goout = goout;
        this.Dalc = Dalc;
        this.Walc = Walc;
        this.health = health;
        this.absences = absences;
        this.G1 = G1;
        this.G2 = G2;
        this.G3 = G3;
    }

    public long getRowNumber() {
        return rowNumber;
    }

    public String getSubject() {
        return subject;
    }

    public String getSchool() {
        return school;
    }

    public String getSex() {
        return sex;
    }

    public String getAge() {
        return age;
    }

    public String getAddress() {
        return address;
    }

    public String getFamsize() {
        return famsize;
    }

    public String getPstatus() {
        return Pstatus;
    }

    public String getMedu() {
        return Medu;
    }

    public String getFedu() {
        return Fedu;
    }

    public String getMjob() {
        return Mjob;
    }

    public String getFjob() {
        return Fjob;
    }

    public String getReason() {
        return reason;
    }

    public String getGuardian() {
        return guardian;
    }

    public String getTraveltime() {
        return traveltime;
    }

    public String getStudytime() {
        return studytime;
    }

    public String getFailures() {
        return failures;
    }

    public String getSchoolsup() {
        return schoolsup;
    }

    public String getFamsup() {
        return famsup;
    }

    public String getPaid() {
        return paid;
    }

    public String getActivities() {
        return activities;
    }

    public String getNursery() {
        return nursery;
    }

    public String getHigher() {
        return higher;
    }

    public String getInternet() {
        return internet;
    }

    public String getRomantic() {
        return romantic;
    }

    public String getFamrel() {
        return famrel;
    }

    public String getFreetime() {
        return freetime;
    }

    public String getGoout() {
        return goout;
    }

    public String getDalc() {
        return Dalc;
    }

    public String getWalc() {
        return Walc;
    }

    public String getHealth() {
        return health;
    }

    public String getAbsences() {
        return absences;
    }

    public String getG1() {
        return G1;
    }

    public String getG2() {
        return G2;
    }

    public String getG3() {
        return G3;
    }
}

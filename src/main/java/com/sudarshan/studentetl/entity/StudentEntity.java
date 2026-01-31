package com.sudarshan.studentetl.entity;

import jakarta.persistence.*;

@Entity
@Table(
        name = "uci_students",
        uniqueConstraints = @UniqueConstraint(
                columnNames = {"student_fingerprint", "subject"}
        )
)
public class StudentEntity {

    protected StudentEntity() {

    }

    public StudentEntity(String studentFingerprint, String subject) {
        this.studentFingerprint = studentFingerprint;
        this.subject = subject;
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "student_fingerprint", nullable = false)
    private String studentFingerprint;

    @Column(nullable = false)
    private String subject;

    @Column(name = "etl_run_id", nullable = false)
    private Long etlRunId;

    public void setEtlRunId(Long etlRunId) {
        this.etlRunId = etlRunId;
    }


    private String school;
    private String sex;
    private String address;
    private String famsize;
    private String pstatus;
    private String mjob;
    private String fjob;
    private String reason;
    private String guardian;


    private int age;
    private int medu;
    private int fedu;
    private int traveltime;
    private int studytime;
    private int failures;


    private boolean schoolsup;
    private boolean famsup;
    private boolean paid;
    private boolean activities;
    private boolean nursery;
    private boolean higher;
    private boolean internet;
    private boolean romantic;


    private int famrel;
    private int freetime;
    private int goout;
    private int dalc;
    private int walc;
    private int health;
    private int absences;
    private int g1;
    private int g2;
    private int g3;



    public void setStudentFingerprint(String studentFingerprint) {
        this.studentFingerprint = studentFingerprint;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public void setSchool(String school) {
        this.school = school;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public void setFamsize(String famsize) {
        this.famsize = famsize;
    }

    public void setPstatus(String pstatus) {
        this.pstatus = pstatus;
    }

    public void setMjob(String mjob) {
        this.mjob = mjob;
    }

    public void setFjob(String fjob) {
        this.fjob = fjob;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public void setGuardian(String guardian) {
        this.guardian = guardian;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public void setMedu(int medu) {
        this.medu = medu;
    }

    public void setFedu(int fedu) {
        this.fedu = fedu;
    }

    public void setTraveltime(int traveltime) {
        this.traveltime = traveltime;
    }

    public void setStudytime(int studytime) {
        this.studytime = studytime;
    }

    public void setFailures(int failures) {
        this.failures = failures;
    }

    public void setSchoolsup(boolean schoolsup) {
        this.schoolsup = schoolsup;
    }

    public void setFamsup(boolean famsup) {
        this.famsup = famsup;
    }

    public void setPaid(boolean paid) {
        this.paid = paid;
    }

    public void setActivities(boolean activities) {
        this.activities = activities;
    }

    public void setNursery(boolean nursery) {
        this.nursery = nursery;
    }

    public void setHigher(boolean higher) {
        this.higher = higher;
    }

    public void setInternet(boolean internet) {
        this.internet = internet;
    }

    public void setRomantic(boolean romantic) {
        this.romantic = romantic;
    }

    public void setFamrel(int famrel) {
        this.famrel = famrel;
    }

    public void setFreetime(int freetime) {
        this.freetime = freetime;
    }

    public void setGoout(int goout) {
        this.goout = goout;
    }

    public void setDalc(int dalc) {
        this.dalc = dalc;
    }

    public void setWalc(int walc) {
        this.walc = walc;
    }

    public void setHealth(int health) {
        this.health = health;
    }

    public void setAbsences(int absences) {
        this.absences = absences;
    }

    public void setG1(int g1) {
        this.g1 = g1;
    }

    public void setG2(int g2) {
        this.g2 = g2;
    }

    public void setG3(int g3) {
        this.g3 = g3;
    }
}

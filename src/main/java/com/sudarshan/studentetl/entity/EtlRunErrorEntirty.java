package com.sudarshan.studentetl.entity;


import jakarta.persistence.*;

@Entity
@Table(name = "etl_run_errors")
public class EtlRunErrorEntirty {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "etl_run_id", nullable = false)
    private ETLRunEntity run;

    @Column(name = "row_number", nullable = false)
    private long rowNumber;

    @Column(name = "student_id", length = 50)
    private String studentId;

    @Column(name = "error_message", nullable = false, length = 500)
    private String errorMessage;

    public EtlRunErrorEntirty() {

    }

    public EtlRunErrorEntirty(ETLRunEntity run, long rowNumber, String studentId, String errorMessage) {
        this.run = run;
        this.rowNumber = rowNumber;
        this.studentId = studentId;
        this.errorMessage = errorMessage;
    }

    public long getId() {
        return id;
    }

    public ETLRunEntity getRun() {
        return run;
    }

    public long getRowNumber() {
        return rowNumber;
    }

    public String getStudentId() {
        return studentId;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}

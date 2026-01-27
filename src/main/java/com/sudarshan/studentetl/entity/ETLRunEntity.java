package com.sudarshan.studentetl.entity;


import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "etl_runs")
public class ETLRunEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    @Column(name = "started_at", nullable = false)
    private Instant startedAt;

    @Column(name = "finished_at")
    private Instant finishedAt;

    @Column(name = "status", nullable = false, length = 20)
    private String status;

    @Column(name = "total_rows" , nullable = false)
    private int totalRows;

    @Column(name="valid_rows", nullable = false)
    private int validRows;

    @Column(name = "invalid_rows", nullable = false)
    private int invalidRows;

    protected ETLRunEntity() {

    }
    public ETLRunEntity(Instant startedAt, String status)
    {
        this.startedAt = startedAt;
        this.status = status;
        this.totalRows = 0;
        this.validRows = 0;
        this.invalidRows = 0;
    }

    public long getId() {
        return id;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getFinishedAt() {
        return finishedAt;
    }

    public String getStatus() {
        return status;
    }

    public int getTotalRows() {
        return totalRows;
    }

    public int getInvalidRows() {
        return invalidRows;
    }

    public int getValidRows() {
        return validRows;
    }

    public void markFinished(Instant finishedAt, String status, int totalRows, int validRows, int invalidRows) {

        this.finishedAt = finishedAt;
        this.totalRows = totalRows;
        this.status = status;
        this.validRows = validRows;
        this.invalidRows = invalidRows;


    }
}

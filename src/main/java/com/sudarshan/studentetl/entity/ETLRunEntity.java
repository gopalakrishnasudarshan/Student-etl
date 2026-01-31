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

    @Column(name = "inserted_rows", nullable = false)
    private int insertedRows;

    @Column(name = "updated_rows", nullable = false)
    private int updatedRows;

    protected ETLRunEntity() {

    }



    public ETLRunEntity(Instant startedAt, String status)
    {
        this.startedAt = startedAt;
        this.status = status;
        this.totalRows = 0;
        this.validRows = 0;
        this.invalidRows = 0;
        this.insertedRows = 0;
        this.updatedRows = 0;

    }

    public int getInsertedRows() {
        return insertedRows;
    }

    public int getUpdatedRows() {
        return updatedRows;
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

    public void markFinished(Instant finishedAt, String status, int totalRows, int validRows, int invalidRows, int insertedRows,int updatedRows) {

        this.finishedAt = finishedAt;
        this.totalRows = totalRows;
        this.status = status;
        this.validRows = validRows;
        this.invalidRows = invalidRows;
        this.invalidRows = insertedRows;
        this.updatedRows = updatedRows;


    }
}

package com.sudarshan.studentetl.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "etl")
public class EtlProperties {

    private String studentsCsvPath;

    public String getStudentsCsvPath() {

        return studentsCsvPath;
    }
    public void setStudentsCsvPath(String studentsCsvPath) {

        this.studentsCsvPath = studentsCsvPath;
    }
}

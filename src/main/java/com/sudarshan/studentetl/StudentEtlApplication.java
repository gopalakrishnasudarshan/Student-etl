package com.sudarshan.studentetl;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class StudentEtlApplication {

	public static void main(String[] args) {
		SpringApplication.run(StudentEtlApplication.class, args);
	}

}

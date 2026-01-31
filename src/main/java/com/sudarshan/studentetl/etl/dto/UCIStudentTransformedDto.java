package com.sudarshan.studentetl.etl.dto;

public record UCIStudentTransformedDto(

        String school,
        String sex,
        String address,
        String famsize,
        String pstatus,
        String mjob,
        String fjob,
        String reason,
        String guardian,


        int age,
        int medu,
        int fedu,
        int traveltime,
        int studytime,
        int failures,


        boolean schoolsup,
        boolean famsup,
        boolean paid,
        boolean activities,
        boolean nursery,
        boolean higher,
        boolean internet,
        boolean romantic,


        int famrel,
        int freetime,
        int goout,
        int dalc,
        int walc,
        int health,
        int absences,
        int g1,
        int g2,
        int g3,


        String subject,
        String studentFingerprint
) {
}

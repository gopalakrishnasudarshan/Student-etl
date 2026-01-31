package com.sudarshan.studentetl.etl.transform;

import com.sudarshan.studentetl.etl.csv.StudentCsvRow;
import com.sudarshan.studentetl.etl.dto.UCIStudentTransformedDto;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Locale;

@Component
public class UciStudentTransformer {

    public UCIStudentTransformedDto transform(StudentCsvRow row, String subject)
    {
        String subj = normalizeSubject(subject);

        String school = norm(row.getSchool());
        String sex = norm(row.getSex());
        String address = norm(row.getAddress());
        String famsize = norm(row.getFamsize());
        String pstatus = norm(row.getPstatus());
        String mjob = norm(row.getMjob());
        String fjob = norm(row.getFjob());
        String reason = norm(row.getReason());
        String guardian = norm(row.getGuardian());

        int age = toInt(row.getAge());
        int medu = toInt(row.getMedu());
        int fedu = toInt(row.getFedu());
        int traveltime = toInt(row.getTraveltime());
        int studytime = toInt(row.getStudytime());
        int failures = toInt(row.getFailures());

        boolean schoolsup = toBool(row.getSchoolsup());
        boolean famsup = toBool(row.getFamsup());
        boolean paid = toBool(row.getPaid());
        boolean activities = toBool(row.getActivities());
        boolean nursery = toBool(row.getNursery());
        boolean higher = toBool(row.getHigher());
        boolean internet = toBool(row.getInternet());
        boolean romantic = toBool(row.getRomantic());

        int famrel = toInt(row.getFamrel());
        int freetime = toInt(row.getFreetime());
        int goout = toInt(row.getGoout());
        int dalc = toInt(row.getDalc());
        int walc = toInt(row.getWalc());
        int health = toInt(row.getHealth());
        int absences = toInt(row.getAbsences());
        int g1 = toInt(row.getG1());
        int g2 = toInt(row.getG2());
        int g3 = toInt(row.getG3());

        String fingerprint = fingerprint(school, sex, age, address,famsize,pstatus,medu,fedu,mjob,fjob,reason,guardian);

        return new UCIStudentTransformedDto(school,
                sex,
                address,
                famsize,
                pstatus,
                mjob,
                fjob,
                reason,
                guardian,


                age,
                medu,
                fedu,
                traveltime,
                studytime,
                failures,


                schoolsup,
                famsup,
                paid,
                activities,
                nursery,
                higher,
                internet,
                romantic,


                famrel,
                freetime,
                goout,
                dalc,
                walc,
                health,
                absences,
                g1,
                g2,
                g3,


                subj,
                fingerprint) ;

    }

    private static String normalizeSubject(String subject) {

        String s = subject == null ? "" : subject.trim().toUpperCase(Locale.ROOT);
        if(!s.equals("MAT") && !s.equals("POR"))
        {
            throw new IllegalArgumentException("Subject must be MAT or POR, got : "+subject);
        }
        return s;
    }
    private static String norm(String s) {
        return s == null ? "" :s.trim();
    }

    private static int toInt(String s)
    {
        if(s == null) return 0;
        String t = s.trim();
        if(t.isEmpty()) return 0;
        return Integer.parseInt(t);
    }

    private static boolean toBool(String s) {

        if(s == null) return false;
        String t = s.trim().toLowerCase(Locale.ROOT);
        return t.equals("yes") || t.equals("true") || t.equals("1");
    }

    private static String fingerprint(Object... parts)
    {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < parts.length; i++)
        {
            if(i > 0) sb.append('|');
            sb.append(parts[i] == null ? "" :parts[i].toString().toLowerCase(Locale.ROOT));
        }
        return sha256Hex(sb.toString());
    }
    private static String sha256Hex(String input)
    {
       try {
           MessageDigest md = MessageDigest.getInstance("SHA-256");
           byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
           StringBuilder hex = new StringBuilder(digest.length * 2);

           for(byte b: digest)
           {
               hex.append(Character.forDigit((b >> 4) & 0xF, 16));
               hex.append(Character.forDigit(b & 0xF, 16));

           }
           return hex.toString();

       }catch(Exception e)
       {
           throw new RuntimeException("Failed to compute fingerprint",e);
       }
    }
}

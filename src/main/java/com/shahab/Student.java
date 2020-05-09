package com.shahab;

import java.io.Serializable;
import java.util.Objects;

public class Student implements Serializable{
    private Integer studentId;
    private Integer examCenterId;
    private String subject;
    private Integer year;
    private Integer quarter;
    private Integer score;
    private String grade;

    public Student() {
    }

    public Student(Integer studentId, Integer examCenterId, String subject, Integer year, Integer quarter, Integer score, String grade) {
        this.studentId = studentId;
        this.examCenterId = examCenterId;
        this.subject = subject;
        this.year = year;
        this.quarter = quarter;
        this.score = score;
        this.grade = grade;
    }

    public Integer getStudentId() {
        return studentId;
    }

    public void setStudentId(Integer studentId) {
        this.studentId = studentId;
    }

    public Integer getExamCenterId() {
        return examCenterId;
    }

    public void setExamCenterId(Integer examCenterId) {
        this.examCenterId = examCenterId;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public Integer getQuarter() {
        return quarter;
    }

    public void setQuarter(Integer quarter) {
        this.quarter = quarter;
    }

    public Integer getScore() {
        return score;
    }

    public void setScore(Integer score) {
        this.score = score;
    }

    public String getGrade() {
        return grade;
    }

    public void setGrade(String grade) {
        this.grade = grade;
    }

    @Override
    public String toString() {
        return "Student{" +
                "studentId=" + studentId +
                ", examCenterId=" + examCenterId +
                ", subject='" + subject + '\'' +
                ", year=" + year +
                ", quarter=" + quarter +
                ", score=" + score +
                ", grade='" + grade + '\'' +
                '}';
    }
    static public Student of(String line) {
        String[] arr = line.split(",");
        return new Student(
                Integer.parseInt(arr[0]),
                Integer.parseInt(arr[1]),
                arr[2],
                Integer.parseInt(arr[3]),
                Integer.parseInt(arr[4]),
                Integer.parseInt(arr[5]),
                arr[6]
        );
    }

}

package com.shahab;

import avro.shaded.com.google.common.collect.Iterables;
import lombok.Data;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

@Data
public class StudentsFile {
    private JavaRDD<String> rdd;

    public void subjectAndLength() {
        rdd
                .filter(rawValue -> !rawValue.contains("student_id,exam_center_id,subject,year,quarter,score,grade"))
                .map(rawValue -> rawValue.split(","))
                .mapToPair(rawValue -> new Tuple2<>(rawValue[2], rawValue[2].length()))
                .foreach(tuple -> System.out.println(tuple._1 + " length: " + tuple._2()));
    }

    public double maxGrade() {
        return rdd
                .filter(s -> !s.contains("student_id,exam_center_id,subject,year,quarter,score,grade"))
                .mapToDouble(rawValue -> Double.parseDouble(rawValue.split(",")[5]))
                .max();
    }

    public double minGrade() {
        return rdd
                .filter(rawValue -> !rawValue.contains("student_id,exam_center_id,subject,year,quarter,score,grade"))
                .mapToDouble(rawValue -> Double.parseDouble(rawValue.split(",")[5]))
                .min();
    }

    public void groupAndSortByYear() {
        rdd
                .filter(rawValue -> !rawValue.contains("student_id,exam_center_id,subject,year,quarter,score,grade"))
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(",")[3], 1L))
                .groupByKey()
                .sortByKey()
                .foreach(tuple -> System.out.println(tuple._1 + " has " + Iterables.size(tuple._2) + " instances"));
    }
}

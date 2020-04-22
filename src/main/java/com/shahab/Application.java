package com.shahab;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class Application {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf =
                new SparkConf()
                        .setAppName("RDD Programming Guide")
                        .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> data = sc.textFile("src/main/resources/students.csv");
        data.persist(StorageLevel.MEMORY_ONLY());

        StudentsFile students = new StudentsFile();
        students.setRdd(data);

        //students.subjectAndLength();
        //System.out.println("Min grade: " + students.minGrade() + " Max grade: " + students.maxGrade());
        students.groupAndSortByYear();

        sc.close();
    }
}

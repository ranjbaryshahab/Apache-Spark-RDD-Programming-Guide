package com.shahab;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.RangePartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.math.Ordering;
import scala.reflect.ClassTag$;


public class Application {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf =
                new SparkConf()
                        .setAppName("RDD Programming Guide")
                        .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> studentsRDD = sc.textFile("src/main/resources/students.csv");
        JavaPairRDD<Integer, Student> studentJavaPairRDD = studentsRDD
                .filter(rawValue -> !rawValue.contains("student_id,exam_center_id,subject,year,quarter,score,grade"))
                .map(rawValue -> {
                    String[] line = rawValue.split(",");
                    return new Student(
                            Integer.parseInt(line[0]),
                            Integer.parseInt(line[1]),
                            line[2],
                            Integer.parseInt(line[3]),
                            Integer.parseInt(line[4]),
                            Integer.parseInt(line[5]),
                            line[6]
                    );
                })
                .mapToPair(student -> new Tuple2<>(student.getYear(), student));

        RangePartitioner rangePartitioner =
                new RangePartitioner(12, studentJavaPairRDD.rdd(), true, Ordering.Int$.MODULE$, ClassTag$.MODULE$.apply(Integer.class));

        studentJavaPairRDD
                .partitionBy(rangePartitioner)
                .mapPartitions(tuple2Iterator -> {
                    //todo
                    return null;
                }).
                take(20).forEach(System.out::println);

        sc.close();
    }
}

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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class Application {
     public static Iterator<Student> sort(Iterator<Tuple2<Integer, Student>> it) {
        List<Tuple2<Integer, Student>> list = new ArrayList<>();
        while (it.hasNext()) {
            list.add(it.next());
        }
         MergeSort ms = new MergeSort(list);
         ms.sortGivenArray();
        return ms.getSortedArray().stream().map(pair -> pair._2).iterator();
    }

    public static JavaRDD<Student> sort(JavaPairRDD<Integer, Student> rdd) {
        final int PARTS_NUMBER = 1;
        RangePartitioner rangePartitioner =
                new RangePartitioner(PARTS_NUMBER, rdd.rdd(), true, Ordering.Int$.MODULE$, ClassTag$.MODULE$.apply(Integer.class));

        return rdd
                .partitionBy(rangePartitioner)
                .mapPartitions(Application::sort, false);
    }

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("RDD Programming Guide")
                .setMaster("local[*]")
                .set("spark.ui.enabled", "True")
                .set("spark.ui.port", "4040");


        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaRDD<String> lineRDD = sc.textFile("src/main/resources/students.csv");

            JavaRDD<Student> studentsRDD =
                    lineRDD.mapPartitionsWithIndex((Integer ind, Iterator<String> it) -> {
                        if (ind == 0)
                            it.next();
                        return it;
                    }, false).map(Student::of);

            JavaPairRDD<Integer, Student> yearRDD = studentsRDD.mapToPair(s -> new Tuple2<>(s.getYear(), s));
            JavaRDD<Student> sortedRDD = sort(yearRDD);

            sortedRDD.saveAsTextFile("src/main/resources/result.txt");
        }

    }
}

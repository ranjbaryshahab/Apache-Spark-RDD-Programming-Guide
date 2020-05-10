package com.shahab;

import org.apache.commons.collections.iterators.IteratorChain;
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

import java.io.Serializable;
import java.util.*;
import java.util.function.Function;


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

    public static <T> JavaRDD<Tuple2<Integer,T>> groupBy( JavaPairRDD<Integer, Student> rdd, AggFunction<T> fun) {
        return rdd.mapPartitions(it -> {
            Map<Integer,T> map = new HashMap<>();
            while (it.hasNext()) {
                Tuple2<Integer,Student> row = it.next();
                Integer key = row._1;
                Student std = row._2;
                T buffer = map.getOrDefault( key, fun.getInitialValue());
                map.put(key, fun.update(buffer, std));
            }
           Iterator<Map.Entry<Integer,T>> mapIterator = map.entrySet().iterator();
           return new Iterator<Tuple2<Integer,T>>() {
               @Override
               public boolean hasNext() {
                   return mapIterator.hasNext();
               }

               @Override
               public Tuple2<Integer,T> next() {
                   Map.Entry<Integer,T> entry = mapIterator.next();
                   return new Tuple2<Integer,T>(entry.getKey(), entry.getValue());
               }
           };
        });
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

            groupBy( yearRDD, new SumAggFunction((Function<Student,Double> & Serializable) (Student std)->std.getScore().doubleValue())).take(10).forEach(System.out::println);

            groupBy( yearRDD, new CountAggFunction(
                    (Function<Student,String> & Serializable) (Student std)->std.getGrade())).take(10).forEach(System.out::println);

            sortedRDD.saveAsTextFile("src/main/resources/result.txt");
        }

    }
}

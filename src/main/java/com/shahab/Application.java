package com.shahab;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.RangePartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;
import scala.Tuple2;
import scala.math.Ordering;
import scala.math.Ordering$;
import scala.reflect.ClassTag$;

import java.util.*;


public class Application {
    static public Iterator<Student>  sort(Iterator<Tuple2<Integer,Student>> it){
        List<Tuple2<Integer,Student>> list = new ArrayList<>();
        while( it.hasNext()){
             list.add(it.next());
        }
        Collections.sort(list, new Comparator<Tuple2<Integer, Student>>() {
            @Override
            public int compare(Tuple2<Integer, Student> o1, Tuple2<Integer, Student> o2) {
                return -1*o1._1.compareTo(o2._1);
            }
        });
        return list.stream().map( pair -> pair._2).iterator();
    }
    public static JavaRDD<Student> sort( JavaPairRDD<Integer,Student> rdd ){
        int PARTS_NUMBER = 3;
        RangePartitioner rangePartitioner = new RangePartitioner( PARTS_NUMBER, rdd.rdd(), true, Ordering.Int$.MODULE$, ClassTag$.MODULE$.apply(Integer.class));

        JavaRDD<Student> sortedRDD = rdd.partitionBy( rangePartitioner)
                                        .mapPartitions( it -> sort(it), false);

        return sortedRDD;
    }
    public static void main( String [] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("RDD Programming Guide")
                                        .setMaster("local[*]")
                                        .set("spark.ui.enabled","True")
                                        .set("spark.ui.port","4040");


        try(JavaSparkContext sc = new JavaSparkContext(conf)){
            JavaRDD<String> lineRDD = sc.textFile("src/main/resources/students.csv");

            JavaRDD<Student> studentsRDD = lineRDD.mapPartitionsWithIndex(
                    (Integer ind, Iterator<String> it) ->  { if ( ind == 0)
                                                                it.next();
                                                             return it;
            }, false).map( line -> Student.of(line));

            JavaPairRDD<Integer,Student> yearRDD  = studentsRDD.mapToPair(s -> new Tuple2<>(s.getYear(), s));
            JavaRDD<Student> sortedRDD = sort( yearRDD);

            sortedRDD.take(10).forEach(System.out::println);
        }

    }
}

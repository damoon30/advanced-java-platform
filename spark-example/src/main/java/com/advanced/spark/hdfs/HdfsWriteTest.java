package com.advanced.spark.hdfs;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class HdfsWriteTest {

    private static final Pattern SPACE = Pattern.compile(" ");
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaHdfsLR")
                .master("local")
                .getOrCreate();
        // 读取hdfs文件
        JavaRDD<String> lines = spark.read().textFile("hdfs://8.136.121.252:9000/fangwen/test1").javaRDD();
//        JavaRDD<String> lines = spark.read().textFile("/Users/fangwen/Documents/bigdata/test.txt").javaRDD();

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

        // 写入hdfs
//        words.saveAsTextFile("hdfs://8.136.121.252:9000/fangwen/test1");
        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        spark.stop();
    }


}

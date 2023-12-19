package com.advanced.spark.eg01;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class Spark01_WorkCount {

    public static void main(String[] args) {
        // 数据格式: 1_2_3_4_5xx_6xx_7x_8_9_10_11,xx,xx


        SparkSession spark = SparkSession
                .builder()
                .appName("HotCategoryTop10")
                .master("local")
                .getOrCreate();
        // 1、读取原始日志
        JavaRDD<String> actionRdd = spark.read().textFile("").javaRDD();

        //2、统计点击数量
        JavaRDD<String> clickActionRdd = actionRdd.filter(action -> {
            String[] arr = action.split("_");
            return !Objects.equals(arr[6], "-1");
        });
        JavaPairRDD<String, Integer> clickPairRDD = clickActionRdd.mapToPair(action -> {
            String[] datas = action.split("_");
            return new Tuple2<>(datas[6], 1);
        }).reduceByKey(Integer::sum);


        //3、统计下单数量，订单字段  1,2,3,4 逗号分割
        JavaRDD<String> orderActionRdd = actionRdd.filter(action -> {
            String[] arr = action.split("_");
            return !Objects.equals(arr[8], "null");
        });
        JavaPairRDD<String, Integer> orderPairRDD = orderActionRdd
                // 拍平
                .flatMap((FlatMapFunction<String, String>) action -> {
                    String[] arr = action.split("_");
                    String[] orders = arr[8].split(",");
                    return Arrays.stream(orders).iterator();
                })
                // 转为pairRdd
                .mapToPair(action -> {
            String[] datas = action.split("_");
            return new Tuple2<>(datas[8], 1);
        }).reduceByKey(Integer::sum);

        //4、统计支付数量
        JavaRDD<String> payActionRdd = actionRdd.filter(action -> {
            String[] arr = action.split("_");
            return !Objects.equals(arr[10], "null");
        });
        JavaPairRDD<String, Integer> payPairRDD = payActionRdd
                // 拍平
                .flatMap((FlatMapFunction<String, String>) action -> {
                    String[] arr = action.split("_");
                    String[] orders = arr[10].split(",");
                    return Arrays.stream(orders).iterator();
                })
                // 转为pairRdd
                .mapToPair(action -> {
                    String[] datas = action.split("_");
                    return new Tuple2<>(datas[10], 1);
                }).reduceByKey(Integer::sum);


        // 5、将品类进行排序，并且取前10名
        //   点击数量排序，下单数量排序，支付数量排序， 元组排序

        // join（内连接），zip（连接方式和数量和位置有关），leftOuterJoin（左外连接），coGroup（connect + group）

        JavaPairRDD<String, Tuple3<Iterable<Integer>, Iterable<Integer>, Iterable<Integer>>> cogroupRdd = clickPairRDD.cogroup(orderPairRDD, payPairRDD);

        JavaPairRDD<String, Tuple3> stringTuple3JavaPairRDD = cogroupRdd.mapValues(t3 -> {
            Iterator<Integer> clickIter = t3._1().iterator();
            Iterator<Integer> orderIter = t3._1().iterator();
            Iterator<Integer> payIter = t3._1().iterator();

            int clickCnt = 0;
            if (clickIter.hasNext()) {
                clickCnt = clickIter.next();
            }
            int orderCnt = 0;
            if (orderIter.hasNext()) {
                orderCnt = orderIter.next();
            }
            int payCnt = 0;
            if (payIter.hasNext()) {
                payCnt = payIter.next();
            }
            return new Tuple3(clickCnt, orderCnt, payCnt);
        });

        List<Tuple2<String, Tuple3>> takeList = stringTuple3JavaPairRDD.sortByKey(false).take(10);

        //结果打印
        takeList.forEach(System.out::println);
    }
}

package com.advanced.flink.main;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TwoStreamConnectMain {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> ds1 = env.fromElements("good good study")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        for (String s1 : s.split(" ")) {
                            collector.collect(s1);
                        }

                    }
                });
        DataStream<String> ds2 = env.fromElements("day day up !")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        for (String s1 : s.split(" ")) {
                            collector.collect(s1);
                        }

                    }
                });
        DataStream<Integer> ds3 = env.fromElements("1 1 2")
                .flatMap(new FlatMapFunction<String, Integer>() {
                    @Override
                    public void flatMap(String s, Collector<Integer> collector) throws Exception {
                        for (String s1 : s.split(" ")) {
                            collector.collect(Integer.valueOf(s1));
                        }

                    }
                });

        // 合并两个数据输出
        ds1.union(ds2).print();

        ConnectedStreams<String, Integer> connect = ds1.connect(ds3);

        // connect可以连接两个不同类型的流（只能连接两个）
        // union可以连接多个流，但是流的类型必须相同
        env.execute("test flink job");
    }

}

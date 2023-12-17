package com.advanced.flink.main;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SocketStreamMain {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input = env.socketTextStream("localhost", 9999);

        DataStream<String> dataStream = input.map(String::toLowerCase).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                for (String word : s.split(",")) {
                    collector.collect(word);
                }
            }
        });

        // 转为元组
        DataStream<Tuple2<String, Integer>> tuple2DataStream = dataStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tuple2DataStream.keyBy(item -> item.f0);
        keyedStream.print();

        // 计数
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);

        // reduce
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = keyedStream.reduce((t0, t1) -> Tuple2.of(t0.f0, t0.f1 + t1.f1));
        reduce.print();
        env.execute("test flink job");
    }
}

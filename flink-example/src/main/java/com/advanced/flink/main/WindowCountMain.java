package com.advanced.flink.main;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WindowCountMain {

    // 滚动窗口 tumbling window，是在流中创建不重叠的相邻窗口。它们是固定长度的窗口，没有重叠
    // 滑动窗口 sliding window, 类似与滚动窗口，但是窗口可以重叠，例如需要统计最后5分钟的数据
    // 会话窗口 session window 当对发生的事件进行分组，事件接近的分到一组（即一个窗口）还可以提供会话间隔的配置参数，该参数指示在关闭会话之前需要等待多长时间。
    // 全局窗口 global window flink将所有元素放到一个窗口
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> input = env.socketTextStream("localhost", 9999);

        DataStream<Tuple2<String, Integer>> dataStream = input.map(String::toLowerCase).flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : s.split(",")) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });
        // 5s一个滚动窗口
        dataStream.keyBy(t->t.f0).window(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum(1).print("tumbling");

        // 每5s统计一次前10s的单词计数
        dataStream.keyBy(t->t.f0).window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))).sum(1).print("sliding");
        env.execute("test flink job");
    }

}
